"""
fno_stocks.py
─────────────
Single source for the FNO stock list (NSE F&O eligible stocks + lot sizes),
read from active_option_tokens (broker=dhan). Mounted identically in
algo.trade, algo.simulator, and algo.scanner's api.py — same router, same
code, every port serves it locally instead of one service depending on
another's port for a simple, slow-changing (1h cache) lookup.
"""

from __future__ import annotations

import asyncio
import logging
import time as _time

from fastapi import APIRouter, Depends, HTTPException

from features import auth as app_auth
from features.mongo_data import MongoData

log = logging.getLogger(__name__)

router = APIRouter()

_FNO_STOCKS_CACHE: dict = {}   # {"data": [...], "fetched_at": float}
_FNO_CACHE_TTL = 3600          # refresh once per hour
_INDEX_ENSURED = False         # only pay create_index's cost once per process


def _fetch_fno_stocks_sync() -> list[dict]:
    """
    Blocking pymongo work — must run via asyncio.to_thread, never awaited
    directly. This process's event loop also drives /ws/internal-ticks live
    tick delivery, so running this synchronously on the loop stalls every
    connected websocket client for as long as the aggregation takes.
    """
    global _INDEX_ENSURED
    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        if not _INDEX_ENSURED:
            try:
                col.create_index(
                    [("broker", 1), ("instrument", 1), ("lot_size", 1)],
                    name="idx_fno_stocks_list",
                    background=True,
                )
            except Exception:
                pass
            _INDEX_ENSURED = True
        pipeline = [
            # instrument_type != "commodity" — MCX underlyings live in
            # /mcx-commodities, not here (they used to leak into this
            # list, e.g. "ALUMINI"/"ALUMINIUM" showing up as NSE stocks).
            {"$match": {"broker": "dhan", "instrument_type": {"$ne": "commodity"}, "lot_size": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": "$instrument",
                "lot_size": {"$first": "$lot_size"},
                "types": {"$addToSet": "$option_type"},
            }},
            {"$project": {"symbol": "$_id", "lot_size": 1, "types": 1, "_id": 0}},
            {"$sort": {"symbol": 1}},
        ]
        # Require actual CE+PE contracts — drop anything synced with only
        # a FUT leg (not option-tradable, so not a valid pick here).
        # maxTimeMS bounds a slow/cold-index run instead of hanging the
        # request (and the shared event loop) indefinitely.
        return [
            {"symbol": s["symbol"], "lot_size": s["lot_size"]}
            for s in col.aggregate(pipeline, maxTimeMS=15000)
            if "CE" in s["types"] and "PE" in s["types"]
        ]
    finally:
        db.close()


async def _refresh_fno_cache() -> list[dict]:
    stocks = await asyncio.to_thread(_fetch_fno_stocks_sync)
    _FNO_STOCKS_CACHE["data"] = stocks
    _FNO_STOCKS_CACHE["fetched_at"] = _time.time()
    return stocks


@router.get("/fno-stocks")
async def get_dhan_fno_stocks(current_user: dict = Depends(app_auth.get_current_user)):
    """
    Returns NSE FNO stock list from active_option_tokens (broker=dhan).
    Cached for 1 hour (per-process — each service keeps its own copy, all
    refreshed from the same Mongo aggregation). Each item: {symbol, lot_size}
    """
    cached = _FNO_STOCKS_CACHE
    if cached.get("data") and (_time.time() - cached.get("fetched_at", 0)) < _FNO_CACHE_TTL:
        return {"ok": True, "count": len(cached["data"]), "stocks": cached["data"]}

    try:
        stocks = await _refresh_fno_cache()
        return {"ok": True, "count": len(stocks), "stocks": stocks}

    except Exception as exc:
        log.error("[FNO STOCKS] fetch error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
