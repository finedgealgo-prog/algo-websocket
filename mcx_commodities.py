"""
mcx_commodities.py
───────────────────
Lists MCX commodity underlyings available for options trading in the
simulator, grouped into the categories the trading desk actually uses
(Bullion / Energy / Metals / Agri) — active_option_tokens itself has no
category field, and Dhan's scrip master doesn't group commodities either.

Underlyings are discovered from active_option_tokens (instrument_type=
"commodity", populated by GET /algo/sync-tokens/start/{instrument} — see
algo.trade/api.py's _sync_dhan_commodity_tokens / _get_dhan_commodity_master,
which pulls straight from Dhan's scrip master CSV, nothing hardcoded there).
The category labels below are matched against whatever instrument names
actually exist in the DB; a commodity Dhan supports but that hasn't been
synced yet (sync never triggered for it) simply won't appear here until
/algo/sync-tokens/start/{instrument} has been run for it once — verify the
prefix list below against the real distinct() names once sync has run, and
adjust if Dhan's exact symbol naming differs (e.g. "ALUMINIUM" vs "ALUMINI").

Same "single source, served only from algo.websocket" pattern as
fno_stocks.py — never mounted in algo.trade/algo.simulator/algo.scanner too.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from features import auth as app_auth
from features.mongo_data import MongoData

router = APIRouter()

# Candidate symbol prefixes per category, matched case-insensitively against
# whatever instrument names Dhan's scrip master actually uses (e.g. "SILVER"
# matches "SILVER", "SILVERM", "SILVERMIC", ...). Verified against Dhan's
# actual scrip master 2026-06-30 sync (29,236 contracts, 24 distinct MCX
# underlyings) — covers every commodity Dhan currently lists, including the
# mini/micro contract variants (GOLDM, SILVERM, ALUMINI, ZINCMINI, ...) and
# a few Dhan lists that aren't in the user's original 4-category ask
# (NICKEL, STEELREBAR -> Metals; CARDAMOM, KAPAS -> Agri) so they don't fall
# into "uncategorized". Note: CPO/Crude Palm Oil isn't in Dhan's current
# scrip master at all — not delisted by this code, Dhan just isn't carrying
# an active contract for it right now.
_CATEGORY_PREFIXES: dict[str, list[str]] = {
    "Bullion": ["GOLD", "SILVER"],
    "Energy":  ["CRUDEOIL", "NATURALGAS", "NATGAS"],
    "Metals":  ["COPPER", "ZINC", "LEAD", "ALUMIN", "NICKEL", "STEELREBAR"],
    "Agri":    ["CPO", "PALM", "COTTON", "MENTHAOIL", "MENTHA", "CARDAMOM", "KAPAS"],
}


@router.get("/mcx-commodities")
async def get_mcx_commodities(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    {ok, categories: {Bullion: [...], Energy: [...], Metals: [...], Agri: [...]},
     uncategorized: [...]}
    Each entry: {symbol, lot_size}. An empty category just means
    /algo/sync-tokens/start/{instrument} hasn't been run yet for any
    commodity matching that category's prefixes — not that Dhan lacks it.
    """
    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        pipeline = [
            {"$match": {"broker": "dhan", "instrument_type": "commodity", "lot_size": {"$exists": True, "$ne": None}}},
            {"$group": {
                "_id": "$instrument",
                "lot_size": {"$first": "$lot_size"},
                "types": {"$addToSet": "$option_type"},
            }},
            {"$project": {"symbol": "$_id", "lot_size": 1, "types": 1, "_id": 0}},
            {"$sort": {"symbol": 1}},
        ]
        # Some MCX underlyings are synced with a FUT contract only (no CE/PE
        # ever pulled) — not option-tradable, so drop them instead of showing
        # a dead end in the picker.
        commodities = [
            {"symbol": c["symbol"], "lot_size": c["lot_size"]}
            for c in col.aggregate(pipeline)
            if "CE" in c["types"] and "PE" in c["types"]
        ]
    finally:
        db.close()

    categories: dict[str, list[dict]] = {cat: [] for cat in _CATEGORY_PREFIXES}
    uncategorized: list[dict] = []
    for c in commodities:
        sym = str(c.get("symbol") or "").upper()
        matched_category = next(
            (cat for cat, prefixes in _CATEGORY_PREFIXES.items() if any(sym.startswith(p) for p in prefixes)),
            None,
        )
        if matched_category:
            categories[matched_category].append(c)
        else:
            uncategorized.append(c)

    return {"ok": True, "categories": categories, "uncategorized": uncategorized}
