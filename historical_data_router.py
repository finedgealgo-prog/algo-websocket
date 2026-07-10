"""
historical_data_router.py
──────────────────────────
Single source for the "historical market data" surface — per-minute OHLCV
(mtm), spot+VIX history, option IV/Greeks history, and the paper-trade
backtest-replay option chain snapshot. Mounted identically across
algo.trade, algo.simulator, and algo.websocket (port 8003 — algo.websocket
is where the rest of the live market-data surface already lives, e.g.
live_quote_socket.py / live_greeks_chain_socket.py / the chain-feed pool in
dhan_ticker.py; this is the next piece moving toward a single "price"
domain covering sockets + LTP + quote + historical + option chain).

All five endpoints are pure Mongo reads (option_chain_historical_data,
option_chain_index_spot, mtm-related collections), with no algo_trades /
trade-execution-state coupling — unlike /ws/execute-orders or /ws/update,
which must stay wherever the trade is actually executing. Safe to serve
identically from any process.
"""

from __future__ import annotations

from collections import Counter as _Counter

from fastapi import APIRouter, Depends, HTTPException, Query

from features import auth as app_auth
from features.mongo_data import MongoData

router = APIRouter()


# ── MTM historical (per-minute OHLCV for leg tokens) ──────────────────────────

@router.get("/algo/mtm/historical-data")
async def mtm_historical_data(
    tokens: str = Query(default=""),
    candle: str = Query(default=""),
    activation_mode: str = Query(default=""),
    current_user: dict = Depends(app_auth.require_current_user),
):
    """
    Return per-minute OHLCV candle data for the given active leg tokens.

    Query params:
        tokens          – comma-separated  e.g. NSE_54812,NSE_54815,BSE_869786
        candle          – ISO timestamp    e.g. 2026-04-08T11:10:21+05:30
        activation_mode – optional; algo-backtest | fast-forward | live

    Only tokens that have an active (entered, not exited) leg on the trade date
    derived from `candle` are returned.

    Backtest:          open = high = low = close
    Fast-forward/Live: real OHLCV from Kite historical_data API (minute candles)
    """
    from features.mtm_historical_data import get_mtm_historical_data

    if not tokens.strip():
        raise HTTPException(status_code=400, detail="tokens param is required")

    db = MongoData()
    try:
        data = get_mtm_historical_data(db, tokens, candle, activation_mode)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"mtm historical data error: {exc}") from exc
    finally:
        db.close()

    return data


# ── Spot + India VIX historical ───────────────────────────────────────────────

@router.get("/algo/spot/historical-data")
async def spot_historical_data(
    underlying: str = Query(default=""),
    candle: str = Query(default=""),
    activation_mode: str = Query(default=""),
    current_user: dict = Depends(app_auth.require_current_user),
):
    """
    Return per-minute spot price history for an underlying index (e.g. NIFTY)
    and India VIX from option_chain_index_spot.

    Query params:
        underlying      – e.g. NIFTY, BANKNIFTY
        candle          – ISO timestamp  e.g. 2025-11-03T15:30:00
        activation_mode – optional; algo-backtest | fast-forward | live

    Response:
        {
          "256265": { timestamp, close },
          "SPOT_NIFTY": { timestamp, close },
          "NSE_00": { timestamp, close }
        }
    """
    from features.spot_historical_data import get_spot_historical_data

    if not underlying.strip():
        raise HTTPException(status_code=400, detail="underlying param is required")

    db = MongoData()
    try:
        data = get_spot_historical_data(db, underlying, candle, activation_mode)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"spot historical data error: {exc}") from exc
    finally:
        db.close()

    return data


# ── Option chain IV/Greeks historical ─────────────────────────────────────────

@router.get("/algo/option-chain/historical-iv")
async def option_chain_historical_iv(
    tokens: str = Query(default=""),
    candle: str = Query(default=""),
    activation_mode: str = Query(default="algo-backtest"),
):
    """
    Return per-minute price + IV + Delta history for option leg tokens
    from option_chain_historical_data.

    Query params:
        tokens          – comma-separated  e.g. NSE_2025110484996,NSE_2025110460049
        candle          – ISO timestamp    e.g. 2025-11-03T15:30:00
        activation_mode – algo-backtest | fast-forward | live

    Response:
        { "NSE_TOKEN": { timestamp, close, iv, delta, oi } }
    """
    from features.iv_historical_data import get_iv_historical_data

    if not tokens.strip():
        raise HTTPException(status_code=400, detail="tokens param is required")

    db = MongoData()
    try:
        data = get_iv_historical_data(db, tokens, candle, activation_mode)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"iv historical data error: {exc}") from exc
    finally:
        db.close()

    return data


# ── Paper-trade backtest-replay option chain snapshot ─────────────────────────

@router.get("/simulator/paper-trade/historical-chain/{instrument}")
async def simulator_pt_historical_chain(
    instrument: str,
    timestamp: str = Query(..., description="ISO timestamp, e.g. 2025-11-03T09:16:00"),
    expiry: str = Query(default=""),
) -> dict:
    """
    Historical twin of /live-greeks-chain/{instrument} for the paper-trade
    backtest replay page — same response contract (instrument, expiry,
    expiries, spot_price, pricing_spot, previous_close, change_pct,
    change_points, atm_strike, strike_interval, india_vix, lot_size,
    chain: {CE, PE}) but sourced from option_chain_historical_data /
    option_chain_index_spot instead of a live broker. Greeks are read
    straight off the stored doc (already pre-computed at backfill time)
    rather than recalculated via Black-Scholes.
    """
    normalized = str(instrument or "").strip().upper()
    norm_ts = str(timestamp or "").strip().replace(" ", "T").rstrip("Z")
    if not normalized:
        raise HTTPException(status_code=400, detail="Instrument is required.")
    if len(norm_ts) < 10:
        raise HTTPException(status_code=400, detail="timestamp is required, e.g. 2025-11-03T09:16:00")

    req_date = norm_ts[:10]
    day_start = f"{req_date}T00:00:00"

    db = MongoData()
    try:
        chain_col = db._db["option_chain_historical_data"]
        spot_col = db._db["option_chain_index_spot"]

        # Nearest minute-tick at or before the requested timestamp, same
        # trading day only — mirrors /algo/option-chain-snapshot's pivot logic.
        pivot = chain_col.find_one(
            {"underlying": normalized, "timestamp": {"$lte": norm_ts, "$gte": day_start}},
            {"_id": 0, "timestamp": 1},
            sort=[("timestamp", -1)],
        )
        # A day's first recorded tick is sometimes a minute or two after the
        # literal 09:15 open — "SOD"/an early seek shouldn't error just because
        # no tick exists yet at-or-before that instant; snap forward to the
        # day's earliest tick instead. Only a genuinely data-less day errors.
        if not pivot:
            pivot = chain_col.find_one(
                {"underlying": normalized, "timestamp": {"$gte": norm_ts, "$lte": day_start[:10] + "T23:59:59"}},
                {"_id": 0, "timestamp": 1},
                sort=[("timestamp", 1)],
            )
        pivot_ts = pivot["timestamp"] if pivot else None
        if not pivot_ts:
            return {"status": "error", "message": f"No historical data for {normalized} on {req_date}."}

        expiries_sorted = sorted(set(
            str(e)[:10] for e in chain_col.distinct("expiry", {"underlying": normalized, "timestamp": pivot_ts}) if e
        ))
        if not expiries_sorted:
            return {"status": "error", "message": f"No option chain rows for {normalized} at {pivot_ts}."}

        # Resolve to exactly ONE expiry — same precedence as /live-greeks-chain:
        # explicit request if valid, else nearest expiry on/after the replay
        # date, else the last available. Without this, leaving `expiry` blank
        # would union every expiry's strikes into one CE/PE list.
        req_expiry = str(expiry or "").strip()[:10]
        if req_expiry and req_expiry in expiries_sorted:
            live_expiry = req_expiry
        else:
            future = [e for e in expiries_sorted if e >= req_date]
            live_expiry = future[0] if future else expiries_sorted[-1]

        raw_rows = list(chain_col.find(
            {"underlying": normalized, "timestamp": pivot_ts, "expiry": live_expiry},
            {"_id": 0, "expiry": 1, "strike": 1, "type": 1, "token": 1, "close": 1,
             "iv": 1, "delta": 1, "gamma": 1, "theta": 1, "vega": 1, "oi": 1},
        ))
        if not raw_rows:
            return {"status": "error", "message": f"No option chain rows for {normalized} {live_expiry} at {pivot_ts}."}

        # ── spot price + previous close ──────────────────────────────────────
        spot_doc = spot_col.find_one(
            {"underlying": normalized, "timestamp": {"$lte": pivot_ts, "$gte": day_start}},
            {"_id": 0, "close": 1, "spot_price": 1},
            sort=[("timestamp", -1)],
        ) or {}
        spot_price = float(spot_doc.get("spot_price") or spot_doc.get("close") or 0)

        prev_doc = spot_col.find_one(
            {"underlying": normalized, "timestamp": {"$lt": day_start}},
            {"_id": 0, "close": 1, "spot_price": 1},
            sort=[("timestamp", -1)],
        ) or {}
        previous_close = float(prev_doc.get("spot_price") or prev_doc.get("close") or 0)
        change_pct = round((spot_price - previous_close) / previous_close * 100, 2) if previous_close else 0.0
        change_points = round(spot_price - previous_close, 2) if previous_close else 0.0

        # ── India VIX: "INDIAVIX"-tagged rows (current backfill) → legacy
        # token-only NSE_00 rows (no "underlying" field, older data) ─────────
        vix_doc = (
            spot_col.find_one(
                {"underlying": "INDIAVIX", "timestamp": {"$lte": pivot_ts}},
                {"_id": 0, "close": 1, "spot_price": 1}, sort=[("timestamp", -1)],
            )
            or spot_col.find_one(
                {"token": "NSE_00", "timestamp": {"$lte": pivot_ts}},
                {"_id": 0, "close": 1, "spot_price": 1}, sort=[("timestamp", -1)],
            )
            or {}
        )
        india_vix = round(float(vix_doc.get("spot_price") or vix_doc.get("close") or 0), 2)

        # ── lot size: same lookup + defaults as /live-greeks-chain ───────────
        lot_size_defaults = {"NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40, "MIDCPNIFTY": 120, "SENSEX": 10, "BANKEX": 15}
        expiry_date_str = str(live_expiry)[:10]
        lot_doc = db._db["lot_sizes"].find_one({
            "underlying": normalized,
            "from_date": {"$lte": expiry_date_str},
            "to_date": {"$gte": expiry_date_str},
        })
        lot_size = int(lot_doc["lot_size"]) if lot_doc else lot_size_defaults.get(normalized, 75)

        # ── chain rows — Greeks already pre-computed at backfill time ───────
        chain: dict[str, list[dict]] = {"CE": [], "PE": []}
        for row in raw_rows:
            opt_type = str(row.get("type") or "").upper()
            if opt_type not in ("CE", "PE"):
                continue
            strike = float(row.get("strike") or 0)
            row_expiry = str(row.get("expiry") or "")[:10]
            strike_label = int(strike) if strike == int(strike) else strike
            symbol = f"{normalized}{row_expiry.replace('-', '')}{strike_label}{opt_type}"
            chain[opt_type].append({
                "strike": strike_label,
                "ltp": float(row.get("close") or 0),
                "iv": round(float(row.get("iv") or 0) * 100, 2),
                "delta": float(row.get("delta") or 0),
                "gamma": float(row.get("gamma") or 0),
                "theta": float(row.get("theta") or 0),
                "vega": float(row.get("vega") or 0),
                "oi": int(row.get("oi") or 0),
                "token": str(row.get("token") or ""),
                "symbol": symbol,
            })
        chain["CE"].sort(key=lambda x: float(x["strike"]))
        chain["PE"].sort(key=lambda x: float(x["strike"]))

        # ── strike interval + ATM: same logic as /live-greeks-chain ─────────
        all_strikes = sorted(set(float(r["strike"]) for r in raw_rows))
        strike_interval = 0.0
        if len(all_strikes) >= 2:
            diffs = [all_strikes[i + 1] - all_strikes[i] for i in range(len(all_strikes) - 1)]
            strike_interval = float(_Counter(diffs).most_common(1)[0][0])

        atm_strike = 0.0
        if all_strikes and spot_price > 0:
            atm_strike = min(all_strikes, key=lambda s: abs(s - spot_price))
        elif all_strikes:
            atm_strike = all_strikes[len(all_strikes) // 2]

        return {
            "status": "success",
            "instrument": normalized,
            "expiry": live_expiry,
            "expiries": expiries_sorted,
            "spot_price": round(spot_price, 2),
            "pricing_spot": round(spot_price, 2),
            "previous_close": round(previous_close, 2),
            "change_pct": change_pct,
            "change_points": change_points,
            "atm_strike": int(atm_strike) if atm_strike == int(atm_strike) else atm_strike,
            "strike_interval": int(strike_interval) if strike_interval == int(strike_interval) else strike_interval,
            "india_vix": india_vix,
            "lot_size": lot_size,
            "chain": chain,
            "timestamp": pivot_ts,
        }
    finally:
        db.close()


@router.get("/simulator/paper-trade/historical-chain-latest-date/{instrument}")
async def simulator_pt_historical_chain_latest_date(instrument: str) -> dict:
    """
    Latest trading day with near-market-open data for this instrument, for the
    backtest replay page's "open with real data" default. Backfill coverage is
    patchy (some instruments/date-ranges have only a single intraday snapshot,
    not a full day) — a naive "most recent timestamp" pick can land on one of
    those, so this specifically looks for a 09:15-09:35 tick (every real
    backfilled trading day has one) and lets the (underlying, timestamp) index
    skip over any gap directly, rather than the frontend walking back one HTTP
    call per calendar day.
    """
    normalized = str(instrument or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="Instrument is required.")

    db = MongoData()
    try:
        doc = db._db["option_chain_historical_data"].find_one(
            {"underlying": normalized, "timestamp": {"$regex": r"^\d{4}-\d{2}-\d{2}T09:(1[5-9]|2[0-9]|3[0-5])"}},
            {"_id": 0, "timestamp": 1},
            sort=[("timestamp", -1)],
        )
        if not doc:
            return {"status": "error", "message": f"No historical data found for {normalized}."}
        return {"status": "success", "date": doc["timestamp"][:10], "timestamp": doc["timestamp"]}
    finally:
        db.close()
