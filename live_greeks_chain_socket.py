"""
live_greeks_chain_socket.py
────────────────────────────
WebSocket replacement for the REST GET /live-greeks-chain/{instrument} poll
(LiveOptionChain.tsx used setInterval(5000) before this — see algo.trade/
api.py's get_live_greeks_chain). Same Greeks-enriched chain shape, pushed
instead of polled.

Reuses fetch_full_chain() (live_option_chain.py) — already backed by the
chain-feed connection pool (dhan_ticker.py's warm_chain_tokens) and a 2s TTL
cache, so this adds no second independent fetch path. One broadcaster loop
per (underlying, expiry) is shared across every client watching that chain —
N clients on the same chain cost exactly one fetch_full_chain() call per
push interval, not N. Push interval matches the cache TTL: pushing faster
would just re-send the same cached chain.

Works identically for index underlyings (NIFTY, BANKNIFTY, ...) and
individual F&O stocks (RELIANCE, TCS, ...) — fetch_full_chain() and
active_option_tokens are already generic over `instrument`, no index/stock
special-casing needed on either side.

Mounted on algo.websocket (port 8003) — this is where the chain-feed pool
and ltp_map physically live, so fetch_full_chain()'s WS-first lookup never
crosses a process boundary here (unlike algo.trade/algo.simulator, which
reach it via CentralTickClient over /ws/internal-ticks).

Endpoint: GET (upgrade) /ws/live-greeks-chain/{instrument}?expiry=...
  - expiry omitted → nearest available expiry is resolved and re-resolved
    each push (cheap distinct() query), matching the REST endpoint's
    default-expiry behavior.
  - Switching expiry is a new connection (new (instrument, expiry) key),
    same shape as the REST endpoint's "click a different expiry tab".
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect

log = logging.getLogger(__name__)

live_greeks_chain_socket_router = APIRouter()

IST = timezone(timedelta(hours=5, minutes=30))
PUSH_INTERVAL_SECONDS = 2.0   # matches live_option_chain.py's _CHAIN_TTL_SECONDS

_LOT_SIZE_DEFAULTS = {'NIFTY': 75, 'BANKNIFTY': 15, 'FINNIFTY': 40, 'MIDCPNIFTY': 120, 'SENSEX': 10, 'BANKEX': 15}


def _resolve_expiries(db, underlying: str) -> list[str]:
    """All available expiries (>= today) for this underlying, broker-aware."""
    from features.broker_gateway import _active_broker  # type: ignore
    broker = _active_broker()
    today = datetime.now(IST).strftime('%Y-%m-%d')
    raw = db['active_option_tokens'].distinct(
        'expiry',
        {'broker': broker, 'instrument': underlying, 'expiry': {'$gte': today}},
    )
    return sorted(str(e)[:10] for e in raw if e)


def _resolve_spot_price(db, underlying: str, expiry: str) -> float:
    from features.broker_gateway import broker_ticker_manager  # type: ignore
    try:
        spot = float(broker_ticker_manager.spot_map.get(str(underlying or '').upper()) or 0.0)
        if spot > 0:
            return spot
    except Exception:
        pass
    # Commodities (options-on-futures) have no index "spot" entry in
    # market_feed_tokens — use the matching FUTCOM contract's LTP instead,
    # same fallback as live_option_chain.py's _fetch_full_chain_from_dhan.
    # FUT contracts are bi-monthly but options are ~monthly, so they never
    # share an exact expiry — use the nearest FUT expiring on/after this
    # option's expiry (the future this option actually settles against),
    # not an exact-date match (which silently finds nothing for commodities).
    try:
        from features.broker_gateway import get_broker_rest_quotes  # type: ignore
        fut_doc = db['active_option_tokens'].find_one(
            {
                'broker': 'dhan', 'instrument': underlying, 'option_type': 'FUT',
                'expiry': {'$gte': str(expiry or '').strip()[:10]},
            },
            {'_id': 0, 'token': 1, 'tokens': 1, 'ws_segment': 1},
            sort=[('expiry', 1)],
        )
        if fut_doc:
            fut_tok = str(fut_doc.get('token') or fut_doc.get('tokens') or '').strip()
            if fut_tok:
                quotes = get_broker_rest_quotes([fut_tok], db, {fut_tok: str(fut_doc.get('ws_segment') or 'MCX_COMM')})
                return float((quotes.get(fut_tok) or {}).get('ltp') or 0.0)
    except Exception:
        pass
    return 0.0


def _resolve_previous_close(db, underlying: str) -> float:
    """Yesterday's actual close — NOT pricing_spot (see _build_chain_payload)."""
    today = datetime.now(IST).strftime('%Y-%m-%d')
    day_start = f'{today}T00:00:00'
    doc = db['option_chain_index_spot'].find_one(
        {'underlying': underlying, 'timestamp': {'$lt': day_start}},
        {'_id': 0, 'close': 1, 'spot_price': 1},
        sort=[('timestamp', -1)],
    ) or {}
    return float(doc.get('spot_price') or doc.get('close') or 0)


def _resolve_india_vix(db) -> float:
    """"INDIAVIX"-tagged rows (current backfill) → legacy token-only NSE_00 rows."""
    doc = (
        db['option_chain_index_spot'].find_one(
            {'underlying': 'INDIAVIX'}, {'_id': 0, 'close': 1, 'spot_price': 1}, sort=[('timestamp', -1)],
        )
        or db['option_chain_index_spot'].find_one(
            {'token': 'NSE_00'}, {'_id': 0, 'close': 1, 'spot_price': 1}, sort=[('timestamp', -1)],
        )
        or {}
    )
    return round(float(doc.get('spot_price') or doc.get('close') or 0), 2)


def _resolve_lot_size(db, underlying: str, expiry: str) -> int:
    expiry_date_str = str(expiry)[:10]
    doc = db['lot_sizes'].find_one({
        'underlying': underlying,
        'from_date': {'$lte': expiry_date_str},
        'to_date': {'$gte': expiry_date_str},
    })
    return int(doc['lot_size']) if doc else _LOT_SIZE_DEFAULTS.get(underlying, 75)


def _compute_atm_and_interval(chain: dict, spot_price: float) -> tuple[float, float]:
    all_strikes = sorted(set(
        float(r.get('strike') or 0) for side in ('CE', 'PE') for r in chain.get(side, [])
    ))
    strike_interval = 0.0
    if len(all_strikes) >= 2:
        diffs = [all_strikes[i + 1] - all_strikes[i] for i in range(len(all_strikes) - 1)]
        strike_interval = float(Counter(diffs).most_common(1)[0][0])
    atm_strike = 0.0
    if all_strikes and spot_price > 0:
        atm_strike = min(all_strikes, key=lambda s: abs(s - spot_price))
    elif all_strikes:
        atm_strike = all_strikes[len(all_strikes) // 2]
    return atm_strike, strike_interval


def _build_chain_payload(db, underlying: str, expiry: str) -> dict:
    from features.live_option_chain import fetch_full_chain  # type: ignore
    from features.broker_gateway import get_active_broker_token_status  # type: ignore

    expiries = _resolve_expiries(db._db, underlying)
    resolved_expiry = expiry or (expiries[0] if expiries else '')
    spot_price = _resolve_spot_price(db._db, underlying, resolved_expiry)
    chain = (
        fetch_full_chain(db, underlying, resolved_expiry, spot_price)
        if resolved_expiry else {'CE': [], 'PE': []}
    )

    previous_close = _resolve_previous_close(db._db, underlying)
    change_pct = round((spot_price - previous_close) / previous_close * 100, 2) if previous_close else 0.0
    change_points = round(spot_price - previous_close, 2) if previous_close else 0.0
    india_vix = _resolve_india_vix(db._db)
    lot_size = _resolve_lot_size(db._db, underlying, resolved_expiry) if resolved_expiry else _LOT_SIZE_DEFAULTS.get(underlying, 75)
    atm_strike, strike_interval = _compute_atm_and_interval(chain, spot_price)

    token_ok, token_msg = True, ''
    try:
        token_ok, token_msg = get_active_broker_token_status()
    except Exception:
        pass

    return {
        'type':                  'chain',
        'instrument':            underlying,
        'expiry':                resolved_expiry,
        'expiries':              expiries,
        'spot_price':            spot_price,
        'pricing_spot':          spot_price,
        'previous_close':        round(previous_close, 2),
        'change_pct':            change_pct,
        'change_points':         change_points,
        'atm_strike':            int(atm_strike) if atm_strike == int(atm_strike) else atm_strike,
        'strike_interval':       int(strike_interval) if strike_interval == int(strike_interval) else strike_interval,
        'india_vix':             india_vix,
        'lot_size':              lot_size,
        'chain':                 chain,
        'broker_session_expired': not token_ok,
        'broker_session_message': token_msg if not token_ok else '',
    }


class _GreeksChainHub:
    """
    Per-(underlying, expiry) broadcaster: one background task computes and
    pushes the chain to every connected client for that key, regardless of
    how many clients are watching.
    """

    def __init__(self) -> None:
        self._clients: dict[tuple[str, str], set[WebSocket]] = {}
        self._tasks:   dict[tuple[str, str], asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def register(self, key: tuple[str, str], ws: WebSocket) -> None:
        async with self._lock:
            existing = self._clients.setdefault(key, set())
            had_clients_already = bool(existing)
            existing.add(ws)
            needs_new_task = key not in self._tasks or self._tasks[key].done()
            if needs_new_task:
                self._tasks[key] = asyncio.create_task(self._broadcaster_loop(key))
        if had_clients_already and not needs_new_task:
            # Broadcaster already running for this chain — don't make this
            # client wait up to PUSH_INTERVAL_SECONDS for the next tick.
            asyncio.create_task(self._send_immediate(key, ws))

    async def unregister(self, key: tuple[str, str], ws: WebSocket) -> None:
        async with self._lock:
            clients = self._clients.get(key)
            if not clients:
                return
            clients.discard(ws)
            if not clients:
                self._clients.pop(key, None)
                task = self._tasks.pop(key, None)
                if task:
                    task.cancel()

    async def _send_immediate(self, key: tuple[str, str], ws: WebSocket) -> None:
        from features.mongo_data import MongoData  # type: ignore
        underlying, expiry = key
        db = MongoData()
        try:
            payload = await asyncio.to_thread(_build_chain_payload, db, underlying, expiry)
            await ws.send_text(json.dumps(payload))
        except Exception:
            pass
        finally:
            db.close()

    async def _broadcaster_loop(self, key: tuple[str, str]) -> None:
        from features.mongo_data import MongoData  # type: ignore
        underlying, expiry = key
        db = MongoData()
        try:
            while True:
                async with self._lock:
                    clients = list(self._clients.get(key) or [])
                if not clients:
                    return
                try:
                    payload = await asyncio.to_thread(_build_chain_payload, db, underlying, expiry)
                    msg = json.dumps(payload)
                    dead: list[WebSocket] = []
                    for ws in clients:
                        try:
                            await ws.send_text(msg)
                        except Exception:
                            dead.append(ws)
                    if dead:
                        async with self._lock:
                            live = self._clients.get(key)
                            if live:
                                for ws in dead:
                                    live.discard(ws)
                except Exception as exc:
                    log.warning('[GreeksChainHub] broadcast error key=%s: %s', key, exc)
                await asyncio.sleep(PUSH_INTERVAL_SECONDS)
        finally:
            db.close()


_hub = _GreeksChainHub()


@live_greeks_chain_socket_router.websocket('/ws/live-greeks-chain/{instrument}')
async def live_greeks_chain_socket(
    websocket: WebSocket,
    instrument: str,
    expiry: str = Query(default=''),
) -> None:
    await websocket.accept()
    underlying = str(instrument or '').strip().upper()
    key = (underlying, str(expiry or '').strip())
    await _hub.register(key, websocket)
    log.info('[live-greeks-chain] connected instrument=%s expiry=%s', underlying, expiry or '-')
    try:
        while True:
            # Client never needs to send anything — just keep the connection
            # alive and detect disconnects.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await _hub.unregister(key, websocket)
        log.info('[live-greeks-chain] disconnected instrument=%s expiry=%s', underlying, expiry or '-')


@live_greeks_chain_socket_router.get('/live-greeks-chain/{instrument}')
async def live_greeks_chain_rest(
    instrument: str,
    expiry: str = Query(default=''),
) -> dict:
    """
    One-shot REST snapshot — same payload shape as the WS push above (see
    _build_chain_payload). For callers that just need a single fetch (e.g.
    PaperTradeNew's main/overlay chain panels) rather than a live-updating
    subscription. Replaces the old algo.trade-only /live-greeks-chain route
    — common/shared market data lives only here (algo.websocket), never
    duplicated into algo.trade/algo.simulator/algo.scanner's own APIs.
    """
    from features.mongo_data import MongoData  # type: ignore

    underlying = str(instrument or '').strip().upper()
    if not underlying:
        raise HTTPException(status_code=400, detail='Instrument is required.')

    db = MongoData()
    try:
        return await asyncio.to_thread(_build_chain_payload, db, underlying, str(expiry or '').strip())
    finally:
        db.close()
