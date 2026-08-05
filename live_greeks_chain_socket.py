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
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect

log = logging.getLogger(__name__)

live_greeks_chain_socket_router = APIRouter()

IST = timezone(timedelta(hours=5, minutes=30))
PUSH_INTERVAL_SECONDS = 2.0   # matches live_option_chain.py's _CHAIN_TTL_SECONDS

# Strikes kept each side of ATM per CE/PE for the live UI chain. fetch_full_
# chain's strike_window param exists exactly for this (see its docstring) —
# 0 means "every strike" (fetch_full_chain's own default/unwindowed
# behavior). Was 30 (ATM±30 per side) to keep a cold/far-month expiry fast,
# but the Trading Terminal wishlist search needs every strike to show up in
# results, and this constant is shared by every consumer of this chain
# endpoint (LiveOptionChain.tsx included) — so unwindowed is now the default
# everywhere, at the cost of a cold/wide expiry being slower to first-load
# (see fetch_full_chain's docstring for the original tradeoff).
UI_CHAIN_STRIKE_WINDOW = 0

_LOT_SIZE_DEFAULTS = {'NIFTY': 75, 'BANKNIFTY': 15, 'FINNIFTY': 40, 'MIDCPNIFTY': 120, 'SENSEX': 10, 'BANKEX': 15}

# Per-process "already kicked off" set — first request for ANY expiry of an
# underlying triggers a background warm of every OTHER expiry's chain too, so
# a user clicking through expiry tabs (weekly → next-weekly → monthly, ...)
# never hits a cold WS subscription one tab at a time. Without this, only
# whichever expiry happens to be actively/repeatedly requested stays warm;
# everything else always starts from ltp=0 and takes a few seconds to catch
# up (see get_broker_rest_quotes' _has_never_seen fast-path for the other
# half of this fix — REST no longer silently skips a never-seen token just
# because another caller held the shared Dhan rate-gate slot).
_warm_lock = threading.Lock()
_warmed_underlyings: set[str] = set()


def _ensure_all_expiries_warm(db_raw, underlying: str, expiries: list[str]) -> None:
    from features.broker_gateway import _active_broker  # type: ignore
    if _active_broker() != 'dhan':
        return   # Kite path REST-fetches the whole chain per-request anyway — no warm needed.
    with _warm_lock:
        if underlying in _warmed_underlyings:
            return
        _warmed_underlyings.add(underlying)

    def _run() -> None:
        from features.broker_gateway import broker_ticker_manager  # type: ignore
        for exp in expiries:
            try:
                docs = list(db_raw['active_option_tokens'].find(
                    {'broker': 'dhan', 'instrument': underlying, 'expiry': {'$regex': f'^{exp}'}},
                    {'_id': 0, 'token': 1, 'tokens': 1, 'ws_segment': 1},
                ))
                by_segment: dict[str, list[str]] = {}
                for d in docs:
                    tok = str(d.get('token') or d.get('tokens') or '').strip()
                    if tok:
                        by_segment.setdefault(str(d.get('ws_segment') or 'NSE_FNO'), []).append(tok)
                for segment, ids in by_segment.items():
                    broker_ticker_manager.warm_chain_tokens(ids, segment)
            except Exception as exc:
                log.warning('[GreeksChainHub] warm-all-expiries error underlying=%s expiry=%s: %s', underlying, exp, exc)

    threading.Thread(target=_run, daemon=True, name=f'warm_all_expiries_{underlying}').start()


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


# underlying -> (day, prev_close) resolved the FIRST time this session. Without
# this, _resolve_previous_close re-picks its source on every single call (every
# REST hit + every WS chain push): Dhan's live prev_close_map when present, else
# the Mongo lookup — and those two don't always agree to the last rupee, so
# change_pct/change_points flickered between two close-but-different numbers
# depending on which source happened to win that call. Same fix as
# execution_socket.py's _fetch_dhan_index_quotes() (_STABLE_PREV_CLOSE there) —
# see the module comment above about this file having a separate, unpatched copy.
_STABLE_PREV_CLOSE: dict[str, tuple[str, float]] = {}


def _resolve_previous_close(db, underlying: str) -> float:
    """Yesterday's actual close — NOT pricing_spot (see _build_chain_payload).

    Prefers Dhan's own RESP_PREV_CLOSE WS packet (dhan_ticker.py's
    prev_close_map), falling back to the Mongo lookup while that map is still
    warming up — then locks whichever value resolved first in place for the
    rest of the trading day (see _STABLE_PREV_CLOSE above) so it can't flicker
    between the two sources on subsequent calls.
    """
    today = datetime.now(IST).strftime('%Y-%m-%d')
    cached = _STABLE_PREV_CLOSE.get(underlying)
    if cached is not None and cached[0] == today and cached[1] > 0:
        return cached[1]

    resolved = 0.0
    try:
        from features.dhan_ticker import _load_dhan_spot_tokens
        from features.broker_gateway import broker_ticker_manager as _tm_glc
        spot_tokens, vix_token = _load_dhan_spot_tokens(db)
        token = vix_token if underlying == 'INDIAVIX' else next(
            (tok for tok, u in spot_tokens.items() if u == underlying), None,
        )
        if token:
            resolved = float(_tm_glc.prev_close_map.get(str(token)) or 0)
    except Exception:
        pass

    if resolved <= 0:
        day_start = f'{today}T00:00:00'
        doc = db['option_chain_index_spot'].find_one(
            {'underlying': underlying, 'timestamp': {'$lt': day_start}},
            {'_id': 0, 'close': 1, 'spot_price': 1},
            sort=[('timestamp', -1)],
        ) or {}
        resolved = float(doc.get('spot_price') or doc.get('close') or 0)

    if resolved > 0:
        _STABLE_PREV_CLOSE[underlying] = (today, resolved)
    return resolved


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
    _ensure_all_expiries_warm(db._db, underlying, expiries)
    resolved_expiry = expiry or (expiries[0] if expiries else '')
    spot_price = _resolve_spot_price(db._db, underlying, resolved_expiry)
    chain = (
        fetch_full_chain(db, underlying, resolved_expiry, spot_price, strike_window=UI_CHAIN_STRIKE_WINDOW)
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


def prewarm_chain_rest(underlyings: list[str]) -> None:
    """
    Runs the exact same _build_chain_payload() a real /live-greeks-chain
    request would, for EVERY future expiry of each underlying, at server
    startup (see ws_main.py) instead of waiting for the first real user to
    pay for it — same all-expiries scope as _prewarm_index_chains
    (dhan_ticker.py)'s WS subscribe below, just the REST/Mongo half of it.

    _prewarm_index_chains already warms the WS *subscription* for these
    underlyings at ticker start — that gets LTP flowing into ltp_map, but a
    freshly-subscribed token's first tick still takes a moment to arrive, and
    until it does, fetch_full_chain() falls back to the Dhan REST quotes+
    depth round trip (get_broker_rest_quotes/get_broker_rest_depth) same as a
    genuinely cold token. Only warming the nearest expiry here left every
    other expiry (which is most of them — an index typically lists 3-6+
    future expiries) paying that ~1-10s cold cost on whichever user picked
    that expiry first, defeating the point of prewarming at all.

    Sequential, not parallel — both across underlyings AND across one
    underlying's own expiries — every REST call in here still goes through
    the same shared Dhan rate gate (broker_gateway.py's
    dhan_quote_post_blocking) every other caller in the app uses, so firing
    many at once would just make each other wait instead of actually running
    any faster, and risks tripping Dhan's real rate limit at exactly the
    moment the server boots. One at a time is the same total background time
    either way.
    """
    from features.mongo_data import MongoData  # type: ignore

    for underlying in underlyings:
        db = MongoData()
        try:
            expiries = _resolve_expiries(db._db, underlying)
            if not expiries:
                log.warning('[GreeksChainPrewarm] %s has no active expiries — skipped', underlying)
                continue
            for expiry in expiries:
                try:
                    _build_chain_payload(db, underlying, expiry)
                except Exception as exc:
                    log.warning('[GreeksChainPrewarm] %s expiry=%s failed: %s', underlying, expiry, exc)
            log.info('[GreeksChainPrewarm] warmed %s (%d expiries)', underlying, len(expiries))
        except Exception as exc:
            log.warning('[GreeksChainPrewarm] %s failed: %s', underlying, exc)
        finally:
            db.close()


class _GreeksChainHub:
    """
    Per-(underlying, expiry) broadcaster: one background task computes and
    pushes the chain to every connected client for that key, regardless of
    how many clients are watching.
    """

    def __init__(self) -> None:
        self._clients: dict[tuple[str, str], set[WebSocket]] = {}
        self._tasks:   dict[tuple[str, str], asyncio.Task] = {}
        self._latest_payloads: dict[tuple[str, str], tuple[float, dict]] = {}
        self._lock = asyncio.Lock()

    def get_cached_payload(self, key: tuple[str, str], max_age_seconds: float = PUSH_INTERVAL_SECONDS + 0.5) -> dict | None:
        cached = self._latest_payloads.get(key)
        if not cached:
            return None
        cached_at, payload = cached
        if time.monotonic() - cached_at > max_age_seconds:
            return None
        return payload

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
        cached = self.get_cached_payload(key)
        if cached is not None:
            try:
                await ws.send_text(json.dumps(cached))
            except Exception:
                pass
            return
        db = MongoData()
        try:
            payload = await asyncio.to_thread(_build_chain_payload, db, underlying, expiry)
            self._latest_payloads[key] = (time.monotonic(), payload)
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
                    self._latest_payloads[key] = (time.monotonic(), payload)
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


@live_greeks_chain_socket_router.websocket('/ws/live-greeks-chain')
async def live_greeks_chain_socket_multi(websocket: WebSocket) -> None:
    """
    Multiplexed sibling of /ws/live-greeks-chain/{instrument} above — one
    connection, many (instrument, expiry) pairs, added/dropped via
    {action:"subscribe"|"unsubscribe", instrument, expiry} messages instead of
    one connection per pair. Matches what the frontend's useLiveChainSocket.ts
    (useLiveChainSocketMulti) has always sent — the mismatch with the old
    single-instrument-in-path-only endpoint (which silently ignored message
    bodies) is why the frontend kept its WS disabled. Reuses the same
    _GreeksChainHub — this is a different ingress onto it, not a second push
    mechanism.
    """
    await websocket.accept()
    held: set[tuple[str, str]] = set()
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            action = str(msg.get('action') or '').strip().lower()
            underlying = str(msg.get('instrument') or '').strip().upper()
            expiry = str(msg.get('expiry') or '').strip()
            if not underlying:
                continue
            key = (underlying, expiry)
            if action == 'subscribe':
                if key not in held:
                    held.add(key)
                    await _hub.register(key, websocket)
            elif action == 'unsubscribe':
                if key in held:
                    held.discard(key)
                    await _hub.unregister(key, websocket)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        for key in list(held):
            try:
                await _hub.unregister(key, websocket)
            except Exception:
                pass


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
    key = (underlying, str(expiry or '').strip())
    cached = _hub.get_cached_payload(key)
    if cached is not None:
        return cached

    db = MongoData()
    try:
        payload = await asyncio.to_thread(_build_chain_payload, db, underlying, str(expiry or '').strip())
        _hub._latest_payloads[key] = (time.monotonic(), payload)
        return payload
    finally:
        db.close()
