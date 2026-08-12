"""
Dedicated websocket entrypoint for the split backend.

Owns the single shared `/ws/live-quotes` hub (features/live_quote_socket.py)
that algo, simulator, scanner and signal/chart frontend pages all connect to
for live LTP/underlying-price updates.

Central-tick mode (new)
────────────────────────
This process holds the ONE broker WebSocket connection for the entire system.
algo.trade and algo.simulator do NOT open their own broker connections; instead
they connect to /ws/internal-ticks here and receive the same tick stream.
Subscribe/unsubscribe requests from any service are forwarded to this process
via POST /ticker/subscribe and POST /ticker/unsubscribe, so only ONE aggregated
subscription set is ever sent to the broker.

Endpoints
─────────
  GET  /ws/live-quotes              frontend LTP hub (existing)
  GET  /ws/live-greeks-chain/{ins}  Greeks-enriched chain push, replaces the
                                     old REST /live-greeks-chain poll (new)
  GET  /ws/internal-ticks           service-to-service tick feed (new)
  POST /ticker/subscribe            aggregate token subscribe (new)
  POST /ticker/unsubscribe          aggregate token unsubscribe (new)
  POST /ticker/warm-chain           aggregate chain-warm request (new)
  GET  /fno-stocks                  FNO stock list (new — same code as
                                     algo.trade/algo.simulator, see
                                     features/fno_stocks.py)
  GET  /algo/mtm/historical-data           \
  GET  /algo/spot/historical-data           } historical market data — same
  GET  /algo/option-chain/historical-iv     } code as algo.trade/algo.simulator,
  GET  /simulator/paper-trade/              } see features/historical_data_
       historical-chain{,-latest-date}/... / router.py (new)
  GET  /health                      status

This is step 1 of consolidating sockets + LTP + quote + historical + option
chain under one "price" domain (see price.finedgealgo.com discussion) — this
process is where that domain's data already physically lives.

Run (from /media/ashok-innoppl/7CD60970D6092C48/algo-backend/algo.websocket):
    uvicorn ws_main:app --reload --port 8003
"""

import asyncio
import json
import logging
import os
import threading
import uuid

from fastapi import APIRouter, Body, FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from features.broker_gateway import broker_ticker_manager as ticker_manager
from features.live_quote_socket import live_quote_socket_router
from live_greeks_chain_socket import live_greeks_chain_socket_router, prewarm_chain_rest
from fno_stocks import router as fno_stocks_router, _refresh_fno_cache
from historical_data_router import router as historical_data_router
from mcx_commodities import router as mcx_commodities_router
from features.mongo_data import MongoData
from features.market_hours_scheduler import run_market_hours_scheduler

log = logging.getLogger(__name__)

app = FastAPI(title="Algo WebSocket Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(live_quote_socket_router)
app.include_router(live_greeks_chain_socket_router)
# Canonical, ONLY home for the "price domain" REST surface — common/shared
# endpoints live here exclusively, never duplicated into algo.trade/
# algo.simulator/algo.scanner's own APIs (see shared/features/fno_stocks.py,
# historical_data_router.py, mcx_commodities.py).
app.include_router(fno_stocks_router)
app.include_router(mcx_commodities_router)
app.include_router(historical_data_router)

# ── Internal tick hub ─────────────────────────────────────────────────────────
# algo.trade and algo.simulator connect here to share this process's ONE broker
# WS connection instead of each opening their own.

_app_loop: asyncio.AbstractEventLoop | None = None


# Phase 3 groundwork (see /home/ashok-innoppl/.claude/plans/crystalline-
# moseying-hoare.md) — this channel is published to on every tick below
# regardless of whether any internal client is connected, so a future
# horizontally-scaled gateway tier (separate live_quote_socket.py processes,
# possibly on other machines, each holding a slice of the browser
# connections) can subscribe here instead of needing a direct WS to this one
# process. Inert today — nothing subscribes to it yet, this process still
# does the local in-process broadcast below exactly as before.
REDIS_TICK_CHANNEL = "algo:ticks"

_redis_pub_client = None


async def _get_redis_pub_client():
    global _redis_pub_client
    if _redis_pub_client is None:
        import redis.asyncio as aioredis
        # Capped — this fires on every broker tick (no consumer yet, see
        # REDIS_TICK_CHANNEL above), and an uncapped pool lets a tick burst
        # open far more concurrent connections than actually needed, which
        # exhausts this process's file-descriptor limit. broadcast()'s
        # try/except already treats a publish failure as a no-op, so a
        # capped-out pool just skips that tick's publish instead of crashing.
        _redis_pub_client = aioredis.Redis(host="localhost", port=6379, db=0, max_connections=20)
    return _redis_pub_client


class _InternalTickHub:
    """
    Broadcasts broker tick payloads to all connected internal service clients
    (algo.trade, algo.simulator).  Frontend clients use /ws/live-quotes —
    this hub is internal-only.
    """

    def __init__(self) -> None:
        self._clients: dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()

    async def register(self, ws: WebSocket, client_id: str) -> None:
        await ws.accept()
        async with self._lock:
            self._clients[client_id] = ws
        log.info("[InternalHub] service connected id=%s total=%d", client_id, len(self._clients))

    async def unregister(self, client_id: str) -> None:
        async with self._lock:
            self._clients.pop(client_id, None)
        log.info("[InternalHub] service disconnected id=%s remaining=%d", client_id, len(self._clients))

    async def broadcast(self, tick_payload: dict) -> None:
        """Broadcast a broker tick payload to all connected services, and
        publish the same payload to Redis (see REDIS_TICK_CHANNEL) for any
        future horizontally-scaled gateway process to consume."""
        changed_ltp: dict = tick_payload.get("changed_ltp_map") or {}
        spot_map:    dict = tick_payload.get("spot_map")         or {}
        if not changed_ltp:
            return

        msg = json.dumps({
            "type": "tick",
            "data": {
                "changed_ltp_map": changed_ltp,
                "spot_map":        spot_map,
                # New, additive keys — a receiver that only reads changed_ltp_map/spot_map
                # (existing behavior, e.g. CentralTickClient before this change) is
                # completely unaffected. Only ever non-empty for F&O legs on the main
                # connection (RESP_FULL packets — see dhan_ticker.py's _handle_binary),
                # so MPP pricing can read fresh bid/ask from a central-tick client too
                # instead of falling back to a REST call every time.
                "changed_oi_map":  tick_payload.get("changed_oi_map")  or {},
                "changed_bid_map": tick_payload.get("changed_bid_map") or {},
                "changed_ask_map": tick_payload.get("changed_ask_map") or {},
                # now_ts comes from the broker ticker's timestamp field
                "now_ts": (tick_payload.get("timestamp") or "")[:19],
            },
        })

        try:
            redis_client = await _get_redis_pub_client()
            await redis_client.publish(REDIS_TICK_CHANNEL, msg)
        except Exception as exc:
            # Never let a Redis hiccup break the existing local broadcast path below.
            log.debug("[InternalHub] redis publish skipped: %s", exc)

        if not self._clients:
            return

        async with self._lock:
            clients = list(self._clients.items())

        dead: list[str] = []
        for cid, ws in clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(cid)

        if dead:
            async with self._lock:
                for cid in dead:
                    self._clients.pop(cid, None)

    def get_status(self) -> dict:
        return {"connected_services": len(self._clients)}


_internal_hub = _InternalTickHub()

# ── Broker tick listener (thread → asyncio bridge) ────────────────────────────


def _on_broker_tick(tick_payload: dict) -> None:
    """
    Registered as a tick listener on the broker ticker.
    Runs in the broker's background thread — schedule the async broadcast
    onto the FastAPI event loop without blocking the tick callback.

    Previously short-circuited when no legacy internal-hub WS client was
    connected — harmless when broadcast() only ever did local WS fanout, but
    it also skipped the Redis publish below now that broadcast() does that
    unconditionally (see REDIS_TICK_CHANNEL), which would make Redis-only
    consumers (a future gateway process with no /ws/internal-ticks client at
    all) never receive anything.
    """
    if _app_loop is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(
            _internal_hub.broadcast(tick_payload),
            _app_loop,
        )
    except Exception as exc:
        log.warning("[InternalHub] broadcast schedule error: %s", exc)


# ── FastAPI startup / shutdown ────────────────────────────────────────────────


def _start_ticker_bg() -> None:
    db = MongoData()
    try:
        if ticker_manager.status == "running":
            ticker_manager.restart(db._db)
        else:
            ticker_manager.start(db._db)
        # Register internal broadcast listener after ticker is running
        ticker_manager.add_tick_listener(_on_broker_tick)
        log.info("[ws_main] internal tick listener registered")
    except Exception:
        log.exception("ticker start error")
    finally:
        db.close()


@app.on_event("startup")
async def _capture_loop() -> None:
    """Capture the running event loop for use in the tick listener thread bridge."""
    global _app_loop
    _app_loop = asyncio.get_running_loop()


# Phase 3 horizontal scale-out: a second (third, fourth, ...) ws_main
# instance meant to hold browser /ws/live-quotes connections must NOT also
# open its own Dhan/Kite WS connection — Dhan allows only 5 total, and the
# whole point of the central-tick design (see central_tick_client.py) is
# exactly one process owns that connection. Set WS_GATEWAY_ONLY=true to skip
# owning the broker connection and instead become a CentralTickClient of the
# primary instance — the same mechanism algo.trade/algo.simulator already
# use, reused here rather than inventing a second one. Unset (the default)
# preserves today's single-instance behavior exactly.
WS_GATEWAY_ONLY = os.getenv("WS_GATEWAY_ONLY", "").strip().lower() in ("1", "true", "yes")
WS_PRIMARY_HUB_URL = os.getenv("WS_PRIMARY_HUB_URL", "http://localhost:8003")


@app.on_event("startup")
async def _auto_start_ticker() -> None:
    if WS_GATEWAY_ONLY:
        from features.central_tick_client import CentralTickClient
        client = CentralTickClient(WS_PRIMARY_HUB_URL)
        ticker_manager.set_central_client(client)
        db = MongoData()
        try:
            client.start(db)
        finally:
            db.close()
        log.info("[STARTUP] Gateway-only mode — central-tick client started against %s", WS_PRIMARY_HUB_URL)
        return

    async def _bg():
        await asyncio.sleep(2)
        try:
            if ticker_manager.status not in ("running", "connecting"):
                threading.Thread(target=_start_ticker_bg, daemon=True).start()
                log.info("[STARTUP] Broker ticker auto-started.")
        except Exception:
            log.exception("[STARTUP] Broker ticker auto-start failed.")
    asyncio.create_task(_bg())


@app.on_event("startup")
async def _start_ticker_market_hours_schedule() -> None:
    """
    Auto-stop the broker WS connection after market close, auto-start it
    again ~09:10 next weekday — see features/market_hours_scheduler.py.
    Independent of _auto_start_ticker above (that one just brings the ticker
    up once on process boot); this keeps it in sync with market hours every
    day after that. /ticker/restart (start) and /ticker/stop (stop) remain
    available as a manual override at any time, market hours or not.
    """
    if WS_GATEWAY_ONLY:
        return  # gateway-only instances don't own a broker connection

    async def _scheduled_start() -> None:
        await asyncio.to_thread(_start_ticker_bg)

    asyncio.create_task(run_market_hours_scheduler(
        name="websocket-ticker",
        start_fn=_scheduled_start,
        stop_fn=ticker_manager.stop,
        is_running_fn=lambda: ticker_manager.status == "running",
    ))


@app.on_event("startup")
async def _auto_prewarm_chain_rest() -> None:
    """
    Pays the first /live-greeks-chain request's REST cost (Dhan quotes+depth
    round trip + Mongo baseline aggregation, ~0.6-1.4s cold) during server
    boot instead of on whichever user loads the page first — see
    prewarm_chain_rest's docstring. Reuses the same admin-configured
    algo_subscribe_index list _prewarm_index_chains (dhan_ticker.py) already
    warms the WS subscription for; empty config (nothing saved in the Prewarm
    Config admin page) means this is a no-op, same as that WS warm already is.

    15s delay — longer than _auto_start_ticker's 2s — so the broker WS
    connection this depends on (spot_map / ltp_map starting to fill) has had
    a real chance to come up first; firing before that would still work (REST
    doesn't need WS to be ready) but wastes the one background attempt racing
    a connection that isn't there yet.
    """
    async def _bg():
        await asyncio.sleep(15)
        try:
            db = MongoData()
            try:
                instruments = await asyncio.to_thread(_get_prewarm_config, db._db)
            finally:
                db.close()
            if not instruments:
                log.info("[STARTUP] Chain REST prewarm skipped — no instruments configured in algo_subscribe_index.")
                return
            threading.Thread(
                target=prewarm_chain_rest,
                args=(instruments,),
                daemon=True,
                name="chain_rest_prewarm",
            ).start()
            log.info("[STARTUP] Chain REST prewarm started for %s", instruments)
        except Exception:
            log.exception("[STARTUP] Chain REST prewarm failed.")
    asyncio.create_task(_bg())


@app.on_event("startup")
async def _auto_prewarm_fno_stocks() -> None:
    """
    Runs the /fno-stocks aggregation once at boot instead of on whichever
    request happens to hit a cold (post-restart) cache — see
    fno_stocks._refresh_fno_cache's docstring on why that cold path, run
    inline on a request, is slow and blocks this process's shared event loop
    (the same loop /ws/internal-ticks depends on).
    """
    async def _bg():
        try:
            stocks = await _refresh_fno_cache()
            log.info("[STARTUP] FNO stocks cache prewarmed (%d symbols).", len(stocks))
        except Exception:
            log.exception("[STARTUP] FNO stocks prewarm failed.")
    asyncio.create_task(_bg())


# ── Internal tick WebSocket endpoint ─────────────────────────────────────────


@app.websocket("/ws/internal-ticks")
async def internal_tick_feed(websocket: WebSocket) -> None:
    """
    algo.trade and algo.simulator connect here.
    They receive the same broker tick stream as this process's own
    live_quote_socket, with no extra broker connection opened on their side.
    """
    client_id = uuid.uuid4().hex
    await _internal_hub.register(websocket, client_id)
    try:
        # Keep the connection alive; services don't send messages here
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await _internal_hub.unregister(client_id)


# ── Token subscribe / unsubscribe endpoints ───────────────────────────────────


@app.post("/ticker/subscribe")
async def ticker_subscribe(body: dict = Body(...)) -> dict:
    """
    Any service POSTs here when it needs to subscribe tokens.
    Aggregated into the single broker WS subscription set.
    """
    tokens:   list[str] = [str(t) for t in (body.get("tokens") or []) if str(t).strip()]
    exchange: str       = str(body.get("exchange") or "NSE_FNO")
    if not tokens:
        return {"subscribed": 0}
    await asyncio.to_thread(ticker_manager.subscribe_tokens, tokens, exchange)
    log.info("[ticker/subscribe] %d tokens exchange=%s", len(tokens), exchange)
    return {"subscribed": len(tokens)}


@app.get("/ticker/restart")
async def ticker_restart() -> dict:
    """
    (Re)connect the hub's single Dhan/Kite WS connection — safe to call
    whether the ticker never started, is stuck "error"/"connecting", or is
    already "running" (_start_ticker_bg picks start() vs restart() itself).
    Needed because _auto_start_ticker only ever fires once, 2s after process
    boot — if that one attempt fails (e.g. a slow Dhan /profile check), the
    ticker is stuck until something calls this.
    """
    await asyncio.to_thread(_start_ticker_bg)
    return {"ok": True, "message": "Ticker restart triggered", "ticker_status": ticker_manager.status}


@app.api_route("/ticker/stop", methods=["GET", "POST"])
async def ticker_stop() -> dict:
    """
    Manually stop the hub's single Dhan/Kite WS connection — the override
    counterpart to /ticker/restart. Also used by the market-hours scheduler
    (see _start_ticker_market_hours_schedule above) to close the connection
    after market close; calling this by hand at any other time works the
    same way and the 09:10 schedule will bring it back up next weekday.
    """
    await asyncio.to_thread(ticker_manager.stop)
    return {"ok": True, "message": "Ticker stop triggered", "ticker_status": ticker_manager.status}


@app.post("/ticker/warm-chain")
async def ticker_warm_chain(body: dict = Body(...)) -> dict:
    """
    algo.trade / algo.simulator POST here (via CentralTickClient.warm_chain_
    tokens) instead of trying to warm a chain locally — only this process
    holds the real dhan_ticker_manager with the chain-feed connection pool
    (see dhan_ticker.py). Forwards straight into that pool.
    """
    tokens:   list[str] = [str(t) for t in (body.get("tokens") or []) if str(t).strip()]
    exchange: str       = str(body.get("exchange") or "NSE_FNO")
    if not tokens:
        return {"warmed": 0}
    await asyncio.to_thread(ticker_manager.warm_chain_tokens, tokens, exchange)
    log.info("[ticker/warm-chain] %d tokens exchange=%s", len(tokens), exchange)
    return {"warmed": len(tokens)}


@app.post("/ticker/unsubscribe")
async def ticker_unsubscribe(body: dict = Body(...)) -> dict:
    """
    Unsubscribe tokens.  No-op if the broker ticker has no unsubscribe
    (Dhan keeps all subscribed tokens for the session lifetime).
    """
    tokens: list[str] = [str(t) for t in (body.get("tokens") or []) if str(t).strip()]
    if tokens:
        try:
            # Only Kite supports mid-session unsubscribe; Dhan ignores this
            if hasattr(ticker_manager._ticker, "unsubscribe"):
                await asyncio.to_thread(
                    ticker_manager._ticker.unsubscribe,
                    [int(t) for t in tokens if t.isdigit()],
                )
        except Exception as exc:
            log.warning("[ticker/unsubscribe] %s", exc)
    return {"unsubscribed": len(tokens)}


# ── Prewarm config (admin) ────────────────────────────────────────────────────

_PREWARM_COLLECTION = "algo_subscribe_index"
_PREWARM_DOC_ID     = "index_instruments"


def _get_available_instruments(db) -> list[str]:
    """
    Distinct instruments from active_option_tokens (broker=dhan).
    This is the source-of-truth list shown in the admin UI — no hardcoded list.
    """
    try:
        from features.market_feed_tokens import get_active_feed_broker  # type: ignore
        broker = get_active_feed_broker(db)
    except Exception:
        broker = "dhan"
    raw = db["active_option_tokens"].distinct("instrument", {"broker": broker})
    return sorted(str(i).strip().upper() for i in raw if str(i).strip())


def _get_prewarm_config(db) -> list[str]:
    """Read admin-saved instruments from algo_subscribe_index. Empty list = nothing prewarmed."""
    doc = db[_PREWARM_COLLECTION].find_one({"_id": _PREWARM_DOC_ID}) or {}
    instruments = doc.get("instruments")
    if isinstance(instruments, list):
        return [str(i).upper() for i in instruments if str(i).strip()]
    return []


@app.get("/ticker/prewarm-config")
async def get_prewarm_config() -> dict:
    """
    Return available instruments (from active_option_tokens) and current
    admin selection (from algo_subscribe_index). No hardcoded list anywhere.
    """
    db = MongoData()
    try:
        available, selected = await asyncio.gather(
            asyncio.to_thread(_get_available_instruments, db._db),
            asyncio.to_thread(_get_prewarm_config, db._db),
        )
        chain_status = {}
        try:
            chain_status = ticker_manager.get_chain_status() if hasattr(ticker_manager, "get_chain_status") else {}
        except Exception:
            pass
        return {
            "all_instruments": available,
            "selected":        selected,
            "chain_status":    chain_status,
        }
    finally:
        db.close()


@app.post("/ticker/prewarm-config")
async def save_prewarm_config(body: dict = Body(...)) -> dict:
    """
    Save which instruments should be prewarmed at next server startup.
    Validates against active_option_tokens (no hardcoded allowed-list).
    Also triggers an immediate warm so admin need not restart the server.
    """
    instruments: list[str] = [
        str(i).strip().upper()
        for i in (body.get("instruments") or [])
        if str(i).strip()
    ]
    if not instruments:
        return {"ok": False, "message": "No instruments provided."}

    db = MongoData()
    try:
        available = await asyncio.to_thread(_get_available_instruments, db._db)
        valid = [i for i in instruments if i in available]
        if not valid:
            return {"ok": False, "message": f"None of the provided instruments exist in active_option_tokens. Available: {available}"}

        await asyncio.to_thread(
            db._db[_PREWARM_COLLECTION].update_one,
            {"_id": _PREWARM_DOC_ID},
            {"$set": {"instruments": valid, "updated_at": __import__("datetime").datetime.utcnow().isoformat()}},
            True,  # upsert
        )
        threading.Thread(
            target=_warm_instruments_now,
            args=(db._db, valid),
            daemon=True,
            name="prewarm_config_apply",
        ).start()
        log.info("[prewarm-config] saved instruments=%s", valid)
        return {"ok": True, "saved": valid}
    finally:
        db.close()


def _warm_instruments_now(db, instruments: list[str]) -> None:
    """Immediately warm all expiries for the given instruments on the chain-feed pool
    (WS subscribe), then the REST quotes+depth+baseline cost too (see
    prewarm_chain_rest) — so saving config in the admin page warms both right
    away instead of only the WS half, with the REST half waiting for next
    restart's _auto_prewarm_chain_rest."""
    try:
        from features.dhan_ticker import dhan_ticker_manager  # type: ignore
        dhan_ticker_manager._prewarm_index_chains(db, instruments)
    except Exception as exc:
        log.warning("[prewarm-config] immediate warm error: %s", exc)
    try:
        prewarm_chain_rest(instruments)
    except Exception as exc:
        log.warning("[prewarm-config] immediate REST prewarm error: %s", exc)


# ── Health ────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health() -> dict:
    return {
        "status":           "ok",
        "ticker_status":    ticker_manager.status,
        "internal_clients": _internal_hub.get_status()["connected_services"],
    }


