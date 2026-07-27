# Horizontal scale-out for algo.websocket

Phase 3 of `/home/ashok-innoppl/.claude/plans/crystalline-moseying-hoare.md`.
Everything here was tested locally (a real second instance served live NIFTY
ticks to its own browser WS connection with zero duplicate broker
connections) but **not applied to the production EC2 host** — that step
needs to be run there, or this session given access to do it.

## How it works

- **Primary instance** (port 8003 today): owns the one real Dhan/Kite WS
  connection, exactly as it already does. No change to how you run this one.
- **Gateway instances**: set `WS_GATEWAY_ONLY=true` and `WS_PRIMARY_HUB_URL`
  when starting `ws_main.py` — it then skips opening its own broker
  connection and instead becomes a `CentralTickClient` of the primary (the
  same mechanism `algo.trade`/`algo.simulator` already use over
  `/ws/internal-ticks`), mirroring the exact same live tick stream. Each
  gateway instance independently serves `/ws/live-quotes` browser
  connections from that mirrored state.
- No shared-state coordination needed between gateway instances beyond that
  — every instance sees identical ticks, so a browser landing on any one of
  them gets the same data. This is also why the reverse-proxy config needs
  no sticky sessions (see apache-reverse-proxy.conf).

## Files here

- `algo-websocket.service` — the primary instance's systemd unit. The three
  existing production unit files (`algo-trade`, `algo-simulator`,
  `algo-scanner`) already declare `After=algo-websocket.service`, so this
  fills a gap that was already expected but never added.
- `algo-websocket-gateway@.service` — systemd **template** unit for gateway
  instances. Enable one per port: `systemctl enable --now
  algo-websocket-gateway@8013.service` (the `%i` becomes `8013`, the port
  uvicorn binds to).
- `apache-reverse-proxy.conf` — WS-aware reverse proxy across the gateway
  pool, using the Apache modules already present but unenabled on this host
  (`mod_proxy_wstunnel`/`mod_proxy_balancer`) rather than introducing nginx.
- `start_gateway.sh` — local launcher for load-testing N gateway instances
  before touching production (`./start_gateway.sh 5`).

## To actually deploy this

1. Copy `algo-websocket.service` to `/etc/systemd/system/` on the prod host,
   `systemctl daemon-reload && systemctl enable --now algo-websocket`.
2. Copy `algo-websocket-gateway@.service` there too, enable as many
   `@<port>` instances as you want capacity for.
3. Enable the Apache modules and drop `apache-reverse-proxy.conf`'s content
   into that host's existing vhost for `websocket.finedgealgo.com` (keep the
   existing SSL directives — this file deliberately doesn't duplicate them).
4. Update each `BalancerMember` line as you add/remove gateway instances.

## What this doesn't solve yet

Getting to genuinely millions of concurrent users still needs real hosting
capacity — more machines, not just more processes on this one box. This
groundwork is what makes adding *that* capacity a config change (more
`algo-websocket-gateway@N` units, more `BalancerMember` lines, potentially
on other hosts pointed at the same `WS_PRIMARY_HUB_URL`) instead of a code
change.
