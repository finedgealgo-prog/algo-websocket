#!/usr/bin/env bash
# Run N local algo.websocket gateway instances for load-testing the
# horizontal-scale-out design before touching production.
#
# Usage:
#   ./start_gateway.sh          # 3 gateway instances on 8013, 8023, 8033
#   ./start_gateway.sh 5        # 5 instances
#   ./start_gateway.sh 3 8003   # 3 instances, primary hub at a non-default port
#
# Prerequisite: the primary instance (the one WITHOUT WS_GATEWAY_ONLY, owns
# the real broker connection) must already be running — see this repo's
# start_all.sh, or `uvicorn ws_main:app --reload --port 8003` directly.
# Each spawned instance here is gateway-only (WS_GATEWAY_ONLY=true): it
# never opens its own broker connection, only mirrors the primary's tick
# stream via CentralTickClient (see ws_main.py's _auto_start_ticker).

set -euo pipefail

COUNT="${1:-3}"
PRIMARY_PORT="${2:-8003}"
BASE_PORT=8010

cd "$(dirname "$0")/.."  # algo.websocket/

echo "Primary hub expected at http://127.0.0.1:${PRIMARY_PORT} (not started by this script)."
echo "Starting ${COUNT} gateway-only instance(s)..."

PID_FILE="/tmp/algo-websocket-gateways.pids"
: > "$PID_FILE"

for i in $(seq 1 "$COUNT"); do
    PORT=$((BASE_PORT + i * 10 + 3))
    echo "  gateway #$i -> port $PORT"
    WS_GATEWAY_ONLY=true \
    WS_PRIMARY_HUB_URL="http://127.0.0.1:${PRIMARY_PORT}" \
        uvicorn ws_main:app --port "$PORT" \
        > "/tmp/algo-websocket-gateway-${PORT}.log" 2>&1 &
    echo $! >> "$PID_FILE"
    disown
done

echo "Done. Logs at /tmp/algo-websocket-gateway-<port>.log — PIDs recorded in $PID_FILE"
echo "Stop all: xargs kill < $PID_FILE"
