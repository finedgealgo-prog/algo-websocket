"""
check_raw_dhan_chain.py
────────────────────────
Standalone debug script — bypasses this app's code completely and hits
Dhan's own /v2/marketfeed/quote REST API directly for every strike of one
(underlying, expiry), so you can see exactly what the broker itself is
returning (last_price, volume, oi, previous close, bid/ask) before blaming
our chain-fetch code.

Usage:
    python3 check_raw_dhan_chain.py SENSEX 2026-09-03
    python3 check_raw_dhan_chain.py NIFTY 2026-09-01 --full-json

Reads the Dhan access token straight from Mongo (kite_market_config), same
as the live app — no separate login needed. Retries automatically on 429
(Dhan's shared 1 req/sec rate gate).
"""

import argparse
import json
import time

import pymongo
import requests

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "stock_data"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("underlying", help="e.g. SENSEX, NIFTY, BANKNIFTY, BANKEX, FINNIFTY, MIDCPNIFTY")
    parser.add_argument("expiry", help="YYYY-MM-DD")
    parser.add_argument("--full-json", action="store_true", help="Also dump full raw JSON for every non-null row")
    args = parser.parse_args()

    underlying = args.underlying.strip().upper()
    expiry = args.expiry.strip()[:10]

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    cfg = db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    token = cfg.get("access_token")
    client_id = cfg.get("user_id")
    if not token or not client_id:
        raise SystemExit("No enabled Dhan credentials found in kite_market_config — login first.")

    col = db["active_option_tokens"]
    docs = list(col.find(
        {"broker": "dhan", "instrument": underlying, "expiry": {"$regex": f"^{expiry}"}},
        {"_id": 0, "token": 1, "strike": 1, "option_type": 1, "symbol": 1, "ws_segment": 1},
    ))
    if not docs:
        raise SystemExit(f"No active_option_tokens found for {underlying} expiry={expiry}")

    docs.sort(key=lambda d: (float(d["strike"]), d["option_type"]))
    by_id = {int(d["token"]): d for d in docs}
    tok_ids = list(by_id.keys())
    segment = docs[0].get("ws_segment") or ("BSE_FNO" if underlying in ("SENSEX", "BANKEX") else "NSE_FNO")
    print(f"{underlying} {expiry}: {len(tok_ids)} contracts, segment={segment}")

    all_data: dict = {}
    for i in range(0, len(tok_ids), 500):
        batch = tok_ids[i:i + 500]
        for attempt in range(10):
            resp = requests.post(
                "https://api.dhan.co/v2/marketfeed/quote",
                headers={"access-token": token, "client-id": client_id, "Content-Type": "application/json"},
                json={segment: batch}, timeout=15,
            )
            if resp.status_code == 200:
                all_data.update(resp.json().get("data", {}).get(segment, {}))
                break
            print(f"  batch {i}: HTTP {resp.status_code} — retrying in 3s...")
            time.sleep(3)
        else:
            print(f"  batch {i}: FAILED after 10 retries")

    print(f"\nGot raw Dhan quotes for {len(all_data)}/{len(tok_ids)} tokens\n")
    print("=" * 100)
    print(f"{'STRIKE':>8} {'TYPE':>4} {'TOKEN':>8}  {'LTP':>10} {'VOL':>8} {'OI':>8} {'CLOSE':>10} {'BID':>10} {'ASK':>10}")
    print("=" * 100)

    for tok_id in tok_ids:
        d = by_id[tok_id]
        v = all_data.get(str(tok_id))
        if v is None:
            print(f"{d['strike']:>8.1f} {d['option_type']:>4} {tok_id:>8}  <no data returned>")
            continue
        depth = v.get("depth") or {}
        buy = (depth.get("buy") or [{}])[0]
        sell = (depth.get("sell") or [{}])[0]
        print(f"{d['strike']:>8.1f} {d['option_type']:>4} {tok_id:>8}  "
              f"{v.get('last_price', 0):>10} {v.get('volume', 0):>8} {v.get('oi', 0):>8} "
              f"{(v.get('ohlc') or {}).get('close', 0):>10} {buy.get('price', 0):>10} {sell.get('price', 0):>10}")

    if args.full_json:
        print("\n\n===== FULL RAW JSON (every non-null entry) =====")
        for tok_id in tok_ids:
            v = all_data.get(str(tok_id))
            if v is None:
                continue
            d = by_id[tok_id]
            print(f"\n--- token={tok_id} strike={d['strike']} type={d['option_type']} symbol={d.get('symbol')} ---")
            print(json.dumps(v, indent=2))


if __name__ == "__main__":
    main()
