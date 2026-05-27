"""
Phase 1 probe — Kalshi WS connection cap.

Empirically determines the maximum number of concurrent authenticated
WebSocket connections per API key on the Kalshi demo host. The Kalshi
docs do not publish a number; community reports cite ~5 concurrent
connections per user. This probe confirms (or refutes) that.

For each N in 1..MAX, opens N concurrent authenticated WS connections,
each subscribing to orderbook_delta on a small ticker set, holds the
connections open for a fixed duration, and counts: handshakes accepted,
handshakes rejected (and why), premature closes, total messages.

Output: probe_kalshi_connections.json with per-N results.
Defaults to demo — Phase 0 found the available credentials are demo-only.

Usage:
    python probe_kalshi_connections.py
    python probe_kalshi_connections.py --max-n 10 --hold 30 --tickers 3
    python probe_kalshi_connections.py --base-ws wss://api.elections.kalshi.com/trade-api/ws/v2
"""

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone

import httpx
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

from kalshi_client.auth import build_headers, load_private_key
from utils.config_loader import load_config

DEFAULT_REST = "https://demo-api.kalshi.co/trade-api/v2"
DEFAULT_WS = "wss://demo-api.kalshi.co/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def fetch_tickers(base_rest: str, n: int) -> list[str]:
    """Fetch n active market tickers from the demo REST endpoint (public)."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"{base_rest}/markets",
            params={"status": "open", "limit": n},
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        data = r.json()
    markets = data.get("markets", [])
    tickers = [m["ticker"] for m in markets if m.get("ticker")]
    if not tickers:
        raise RuntimeError("No active markets returned from demo REST")
    return tickers[:n]


async def run_one_connection(
    conn_id: int,
    ws_url: str,
    api_key_id: str,
    private_key,
    tickers: list[str],
    hold_seconds: float,
) -> dict:
    """Open one WS connection, subscribe, hold, count messages. Returns result dict."""
    started = time.monotonic()
    result = {
        "conn_id": conn_id,
        "handshake": "pending",
        "subscribe_ack": False,
        "messages": 0,
        "snapshot_count": 0,
        "delta_count": 0,
        "error": None,
        "close_code": None,
        "elapsed_s": 0.0,
    }
    try:
        headers = build_headers(api_key_id, private_key, "GET", WS_PATH)
        async with websockets.connect(
            ws_url, extra_headers=headers, open_timeout=15, ping_interval=20
        ) as ws:
            result["handshake"] = "ok"
            sub_msg = {
                "id": conn_id,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": tickers,
                },
            }
            await ws.send(json.dumps(sub_msg))

            deadline = time.monotonic() + hold_seconds
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=max(0.1, deadline - time.monotonic())
                    )
                except asyncio.TimeoutError:
                    break
                except ConnectionClosed as e:
                    result["close_code"] = e.code
                    result["error"] = f"closed: {e.reason}"
                    break
                result["messages"] += 1
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                mtype = msg.get("type")
                if mtype == "subscribed":
                    result["subscribe_ack"] = True
                elif mtype == "orderbook_snapshot":
                    result["snapshot_count"] += 1
                elif mtype == "orderbook_delta":
                    result["delta_count"] += 1
                elif mtype == "error":
                    result["error"] = json.dumps(msg)
    except InvalidStatusCode as e:
        result["handshake"] = "rejected"
        result["error"] = f"HTTP {e.status_code}"
    except Exception as e:
        result["handshake"] = result["handshake"] if result["handshake"] != "pending" else "error"
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    result["elapsed_s"] = round(time.monotonic() - started, 2)
    return result


async def probe_n_concurrent(
    n: int,
    ws_url: str,
    api_key_id: str,
    private_key,
    tickers: list[str],
    hold_seconds: float,
) -> dict:
    """Open N concurrent connections, wait for all to finish, return aggregate."""
    print(f"\n→ N={n}: opening {n} concurrent connections, holding {hold_seconds}s")
    tasks = [
        asyncio.create_task(
            run_one_connection(i, ws_url, api_key_id, private_key, tickers, hold_seconds)
        )
        for i in range(1, n + 1)
    ]
    conns = await asyncio.gather(*tasks)

    handshakes_ok = sum(1 for c in conns if c["handshake"] == "ok")
    rejected = sum(1 for c in conns if c["handshake"] == "rejected")
    errors = sum(1 for c in conns if c["handshake"] not in ("ok", "rejected"))
    total_msgs = sum(c["messages"] for c in conns)
    print(
        f"   handshakes_ok={handshakes_ok}  rejected={rejected}  errors={errors}  total_msgs={total_msgs}"
    )
    for c in conns:
        if c["handshake"] != "ok" or c["error"]:
            print(f"     conn#{c['conn_id']}: {c['handshake']}  err={c['error']}")

    return {
        "n": n,
        "handshakes_ok": handshakes_ok,
        "rejected": rejected,
        "errors": errors,
        "total_messages": total_msgs,
        "connections": conns,
    }


async def main(args) -> None:
    cfg = load_config(args.config)
    if not cfg.api.kalshi_api_key or not cfg.api.kalshi_private_key:
        raise SystemExit("Kalshi credentials missing in config")
    private_key = load_private_key(cfg.api.kalshi_private_key)
    api_key_id = cfg.api.kalshi_api_key

    print(f"[{utcnow()}] Probing Kalshi connection cap")
    print(f"  WS host: {args.base_ws}")
    print(f"  Key ID:  {api_key_id[:8]}…")

    tickers = await fetch_tickers(args.base_rest, args.tickers)
    print(f"  Tickers: {tickers}")

    results: list[dict] = []
    for n in range(1, args.max_n + 1):
        try:
            r = await probe_n_concurrent(
                n, args.base_ws, api_key_id, private_key, tickers, args.hold
            )
        except Exception as e:
            print(f"   ERROR at N={n}: {e}")
            r = {"n": n, "error": str(e)}
        results.append(r)
        await asyncio.sleep(args.cooldown)

    out = {
        "tested_at": utcnow(),
        "host": args.base_ws,
        "rest_host": args.base_rest,
        "hold_seconds": args.hold,
        "cooldown_seconds": args.cooldown,
        "tickers": tickers,
        "results": results,
    }
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\n[{utcnow()}] Wrote {args.output}")

    print("\nSummary (cap = first N where rejected > 0 or handshakes_ok < N):")
    cap = None
    for r in results:
        if "error" in r and "n" in r and len(r) == 2:
            continue
        if r.get("rejected", 0) > 0 or r.get("handshakes_ok", 0) < r["n"]:
            cap = r["n"] - 1
            break
    if cap is None:
        print(f"  No cap observed up to N={args.max_n}. Try a higher --max-n.")
    else:
        print(f"  Observed concurrent connection cap: {cap}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 1 Kalshi WS connection cap probe")
    p.add_argument("-c", "--config", default="config.live.yaml")
    p.add_argument("--base-ws", default=DEFAULT_WS, help="WS URL to probe (default: demo)")
    p.add_argument("--base-rest", default=DEFAULT_REST, help="REST URL for ticker fetch")
    p.add_argument("--max-n", type=int, default=8, help="Maximum concurrent conns to try")
    p.add_argument("--hold", type=float, default=20.0, help="Hold each iteration this long (s)")
    p.add_argument("--cooldown", type=float, default=3.0, help="Sleep between iterations (s)")
    p.add_argument("--tickers", type=int, default=3, help="Markets to subscribe per conn")
    p.add_argument("-o", "--output", default="probe_kalshi_connections.json")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
