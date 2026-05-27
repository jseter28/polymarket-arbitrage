"""
Phase 2 probe — Kalshi WS instrument-per-conn cap.

Opens ONE authenticated WS connection and progressively grows its
subscription via `update_subscription` `add_markets` through a series
of step sizes (100, 500, 1k, 2.5k, 5k, 10k). At each step, holds the
subscription for a fixed duration and measures throughput, snapshot
arrival, delta rate, sequence gaps, and any errors / disconnects.

Kalshi docs do NOT publish a per-connection instrument limit and
explicitly recommend multiplexing many markets onto one connection
via `update_subscription`. This probe verifies that under load.

Output: probe_kalshi_instruments.json with per-step metrics.

Usage:
    python probe_kalshi_instruments.py
    python probe_kalshi_instruments.py --hold 60 --steps 100,500,1000,2500,5000
    python probe_kalshi_instruments.py --base-ws wss://api.elections.kalshi.com/trade-api/ws/v2
"""

import argparse
import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from kalshi_client.auth import build_headers, load_private_key
from utils.config_loader import load_config

DEFAULT_REST = "https://demo-api.kalshi.co/trade-api/v2"
DEFAULT_WS = "wss://demo-api.kalshi.co/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def fetch_tickers(base_rest: str, n: int) -> list[str]:
    """Page through demo /markets until we have n active tickers."""
    tickers: list[str] = []
    cursor = ""
    page = 1000
    async with httpx.AsyncClient(timeout=30.0) as client:
        while len(tickers) < n:
            params: dict = {"status": "open", "limit": page}
            if cursor:
                params["cursor"] = cursor
            r = await client.get(
                f"{base_rest}/markets",
                params=params,
                headers={"Accept": "application/json"},
            )
            r.raise_for_status()
            d = r.json()
            mlist = d.get("markets", [])
            tickers.extend(m["ticker"] for m in mlist if m.get("ticker"))
            cursor = d.get("cursor", "")
            if not cursor or not mlist:
                break
    return tickers[:n]


class Counter:
    """Per-step rolling counters; reset between steps."""

    def __init__(self) -> None:
        self.msgs = 0
        self.snapshots = 0
        self.deltas = 0
        self.subscribed_acks = 0
        self.subscription_updates = 0
        self.errors: list[dict] = []
        self.last_seq_by_sid: dict[int, int] = {}
        self.seq_gaps: list[dict] = []
        self.seq_seen: int = 0
        self.bytes_in: int = 0
        self.started_at: float = 0.0

    def reset(self) -> None:
        self.msgs = 0
        self.snapshots = 0
        self.deltas = 0
        self.subscribed_acks = 0
        self.subscription_updates = 0
        self.errors = []
        self.seq_gaps = []
        self.seq_seen = 0
        self.bytes_in = 0
        self.started_at = time.monotonic()

    def observe(self, raw: str) -> None:
        self.msgs += 1
        self.bytes_in += len(raw)
        try:
            msg = json.loads(raw)
        except Exception:
            return
        mtype = msg.get("type")
        if mtype == "subscribed":
            self.subscribed_acks += 1
            return
        if mtype == "subscription_updated":
            self.subscription_updates += 1
            return
        if mtype == "error":
            self.errors.append(msg)
            return
        if mtype == "orderbook_snapshot":
            self.snapshots += 1
        elif mtype == "orderbook_delta":
            self.deltas += 1
        sid = msg.get("sid")
        seq = msg.get("seq")
        if sid is None or seq is None:
            return
        self.seq_seen += 1
        prev = self.last_seq_by_sid.get(sid)
        if prev is not None and seq != prev + 1:
            self.seq_gaps.append({"sid": sid, "prev": prev, "got": seq, "type": mtype})
        self.last_seq_by_sid[sid] = seq

    def snapshot(self, step_size: int, hold_s: float) -> dict:
        elapsed = max(0.001, time.monotonic() - self.started_at)
        return {
            "step_size": step_size,
            "hold_seconds": hold_s,
            "elapsed_s": round(elapsed, 2),
            "msgs_total": self.msgs,
            "msgs_per_sec": round(self.msgs / elapsed, 1),
            "snapshots": self.snapshots,
            "deltas": self.deltas,
            "subscribed_acks": self.subscribed_acks,
            "subscription_updates": self.subscription_updates,
            "bytes_in": self.bytes_in,
            "kbytes_per_sec": round(self.bytes_in / 1024.0 / elapsed, 1),
            "seq_records_seen": self.seq_seen,
            "seq_gap_count": len(self.seq_gaps),
            "seq_gap_rate_pct": round(100.0 * len(self.seq_gaps) / max(1, self.seq_seen), 3),
            "seq_gaps_sample": self.seq_gaps[:5],
            "errors": self.errors[:5],
            "error_count": len(self.errors),
        }


async def hold(ws, counter: Counter, hold_s: float) -> bool:
    """Drain frames for hold_s seconds. Returns False on disconnect."""
    deadline = time.monotonic() + hold_s
    while time.monotonic() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            return True
        except ConnectionClosed:
            return False
        counter.observe(raw)
    return True


async def run(args: argparse.Namespace) -> None:
    steps: list[int] = [int(s) for s in args.steps.split(",")]
    max_tickers = max(steps)

    cfg = load_config(args.config)
    private_key = load_private_key(cfg.api.kalshi_private_key)
    api_key_id = cfg.api.kalshi_api_key

    print(f"[{utcnow()}] Probing Kalshi instrument cap")
    print(f"  WS:    {args.base_ws}")
    print(f"  Steps: {steps}  (hold {args.hold}s each)")

    print(f"  Fetching {max_tickers} tickers from demo REST…")
    tickers = await fetch_tickers(args.base_rest, max_tickers)
    print(f"  Got {len(tickers)} tickers")
    if len(tickers) < max_tickers:
        print(f"  ⚠ Requested {max_tickers}, demo only returned {len(tickers)}. Adjusting steps.")
        steps = [s for s in steps if s <= len(tickers)] + [len(tickers)]
        steps = sorted(set(steps))

    headers = build_headers(api_key_id, private_key, "GET", WS_PATH)
    results: list[dict] = []

    async with websockets.connect(
        args.base_ws,
        extra_headers=headers,
        open_timeout=20,
        ping_interval=20,
        max_size=16 * 1024 * 1024,
    ) as ws:
        counter = Counter()
        currently_subscribed = 0
        sid: int | None = None

        for step_idx, step_size in enumerate(steps, start=1):
            to_add = tickers[currently_subscribed:step_size]
            print(f"\n→ step {step_idx}: subscribed_size={step_size} (adding {len(to_add)} markets)")
            counter.reset()

            if currently_subscribed == 0:
                sub_id = 1
                msg = {
                    "id": sub_id,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta"],
                        "market_tickers": to_add,
                    },
                }
                await ws.send(json.dumps(msg))
                # First message we expect is the "subscribed" ack with sid.
                # Drain a tiny bit to grab it.
                try:
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10.0)
                        counter.observe(raw)
                        m = json.loads(raw)
                        if m.get("type") == "subscribed":
                            # sid is nested under msg in the subscribed ack,
                            # but at the top level on subsequent data messages.
                            sid = m.get("msg", {}).get("sid")
                            print(f"   subscribed sid={sid}")
                            break
                except asyncio.TimeoutError:
                    print("   ⚠ no subscribed ack within 10s")
            else:
                # Add markets in batches of 500 to keep the message manageable.
                batch_size = 500
                for batch_start in range(0, len(to_add), batch_size):
                    batch = to_add[batch_start : batch_start + batch_size]
                    msg = {
                        "id": 100 + step_idx * 10 + batch_start // batch_size,
                        "cmd": "update_subscription",
                        "params": {
                            "sids": [sid],
                            "market_tickers": batch,
                            "action": "add_markets",
                        },
                    }
                    await ws.send(json.dumps(msg))

            still_open = await hold(ws, counter, args.hold)
            snap = counter.snapshot(step_size, args.hold)
            print(
                f"   msgs={snap['msgs_total']} ({snap['msgs_per_sec']}/s, {snap['kbytes_per_sec']} KB/s)  "
                f"snaps={snap['snapshots']}  deltas={snap['deltas']}  "
                f"seq_gaps={snap['seq_gap_count']}  errors={snap['error_count']}"
            )
            results.append(snap)
            currently_subscribed = step_size

            if not still_open:
                print("   ⚠ connection closed mid-step; stopping")
                results[-1]["disconnect"] = True
                break

            # Stop on material degradation.
            if snap["seq_gap_rate_pct"] >= 1.0 or snap["error_count"] > 0:
                print("   ⚠ degradation threshold hit; stopping")
                break

    out = {
        "tested_at": utcnow(),
        "host": args.base_ws,
        "rest_host": args.base_rest,
        "hold_seconds": args.hold,
        "results": results,
    }
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\n[{utcnow()}] Wrote {args.output}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 2 Kalshi WS instrument cap probe")
    p.add_argument("-c", "--config", default="config.live.yaml")
    p.add_argument("--base-ws", default=DEFAULT_WS)
    p.add_argument("--base-rest", default=DEFAULT_REST)
    p.add_argument("--steps", default="100,500,1000,2500,5000,10000")
    p.add_argument("--hold", type=float, default=30.0, help="Hold each step this long (s)")
    p.add_argument("-o", "--output", default="probe_kalshi_instruments.json")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
