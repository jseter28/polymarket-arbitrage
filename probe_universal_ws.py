"""
Universal WS production-module probe.

One-shot exerciser for `polymarket_client.universal_ws.PolymarketUniversalWS`.
Mirrors the structure of `probe_ws_sharded.py` but tests the production module,
not the in-process probe.

Usage:
    python3 probe_universal_ws.py --duration 900 --output probe_universal_15min.json
    python3 probe_universal_ws.py --duration 180 --output probe_universal_smoke.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from polymarket_client.universal_ws import PolymarketUniversalWS


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def status_logger(ws: PolymarketUniversalWS, period_s: float, stop: asyncio.Event) -> None:
    """Print pool status every period_s seconds."""
    try:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=period_s)
                return
            except asyncio.TimeoutError:
                pass
            st = ws.status()
            shards_connected = sum(1 for s in st["shards"] if s["state"] == "connected")
            shards_first = sum(1 for s in st["shards"] if s["first_msg_received"])
            shards_quarantined = sum(1 for s in st["shards"] if s["state"] == "quarantined")
            total_msgs = sum(s["message_count"] for s in st["shards"])
            print(
                f"[{utcnow()}] status: uptime={st['uptime_s']}s "
                f"shards_total={st['shard_count']} connected={shards_connected} "
                f"first_msg={shards_first} quarantined={shards_quarantined} "
                f"markets={st['market_count']} msgs={total_msgs} "
                f"queue={st['queue_depth']}/{st['queue_maxsize']} drops={st['drops']}",
                flush=True,
            )
    except asyncio.CancelledError:
        raise


async def update_consumer(ws: PolymarketUniversalWS, stop: asyncio.Event, counter: list[int]) -> None:
    """Drain iter_updates() into a counter so we can confirm consumer-visible yield rate."""
    try:
        async for _mid, _book in ws.iter_updates():
            counter[0] += 1
            if stop.is_set():
                return
    except asyncio.CancelledError:
        raise


async def main() -> None:
    parser = argparse.ArgumentParser(description="Universal WS production-module probe")
    parser.add_argument("--duration", type=int, required=True, help="Probe duration in seconds")
    parser.add_argument("--output", type=str, required=True, help="Output JSON path")
    parser.add_argument("--shard-size", type=int, default=100, help="Markets per shard")
    parser.add_argument("--max-markets", type=int, default=5000, help="Universe cap")
    parser.add_argument("--status-period", type=float, default=30.0, help="Status print period seconds")
    args = parser.parse_args()

    # Bot-compatible log style: timestamp | LEVEL | logger | message
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(f"[{utcnow()}] starting PolymarketUniversalWS: max_markets={args.max_markets} "
          f"shard_size={args.shard_size} duration={args.duration}s")

    ws = PolymarketUniversalWS(
        shard_size=args.shard_size,
        max_markets=args.max_markets,
    )

    consumer_counter = [0]
    stop = asyncio.Event()
    status_task: asyncio.Task | None = None
    consumer_task: asyncio.Task | None = None
    start_mono = time.monotonic()
    start_wall = utcnow()
    err: str | None = None

    try:
        await ws.start(market_ids=None)
        status_task = asyncio.create_task(status_logger(ws, args.status_period, stop))
        consumer_task = asyncio.create_task(update_consumer(ws, stop, consumer_counter))

        try:
            await asyncio.wait_for(stop.wait(), timeout=args.duration)
        except asyncio.TimeoutError:
            pass
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        print(f"[{utcnow()}] FATAL during probe: {err}", flush=True)
    finally:
        stop.set()
        if status_task and not status_task.done():
            status_task.cancel()
            try:
                await status_task
            except (asyncio.CancelledError, Exception):
                pass
        if consumer_task and not consumer_task.done():
            consumer_task.cancel()
            try:
                await consumer_task
            except (asyncio.CancelledError, Exception):
                pass
        await ws.stop()

    elapsed = time.monotonic() - start_mono
    end_status = ws.status()
    total_msgs = sum(s["message_count"] for s in end_status["shards"])
    total_bytes = sum(s["bytes_received"] for s in end_status["shards"])
    total_sessions = sum(s["session_count"] for s in end_status["shards"])
    shards_with_first_msg = sum(1 for s in end_status["shards"] if s["first_msg_received"])
    shards_quarantined = sum(1 for s in end_status["shards"] if s["state"] == "quarantined")
    # An "unrecovered" shard never reached its first message — connect failures, network down, etc.
    shards_unrecovered = sum(
        1 for s in end_status["shards"] if not s["first_msg_received"]
    )
    shards_one_session_only = sum(
        1 for s in end_status["shards"] if s["session_count"] <= 1
    )

    summary = {
        "probe": {
            "type": "universal_ws",
            "duration_target_s": args.duration,
            "duration_actual_s": round(elapsed, 2),
            "start_wall_utc": start_wall,
            "end_wall_utc": utcnow(),
            "shard_size": args.shard_size,
            "max_markets": args.max_markets,
            "fatal_error": err,
        },
        "aggregate": {
            "shard_count": end_status["shard_count"],
            "market_count": end_status["market_count"],
            "token_count": end_status["token_count"],
            "total_messages": total_msgs,
            "total_bytes": total_bytes,
            "msg_per_sec": round(total_msgs / elapsed, 1) if elapsed else 0,
            "total_sessions": total_sessions,
            "shards_with_first_msg": shards_with_first_msg,
            "shards_unrecovered": shards_unrecovered,
            "shards_quarantined": shards_quarantined,
            "shards_one_session_only": shards_one_session_only,
            "drops": end_status["drops"],
            "iter_updates_yielded": consumer_counter[0],
            "yield_per_sec": round(consumer_counter[0] / elapsed, 1) if elapsed else 0,
            "queue_depth_final": end_status["queue_depth"],
            "queue_maxsize": end_status["queue_maxsize"],
        },
        "shards": end_status["shards"],
    }

    with open(args.output, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"[{utcnow()}] wrote {args.output}")
    print(json.dumps({
        "shards": summary["aggregate"]["shard_count"],
        "markets": summary["aggregate"]["market_count"],
        "duration_s": summary["probe"]["duration_actual_s"],
        "total_messages": summary["aggregate"]["total_messages"],
        "msg_per_sec": summary["aggregate"]["msg_per_sec"],
        "iter_updates_yielded": summary["aggregate"]["iter_updates_yielded"],
        "shards_with_first_msg": summary["aggregate"]["shards_with_first_msg"],
        "shards_unrecovered": summary["aggregate"]["shards_unrecovered"],
        "drops": summary["aggregate"]["drops"],
        "fatal_error": summary["probe"]["fatal_error"],
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
