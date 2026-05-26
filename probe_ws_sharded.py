"""
Sharded WS stability probe.

Opens N parallel WebSocket connections to Polymarket, each subscribing to a
disjoint slice of token assets (max ~200 tokens per shard, well under the
~500-instrument single-connection cap reported by NautilusTrader and the
agentbets.ai guide).

Used to validate that the production architectural answer — "shard the
subscription pool across many connections" — actually works in practice.

Usage:
    python probe_ws_sharded.py --markets 300 --shards 3 --duration 900 --output probe_sharded_3x100.json
    python probe_ws_sharded.py --markets 1000 --shards 10 --duration 900 --output probe_sharded_10x100.json
"""

import argparse
import asyncio
import json
import statistics
import time
from datetime import datetime, timezone

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

GAMMA_URL = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

STALE_TIMEOUT_S = 45.0  # if no msg for this long, treat shard as dead


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def fetch_top_markets(n: int) -> list[dict]:
    markets: list[dict] = []
    offset = 0
    page = 100
    async with httpx.AsyncClient(timeout=30.0) as client:
        while len(markets) < n:
            params = {
                "closed": "false",
                "active": "true",
                "order": "volume24hr",
                "ascending": "false",
                "limit": page,
                "offset": offset,
            }
            r = await client.get(f"{GAMMA_URL}/markets", params=params)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            for m in data:
                clob_ids_raw = m.get("clobTokenIds") or ""
                if not clob_ids_raw:
                    continue
                try:
                    ids = json.loads(clob_ids_raw)
                except Exception:
                    continue
                if len(ids) < 2 or not ids[0] or not ids[1]:
                    continue
                markets.append({
                    "id": str(m.get("id", "")),
                    "yes": str(ids[0]).strip(),
                    "no": str(ids[1]).strip(),
                    "vol24h": float(m.get("volume24hr") or 0),
                })
                if len(markets) >= n:
                    break
            if len(data) < page:
                break
            offset += page
            await asyncio.sleep(0.15)
    return markets[:n]


class ShardStats:
    def __init__(self, shard_id: int, tokens: int) -> None:
        self.shard_id = shard_id
        self.tokens = tokens
        self.connect_attempts = 0
        self.connect_successes = 0
        self.session_uptimes: list[float] = []
        self.session_reasons: list[str] = []
        self.open_failures: list[str] = []
        self.messages: int = 0
        self.bytes: int = 0
        self.first_msg_latency_ms: list[float] = []

    def to_dict(self) -> dict:
        return {
            "shard_id": self.shard_id,
            "tokens": self.tokens,
            "connect_attempts": self.connect_attempts,
            "connect_successes": self.connect_successes,
            "sessions_ended": len(self.session_uptimes),
            "open_failures": len(self.open_failures),
            "uptimes": [round(u, 1) for u in self.session_uptimes],
            "reasons": self.session_reasons,
            "open_failure_reasons": self.open_failures,
            "mean_uptime_s": round(statistics.mean(self.session_uptimes), 1) if self.session_uptimes else 0,
            "median_uptime_s": round(statistics.median(self.session_uptimes), 1) if self.session_uptimes else 0,
            "min_uptime_s": round(min(self.session_uptimes), 1) if self.session_uptimes else 0,
            "max_uptime_s": round(max(self.session_uptimes), 1) if self.session_uptimes else 0,
            "messages": self.messages,
            "bytes": self.bytes,
            "first_msg_latency_ms": [round(x, 1) for x in self.first_msg_latency_ms],
        }


async def heartbeat_task(ws, stop: asyncio.Event) -> None:
    try:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=10.0)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await ws.send("PING")
            except Exception:
                return
    except asyncio.CancelledError:
        raise


async def shard_recv(ws, stats: ShardStats, connect_mono: float, last_msg_box: list[float]) -> None:
    first_logged = False
    async for raw in ws:
        if not first_logged:
            stats.first_msg_latency_ms.append((time.monotonic() - connect_mono) * 1000.0)
            first_logged = True
        last_msg_box[0] = time.monotonic()
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        stats.messages += 1
        stats.bytes += len(raw)


async def stale_watchdog(last_msg_box: list[float], ws, stop: asyncio.Event) -> str | None:
    try:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=5.0)
                return None
            except asyncio.TimeoutError:
                pass
            silence = time.monotonic() - last_msg_box[0]
            if silence > STALE_TIMEOUT_S:
                try:
                    await ws.close(code=1000, reason="probe stale watchdog")
                except Exception:
                    pass
                return f"stale_watchdog: {silence:.1f}s of silence"
        return None
    except asyncio.CancelledError:
        raise


async def run_shard_session(assets_ids: list[str], deadline_mono: float, stats: ShardStats) -> None:
    stats.connect_attempts += 1
    connect_mono = time.monotonic()
    try:
        ws = await websockets.connect(
            WS_URL,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
            max_size=None,
            open_timeout=15,
        )
    except Exception as e:
        stats.open_failures.append(f"{type(e).__name__}: {e}")
        return

    stats.connect_successes += 1
    last_msg_box = [time.monotonic()]
    try:
        await ws.send(json.dumps({"assets_ids": assets_ids, "type": "market"}))

        stop = asyncio.Event()
        hb = asyncio.create_task(heartbeat_task(ws, stop))
        rx = asyncio.create_task(shard_recv(ws, stats, connect_mono, last_msg_box))
        wd = asyncio.create_task(stale_watchdog(last_msg_box, ws, stop))

        remaining = max(0.0, deadline_mono - time.monotonic())
        done, _pending = await asyncio.wait(
            {rx, wd},
            timeout=remaining,
            return_when=asyncio.FIRST_COMPLETED,
        )

        stop.set()
        for t in (hb, rx, wd):
            if not t.done():
                t.cancel()
        for t in (hb, rx, wd):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

        uptime = time.monotonic() - connect_mono

        if rx in done:
            exc = rx.exception()
            if exc is None:
                reason = "recv_clean_exit"
            elif isinstance(exc, ConnectionClosed):
                code = getattr(exc, "code", "?")
                reason = f"ConnectionClosed code={code}: {exc}"
            else:
                reason = f"recv_exception: {type(exc).__name__}: {exc}"
        elif wd in done:
            reason = (wd.result() or "stale_watchdog") if not wd.exception() else f"watchdog_err: {wd.exception()}"
        else:
            reason = "deadline_reached"

        stats.session_uptimes.append(uptime)
        stats.session_reasons.append(reason)
    finally:
        try:
            await ws.close()
        except Exception:
            pass


async def shard_supervisor(shard_id: int, assets_ids: list[str], deadline_mono: float) -> ShardStats:
    """Run one shard: connect, stream, reconnect on disconnect until deadline."""
    stats = ShardStats(shard_id=shard_id, tokens=len(assets_ids))
    session_n = 0
    while time.monotonic() < deadline_mono:
        session_n += 1
        print(f"[{utcnow()}] shard #{shard_id} session #{session_n} connecting "
              f"({len(assets_ids)} tokens)...", flush=True)
        await run_shard_session(assets_ids, deadline_mono, stats)
        last_up = stats.session_uptimes[-1] if stats.session_uptimes else None
        last_reason = (stats.session_reasons[-1] if stats.session_reasons else
                       (stats.open_failures[-1] if stats.open_failures else "?"))
        up_str = f"{last_up:.1f}s" if last_up is not None else "open_failed"
        print(f"[{utcnow()}] shard #{shard_id} session #{session_n} "
              f"ended after {up_str}: {last_reason}", flush=True)
        if time.monotonic() < deadline_mono:
            await asyncio.sleep(1.0)
    return stats


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--markets", type=int, required=True, help="Top N markets by 24h volume across all shards")
    parser.add_argument("--shards", type=int, required=True, help="Number of WS connections to spread markets across")
    parser.add_argument("--duration", type=int, required=True, help="Probe duration in seconds")
    parser.add_argument("--output", type=str, required=True, help="Output JSON path")
    args = parser.parse_args()

    if args.shards <= 0:
        raise SystemExit("--shards must be positive")

    print(f"[{utcnow()}] fetching top {args.markets} markets from Gamma...")
    markets = await fetch_top_markets(args.markets)
    print(f"[{utcnow()}] fetched {len(markets)} markets")

    # Split markets across shards (interleaved so each shard sees mixed volume profile)
    shard_assignments: list[list[dict]] = [[] for _ in range(args.shards)]
    for i, m in enumerate(markets):
        shard_assignments[i % args.shards].append(m)

    shard_assets: list[list[str]] = []
    for ms in shard_assignments:
        ids: list[str] = []
        for m in ms:
            ids.append(m["yes"])
            ids.append(m["no"])
        shard_assets.append(ids)

    for i, a in enumerate(shard_assets):
        print(f"  shard #{i}: {len(shard_assignments[i])} markets, {len(a)} tokens")

    start_mono = time.monotonic()
    deadline = start_mono + args.duration
    start_wall = utcnow()
    print(f"[{start_wall}] sharded probe begin: {args.shards} shards, "
          f"{len(markets)} markets total, target {args.duration}s")

    tasks = [
        asyncio.create_task(shard_supervisor(i, ids, deadline))
        for i, ids in enumerate(shard_assets)
    ]
    shard_stats: list[ShardStats] = await asyncio.gather(*tasks)

    elapsed = time.monotonic() - start_mono

    # Aggregate
    total_messages = sum(s.messages for s in shard_stats)
    total_bytes = sum(s.bytes for s in shard_stats)
    total_sessions = sum(len(s.session_uptimes) for s in shard_stats)
    total_open_failures = sum(len(s.open_failures) for s in shard_stats)
    all_uptimes = [u for s in shard_stats for u in s.session_uptimes]

    summary = {
        "probe": {
            "type": "sharded",
            "shards": args.shards,
            "markets_total": len(markets),
            "tokens_total": sum(s.tokens for s in shard_stats),
            "tokens_per_shard": [s.tokens for s in shard_stats],
            "target_duration_s": args.duration,
            "actual_duration_s": round(elapsed, 2),
            "start_wall_utc": start_wall,
            "end_wall_utc": utcnow(),
            "stale_timeout_s": STALE_TIMEOUT_S,
        },
        "aggregate": {
            "total_messages": total_messages,
            "total_bytes": total_bytes,
            "msg_per_sec": round(total_messages / elapsed, 1) if elapsed else 0,
            "total_sessions_ended": total_sessions,
            "total_open_failures": total_open_failures,
            "shards_that_never_reconnected": sum(1 for s in shard_stats if s.connect_successes <= 1 and s.session_reasons == ["deadline_reached"]),
            "uptime_mean_s": round(statistics.mean(all_uptimes), 1) if all_uptimes else 0,
            "uptime_median_s": round(statistics.median(all_uptimes), 1) if all_uptimes else 0,
            "uptime_min_s": round(min(all_uptimes), 1) if all_uptimes else 0,
            "uptime_max_s": round(max(all_uptimes), 1) if all_uptimes else 0,
        },
        "shards": [s.to_dict() for s in shard_stats],
    }

    with open(args.output, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[{utcnow()}] wrote {args.output}")
    print(json.dumps({
        "shards": args.shards,
        "tokens_total": summary["probe"]["tokens_total"],
        "duration_s": round(elapsed, 1),
        "total_sessions": total_sessions,
        "msg_per_sec": summary["aggregate"]["msg_per_sec"],
        "mean_uptime_s": summary["aggregate"]["uptime_mean_s"],
        "shards_unbroken": summary["aggregate"]["shards_that_never_reconnected"],
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
