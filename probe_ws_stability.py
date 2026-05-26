"""
Standalone WS stability probe.

Measures Polymarket WebSocket disconnect rate as a function of the number of
subscribed token assets. Does NOT depend on the bot's polymarket_client module;
talks to Gamma + the public CLOB WS endpoint directly so the probe is an
isolated experiment.

Usage:
    python probe_ws_stability.py --markets 50  --duration 900 --output probe_top50.json
    python probe_ws_stability.py --markets 500 --duration 900 --output probe_top500.json
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

STALE_TIMEOUT_S = 45.0  # if no message received in this long, treat connection as dead


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def fetch_top_markets(n: int) -> list[dict]:
    """Top-N active markets by 24h volume, paginated from Gamma."""
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
                    "question": (m.get("question") or "")[:80],
                    "volume24h": float(m.get("volume24hr") or 0),
                    "yes": str(ids[0]).strip(),
                    "no": str(ids[1]).strip(),
                })
                if len(markets) >= n:
                    break
            if len(data) < page:
                break
            offset += page
            await asyncio.sleep(0.15)
    return markets[:n]


class ProbeStats:
    def __init__(self) -> None:
        self.connect_attempts = 0
        self.connect_successes = 0
        self.session_uptimes: list[float] = []  # seconds each successful connection lasted
        self.session_reasons: list[str] = []     # parallel to session_uptimes
        self.failed_opens: list[str] = []        # connect-failed reasons (didn't open at all)
        self.messages_by_type: dict[str, int] = {}
        self.total_bytes_received = 0
        self.first_message_after_connect_ms: list[float] = []

    def record_open_fail(self, reason: str) -> None:
        self.failed_opens.append(reason)

    def record_session_end(self, uptime_s: float, reason: str) -> None:
        self.session_uptimes.append(uptime_s)
        self.session_reasons.append(reason)

    def record_message(self, raw: str) -> None:
        self.total_bytes_received += len(raw)
        if raw.strip() == "PONG":
            self._bump("PONG")
            return
        try:
            data = json.loads(raw)
        except Exception:
            self._bump("_unparseable")
            return
        if isinstance(data, list):
            for item in data:
                et = item.get("event_type") if isinstance(item, dict) else None
                self._bump(et or "_no_type")
        elif isinstance(data, dict):
            self._bump(data.get("event_type") or "_no_type")
        else:
            self._bump("_unknown_shape")

    def _bump(self, key: str) -> None:
        self.messages_by_type[key] = self.messages_by_type.get(key, 0) + 1


async def heartbeat_task(ws, stop: asyncio.Event) -> None:
    """Send app-level PING every 10s until stop is set or send fails."""
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


async def recv_task(ws, stats: ProbeStats, connect_monotonic: float,
                    last_msg_box: list[float]) -> None:
    """Iterate messages; update last-message timestamp box."""
    first_logged = False
    async for raw in ws:
        if not first_logged:
            stats.first_message_after_connect_ms.append((time.monotonic() - connect_monotonic) * 1000.0)
            first_logged = True
        last_msg_box[0] = time.monotonic()
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        stats.record_message(raw)


async def stale_watchdog(last_msg_box: list[float], ws, stop: asyncio.Event) -> str | None:
    """Force-close the WS if no message received for STALE_TIMEOUT_S."""
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


async def run_session(assets_ids: list[str], deadline_mono: float, stats: ProbeStats) -> None:
    stats.connect_attempts += 1
    connect_mono = time.monotonic()
    try:
        ws = await websockets.connect(
            WS_URL,
            ping_interval=None,    # we use app-level PING
            ping_timeout=None,
            close_timeout=5,
            max_size=None,         # do not cap large messages
            open_timeout=15,
        )
    except Exception as e:
        stats.record_open_fail(f"{type(e).__name__}: {e}")
        return

    stats.connect_successes += 1
    last_msg_box = [time.monotonic()]
    try:
        sub = json.dumps({"assets_ids": assets_ids, "type": "market"})
        await ws.send(sub)

        stop = asyncio.Event()
        hb = asyncio.create_task(heartbeat_task(ws, stop))
        rx = asyncio.create_task(recv_task(ws, stats, connect_mono, last_msg_box))
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

        stats.record_session_end(uptime, reason)
    finally:
        try:
            await ws.close()
        except Exception:
            pass


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * q
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--markets", type=int, required=True, help="Top N markets by 24h volume")
    parser.add_argument("--duration", type=int, required=True, help="Probe duration in seconds")
    parser.add_argument("--output", type=str, required=True, help="Output JSON path")
    args = parser.parse_args()

    print(f"[{utcnow()}] fetching top {args.markets} markets from Gamma...")
    markets = await fetch_top_markets(args.markets)
    print(f"[{utcnow()}] fetched {len(markets)} markets (requested {args.markets})")

    assets_ids: list[str] = []
    for m in markets:
        assets_ids.append(m["yes"])
        assets_ids.append(m["no"])
    print(f"[{utcnow()}] subscribing to {len(assets_ids)} tokens ({len(markets)} markets x 2)")

    stats = ProbeStats()
    start_mono = time.monotonic()
    deadline = start_mono + args.duration
    start_wall = utcnow()

    print(f"[{start_wall}] probe begin: target {args.duration}s")
    session_n = 0
    while time.monotonic() < deadline:
        session_n += 1
        print(f"[{utcnow()}] session #{session_n} connecting...", flush=True)
        await run_session(assets_ids, deadline, stats)
        last_up = stats.session_uptimes[-1] if stats.session_uptimes else None
        last_reason = stats.session_reasons[-1] if stats.session_reasons else (
            stats.failed_opens[-1] if stats.failed_opens else "?"
        )
        up_str = f"{last_up:.1f}s" if last_up is not None else "open_failed"
        print(f"[{utcnow()}] session #{session_n} ended after {up_str}: {last_reason}", flush=True)
        if time.monotonic() < deadline:
            await asyncio.sleep(1.0)

    elapsed = time.monotonic() - start_mono
    ups = stats.session_uptimes

    summary = {
        "probe": {
            "tokens_subscribed": len(assets_ids),
            "markets_subscribed": len(markets),
            "target_duration_s": args.duration,
            "actual_duration_s": round(elapsed, 2),
            "start_wall_utc": start_wall,
            "end_wall_utc": utcnow(),
            "stale_timeout_s": STALE_TIMEOUT_S,
        },
        "connections": {
            "attempts": stats.connect_attempts,
            "successes": stats.connect_successes,
            "open_failures": len(stats.failed_opens),
            "sessions_ended": len(ups),
        },
        "uptime_seconds": {
            "count": len(ups),
            "mean": round(statistics.mean(ups), 2) if ups else 0,
            "median": round(statistics.median(ups), 2) if ups else 0,
            "p10": round(percentile(ups, 0.10), 2) if ups else 0,
            "p90": round(percentile(ups, 0.90), 2) if ups else 0,
            "min": round(min(ups), 2) if ups else 0,
            "max": round(max(ups), 2) if ups else 0,
            "all": [round(u, 2) for u in ups],
        },
        "session_reasons": stats.session_reasons,
        "open_failure_reasons": stats.failed_opens,
        "messages_by_type": stats.messages_by_type,
        "total_bytes_received": stats.total_bytes_received,
        "first_message_latency_ms": {
            "count": len(stats.first_message_after_connect_ms),
            "values": [round(x, 1) for x in stats.first_message_after_connect_ms],
        },
        "top_markets_sample": [
            {"q": m["question"], "vol24h": round(m["volume24h"], 0)} for m in markets[:5]
        ],
    }

    with open(args.output, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[{utcnow()}] wrote {args.output}")
    print(json.dumps({
        "tokens": len(assets_ids),
        "duration_s": round(elapsed, 1),
        "sessions": len(ups),
        "open_failures": len(stats.failed_opens),
        "mean_uptime_s": summary["uptime_seconds"]["mean"],
        "median_uptime_s": summary["uptime_seconds"]["median"],
        "min_uptime_s": summary["uptime_seconds"]["min"],
        "max_uptime_s": summary["uptime_seconds"]["max"],
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
