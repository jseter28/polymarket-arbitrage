"""
Phase 3 probe — Kalshi WS sequence number semantics.

Phase 2 established that seq is per-sid, monotonic, and gap-free under
normal flow (5,082 messages across one 10k-ticker subscription, 0 gaps).
This probe nails the remaining undocumented behavior:

  A. Multi-sid independence: are seq counters independent across
     subscriptions on the same connection?
  B. Reconnect: does seq restart, and does sid stay stable on a fresh
     subscribe of the same markets?
  C. get_snapshot: does `update_subscription action=get_snapshot` emit
     a fresh snapshot with usable seq continuation, or does it reset?

Output: probe_kalshi_seq.json with raw per-message trace for each test.

Usage:
    python probe_kalshi_seq.py
    python probe_kalshi_seq.py --hold 30
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
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"{base_rest}/markets",
            params={"status": "open", "limit": n},
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        return [m["ticker"] for m in r.json().get("markets", [])][:n]


async def open_ws(args, api_key_id, private_key):
    headers = build_headers(api_key_id, private_key, "GET", WS_PATH)
    return await websockets.connect(
        args.base_ws,
        extra_headers=headers,
        open_timeout=20,
        ping_interval=20,
        max_size=16 * 1024 * 1024,
    )


async def drain(ws, seconds: float) -> list[dict]:
    """Drain frames for `seconds` and return as parsed dicts."""
    out: list[dict] = []
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.05, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            break
        except ConnectionClosed:
            break
        try:
            out.append(json.loads(raw))
        except Exception:
            continue
    return out


def summarize_seq(messages: list[dict]) -> dict:
    """Per-sid seq trace: first seq, last seq, count, gaps."""
    per_sid: dict[int, dict] = defaultdict(lambda: {"first": None, "last": None, "count": 0, "gaps": []})
    for m in messages:
        if m.get("type") in ("subscribed", "subscription_updated", "error"):
            continue
        sid = m.get("sid")
        seq = m.get("seq")
        if sid is None or seq is None:
            continue
        rec = per_sid[sid]
        if rec["first"] is None:
            rec["first"] = seq
        elif rec["last"] is not None and seq != rec["last"] + 1:
            rec["gaps"].append({"prev": rec["last"], "got": seq})
        rec["last"] = seq
        rec["count"] += 1
    return {f"sid_{sid}": v for sid, v in per_sid.items()}


async def test_a_multi_sid(args, api_key_id, private_key, all_tickers) -> dict:
    """Two subscribes on one conn — independent seq counters?"""
    print(f"\n=== TEST A: multi-sid seq independence ===")
    set_a = all_tickers[0:5]
    set_b = all_tickers[5:10]
    ws = await open_ws(args, api_key_id, private_key)
    try:
        await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": set_a}}))
        await ws.send(json.dumps({"id": 2, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": set_b}}))
        msgs = await drain(ws, args.hold)
    finally:
        await ws.close()

    acks = [m for m in msgs if m.get("type") == "subscribed"]
    sids = [a["msg"].get("sid") for a in acks]
    summary = summarize_seq(msgs)
    print(f"  acks={len(acks)}  sids={sids}  total_msgs={len(msgs)}")
    for k, v in summary.items():
        print(f"  {k}: first={v['first']} last={v['last']} count={v['count']} gaps={len(v['gaps'])}")
    return {
        "tickers_set_a": set_a,
        "tickers_set_b": set_b,
        "acks": acks,
        "sid_a": sids[0] if len(sids) > 0 else None,
        "sid_b": sids[1] if len(sids) > 1 else None,
        "summary": summary,
        "msg_count": len(msgs),
    }


async def test_b_reconnect(args, api_key_id, private_key, all_tickers) -> dict:
    """Two separate connect+subscribe cycles for the same tickers — does sid persist, does seq restart?"""
    print(f"\n=== TEST B: reconnect seq behavior ===")
    set_x = all_tickers[0:10]

    ws1 = await open_ws(args, api_key_id, private_key)
    try:
        await ws1.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": set_x}}))
        first_msgs = await drain(ws1, args.hold)
    finally:
        await ws1.close()

    sid1 = next((m["msg"].get("sid") for m in first_msgs if m.get("type") == "subscribed"), None)
    last_seq1 = max((m["seq"] for m in first_msgs if m.get("seq") is not None), default=None)
    print(f"  1st conn: sid={sid1}  last_seq_observed={last_seq1}  msgs={len(first_msgs)}")

    await asyncio.sleep(2.0)

    ws2 = await open_ws(args, api_key_id, private_key)
    try:
        await ws2.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": set_x}}))
        second_msgs = await drain(ws2, args.hold)
    finally:
        await ws2.close()

    sid2 = next((m["msg"].get("sid") for m in second_msgs if m.get("type") == "subscribed"), None)
    first_seq2 = next((m["seq"] for m in second_msgs if m.get("seq") is not None), None)
    print(f"  2nd conn: sid={sid2}  first_seq_observed={first_seq2}  msgs={len(second_msgs)}")

    return {
        "tickers": set_x,
        "first_conn": {"sid": sid1, "last_seq": last_seq1, "msg_count": len(first_msgs)},
        "second_conn": {"sid": sid2, "first_seq": first_seq2, "msg_count": len(second_msgs)},
        "sid_changed": sid1 != sid2,
        "seq_restarted": first_seq2 == 1 if first_seq2 is not None else None,
    }


async def test_c_get_snapshot(args, api_key_id, private_key, all_tickers) -> dict:
    """Mid-stream get_snapshot — fresh snapshot emitted, seq continuation usable?"""
    print(f"\n=== TEST C: mid-stream get_snapshot ===")
    set_y = all_tickers[0:10]

    ws = await open_ws(args, api_key_id, private_key)
    try:
        await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": set_y}}))

        before_msgs = await drain(ws, args.hold)
        sid = next((m["msg"].get("sid") for m in before_msgs if m.get("type") == "subscribed"), None)
        last_seq_before = max((m["seq"] for m in before_msgs if m.get("seq") is not None), default=None)
        print(f"  before: sid={sid}  last_seq={last_seq_before}  msgs={len(before_msgs)}")

        await ws.send(json.dumps({
            "id": 99,
            "cmd": "update_subscription",
            "params": {"sids": [sid], "market_tickers": [], "action": "get_snapshot"},
        }))
        await asyncio.sleep(0.2)
        after_msgs = await drain(ws, args.hold)
        first_seq_after = next((m["seq"] for m in after_msgs if m.get("seq") is not None), None)
        new_snapshots = [m for m in after_msgs if m.get("type") == "orderbook_snapshot"]
        print(f"  after get_snapshot: first_seq={first_seq_after}  new_snapshots={len(new_snapshots)}  total_msgs={len(after_msgs)}")
    finally:
        await ws.close()

    return {
        "tickers": set_y,
        "sid": sid,
        "before": {"last_seq": last_seq_before, "msg_count": len(before_msgs)},
        "after": {"first_seq": first_seq_after, "msg_count": len(after_msgs), "new_snapshot_count": len(new_snapshots)},
        "seq_continues": (
            first_seq_after is not None and last_seq_before is not None and first_seq_after > last_seq_before
        ),
    }


async def main(args: argparse.Namespace) -> None:
    cfg = load_config(args.config)
    private_key = load_private_key(cfg.api.kalshi_private_key)
    api_key_id = cfg.api.kalshi_api_key

    print(f"[{utcnow()}] Phase 3 seq probe — host {args.base_ws}")
    tickers = await fetch_tickers(args.base_rest, 20)
    print(f"  Got {len(tickers)} tickers")

    out = {
        "tested_at": utcnow(),
        "host": args.base_ws,
        "hold_seconds": args.hold,
        "tickers_pool": tickers,
        "test_a": await test_a_multi_sid(args, api_key_id, private_key, tickers),
        "test_b": await test_b_reconnect(args, api_key_id, private_key, tickers),
        "test_c": await test_c_get_snapshot(args, api_key_id, private_key, tickers),
    }
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n[{utcnow()}] Wrote {args.output}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Phase 3 Kalshi WS sequence probe")
    p.add_argument("-c", "--config", default="config.live.yaml")
    p.add_argument("--base-ws", default=DEFAULT_WS)
    p.add_argument("--base-rest", default=DEFAULT_REST)
    p.add_argument("--hold", type=float, default=20.0, help="Drain duration per phase (s)")
    p.add_argument("-o", "--output", default="probe_kalshi_seq.json")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
