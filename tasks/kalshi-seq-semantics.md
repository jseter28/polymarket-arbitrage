# Kalshi WS Sequence & Subscription Semantics

Empirical reference for Phase 4 (`KalshiUniversalWS`) book-state machine.
Findings derived from Phase 1–3 probes against `wss://demo-api.kalshi.co/trade-api/ws/v2` on 2026-05-26.
Sources: `probe_kalshi_connections.json`, `probe_kalshi_instruments.json`, `probe_kalshi_seq.json`.

The Kalshi docs do not formally specify `seq` scope or reconnect-resync semantics; this file is the project's source of truth.

---

## 1. `seq` scope

- **Per-`sid`, monotonically increasing by 1 per message on that sid.**
- Gap-free under normal flow. Phase 2 observed 0 gaps in 5,082 messages on a single sid covering 10k subscribed markets.
- The first `orderbook_snapshot` after subscribe starts at `seq=1`. Every subsequent message (snapshot or delta) on that sid is `seq + 1`.

## 2. `sid` lifecycle

- A `sid` (subscription id) is created on the first `subscribe` per `(channel, connection)`.
- On a freshly opened connection, the first subscribe returns `sid=1`. Subsequent fresh connections also start at `sid=1`.
- **One `sid` per `(channel, connection)`.** A second `subscribe` to the same channel on the same conn does NOT create a second `sid` — Kalshi merges the new tickers into the existing `sid` and replies with `type=ok` (not `type=subscribed`), with the body listing all currently-subscribed market_tickers.
- Therefore: do not design Phase 4 around multiple `sid`s for `orderbook_delta` on one conn. There is exactly one.
- `sid` does NOT persist across connections. A reconnected client always starts a fresh `sid=1`.

## 3. Subscribe ack message shape

The `subscribed` ack nests `sid` under `msg.sid`:

```json
{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":1}}
```

Top-level `sid` only appears on subsequent data messages (`orderbook_snapshot`, `orderbook_delta`). This is a structural difference that bit the Phase 2 probe — handle the ack and data shapes separately.

## 4. Reconnect behavior

- New connection → new `(sid=1, seq=1)`.
- All markets must be re-subscribed; the server does not remember prior subscriptions across the disconnect.
- **Phase 4 must NOT cross-reference `sid` across connections.** Per-connection sid+seq tracking is the right abstraction.

## 5. Gap recovery via `update_subscription get_snapshot`

When a seq gap is detected on a `sid`, recovery is non-destructive:

```json
{
  "id": <any non-zero int>,
  "cmd": "update_subscription",
  "params": {
    "sids": [<sid>],
    "market_tickers": ["TICKER1", "TICKER2"],
    "action": "get_snapshot"
  }
}
```

- The server emits a fresh `orderbook_snapshot` for each market in `market_tickers`.
- The `seq` continues from the current sid counter (does NOT reset). Phase 3 observed `seq=1,2` before → `seq=3,4` after.
- **`market_tickers` is required.** An empty list produces no snapshots.
- The connection stays open; no resubscribe needed.

## 6. Subscription mutation

Use `update_subscription` to grow / shrink the active subscription without dropping the conn:

- `action: "add_markets"` — adds markets to the existing sid. Phase 2 confirmed batches of 500 work cleanly up to 10k cumulative markets on one sid.
- `action: "delete_markets"` — removes markets. Untested directly; expected to mirror add_markets.
- `action: "get_snapshot"` — see section 5.

## 7. Connection caps

- **Demo:** 10 concurrent auth WS conns per API key. Conn 11+ rejected with `HTTP 429` at handshake. Hard cap.
- **Production:** Community-reported ~5 conns/user. Unverified — assume the stricter number for Phase 4 sizing, retest when prod creds arrive.

## 8. Instrument caps

- **Demo:** no observed cap up to 10,000 instruments on a single auth WS conn. Sustained 169 msg/s / 111 KB/s with zero errors and zero seq gaps at that level.
- The Kalshi docs do not publish a per-conn instrument limit; this matches.

## 9. Known message-shape quirks

- `yes_dollars_fp` and `no_dollars_fp` keys are **omitted entirely** from `orderbook_snapshot` when that side has no resting offers. Do not treat their absence as an error.
- Server treats `id: 0` in client commands as "no id." Always use a non-zero correlation id.
- `use_yes_price` default flip in progress per Kalshi changelog; set explicitly in commands to be future-safe.

---

## Phase 4 implications

1. **One sid per shard conn for `orderbook_delta`.** State machine keys book updates by `(conn_id, market_ticker)` since there's no meaningful within-conn sid disambiguation.
2. **Per-conn seq tracker** — `int` per conn, advances on every snapshot/delta. Reset on disconnect.
3. **Gap detection**: compare incoming `seq` against `last_seq + 1`. On gap, identify affected markets (track which markets appeared in the gap window if you can; otherwise trigger `get_snapshot` for the conn's full ticker set).
4. **Gap recovery**: `update_subscription get_snapshot` with the affected markets. Apply the resulting snapshots; resume delta application from the new seq baseline.
5. **Reconnect = nuclear option**: drop sid, drop seq counter, drop all books on that conn, reopen, re-subscribe, await fresh snapshots before yielding any data downstream.
