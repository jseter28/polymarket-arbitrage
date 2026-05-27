# Kalshi Universal WS — Active Workstream

**Focus:** Build `KalshiUniversalWS` as a sharded ingest layer parallel to `PolymarketUniversalWS`. Soak/visibility layer, **not** wired into the bot's `DataFeed` or `CrossPlatformArbEngine` in this workstream. Plan written 2026-05-26 from systems-think session.

**Sequencing note:** Polymarket feed-speed workstream in `tasks/todo.md` has open items (instrumentation gap, mempool prototype, N>1 sharding decision). Decide before starting Phase 0 whether this runs in parallel, interleaved, or after.

---

## Architecture decisions (locked)

These are fixed inputs to every phase below — do not relitigate without explicit revisit.

- **Universe:** all active Kalshi markets (`status=active`), no volume floor, no category filter.
- **Accuracy model:** WS-only, strict sequence correctness. Gap-free `seq` enforced per `sid`; on gap, force resync via `update_subscription` with `action: "get_snapshot"` (no REST cross-check audit).
- **Channel:** `orderbook_delta` only. Server emits initial `orderbook_snapshot` then incremental `orderbook_delta` on the same subscription stream.
- **Endpoint (prod):** `wss://api.elections.kalshi.com/trade-api/ws/v2` (matches existing REST host in `config.yaml`).
- **Endpoint (demo, current soak target):** `wss://demo-api.kalshi.co/trade-api/ws/v2` — Phase 0 found the available credentials are demo-only. Soak runs against demo until production creds are issued; flipping is a single URL change.
- **Auth:** RSA-PSS-signed handshake headers (`KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`, `KALSHI-ACCESS-TIMESTAMP`). Signing string = `timestamp + "GET" + "/trade-api/ws/v2"`. Verified working end-to-end against the demo host (Phase 0 smoke test).
- **Sharding model is inverted from Polymarket.** Kalshi: ~5 concurrent conns per user (community-cited, official cap unconfirmed), no documented per-conn instrument cap. Polymarket: ~500 instruments/conn, no conn cap. → Strategy: **fewer fat conns**, default 3, max 5 in code (reserve headroom for ad-hoc dev sessions).
- **Public surface mirrors `PolymarketUniversalWS`:** `start()`, `iter_updates()`, `get_book()`, `status()`, `stop()`. No new abstract base class until the second universe is shipped — premature abstraction risk.
- **Consumer:** probe dashboard at port 8889 (extended). No bot/DataFeed/CrossPlatformArbEngine integration in this workstream — explicit follow-up.
- **Quirks to encode:** snapshot may omit `yes_dollars_fp` / `no_dollars_fp` keys entirely when empty (not `[]`); never use `id: 0` as correlation; set `use_yes_price=true` explicitly to be future-safe; `market_lifecycle_v2` channel powers prune-on-settle.

---

## Phase 0 — Kalshi auth signing primitive (prereq) — ✅ COMPLETE

**Where:** new `kalshi_client/auth.py`, wired into `kalshi_client/api.py` and (later) `kalshi_client/universal_ws.py`.

**Tasks:**
- [x] `cryptography>=42.0.0` added to `requirements.txt` (already installed at 46.0.3).
- [x] `kalshi_client/auth.py::sign(timestamp_ms, method, path, private_key) -> str` — RSA-PSS+SHA-256, MGF1, salt_length=32, base64.
- [x] `kalshi_client/auth.py::build_headers(api_key_id, private_key, method, path) -> dict[str, str]` — produces the three required `KALSHI-ACCESS-*` headers; uses `int(time.time() * 1000)` for the timestamp.
- [x] `kalshi_client/auth.py::load_private_key(pem) -> RSAPrivateKey` — parses PEM, raises `ValueError` on invalid input.
- [x] `ApiConfig` (`utils/config_loader.py`) surfaces `kalshi_api_key`, `kalshi_private_key`, and `kalshi_ws_url`. (Note: `kalshi_env` field deliberately deferred to Phase 4.)
- [x] `tests/test_kalshi_auth.py` — 4 tests cover sign round-trip, header structure, PEM round-trip, PEM-invalid rejection. All pass.
- [x] `tests/test_config_kalshi_creds.py` — verifies `load_config` exposes the new fields.
- [x] `test_kalshi_connection.py` — runnable smoke test at repo root, mirrors `test_connection.py` shape.
- [x] Full pytest suite passes (74 tests, no regressions).
- [x] `kalshi_client/auth.py` is mypy-clean.

**Result of live verification (2026-05-26):** smoke test against `demo-api.kalshi.co/trade-api/v2/portfolio/balance` returns `HTTP 200` with the demo $100 balance. **Auth chain VERIFIED end-to-end.**

**Important credential finding:** The `kalshi_api_key` in `config.live.yaml` is a **Kalshi demo/sandbox key**, not production. Probed paths:
- `api.elections.kalshi.com` (production) → `HTTP 401 {"code":"authentication_error","details":"NOT_FOUND"}`
- `trading-api.kalshi.com` → redirects to elections
- `api.kalshi.com` → DNS does not resolve (host retired)
- `demo-api.kalshi.co` (sandbox) → `HTTP 200` ✅

The smoke test now defaults to the demo host and accepts `--base-url` to point elsewhere. The soak workstream proceeds against demo until production credentials are issued. Switching to prod is a one-CLI-flag change for the smoke test and a one-line URL change for the WS layer when it's built.

**Effort:** S. **Gain:** unblocks everything else.

---

## Phase 1 — Connection cap probe — ✅ COMPLETE

**Where:** `probe_kalshi_connections.py`. Output: `probe_kalshi_connections.json`.

**Tasks:**
- [x] Script opens N concurrent authenticated WS connections (N=1..12), each subscribing to 3 demo tickers via `orderbook_delta`.
- [x] Per-iteration: handshake count, rejection count, premature closes, message totals — all written to JSON.
- [x] Found rejection signal: **HTTP 429** on the handshake (not a close code; rejection is at connection-establishment time).

**Result (2026-05-26, demo host):**
- N=1..10 → all handshakes accepted, all subscribes ACK'd, snapshots received.
- N=11 → 10 of 11 accepted; 11th rejected with `HTTP 429`.
- N=12 → 10 of 12 accepted; 2 rejected with `HTTP 429`.
- **Demo concurrent-connection cap = 10, hard, signaled with HTTP 429.**
- Community-reported "5 concurrent conns" not corroborated on demo. Production may still enforce 5 — verify when prod creds arrive (re-run with `--base-ws wss://api.elections.kalshi.com/trade-api/ws/v2`).

**Phase 4 implication:** Pool default of 3 / max 5 still right under the assumption prod is stricter. Demo allows headroom up to 10 if Phase 2 needs more parallelism for the instrument-cap probe.

**Effort:** S. **Gain:** confirmed conn cap → sizing for Phase 4 pool.

---

## Phase 2 — Instrument-per-conn cap probe — ✅ COMPLETE

**Where:** `probe_kalshi_instruments.py`. Output: `probe_kalshi_instruments.json`.

**Tasks:**
- [x] One auth WS conn; subscribe to `orderbook_delta` starting at 100 markets.
- [x] Grow via `update_subscription` `add_markets` through 500 → 1k → 2.5k → 5k → 10k tickers.
- [x] Per-step measurement: msg/sec, KB/sec, snapshot count, delta count, seq gap rate, errors.
- [x] Fixed protocol bug discovered live: the `subscribed` ack nests `sid` under `msg.sid` (not top-level); top-level `sid` only appears on subsequent data messages.

**Result (2026-05-26, demo host):**

| Step | Subscribed | msg/s | KB/s | snaps | deltas | seq gaps | errors |
|------|-----------|-------|------|-------|--------|----------|--------|
| 1    | 100       | 3.4   | 0.4  | 100   | 0      | 0        | 0      |
| 2    | 500       | 13.6  | 2.2  | 400   | 6      | 0        | 0      |
| 3    | 1,000     | 16.7  | 3.1  | 500   | 0      | 0        | 0      |
| 4    | 2,500     | 54.9  | 14.3 | 1,500 | 145    | 0        | 0      |
| 5    | 5,000     | 84.6  | 33.9 | 2,500 | 33     | 0        | 0      |
| 6    | 10,000    | 169.4 | 111.4| 5,000 | 72     | **0**    | 0      |

**Findings:**
- One auth WS conn sustains 10k subscribed instruments cleanly. No degradation across any step.
- `update_subscription add_markets` in batches of 500 is reliable; every newly-added market emits its initial snapshot.
- `sid=1` for all 10k markets — they all live on a single subscription. Kalshi multiplexes generously.
- 0 seq gaps across 5,082 messages → `seq` is per-`sid`, monotonic, gap-free under normal conditions (also: strong partial answer to Phase 3).
- 111 KB/s at 10k tickers on demo. Well within budget — production throughput likely higher because more markets will have active deltas.

**Phase 4 sizing:** Confirmed — **1–3 fat conns** can cover the full active Kalshi universe (21k markets on demo today). "Fewer fat conns" sharding strategy validated.

**Effort:** M. **Gain:** locks the per-conn capacity story. Phase 4 conn pool sizing is now data-driven, not a guess.

---

## Phase 3 — Sequence/delta correctness probe — ✅ COMPLETE

**Where:** `probe_kalshi_seq.py`. Output: `probe_kalshi_seq.json`. Reference: `tasks/kalshi-seq-semantics.md`.

**Tasks:**
- [x] Test A (multi-sid independence) — revealed instead that **Kalshi enforces one `sid` per `(channel, connection)`**. A second `subscribe` to the same channel returns `type=ok` (not `type=subscribed`) and merges new tickers into the existing sid. Phase 4 cannot rely on multiple sids per conn for `orderbook_delta`.
- [x] Test B (reconnect behavior) — confirmed: new conn → fresh `sid=1`, fresh `seq=1`. Per-conn state, no cross-conn sid carryover.
- [x] Test C (`get_snapshot`) — confirmed working with explicit `market_tickers`: emits fresh `orderbook_snapshot` per requested market, `seq` continues from current counter (Phase 3 inline test: seq 1,2 before → seq 3,4 after).
- [x] Captured the structural quirk that `subscribed` ack nests `sid` under `msg.sid` (top-level on data messages, nested on the ack).
- [x] `tasks/kalshi-seq-semantics.md` written as the project's definitive Kalshi WS reference.

**Phase 4 implications (locked):**
1. State machine keys book updates by `(conn_id, market_ticker)` — one sid per conn means within-conn sid is degenerate.
2. Per-conn seq tracker; reset on disconnect.
3. Gap recovery = `update_subscription get_snapshot` with affected `market_tickers`; do not reconnect on gap unless the gap recovery itself fails.
4. Reconnect = nuclear: drop sid, drop seq, drop books on that conn, await fresh snapshots before yielding downstream.

**Effort:** M. **Gain:** Phase 4 book-state design is now grounded in observed behavior rather than docs-guessing.

---

## Phase 4 — `KalshiUniversalWS` ingest module — ✅ COMPLETE

**Where:** new `kalshi_client/universal_ws.py`, modeled on `polymarket_client/universal_ws.py`.

**Public surface (matches Polymarket):**
```python
ws = KalshiUniversalWS(api_key, private_key, conn_count=3)
await ws.start()
async for update in ws.iter_updates(): ...
book = ws.get_book(market_ticker)
status = ws.status()
await ws.stop()
```

**Internal design:**
- Connection pool: N=3 default, capped at the empirical max from Phase 1 minus 1 (headroom).
- Sharding: distribute markets across conns by `hash(market_ticker) % N`. Fat conns expected; rebalance only on a markedly degraded conn.
- Per-conn supervisor: signed handshake → subscribe (multi-market) → consume snapshot then deltas → maintain `OrderBook` keyed by `market_ticker`.
- Strict seq correctness per `sid` (using rule established in Phase 3). On gap: emit `update_subscription` `get_snapshot` for that `sid`, discard deltas until fresh snapshot arrives, then resume.
- Reconnect-with-backoff identical to `PolymarketUniversalWS._shard_supervisor`. Re-subscribe on reconnect; treat all books on that conn as stale until snapshot replays.
- Telemetry: per-conn `connect_ms`, `reconnect_count`, `msg_count`, `drop_count`, `seq_gap_count`, `snapshot_resync_count`, `recv_to_apply_us` p50/p99.
- Encode quirks: handle absent `yes_dollars_fp` / `no_dollars_fp` keys, never use `id: 0`, set `use_yes_price=true` on subscribe.

**Tasks:**
- [x] Skeleton `KalshiUniversalWS` class with `KalshiConnState` dataclass mirroring Polymarket's `ShardState`.
- [x] `_conn_supervisor` + `_run_conn_session`: handshake via `build_headers`, initial subscribe (batched at 500 if huge), recv loop, reconnect with exp backoff (cap 30s) and 3-short-session quarantine (60s).
- [x] `_apply_snapshot` + `_apply_delta` handling `yes_dollars_fp` / `no_dollars_fp` (omit-when-empty quirk), slot-replace immutability contract.
- [x] `_track_seq` — per-conn seq tracker (one sid per conn per Phase 3 finding).
- [x] `_resync_conn` — `update_subscription get_snapshot` for all tickers on the conn; seq continues per Phase 3.
- [x] `iter_updates()` — bounded queue, drop-oldest, latest-wins coalesce at yield time via `KalshiOrderBook.to_unified_orderbook()`.
- [x] `status()` — field names match `PolymarketUniversalWS` for drop-in dashboard compat; adds Kalshi-specific keys (`sid`, `last_seq`, `snapshot_count`, `delta_count`, `seq_gap_count`, `snapshot_resync_count`).
- [x] 32 unit tests in `tests/test_kalshi_universal_ws.py` covering partition, apply paths, seq tracking, message routing, queue, status, backoff.
- [x] `kalshi_client/__init__.py` exports `KalshiUniversalWS`.

**Result of live verification:**
1. *Smoke (3 tickers, 2 conns, 15s):* both conns connected, sid=1 each, 3 snapshots received, 0 seq gaps, 0 drops.
2. *Mini soak (full demo universe, 3 conns, ~90s):*
   - **67,946 markets tracked** across 3 sharded conns (~22.5k each).
   - All 3 conns connected, all sid=1 (one-sid-per-conn confirmed).
   - Initial subscribe + snapshot flood completed in ~30s.
   - Steady-state: ~143k total messages observed; one hot market drove conn#0 to 69k deltas alone.
   - **0 drops, queue_depth stayed at 0** (consumer kept up).
   - **88 seq gaps total (~0.06% rate)** under load — well below the 1% degradation threshold; `_resync_conn` exercised automatically each time.

**Lines:** `kalshi_client/universal_ws.py` ≈ 540 lines (vs. plan budget 500–600). Tests ≈ 470 lines.

**Atomic commits:** Phase 4a `17f3a3f` (skeleton + data path), Phase 4b `20ec9ad` (supervisor + seq + gap recovery), Phase 4c (this) — `__init__` export + plan update.

**Effort:** L. **Gain:** the workstream's core deliverable is live. Phases 5–7 layer additional behavior on top.

---

## Phase 5 — Active-market discovery + lifecycle channel — ✅ COMPLETE

**Where:** extends `kalshi_client/universal_ws.py`, uses `market_lifecycle_v2`.

**Goal:** "All active markets" is a moving target. Bootstrap via REST; maintain via the lifecycle channel; periodic REST refresh as safety net.

**Tasks:**
- [x] On `start()`: bootstrap via existing `_fetch_open_market_tickers()` (already there from Phase 4); shard via `_partition_by_hash`.
- [x] Conn 0 carries the `market_lifecycle_v2` subscription (separate sid, captured via `KalshiConnState.lifecycle_sid`).
- [x] `market_created` / `market_activated` → `_dispatch_add` via `update_subscription add_markets` to the hash-target conn.
- [x] `market_deactivated` / `market_settled` / `market_determined` → `_dispatch_remove` via `delete_markets`; drops `_books[ticker]`.
- [x] KXMVE-prefixed markets included in initial subscribe; REST reconcile is the safety net for their state changes (lifecycle channel doesn't emit for them).
- [x] `_reconcile_loop` every `reconcile_interval_s` (default 900s); `_reconcile_loop_once` extracted for unit tests + smoke.
- [x] Added per-conn telemetry: `lifecycle_sid`, `lifecycle_event_count`, `lifecycle_add_count`, `lifecycle_remove_count`. Top-level: `reconcile_count`, `reconcile_last_added`, `reconcile_last_removed`.
- [x] 12 new unit tests: `TestPrepareAddRemove`, `TestLifecycleEventHandling`, `TestReconcileDiff`, `TestLifecycleTelemetry`. Full suite: 118 passing.

**Result of live smoke (2026-05-26, 45s, 2 conns, 3 tickers, reconcile_interval_s=20):**
- Lifecycle channel subscribed cleanly on conn 0 (`lifecycle_sid=1`, separate from orderbook sid).
- **227 lifecycle events** received in the first ~20s (mostly `market_metadata_updated`-class — telemetry only).
- `_reconcile_loop_once` ran (`reconcile_count=1`), zero-diff against REST as expected for an in-flight subscription set.
- One transient reconnect on conn 0 was handled cleanly; lifecycle_sid reset to None per `_reset_conn_state`. Re-subscribe path on reconnect needs further observation in Phase 7 soak — the smoke window was too short to confirm lifecycle re-acks consistently.

**Effort:** M. **Gain:** the layer holds its "all active markets" guarantee through the day; no manual restarts to pick up new markets.

---

## Phase 6 — Probe dashboard integration on port 8889

**Where:** extends `probe_universal_ws_dashboard.py` and its embedded SPA.

**Goal:** Two-universe view in one dashboard. No new port unless the SPA can't host both cleanly.

**Tasks:**
- [ ] CLI flag: `--kalshi` (off by default until creds are wired in local config). When set, instantiate `KalshiUniversalWS` alongside `PolymarketUniversalWS`.
- [ ] Add `/api/kalshi/status` and `/api/kalshi/markets` endpoints mirroring Polymarket equivalents.
- [ ] SPA: add `#kalshi` route (markets list) and `#kalshi/<ticker>` route (single-market view).
- [ ] Status page: side-by-side Polymarket / Kalshi telemetry blocks.
- [ ] Persistence: extend `--output` JSON to include `kalshi` section with parallel structure.
- [ ] Confirm CSS specificity fix (commit `28ffb57`) still holds with the new routes.

**Effort:** M. **Gain:** soak telemetry is observable; the universe is browseable.

---

## Phase 7 — 24h soak + telemetry capture

**Where:** runs `probe_universal_ws_dashboard.py --kalshi --duration 86400 --output probe_universal_kalshi_24h.json`.

**Goal:** Prove stability of the combined two-universe ingest layer over 24h, capture the telemetry numbers that justify (or refute) calling this "fastest + most accurate."

**Tasks:**
- [ ] Run the 24h soak with both universes active.
- [ ] Capture and review at end-of-soak: per-conn drops, total `seq` gaps, snapshot-resync triggers per conn, p50/p99 message latency, market-count stability, lifecycle event counts, any reconnect storms.
- [ ] Compare Kalshi message latency vs. previously documented Polymarket figures from `probe_universal_15min.json`.
- [ ] If `seq` gap rate > 0.1% sustained, treat as a Phase 4 correctness bug, not a soak result.
- [ ] Write a short `probe_universal_kalshi_24h.md` summary alongside the JSON — same format as the existing Polymarket soak reports.

**Effort:** S (mostly waiting). **Gain:** evidence the layer holds up. Gate before any conversation about wiring it into the bot.

---

## Explicit non-goals (this workstream)

- No integration with `DataFeed`, `ArbEngine`, `CrossPlatformArbEngine`, or `ExecutionEngine`.
- No REST-vs-WS accuracy audit (rejected in design phase — soak-only context, strict seq correctness is the chosen guarantee).
- No abstract base class shared between Polymarket and Kalshi universal layers (defer until a third universe forces it).
- No new port. Dashboard stays at 8889.
- No category filtering or volume threshold on the market universe.
- No mempool / on-chain Kalshi work — Kalshi is centralized; not applicable.

---

## Open questions

- [ ] Does the existing Polymarket feed-speed workstream (`tasks/todo.md`) have to land first, or can this run in parallel? (Conflict: both touch the probe dashboard codebase; both compete for soak time.)
- [ ] Production Kalshi credentials — when issued, drop them into `config.live.yaml` and re-run `test_kalshi_connection.py --base-url https://api.elections.kalshi.com/trade-api/v2` to verify they're accepted on prod. Until then, soak proceeds against `demo-api.kalshi.co`.
- [ ] Resolution of `seq` scope (Phase 3) may change Phase 4 design. Don't lock Phase 4 details until Phase 3 lands.
- [x] Phase 0 auth scope: handshake-only confirmed in scope; REST trade-endpoint wiring (positions, fills, orders) deferred to a separate workstream when the bot needs to trade Kalshi.
