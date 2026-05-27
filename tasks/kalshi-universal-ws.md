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

## Phase 1 — Connection cap probe

**Where:** new `probe_kalshi_ws_connections.py`.

**Goal:** Empirically confirm the community-reported ~5 concurrent conns/user cap. Avoid extrapolation from undocumented limits.

**Tasks:**
- [ ] Script opens N authenticated WS connections sequentially (N=1..8), each subscribing to a small ticker set (5 markets).
- [ ] Observe: which N triggers server-side rejection (close code, error message). Log the exact failure mode.
- [ ] Output: `probe_kalshi_connections.json` with per-N result (accepted | rejected, server message), plus a one-line summary in the log.
- [ ] Record close codes / error payloads in the result file — useful when designing reconnect logic later.

**Effort:** S. **Gain:** confirmed max conn count → drives the default+max conn pool size in Phase 4.

---

## Phase 2 — Instrument-per-conn cap probe

**Where:** new `probe_kalshi_ws_instruments.py`.

**Goal:** Empirically determine if there is any practical instrument-per-conn ceiling (docs say no, but verify under load).

**Tasks:**
- [ ] Open one authenticated conn. Subscribe to `orderbook_delta` for 100 markets initially.
- [ ] Use `update_subscription` with `action: "add_markets"` to progressively grow subscription to 500, 1k, 2.5k, 5k, 10k tickers.
- [ ] At each step measure for ≥60 s: msg/sec, p50/p99 frame size, drop/disconnect events, `seq` gaps observed.
- [ ] Output: `probe_kalshi_instruments.json` — per-step throughput + stability table.
- [ ] Stop at the first N where stability degrades materially (define "material" as ≥1% gap rate over the 60s window) or at 10k, whichever comes first.

**Effort:** M. **Gain:** practical instrument-per-conn ceiling for the universal layer. Determines whether 3 conns can cover all active Kalshi markets or sharding needs to fan harder.

---

## Phase 3 — Sequence/delta correctness probe

**Where:** new `probe_kalshi_ws_seq.py`.

**Goal:** Resolve the **officially undocumented** `seq` scope question (per-`sid`? per-market? per-conn?) and verify gap-recovery via `get_snapshot` works as expected.

**Tasks:**
- [ ] Subscribe to ~100 markets on one conn. Log every `(sid, seq, market_ticker, type)` tuple for 5 minutes.
- [ ] Determine `seq` scope empirically — does it increment per-`sid` regardless of market, or per-market within the `sid`, or per-conn across `sid`s?
- [ ] Force a 5-second network interruption (kill+restart, or local firewall drop). Reconnect, resubscribe, observe `seq` reset behavior.
- [ ] Issue `update_subscription` with `action: "get_snapshot"` on an active `sid`. Verify the resulting `orderbook_snapshot` arrives with a clean `seq` continuation usable for resync.
- [ ] Document findings in `tasks/kalshi-seq-semantics.md` — definitive reference for Phase 4's book-state machine.

**Effort:** M. **Gain:** locks the contract the Phase 4 ingest layer relies on. Without this we are guessing.

---

## Phase 4 — `KalshiUniversalWS` ingest module

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
- [ ] Skeleton `KalshiUniversalWS` class with same dataclass + state shape as Polymarket version.
- [ ] `_conn_supervisor`: handshake, subscribe, recv loop, reconnect with exp backoff (max 60s).
- [ ] `_apply_snapshot` + `_apply_delta` (handles `side`, `price_dollars`, `delta_fp`).
- [ ] `_seq_tracker` per `sid` (data structure determined by Phase 3 findings).
- [ ] `_resync_subscription(sid)` — issues `update_subscription` `get_snapshot`, drains stale until snapshot arrives.
- [ ] `iter_updates()` async generator using a bounded queue (mirror Polymarket's pattern — apply lessons from `tasks/todo.md` Problem #1: latest-wins coalesce per-market, separate apply task from recv).
- [ ] Status struct with per-conn telemetry surface.
- [ ] Unit tests: snapshot apply, delta apply, gap detection, resync trigger, missing-side-key tolerance, reconnect-with-stale-flush.

**Effort:** L. **Gain:** the workstream's core deliverable.

---

## Phase 5 — Active-market discovery + lifecycle channel

**Where:** extends `kalshi_client/universal_ws.py`, uses `market_lifecycle_v2`.

**Goal:** "All active markets" is a moving target. Bootstrap via REST; maintain via the lifecycle channel; periodic REST refresh as safety net.

**Tasks:**
- [ ] On `start()`: call `KalshiClient.list_all_markets(status="open")` and shard across conns.
- [ ] Dedicate one conn (or reuse conn 0) to `market_lifecycle_v2` subscription.
- [ ] Handle `activated` → `update_subscription` `add_markets` to the appropriate shard.
- [ ] Handle `deactivated` / `settled` / `determined` → `update_subscription` `delete_markets` + drop book from state.
- [ ] Filter out KXMVE-prefixed (multivariate) tickers — excluded from `market_lifecycle_v2` per docs; treat their absence as load-bearing.
- [ ] Periodic REST refresh every 15 min — reconcile against current subscribed set; add missing, drop ghosts.
- [ ] Surface `market_count` and `lifecycle_event_count` in `status()`.
- [ ] Tests: simulated lifecycle events drive correct add/delete; REST drift reconcile fires correctly.

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
