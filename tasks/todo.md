# Feed Speed & Mempool — Active Workstream

**Focus:** Minimize end-to-end latency from market event to actionable signal. Audit complete (`AUDIT_polymarket_feed.md`, 2026-05-26). This file tracks the pending fixes ordered by gain/effort.

---

## Problem #1 — Synchronous single-consumer fanout (HIGHEST IMPACT)

**Where:** `core/data_feed.py:210` → `main.py:172` (and mirror in `run_with_dashboard.py:214`).

**What's wrong:**
- `_stream_orderbooks` calls `self.on_update(market_id, state)` synchronously inside the WebSocket receive coroutine.
- The registered callback (`_on_market_update`) then runs `self.arb_engine.analyze(market_state)` inline on that same coroutine.
- Result: one slow market's analysis head-of-line-blocks WS reads for **every other market**. The connection is open and Polymarket has already pushed the next update, but the bot can't read it because it's still doing arb math on the previous market.
- Throughput ceiling is whatever the slowest `analyze()` call takes, not whatever the WS can deliver. Under the 3,235 msg/s observed in `probe_universal_15min.json`, this is the dominant latency source.

**The fix (R2 in audit):**
- Insert a bounded `asyncio.Queue[tuple[str, MarketState]]` between WS recv and analyze.
- Producer (`_stream_orderbooks`) does `put_nowait`; on `QueueFull`, drop the oldest and enqueue the new one (latest-wins per market — stale book snapshots are worthless for arb).
- Consumer is a separate `_analyze_worker` task that drains the queue and calls `arb_engine.analyze`.
- Start with **N=1 worker** to preserve per-market ordering (`_check_expired_opportunities` in `arb_engine.py` assumes ordered updates per market via `datetime.utcnow()`). Scale to N>1 only with per-market sharding (hash `market_id` → worker).

**Tasks:**
- [x] Add `_analyze_queue` and `_pending` dict to `DataFeed.__init__` in `core/data_feed.py`
- [x] Replace synchronous `on_update` call at `core/data_feed.py:210` with queue put + latest-wins coalesce (kept `on_update` as legacy backward-compat; bot uses queue)
- [x] Add `_analyze_worker` task in `main.py` and `run_with_dashboard.py`; start it from `TradingBot.start()` instead of registering `on_update` callback
- [x] Write a regression test that injects a deliberately-slow `analyze()` on market X and asserts updates for market Y are not delayed (`tests/test_data_feed.py::test_producer_does_not_block_on_slow_consumer` — 1000 puts in <100ms with 20ms/call consumer)
- [x] Verify `_check_expired_opportunities` still behaves correctly with N=1 worker (single consumer + per-market latest-wins preserves per-market temporal order; documented in `_analyze_worker` docstring)
- [ ] Decide on N>1 sharding strategy before scaling (open question Q4 in audit) — deferred; documented in worker docstring that sharding by `hash(market_id) % N` is required

**Effort:** M. **Expected gain:** removes head-of-line blocking across markets; restores per-market independent reaction time under burst load.

**Verification next steps (live):** run the bot under load and watch `logs/latency.json` over a soak — the `recv_to_analyze_us` p99 should be dramatically lower than the `analyze_us` p99 (was equivalent before R2 because they ran on the same coroutine).

---

## Problem #2 — `copy.deepcopy` on every book yield

**Where:** `polymarket_client/universal_ws.py` and `polymarket_client/api.py` (R1 in audit).

**What's wrong:**
- Each book snapshot yielded to consumers goes through `copy.deepcopy`, which traverses the entire object graph and reallocates everything. Estimated 100–500 µs per yield based on Python attribute-traversal costs (not benchmarked — see instrumentation gap).
- At 3,235 msg/s observed, that's 0.3–1.6 seconds/sec of pure CPU on deepcopy alone.

**The fix (R1 in audit):**
- `OrderBook` is a shallow dataclass tree. `PriceLevel` is *not* technically frozen (audit detail was wrong) but is de-facto immutable — every "update" in the codebase replaces the slot (`levels[i] = PriceLevel(...)`), never mutates.
- Added `clone()` to `OrderBookSide`, `TokenOrderBook`, and `OrderBook`. New list per side; PriceLevels shared by reference (immutable contract).

**Tasks:**
- [x] Identify all `copy.deepcopy` call sites on the hot path (`polymarket_client/api.py:884`, `polymarket_client/universal_ws.py:925`)
- [x] Replace with manual shallow-clone in both `universal_ws.py` and `api.py`; removed now-unused `import copy`
- [x] Benchmark before/after: **60.8× speedup** (101.5 µs → 1.67 µs per snapshot). At 3,235 msg/s, reclaims ~32% of one CPU core.
- [x] 6 unit tests in `tests/test_models.py` covering list-independence, ref-sharing of PriceLevels, and the slot-replacement mutation patterns from `_apply_book_snapshot` and `_apply_price_change`.

**Effort:** S. **Actual gain:** 60.8× faster snapshot path (audit estimate was conservative).

---

## Problem #3 — Silent WS loss / reconnect

**Where:** `polymarket_client/api.py:600-613` (R3 in audit).

**What's wrong:**
- Reconnect logic exists but doesn't surface the gap to consumers. After a reconnect, downstream code has no way to know "I just missed N seconds of updates for market X."
- Risk: bot acts on a stale `MarketState` because the last update before disconnect is still cached, and `analyze()` happily runs on it.

**The fix (shipped):**
- Age-based staleness model: a state is stale if `(now - state.order_book.recv_mono_ns) > threshold` or if `recv_mono_ns is None`. Works for any source (WS, REST, simulation, future mempool) and self-clears when fresh data arrives — no producer→consumer signaling needed.
- WS reconnect loop with exponential backoff replaces the previous "fall through to REST on first WS hiccup" behavior. Falls back to REST only after 6 consecutive zero-progress failures (~61 s of retry).

**Tasks:**
- [x] Add `MarketState.is_stale(now_mono_ns, max_age_ns)` method in `polymarket_client/models.py`
- [x] Mark stale via timestamp comparison — no explicit "mark all stale" needed; aging out is automatic
- [x] Skip analyze if `state.is_stale(...)` in `_analyze_worker` (both `main.py` and `run_with_dashboard.py`); `_stale_skips` counter surfaced in monitoring log
- [x] WS reconnect loop with backoff in `polymarket_client/api.py::stream_orderbook`; mirrors `universal_ws._shard_supervisor` pattern; logs disconnect duration and gap on reconnect
- [x] 4 new unit tests in `tests/test_models.py` cover None, fresh, stale, boundary

**Effort:** S–M. **Gain:** correctness (eliminates a class of false-positive opportunities on stale data) + protection against WS flapping (transient blips no longer collapse to 5-min REST cadence).

**Threshold:** 30 s (hardcoded as `_STALENESS_THRESHOLD_NS` per entry point). Tune if false-skip rate is high on illiquid markets — for hunting arbs that live for seconds, anything older is already useless.

---

## Mempool Integration — Open Investigation

**Audit finding (Section 4):** mempool ingest is **absent** in this codebase. No `eth_subscribe`, no `web3` in `requirements.txt`, no Polymarket Exchange contract ABI, no Polygon RPC config. Anything claiming "we use mempool data" today is wrong — only WSS (Polymarket CLOB) and REST (Gamma + Kalshi).

**What mempool would add:**
- Pre-confirmation visibility into `fillOrder` / `matchOrders` / `cancelOrder` calls hitting the Polymarket CTF Exchange contract on Polygon.
- WSS = *fact* (post-match, engine-published, ~100% finality). Mempool = *intent* (pending tx, not all land, reorg risk).
- Lead time is the prize: see a likely fill *before* the matching engine publishes it on WSS.

**What it would NOT add (and would corrupt):**
- Anything in this codebase that assumes finality: `core/portfolio.py` (positions, P&L), `core/risk_manager.py` (exposure caps), `core/execution.py` (order state). Naively feeding intent into these is a correctness bug, not a speed win. Mempool signals must stay in a *separate* predictive lane.

**Open questions before building:**
- [ ] Which provider? (Alchemy / bloXroute / Blocknative / Chainstack / QuickNode all expose Polygon mempool with varying coverage and latency. bloXroute and Blocknative typically have the lowest latency due to colocated infra; the others are commodity nodes.)
- [ ] Server-side filtering or client-side? Client-side filtering of full Polygon mempool over the wire is bandwidth + CPU disaster. Must filter to Polymarket contract addresses at the provider.
- [ ] Decoder: need CTF Exchange ABI + per-method decode logic. Add `web3` or `eth-abi` to `requirements.txt`. Decode off the WS thread (recommended pattern — do not deserialize on the recv coroutine).
- [ ] What's the consumer? A new `MempoolSignal` type feeding a *separate* predictive engine, not `ArbEngine`. Keep it isolated from the WSS-canonical pipeline until lead-time and false-positive rate are characterized.
- [ ] Measurement plan: log `(tx_hash, mempool_observed_ts, wss_observed_ts)` triples to a file; compute observed lead time and false-positive rate (txs seen in mempool that never landed or got replaced) over a week of real data before any trading logic depends on it.

**Tasks (not started, sequenced):**
- [ ] Decide provider — get a quote / trial key from bloXroute and Blocknative; compare their advertised Polygon mempool latency.
- [ ] Prototype: standalone script that subscribes to filtered pending txs (Polymarket CTF Exchange address only), decodes them, and logs to JSON. No bot integration yet.
- [ ] Run mempool prototype + bot WSS in parallel for one week; produce lead-time / false-positive histogram from the logs.
- [ ] **Only after that data:** decide if a `MempoolSignal` lane is worth building.

---

## Instrumentation Gap (Problem #0 — Prerequisite)

All latency numbers in the audit are **inferred**, not measured. The bot has no monotonic timestamps on the hot path, so we cannot prove any of the above fixes worked.

**Minimum patch (audit Section 7):**
- [ ] Add `time.monotonic_ns()` timestamp at WS frame receipt in `polymarket_client/api.py` (decode start)
- [ ] Carry it through `MarketState.recv_ts_ns`
- [ ] Stamp again entering `ArbEngine.analyze`, again at signal emission, again at `submit_signal`
- [ ] Log per-stage deltas to a histogram (stdlib counters are fine; no need for Prometheus today)

Do this **first** so subsequent fixes can be proven, not asserted.
