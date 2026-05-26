# Polymarket Feed Pipeline — Latency & Source-Differential Audit

Read-only audit. Scope: the actual feed pipeline in this repo. No mempool ingestion exists; the audit treats Phase 2 as a thought experiment grounded in real downstream consumers.

Probe artifacts referenced: `probe_universal_15min.json` (15 min, 50 shards, 4889 markets — primary observed source), `probe_sharded_50x100.json` (15 min, 50 shards x 100 markets), `probe_top500.json` / `probe_top1000.json` / `probe_top2000.json` (single-connection saturation curve). Wall-clock dates in artifacts are 2026-05-23 / 2026-05-26.

---

## 1. Executive summary

- **No mempool ingestion exists.** No `eth_subscribe`, no Polygon RPC config, no CTF Exchange ABI decode, no provider client. The bot consumes Polymarket CLOB WSS + Gamma REST + Kalshi REST only. Phase-2 differential is reframed as "what mempool would buy you" (Section 4).
- **Bot signal path is WSS-driven, sub-second end-to-end** ([observed]: 3,235 msg/s aggregate ingest in the universal probe, p50 first-message latency 482-924 ms by load tier). But **no monotonic timestamps are written anywhere on the bot's WS read → analyze → place_order path**, so p50/p99 ingest-to-signal is `[no instrumentation]`. The only timing data captured today is "opportunity duration" (how long an arb persisted before prices moved), not detection latency.
- **Top single bottleneck: synchronous, single-consumer fanout.** `DataFeed._stream_orderbooks` `await`s the WS generator and calls `self.on_update` inline (`core/data_feed.py:149-158`). `on_update` synchronously runs `ArbEngine.analyze` on the loop (`main.py:172`, `run_with_dashboard.py:214`). One slow market analysis blocks every subsequent WS message for every other market. This is amplified by deep-copying every yielded book (`polymarket_client/api.py:884`).
- **`universal_ws.py` is not wired into the bot.** Its `iter_updates()` queue (maxsize=10000) reports **240,112 drops** in 15 minutes ([observed], `probe_universal_15min.json:24`), i.e. the consumer is already losing ~8% of wake-ups under the sole consumer `probe_universal_ws_dashboard.py` is providing. If you wire it into `DataFeed` without addressing fanout, drops will worsen.
- **Gamma REST polling is on the hot startup path**, not the steady-state hot path — but the bot's REST fallback (`_stream_rest_orderbooks`, `polymarket_client/api.py:615`) is a 5000-market round-robin with 50 ms inter-request sleep, which is on the order of **5 minutes/cycle** at full universe. Any time the WS exits cleanly or errors (line 605/608), the bot silently switches to this and every downstream latency budget evaporates.

---

## 2. Architecture map

Two ingest paths, two FastAPI dashboards, single shared module (`polymarket_client.models`). The paths do **not** converge — `universal_ws` is consumed only by the probe dashboard.

```
                                Polymarket            Kalshi
                               (Gamma + CLOB)         (REST only)
                                     |                    |
   ┌─────────────────────────────────┼────────────────────┼──────────────┐
   |                                 |                    |              |
   |  BOT PATH (main.py / run_with_dashboard.py)                         |
   |                                                                     |
   |  PolymarketClient.list_markets  ──► REST paginate Gamma             |
   |    polymarket_client/api.py:229   (200ms/page × ~50 pages = ~10s)   |
   |                                                                     |
   |  PolymarketClient._stream_websocket_orderbooks                      |
   |    polymarket_client/api.py:886                                     |
   |    └── 1 connection, top-N by vol24h, cap=750 (config.yaml)         |
   |        ws.recv → json.loads → _apply_book_snapshot / _apply_price_change
   |        → copy.deepcopy(book) → yield                                |
   |                                  │                                  |
   |                                  ▼                                  |
   |  DataFeed._stream_orderbooks      (core/data_feed.py:141)           |
   |    └── self._order_books[mid] = ob   (sync dict write)              |
   |        self._update_market_state(mid)  (line 158)                   |
   |        └── self.on_update(mid, state)  (sync callback, line 210)    |
   |                                  │                                  |
   |                                  ▼                                  |
   |  TradingBot._on_market_update     (main.py:162)                     |
   |  TradingBotWithDashboard._on_market_update  (run_with_dashboard.py:204) |
   |    └── ArbEngine.analyze(state)  (core/arb_engine.py:117)           |
   |        └── _check_bundle_arbitrage / _check_market_making           |
   |            └── if signal: asyncio.create_task(ExecutionEngine.submit_signal)
   |                                  │                                  |
   |                                  ▼                                  |
   |  ExecutionEngine._process_signals   (core/execution.py:130)         |
   |    └── asyncio.Queue → _execute_signal → _place_order               |
   |        └── RiskManager.check_order  (core/risk_manager.py)          |
   |        └── PolymarketClient.place_order  (api.py:967)               |
   |                                                                     |
   |  Side-channel: position polling                                     |
   |  DataFeed._position_refresh_loop   (core/data_feed.py:167)          |
   |    └── REST GET /positions every 5s                                 |
   |                                                                     |
   |  Side-channel: Kalshi (run_with_dashboard.py only)                  |
   |  KalshiClient.list_all_markets + MarketMatcher.find_matches         |
   |    (one-shot text-similarity matching, no live streaming wired to   |
   |    CrossPlatformArbEngine in steady state — matching runs in a      |
   |    ThreadPoolExecutor at startup, run_with_dashboard.py:315-388)    |
   |                                                                     |
   |  Side-channel: bot dashboard fanout                                 |
   |  DashboardIntegration.add_opportunity/add_signal/add_trade          |
   |    (dashboard/integration.py:173-233) — uses asyncio.create_task    |
   |    to broadcast over WS to browser clients. Runs in the bot's       |
   |    asyncio loop (uvicorn server is also in-loop, run_with_dashboard.py:202)
   |                                                                     |
   └─────────────────────────────────────────────────────────────────────┘

   ┌─────────────────────────────────────────────────────────────────────┐
   |  PROBE PATH (probe_universal_ws_dashboard.py — port 8889)           |
   |                                                                     |
   |  PolymarketUniversalWS.start()    (polymarket_client/universal_ws.py:420)
   |    └── _fetch_active_markets  → Gamma /events + /markets paginate   |
   |    └── _partition_round_robin → N shards of ~100 markets / 200 tokens
   |    └── N parallel _shard_supervisor tasks (one WS each)             |
   |         └── _run_shard_session → _shard_recv → _apply_*             |
   |             → _record_market_event / _fanout / _enqueue_market_id   |
   |                                  │                                  |
   |                                  ▼                                  |
   |  PolymarketUniversalWS._queue: asyncio.Queue(maxsize=10000)         |
   |    drop-oldest on full  (universal_ws.py:931-946)                   |
   |                                  │                                  |
   |                                  ▼                                  |
   |  Consumer 1: iter_updates()  →  probe_universal_ws_dashboard.consumer
   |                                  (counts only, line 1545)           |
   |  Consumer 2: per-market subscribers (FastAPI WS clients in          |
   |              markets-view drill-in)  via _fanout (line 792)         |
   |                                                                     |
   |  ** Nothing in this path crosses into core/arb_engine.py. **        |
   └─────────────────────────────────────────────────────────────────────┘
```

Key architectural observations:

- **`DataFeed` and `PolymarketUniversalWS` reimplement the same WS protocol** (book snapshot + price_change delta + token→market mapping). Two copies of `_apply_book_snapshot` / `_apply_price_change`: `polymarket_client/api.py:800` and `polymarket_client/universal_ws.py:816`. Any protocol fix needs to land in both.
- **The CLAUDE.md note about FastAPI in a `threading.Thread`** is stale. `threading` is imported in `run_with_dashboard.py:20` but only the unused `import` remains; uvicorn is started inline as an asyncio task at `run_with_dashboard.py:202`. `DashboardIntegration` correctly uses `asyncio.create_task` (`dashboard/integration.py:190`, `:209`, `:230`) so the "don't await dashboard calls" guidance is moot in practice — they're fire-and-forget on the same loop. This is in fact a *latency risk* (see Bottleneck #4) because dashboard broadcast coroutines compete with `_on_market_update` for loop time.
- **No mempool**. `grep` confirms zero references to `eth_subscribe`, `pendingTransactions`, Alchemy/bloXroute/Blocknative/Chainstack/QuickNode, Polygon RPC URLs, or CTF Exchange contract addresses anywhere in the tree.

---

## 3. Latency profile

All latency cells are labeled `[observed]`, `[inferred]`, or `[no instrumentation]`. Where instrumentation is missing, the proposed insertion point is listed in Section 7.

### 3.1 WSS — bot path (`PolymarketClient._stream_websocket_orderbooks`)

| Stage | Code | p50 | p99 | Label |
|---|---|---|---|---|
| Gamma `/markets` paginate (startup, one-shot) | `polymarket_client/api.py:251-289` | ~10 s (50 pages × 200 ms request + 150 ms sleep) | ~15 s | [inferred] — derived from `await asyncio.sleep(0.15)` at line 283 + httpx default timeouts |
| WS connect + first message | `polymarket_client/api.py:768-781` | **482 ms @ 500 mkts** / **759 ms @ 1000 mkts** / **803 ms @ 2000 mkts (median)** | 924 ms @ 2000 mkts | [observed] — `probe_top500.json:44`, `probe_top1000.json:44`, `probe_top2000.json:65-78` |
| WS server send → client recv | network | ~50 ms | ~150 ms | [inferred] — typical TCP/TLS over residential/cloud |
| `ws.recv()` → `json.loads` | `polymarket_client/api.py:903-911` | sub-ms typical | unknown — frames can be lists of ~10 deltas; large book snapshots are KB-scale | [no instrumentation] — patch site: line 909 (timestamp after `json.loads`) |
| `_apply_book_snapshot` / `_apply_price_change` (state mutation + sort) | `polymarket_client/api.py:800-880` | sub-ms | sort is O(N log N) per side, levels capped implicitly by Polymarket | [no instrumentation] — patch site: line 837 (after combined book mutated) |
| `copy.deepcopy(self._ws_combined_books[market_id])` | `polymarket_client/api.py:884` (called from 921, 924) | **~100-500 μs** per yield | scales with book depth; runs on **every** book + price_change yield | [inferred] — `copy.deepcopy` of nested dataclasses is dominantly attribute traversal cost. Recommended bench. |
| `DataFeed._stream_orderbooks` recv yield → callback | `core/data_feed.py:149-210` | sub-ms (sync dict writes + datetime.utcnow) | sub-ms | [no instrumentation] — patch site: line 154 (after `_last_update[market_id] = now`) |
| `ArbEngine.analyze` (synchronous on loop) | `core/arb_engine.py:117-142` | sub-ms typical (a few `_check_*` calls + 2 dict lookups) | unknown — `_check_expired_opportunities` iterates `self._active_opportunities` (line 149) | [no instrumentation] — patch site: line 117 / 142 (wrap analyze in monotonic block) |
| `asyncio.create_task(ExecutionEngine.submit_signal)` → queue.put | `main.py:177`, `core/execution.py:127` | sub-ms (asyncio.Queue.put is constant) | sub-ms | [no instrumentation] |
| ExecutionEngine signal queue wait (1.0 s timeout per iteration) | `core/execution.py:136-141` | up to **1.0 s** when queue idle (waits for timeout) | 1.0 s | [observed] — `asyncio.wait_for(... timeout=1.0)`. Note: not a wait on the *current* signal, but bounds re-check granularity. When queue is non-empty, dispatch is immediate. |
| `_place_order` round-trip (live mode, POST /order) | `core/execution.py:253` → `polymarket_client/api.py:1013` | network-bound; CLOB POST ~100-300 ms in normal conditions | not measured | [inferred] — Polymarket CLOB published SLAs; not instrumented here |

**End-to-end book-change-event → place_order POST sent:**
- p50: **`[no instrumentation]`** — but bottom-up arithmetic from observed pieces (recv 50 ms + json/parse <1 ms + deepcopy ~0.3 ms + analyze ~0.5 ms + queue ~0 + place 200 ms) ≈ **~250 ms p50**. [inferred].
- p99: unknown — dominated by single-consumer head-of-line blocking (Bottleneck #1). Almost certainly multi-second under burst load. [inferred]

### 3.2 WSS — universal path (`PolymarketUniversalWS`)

| Stage | Code | Value | Label |
|---|---|---|---|
| Gamma `/events` paginate | `polymarket_client/universal_ws.py:144-218` | ~15 s for ~100 pages @ 150 ms each | [inferred] |
| Gamma `/markets` paginate | `universal_ws.py:238-345` | ~15 s for ~50 pages | [inferred] |
| Total startup before first WS frame | `universal_ws.py:420-479` | **~30-45 s** (sequential) | [inferred] |
| Per-shard WS connect + first msg | `universal_ws.py:1035-1063` | bounded by 100 mkts/shard → falls in the 500-mkt regime, **~500 ms median** | [inferred] from `probe_top500.json:44` (482.8 ms @ 1000 tokens / 500 mkts ≈ 1 shard) |
| Aggregate ingest rate (50 shards × 100 mkts) | `universal_ws.py:_shard_recv` | **3,235 msg/s** sustained over 927 s, **1.84 GB** received | [observed] — `probe_universal_15min.json:18` |
| Aggregate yield rate from `iter_updates` | `universal_ws.py:498-523` | **2,967 yields/s** | [observed] — `probe_universal_15min.json:25` |
| Drop rate on `_queue` (10000-deep, drop-oldest) | `universal_ws.py:931-946` | **240,112 drops over 927 s = 259 drops/s = 8.0% of yields lost** | [observed] — `probe_universal_15min.json:24,25` |
| Final queue depth | | **10000 / 10000** (saturated at run end) | [observed] — `probe_universal_15min.json:27` |
| Session uptime distribution (15 min run, 50 shards) | `universal_ws.py:_run_shard_session` | 46/50 shards: 1 session for full 927 s. 4 shards reconnected once or twice (ConnectionClosed 1006). Zero quarantined. | [observed] — `probe_universal_15min.json:20-23,46-797 (per-shard "session_count")` |
| Sharding sweet spot | comparison `probe_top500.json` vs `probe_top2000.json` | **At 2000 markets / 4000 tokens on a single connection: 12 reconnects in 600 s, mean uptime 49 s** ([observed] `probe_top2000.json:13-38`). At 500 mkts / 1000 tokens: 1 session, 600 s, no drops. **Per-connection instrument cap confirmed at ~2000 markets / 4000 tokens** ([observed]). Stay below. |
| `_record_market_event` per-market metrics update | `universal_ws.py:768-790` | sub-ms (dict ops + deque append + `popleft` while loop on 60s window) | [no instrumentation] |
| `_fanout` to FastAPI subscriber queues | `universal_ws.py:792-814` | sub-ms per subscriber; **bounded queue with drop-oldest semantics**, counter `_subscriber_drops` exposed but not in `status()` | [no instrumentation] |

### 3.3 Gamma REST polling (bot fallback)

`PolymarketClient._stream_rest_orderbooks` (`polymarket_client/api.py:615-697`) is the silent fallback when WS exits or errors (line 605, 608). Settings (line 636-640):

| Parameter | Value | Source |
|---|---|---|
| `active_batch_size` | 500 markets per rotation | `api.py:636` |
| `markets_per_request_batch` | 20 | line 637 |
| `request_delay` | 50 ms between CLOB `/book` calls | line 638 |
| `batch_delay` | 300 ms between 20-market batches | line 639 |
| `rotation_delay` | 2.0 s after full 500-market batch | line 640 |

**Time per full 5000-market cycle** = `(5000/500 rotations) × ((500/20 batches) × (20 × 50 ms + 300 ms) + 2 s)` ≈ `10 × (25 × 1.3 + 2)` ≈ **345 s ≈ 5.75 min**. [inferred from explicit `asyncio.sleep` values]. Every market in the tail of the rotation sees **multi-minute staleness**. Two CLOB requests per market (YES + NO tokens, line 665-666) — so the effective per-market refresh interval is double if both legs are fetched serially. Currently they are, line 665-666.

**This fallback is silent** — line 605/608 log warnings but the bot keeps running. There is no metric exposing "we are in REST fallback mode" to the dashboard or RiskManager. If a hostile network event drops the WS and the bot transitions to REST, every arb opportunity goes to ~5 min staleness without alarming. **Bottleneck #5 in Section 5.**

### 3.4 Kalshi REST polling

`KalshiClient.stream_orderbooks` (`kalshi_client/api.py:395-430`):

| Parameter | Value |
|---|---|
| `batch_size` | 100 markets per parallel fetch |
| Per-batch | `asyncio.gather([get_orderbook_unified(t) for t in batch])` |
| `rotation_delay` | 2.0 s between batches |

For ~5000 Kalshi markets at 100 per parallel batch + 2 s delay: `(5000/100) × 2 s` = **100 s/cycle minimum**, assuming each parallel batch completes in <2 s. **Note:** `_start_kalshi_monitoring` (`run_with_dashboard.py:259-313`) **never starts `stream_orderbooks`**. It calls `list_all_markets` once and then runs a one-shot `MarketMatcher.find_matches` in a `ThreadPoolExecutor` (line 315-388). The `CrossPlatformArbEngine` exists but no continuous Kalshi-orderbook feed is wired into it; `cross_platform_arb.py:CrossPlatformArbEngine` is instantiated but its detect loop is dormant in steady state. **This is a logic gap, not a latency gap** — the cross-platform path is structurally incomplete, so reasoning about its p50 is premature.

---

## 4. Source differential — mempool ABSENT

**Finding: mempool ingest does not exist in this repo.** Grep confirms:

- No `eth_subscribe`, `eth_sendRawTransaction`, `newPendingTransactions`, `pendingTransactions` calls.
- No web3.py, web3 import, or any RPC provider client (Alchemy / bloXroute / Blocknative / Chainstack / QuickNode).
- No CTF Exchange contract address (`0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` or the proxy), no UMA optimistic-oracle decode, no `fillOrder` / `matchOrders` / `cancelOrder` ABI.
- No Polygon RPC URL in `config.yaml`, `config.live.yaml`, or `requirements.txt`. `requirements.txt` has `websockets`, `httpx`, `fastapi`, `uvicorn`, `pyyaml` — no `web3`, no `eth-abi`.

What the bot consumes today is the **CLOB-engine-published feed**, which is *post-match*. That means by the time a `book` snapshot or `price_change` event arrives over WSS, the order has already been matched (or placed, or cancelled) on Polymarket's matching engine. The bot reacts to facts, not intents.

### 4.1 Comparison

| Dimension | Current WSS feed | Hypothetical Polygon mempool feed |
|---|---|---|
| Semantic | **Post-engine fact**: book state after matching | **Pre-confirmation intent**: a tx broadcast to mempool, not yet mined, not yet matched on-chain |
| Earliest signal | After Polymarket's matching engine publishes | When a `fillOrder` / `cancelOrder` tx is gossiped — milliseconds before inclusion in a Polygon block |
| Coverage | 100% of matched book changes (modulo `iter_updates` drops, §3.2) | Only on-chain CTF Exchange interactions; doesn't see CLOB internal matching that happens off-chain before settlement |
| Finality | Effectively final — book reflects matched orders | **Non-final**: txs can be reverted, frontrun, replaced (RBF analogues on EVM are nonce-based; tx replacement via higher gas price is normal). Order of execution depends on validator inclusion |
| Failure modes | Connection drop (ConnectionClosed 1006 — observed in §3.2), book snapshot/delta ordering races (`universal_ws.py:878-879` already handles deltas-before-snapshot by dropping) | Tx pool node lag; reorgs (rare on Polygon but real); spam txs; private-mempool routing (Polygon RPC providers vary on what they expose); ABI version skew when contract upgrades |
| Decoding burden | JSON over WSS — already parsed end-to-end | Raw tx → RLP-decode → ABI-decode method selector → decode `(token_id, taker_amount, ...)`. Requires CTF Exchange ABI committed in repo and kept in sync. |
| Cost | Free (public WS) | Provider fees ($100s-$1000s/month for stable mempool streams from bloXroute / Chainstack / Alchemy supernode). Latency varies wildly by provider |
| Stale-read cost | A late book event tells you what the engine already did — you can't act on it | A pending tx tells you what is *about* to happen — but if it doesn't mine, you've placed a trade against a fiction |

### 4.2 Which downstream consumers would benefit vs be miscorrupted

Looking at the actual `Opportunity` consumers in this repo:

**Would benefit** (purely reactive, latency-sensitive, can tolerate occasional false positives):
- `ArbEngine._check_bundle_arbitrage` (`core/arb_engine.py:276-410`). If a mempool feed signaled "large buy on YES coming," the bundle-long detector could *pre-quote* — place buy NO before the YES side compresses. But this requires changing the abstraction: today the engine consumes `MarketState`, not `IntentEvent`. New plumbing.
- `ArbEngine._check_market_making` (`arb_engine.py:465-574`). MM is the textbook use case for intent visibility: pull/replace quotes if a sweeping market order is incoming. Same plumbing concern.

**Would be miscorrupted** (assume finality, would book P&L from non-final events):
- `Portfolio` (`core/portfolio.py`). It tracks position deltas from `Trade` objects (see `_update_simulated_position` in `polymarket_client/api.py:1170-1201`). If a mempool intent caused us to update position state and the tx then reverted, P&L would drift permanently. Position must only update on confirmed fills.
- `RiskManager` (`core/risk_manager.py`). Reads PnL via `update_pnl()` in `main.py:196-199` and `_monitoring_loop`. Any non-finalized exposure leaking in would falsely trip exposure caps or daily-loss kill switch.
- `ExecutionEngine.handle_fill` (`core/execution.py`). Anchored on `Trade` objects with `fee` set (`api.py:1138-1153`). Speculative pre-fills would have no fee info and would poison execution stats.

**Conclusion for Phase 2 framing**: adding mempool would require a **strict separation** between `IntentEvent` (mempool, advisory, can revert) and `Trade` (CLOB, fact, used for accounting). The current `Opportunity → Signal → Order → Trade` pipeline is tightly coupled and has no such separation. A naive integration that injects intents as if they were trades will corrupt P&L and risk. The minimum honest engineering cost is:

1. New `IntentEvent` type alongside `MarketState`, surfaced separately to detectors.
2. A new detector channel (alongside `_check_bundle_arbitrage`) that consumes intents to drive *quote management* (pull, replace, pre-position) — but never to update `Portfolio` or `RiskManager.update_pnl`.
3. ABI decoder + provider client + reorg-aware "intent expiry" (drop intents not mined within N seconds).
4. Stale-read guard: if WSS goes stale (`get_staleness` exists at `core/data_feed.py:253`) **and** mempool is hot, the bot is flying blind on confirmation; need explicit posture for that.

Given the current single-consumer fanout bottleneck (§5 Bottleneck #1), adding a second high-rate ingest before fixing fanout will compound the head-of-line problem. **Recommendation: solve fanout first.**

---

## 5. Bottlenecks ranked

| # | Location | Observed cost | Difficulty | Expected gain |
|---|---|---|---|---|
| 1 | **Single-consumer fanout: synchronous `ArbEngine.analyze` on the WS recv coroutine** — `core/data_feed.py:210` calls `self.on_update` sync, which in `main.py:172` runs `arb_engine.analyze(market_state)` inline. Every market's analysis serializes behind every other market. | `[no instrumentation]` — but with 3,235 msg/s observed (universal probe) and analyze being O(active_opportunities) per call, a slow market with many tracked opportunities head-of-line-blocks every other market. | M | **Largest gain.** Decouple via `asyncio.Queue` between WS recv and analyze, or shard analyze across workers. Restores per-market independence under burst load. |
| 2 | **`copy.deepcopy` on every yielded book** — `polymarket_client/api.py:884` (bot path) and `polymarket_client/universal_ws.py:923` (probe path). Called on every `book` snapshot and every `price_change` (lines 921, 924, 1003, 1005). | `[inferred]` ~100-500 μs per yield, dominated by attribute traversal. At 3000 yields/s in the universal path, this is 300-1500 ms/s of CPU. | S | Replace `copy.deepcopy` with a hand-written shallow constructor that copies the `PriceLevel` list (`list(side.levels)` is O(N), enough for read-only consumer). Realistic 5-10× speedup on the snapshot path. |
| 3 | **`universal_ws._queue` drops at 8% under sole consumer** — `polymarket_client/universal_ws.py:931-946`, `probe_universal_15min.json:24-25`: 240,112 drops / 2,752,497 yielded = 8.0%. Queue maxes at 10000 and saturates (final depth = 10000). The drop-oldest is on the *wake-up* notification, not the book state — but consumers that need event-per-update (the probe dashboard's per-market WS) miss events. | `[observed]` 259 drops/sec sustained. | M | Three options: (a) raise `queue_maxsize`, but that just buys time; (b) make consumers faster (Bottleneck #2 reduces consumer pressure); (c) replace drop-oldest with per-market coalescing (only the most recent `market_id` per market needs to be live). Option (c) is the right fix — `set[str]` + `asyncio.Event` instead of `Queue[str]`. |
| 4 | **Dashboard broadcast on every `add_opportunity` / `add_signal` / `add_trade`** — `dashboard/integration.py:190`, `:209`, `:230`. Each call spawns `asyncio.create_task(dashboard_state.broadcast(...))` which iterates connected browser WS clients and sends JSON. Runs in same loop as `_on_market_update`. | `[no instrumentation]` — likely sub-ms with 0-2 dashboard clients; scales linearly with browser tabs. | S | Coalesce: have `DashboardIntegration._update_loop` (line 71, already runs every 1.0 s) be the sole broadcaster. Add to a buffer in the sync methods, flush in the loop. Removes loop interruption on every opportunity. |
| 5 | **Silent REST fallback when WS exits** — `polymarket_client/api.py:601-613`. On WS clean exit *or* exception, falls through to `_stream_rest_orderbooks` which has a ~5-minute cycle time per the explicit sleep values at line 636-640. No metric, no dashboard alarm, no risk-manager flag. | Cycle time `[inferred]` ~345 s for 5000 markets. WS-loss frequency `[observed]` rare at <750 markets (probe_top500/1000 show 0 reconnects in 600 s), but at higher load the ConnectionClosed 1006 is common (probe_top2000 = 12 in 600 s). | M | Add a `_in_rest_fallback: bool` flag, surface in `DashboardIntegration._update_state`, and trip a risk-manager pause if active for >30 s. Honest fix: when WS dies, **reconnect** rather than fall through; only use REST as cold-start. The current code can never return to WS within a single session. |
| 6 | **`json.loads` in the WS read coroutine** — `polymarket_client/api.py:909`, `universal_ws.py:991`. Standard lib `json.loads` on every frame, no streaming, no `orjson`. | `[no instrumentation]`. Universal probe received **1.84 GB / 927 s = 2.0 MB/s**, ~3000 msg/s, average frame ≈ 670 bytes. `json.loads` on 670 bytes is ~10-30 μs. At 3000/s = 30-90 ms/s of CPU. | S | `pip install orjson` (already common); drop-in 2-5× faster. Modest aggregate gain but cheap. |
| 7 | **`_check_expired_opportunities` linear scan** — `core/arb_engine.py:144-181`. Every `analyze()` call iterates `self._active_opportunities` (line 149) checking timing on each entry. Currently bounded but unbounded in principle if opportunities pile up faster than expiry. | `[no instrumentation]`. For typical low-dozens of active opportunities this is sub-ms. With 100s of active arbs (high burst) it competes with the per-market analysis cost. | S | Index `_active_opportunities` by `market_id`, only scan the slice on each callback. Trivial. |
| 8 | **`_stream_rest_orderbooks` fetches YES and NO books serially** — `polymarket_client/api.py:665-666`. Two awaits where one `asyncio.gather` would parallelize. | `[inferred]` doubles the fallback cycle time. | S | `await asyncio.gather(...)`. Two-line fix when in this fallback. |
| 9 | **GC churn from per-yield `OrderBook` deep copies + `MarketState` dataclass instantiation** — `core/data_feed.py:197-203` builds a fresh `MarketState` dataclass on every update. | `[no instrumentation]`. | M | Reuse a thread-local `MarketState` or pass field references. Premature without measurement. |
| 10 | **No backpressure surface between `DataFeed` and `ArbEngine`** — if `analyze` slows, the WS reader blocks (Bottleneck #1) but there is no `[backpressure_observed_ms]` metric. | `[no instrumentation]`. | M | After fixing #1, add a queue-depth metric so this stays visible. |

---

## 6. Recommendations

Top 3, ordered by gain/effort:

### R1. Replace `copy.deepcopy` on book yields (Bottleneck #2)

**File:** `polymarket_client/api.py:884` and `polymarket_client/universal_ws.py:923-925`.

```python
def _snapshot_orderbook(self, market_id: str) -> OrderBook:
    return copy.deepcopy(self._ws_combined_books[market_id])
```

Replace with a manual constructor that copies `levels` lists only (PriceLevel is a frozen dataclass; the OrderBook dataclass tree is shallow). Expected: **5-10× faster snapshot path**, immediately reduces CPU on both bot and universal paths. Effort: S (single-day change, both files).

### R2. Decouple WS recv from `ArbEngine.analyze` via an `asyncio.Queue` (Bottleneck #1)

**Files:** `core/data_feed.py:141-165` (producer), `main.py:162-177` / `run_with_dashboard.py:204-232` (consumer).

Today: `_stream_orderbooks` calls `self.on_update(...)` synchronously (line 210), which runs `analyze` on the WS coroutine. Replace with a bounded `asyncio.Queue[tuple[str, MarketState]]` written by `_stream_orderbooks` and drained by N analyzer tasks (start with N=1 to verify correctness, then scale). Use per-market coalescing on the producer side (only the latest `MarketState` per market matters for arb detection).

This is the single biggest latency-recoverability change. With it, a slow `_check_market_making` on market X no longer head-of-line-blocks book updates for markets Y, Z. Effort: M (must verify ordering semantics — `_check_expired_opportunities` assumes ordered updates per market).

### R3. Make WS-loss visible and recovered, not silent (Bottleneck #5)

**File:** `polymarket_client/api.py:600-613`.

```python
if self.use_websocket:
    try:
        async for item in self._stream_websocket_orderbooks(market_ids):
            yield item
        logger.warning("WS stream exited without error; falling back to REST polling")
    except Exception as e:
        logger.warning(...)
async for item in self._stream_rest_orderbooks(market_ids):
    yield item
```

Two specific changes:
1. **Loop, don't fall through:** wrap the WS branch in a reconnect loop with backoff (mirror `universal_ws.py:_shard_supervisor`, line 1146). WS-loss should be a transient blip, not a permanent regression to 5-min staleness.
2. **Expose state:** add `self._mode: Literal["ws", "rest"]` to `PolymarketClient`, expose via `DataFeed`, surface in `DashboardIntegration._update_state` (`dashboard/integration.py:84`). If `_mode == "rest"` for >30 s, `RiskManager` should pause new orders (current code happily places at 5-min-stale prices).

Effort: M. High asymmetric value — protects against the failure mode where the bot looks healthy but is trading on multi-minute stale data.

---

## 7. Instrumentation gaps — minimum patch set

The latency profile in §3 is mostly `[no instrumentation]` on the WSS bot path. The minimum patch to make p50/p99 measurable end-to-end:

| Patch site | What to add | Why |
|---|---|---|
| `polymarket_client/api.py:909` (after `data = json.loads(raw)`) | `recv_mono = time.monotonic()` attached to msg dict or yielded tuple | Anchor T0 for "WS-frame received" |
| `polymarket_client/api.py:837` (inside `_apply_book_snapshot`, after `combined.timestamp = ...`) | record `apply_mono = time.monotonic()`; histogram `apply_mono - recv_mono` | Cost of snapshot apply |
| `polymarket_client/api.py:921` (after `yield (mid, self._snapshot_orderbook(mid))`) | record `yield_mono`; histogram `yield_mono - apply_mono` | Cost of deepcopy + yield |
| `core/data_feed.py:154` (after `self._last_update[market_id] = datetime.utcnow()`) | record `feed_mono`; histogram `feed_mono - yield_mono` (need to pass yield_mono through, e.g. by changing the yield tuple to `(mid, OrderBook, recv_mono)`) | DataFeed dispatch overhead |
| `core/arb_engine.py:117` and `:142` | wrap `analyze` body in monotonic block; histogram per market_id | Detector cost, isolates which markets are slow |
| `core/execution.py:127` (in `submit_signal`) | record `enqueue_mono`; histogram from upstream recv_mono | End-to-end signal-time |
| `core/execution.py:151` (in `_execute_signal`) | record `dequeue_mono`; histogram `dequeue_mono - enqueue_mono` | Signal queue dwell |
| `core/execution.py:253` (in `_place_order`, before/after the network call) | record `placed_mono`; histogram of place latency | API-side latency |

Aggregate via a tiny `Histogram` accumulator written to a JSON file every N seconds (mirror `probe_universal_ws_dashboard.HistoryRecorder` pattern), or push into `DashboardIntegration` if you want live visibility.

For `universal_ws.py`, the equivalent insertion points are:
- `universal_ws.py:977` (after `last_msg_box[0] = time.monotonic()` — already taken, just needs to be propagated)
- `universal_ws.py:1001` / `:1005` (after enqueue): tag latency on the way out

The `_subscriber_drops` counter at `universal_ws.py:406, 810, 814` is **not exposed in `status()`** (`status()` returns `_drops` only, line 542). One-line fix.

Also: `DataFeed.get_staleness(market_id)` exists at `core/data_feed.py:253` but is **not called by anyone in the bot**. Wire it into the monitoring loop or risk manager as a stale-data check.

---

## 8. Open questions

1. **Was the 8% drop rate in `probe_universal_15min.json` consumer-bound or producer-bound?** The probe consumer only counts; if the consumer is fast (just `consumer_counter[0] += 1` at `probe_universal_ws_dashboard.py:1548`), then drops are pure produce-rate-exceeds-yield-rate. Or the FastAPI per-market subscriber fanout (lines 792-814) is what's slowing the loop. Need a clean test: run the probe with subscribers explicitly disabled to bisect.
2. **Is the per-connection cap actually instrument count or message rate?** CLAUDE.md states instrument count (~500/conn). `probe_top2000.json` shows 4000 tokens = 12 reconnects in 600 s, mean uptime 49 s — confirms a cap somewhere around 2000-4000 tokens. But is the cap fixed or load-dependent (high-volume tokens push you over earlier)? The 50×100-market shard runs hold for full 900 s; that's enough evidence to keep sharding, but the *exact* cap isn't established.
3. **What's the actual variance in book-update arrival between Polymarket's matching-engine-emit and our WS receive?** Polymarket doesn't publish a server-side timestamp in WS messages (or this repo doesn't decode it — confirm by reading raw frames). Without it, "WS latency" is a one-sided measurement.
4. **Does `_check_expired_opportunities` correctness depend on every market update arriving in order?** R2 (queue between feed and analyze) reorders if multiple analyzer workers exist. The function uses `datetime.utcnow()` for expiry, so per-market temporal order matters. Confirm before scaling N>1.
5. **Should the bot use `PolymarketUniversalWS` as its `DataFeed` backend?** Today's `DataFeed` uses single-WS via `PolymarketClient`, capped at 750 markets (`config.yaml`). The universal sharded path is proven stable to 5000 markets but with 8% drop rate. Migrating is non-trivial — `universal_ws` doesn't expose the `on_update(market_id, MarketState)` semantic, only `iter_updates()` + `get_book()`. A `DataFeed` wrapper around `PolymarketUniversalWS` is plausible but materially changes the per-market refresh cadence semantics.
6. **Is the `MarketMatcher.find_matches` ThreadPoolExecutor block** (`run_with_dashboard.py:315-388`) **competing for the GIL with the bot's asyncio loop?** Python's GIL means CPU-bound matching on a thread does block other Python code intermittently. With matching running for minutes at startup, this could spike `analyze` latency. Profile under live conditions.
7. **`requirements.txt` references `web3` or any RPC client?** Re-verify; my grep found none. If mempool work is planned for Phase 2, the dependency set is currently zero-prepared — `web3.py` + `eth-abi` + provider SDK would all be new additions.
