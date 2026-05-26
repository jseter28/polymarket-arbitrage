# Markets View — Plan (LOCKED)

Locked: 2026-05-26.

## Goal

Add two new views to the existing Universal WS soak dashboard at `localhost:8889`:

1. **Markets browser** — sidebar of categories (politics, sports, crypto, …) + a grid of markets in the selected category, sortable by volume / activity / freshness.
2. **Market detail (live)** — drill into one market and see its full live feed: YES/NO order book ladder, recent price-change events, message rate, metadata.

Single-page app with three views, hash-routed.

## Architecture decision

**Hybrid HTTP + WebSocket.**

| Data shape | Primitive | Why |
|---|---|---|
| Aggregate status / charts / shards | HTTP polling (existing) | Broadcast, low-frequency, snapshot-shaped |
| Category list | HTTP GET | One-shot lookup |
| Market list (grid) | HTTP poll every 3s | Snapshot of ~100 markets, refreshes slowly |
| Market detail initial state | HTTP GET (or WS snapshot frame) | One-shot |
| **Live order book + events for one market** | **WebSocket** | Stream-shaped, per-tab subscription, low-latency |

Principle: WebSocket where data legitimately streams. HTTP where it's a current snapshot.

## URL routing

| Hash | View |
|---|---|
| `#status` (default) | Existing dashboard: tiles, charts, shards table, events log |
| `#markets` | Category sidebar + market grid for whatever category is selected |
| `#cat/<tag_slug>` | Same as `#markets`, with `<tag_slug>` selected |
| `#market/<market_id>` | Sidebar + live drill-in for one market |

Top nav has two tabs: **Status** | **Markets**. Browser back-button works for free.

---

## UI layout

```
┌─────────────────────────────────────────────────────────────┐
│ Universal WS Soak              [Status] [Markets]   6h 14m  │  top nav (existing
├──────────────────┬──────────────────────────────────────────┤   header stays in
│ Categories       │                                          │   #status view)
│ ────────────     │   <market grid OR drill-in OR status>    │
│ Sports     230   │                                          │
│ Politics   142   │                                          │
│ Crypto      89   │                                          │
│ Tech        57   │                                          │
│ Climate      6   │                                          │
│ ...              │                                          │
└──────────────────┴──────────────────────────────────────────┘
```

### Market grid

```
Question                        YES    NO   spread  vol24h   last upd  msg/m
Spurs vs. Thunder               0.48   0.52  0.02   $11.8M    0.4s     87
Will BTC hit $150k by Jun 30?   0.34   0.66  0.01   $5.8M     1.1s     62
US-Iran peace deal by May 31?   0.05   0.95  0.01   $4.3M    12.3s      8
```

Sortable columns. Default: volume desc. Rows with `last upd > 60s` rendered muted.

### Drill-in view

```
Spurs vs. Thunder · ends 2026-06-15 17:00 UTC
tags: sports, nba   vol24h: $11.8M   liquidity: $245k

┌─ YES book ──────────┐  ┌─ NO book ───────────┐
│ ask 0.49   $12,420  │  │ ask 0.52   $8,140   │
│ ask 0.48   $24,180  │  │ ask 0.51   $14,200  │
│ ─── 0.475 ───       │  │ ─── 0.505 ───       │
│ bid 0.47   $19,250  │  │ bid 0.50   $22,310  │
│ bid 0.46   $8,440   │  │ bid 0.49   $11,180  │
└─────────────────────┘  └─────────────────────┘
spread: $0.005   implied: 49.0%   msg rate (60s): ▁▂▃▅▆▇▆▅▃▂▁  14/s

Recent events
─────────────
t+6:14:01  YES  bid 0.47  →  0.475   $19,250
t+6:14:01  YES  ask 0.50  →  0.49    $12,420
t+6:13:58  NO   bid 0.50  →  0.495   $22,310
t+6:13:55  YES  trade @  0.475   $1,200
```

Book updates apply directly from incoming WS frames. Recent events prepend; scroll back to see more (capped at 20 in memory per market).

---

## Backend changes

### File: `polymarket_client/universal_ws.py`

**1. Capture tags during fetch.**
- `_fetch_active_markets` currently calls Gamma `/markets`. Add a second call to `/events?active=true&closed=false&limit=500` (paginated) to build an `event_id → tags[]` map.
- Join: each market carries an `eventId`; look up its event's tags.
- Extend the per-market record with: `tag_ids: list[int]`, `tag_slugs: list[str]`, `event_id: str`, `end_date: datetime | None`.
- Markets without a matching event go into the `uncategorized` bucket rather than being dropped.

**2. Per-market metrics state.**
Add to `PolymarketUniversalWS.__init__`:
```
self._market_msg_count: dict[str, int] = {}
self._market_last_msg_at: dict[str, float] = {}    # monotonic
self._market_msg_timestamps: dict[str, deque[float]] = {}  # rolling 60s window
self._market_recent_events: dict[str, deque[dict]] = {}    # last 20 events
self._market_subscribers: dict[str, set[asyncio.Queue]] = {}
```

In `_apply_book_snapshot` and `_apply_price_change`:
- Increment counter
- Update last_msg_at
- Append timestamp; drop entries older than 60s
- Append event dict to recent_events deque (maxlen=20)
- Fan out to subscribers (see WS protocol below)

**3. New public methods.**

```python
def categories(self) -> list[dict]:
    """Return [{"tag_id": 2, "slug": "politics", "label": "Politics", "count": 142}, ...]"""

def markets_by_tag(self, tag_slug: str, sort: str = "volume", limit: int = 100) -> list[dict]:
    """Return market summary rows for the grid. Each row has:
       market_id, question, yes_bid, yes_ask, no_bid, no_ask, spread,
       volume_24h, last_msg_age_s, msg_rate_1min, tags."""

def market_detail(self, market_id: str) -> dict | None:
    """Full snapshot: metadata + full books + recent_events + msg_rate.
       Returns None if market_id not in cache."""

async def subscribe_market(self, market_id: str, queue: asyncio.Queue) -> None:
    """Register a subscriber's queue for events on this market."""

async def unsubscribe_market(self, market_id: str, queue: asyncio.Queue) -> None:
    """Deregister. Idempotent."""
```

### File: `probe_universal_ws_dashboard.py`

**1. New HTTP endpoints (read-only).**

```
GET /api/categories
GET /api/markets?tag=<slug>&sort=<volume|msg_rate|last_update>&limit=<N>
GET /api/markets/{market_id}    # snapshot only — no streaming
```

**2. New WebSocket endpoint.**

```
WS /ws
```

Single WS connection per browser tab. Multiplexed by topic.

**Protocol — browser → server:**
```json
{"op": "subscribe",   "market_id": "2275954"}
{"op": "unsubscribe", "market_id": "2275954"}
{"op": "ping"}
```

**Protocol — server → browser:**
```json
{"type": "snapshot",     "market_id": "X", "data": <full market_detail dict>}
{"type": "book_update",  "market_id": "X", "token": "YES", "bids": [...], "asks": [...]}
{"type": "price_change", "market_id": "X", "token": "YES", "side": "BUY", "price": 0.47, "size": 19000}
{"type": "pong"}
{"type": "error",        "message": "..."}
```

On `subscribe`: server immediately sends a `snapshot` (current full state), then registers the subscriber's queue. From then on, the server pushes `book_update` / `price_change` frames as universal_ws fans them out, until the browser sends `unsubscribe` or disconnects.

**Server-side per-connection state.**
Each WS handler holds:
- An `asyncio.Queue` (bounded, maxsize=500, drop-oldest semantics) that universal_ws's fanout pushes into
- A `set[market_id]` of active subscriptions
- A heartbeat task

On disconnect: unsubscribe all market_ids, close the queue, cancel heartbeat.

**Heartbeat.**
Server expects an `op: ping` from browser every 30s; replies `type: pong`. If no ping in 90s, server closes the connection. Browser-side does the same — if no pong in 90s, reconnect.

### File: `probe_universal_ws_dashboard.py` (HTML/JS — embedded)

**1. Top nav.**
Two tabs that change `location.hash` to `#status` or `#markets`.

**2. Hash router.**
On `hashchange` / `load`: parse hash, show the matching view, hide the others. Three views in DOM (existing status panel + new markets/detail panels), `display:none` on inactive ones.

**3. Sidebar (visible in `#markets` and `#market/X`).**
Fetches `/api/categories` once on first show; refreshes every 60s (counts are slow-moving). Highlights the active slug.

**4. Market grid (visible in `#markets` / `#cat/X`).**
Fetches `/api/markets?tag=<slug>` every 3s. Renders a sortable table. Click a row → set `location.hash = #market/<id>`.

**5. Drill-in (visible in `#market/X`).**
On entry:
- Open WebSocket if not already open
- Send `{"op": "subscribe", "market_id": X}`
- Receive `snapshot` → render initial state
- Apply incoming `book_update` / `price_change` frames to the rendered book

On leave:
- Send `{"op": "unsubscribe", "market_id": X}`
- Keep the WS open for the next market

The WS stays open for the lifetime of the tab.

**6. Browser-side WS client.**
States: `closed → connecting → open → reconnecting`. On `close`, exponential backoff (1s → 30s max) reconnect; on re-open, re-send subscribes for any active market_id. App-level ping every 30s; reconnect on 90s pong timeout.

---

## Phasing

Each phase ships independently.

### Phase 1 — Browse (no live updates yet)
- Tag capture in `_fetch_active_markets`
- `/api/categories` and `/api/markets?tag=X`
- Top nav + sidebar + market grid (3s HTTP polling)
- Acceptance: click "Politics" in sidebar → see all politics markets, sorted by volume, with YES/NO/spread/last-update visible and refreshing

### Phase 2 — Drill-in with live feed
- Per-market metrics state in `universal_ws`
- Per-market subscription bus + fanout
- `WS /ws` endpoint + protocol
- Drill-in view: order book ladder, metadata, msg_rate
- Browser-side WS client + hash routing for `#market/X`
- Acceptance: click a market in the grid → live order book renders, ticks update in real time

### Phase 3 — Polish
- Recent-events stream in drill-in (text list of last 20 price changes)
- msg/sec sparkline (1s buckets, last 60s)
- Sort options in market grid (volume / msg_rate / last_update)
- Stale-market highlighting (last_update > 60s rendered muted)
- Acceptance: drill-in shows full event flow + sparkline; grid is fully interactive

---

## Out of scope

- Per-market trading or order entry (this is observability only)
- Persistence across process restarts (still in-memory only)
- Authentication / multi-user / TLS
- Historical replay or rewind
- Cross-tab subscription deduplication (each tab gets its own WS)
- Real-time updates on the market grid in Phase 1 (HTTP poll is sufficient for this view)
- Tag editing or curation (we use Polymarket's tags as-is)

---

## Risks and mitigations

- **WS connection drops mid-stream.** Browser detects via 90s pong timeout → exponential backoff reconnect → re-send active subscriptions. User sees brief stale book; fresh data resumes within seconds.

- **Subscriber queue overflow.** If a browser tab falls behind, the per-connection queue (maxsize=500) drops oldest. Mirrors the universal_ws upstream queue pattern. No silent data loss in the *book* — book state is always current; only intermediate events may be coalesced.

- **Tag join failures.** Markets whose event_id isn't in `/events` response go into `uncategorized` rather than being dropped. Counts may shift over time as `/events` pagination completes.

- **Memory growth.** Per-market recent-events deques: 20 × 4,900 markets × ~200 bytes ≈ 20MB. Per-market msg timestamps: 60s window × ~14/s peak × 4,900 = ~4M floats = ~32MB worst case. Combined: ~60MB. Comfortable inside the existing memory budget.

- **Sidebar count drift.** Tag counts are computed at boot from cached metadata. New markets that resolve / new markets added during a run won't update the sidebar until restart. Acceptable for a 24h soak window.

- **Polymarket reconnect causes book replacement.** When a shard reconnects, its first frame is a fresh `book` snapshot. Browsers viewing that market see the book replaced wholesale. UI handles this correctly because the snapshot frame is treated as a full replacement, not a patch.

---

## File touch list

| File | Change |
|---|---|
| `polymarket_client/universal_ws.py` | Tag capture in fetch; per-market metrics state; subscription bus; new public methods. |
| `probe_universal_ws_dashboard.py` | New HTTP endpoints + WS endpoint + protocol; expand embedded HTML/JS with nav, sidebar, grid, drill-in, WS client. |

No other files touched. Existing endpoints (`/api/status`, `/api/history`, `/api/events`) keep working unchanged.

---

## Resolved decisions (no more open questions)

- **Live-feed mechanism:** WebSocket
- **Tag fetch source:** `/events` then join by event_id
- **Per-market event retention:** 20 events × 4,900 markets ≈ 20MB; bounded
- **Sort default for market grid:** volume_24h desc; user-selectable in Phase 3
- **Stale threshold:** last_update > 60s renders muted gray
- **WS heartbeat:** 30s ping, 90s pong timeout
- **Reconnect:** exponential backoff 1s → 30s max
