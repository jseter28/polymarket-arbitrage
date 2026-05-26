# Polymarket WebSocket Research

## TL;DR

The Polymarket CLOB market WebSocket has **no documented rate limit, asset_ids cap, or connection cap**, but a community-confirmed **undocumented practical limit of ~500 instruments per connection** exists: above it, the server stops sending initial book snapshots and (per multiple bug reports) the connection later closes abnormally with code **1006** after roughly 15–30 minutes. Production-grade adapters (NautilusTrader, `@nevuamarkets/poly-websockets`) deal with this by **sharding subscriptions across multiple connections** (NautilusTrader's default is **200 per connection**, configurable up to 500) and adding a **data-inactivity watchdog** (~120 s) that force-reconnects, since the application-level PING/PONG keeps reporting "healthy" even after the stream silently freezes. Cloudflare sits in front of the endpoint and enforces a ~100 s idle timeout, which is the most-cited mechanical reason heartbeats are necessary at all.

The bot's observed pattern (100 tokens stable, 1000–4000 tokens dropping at 42–49 s mean uptime with code 1006) is **consistent with the 500-per-connection ceiling** plus the silent-freeze bug — at 1000+ tokens on a single socket, you are above the practical cap, and a single overloaded socket can fail much faster than the 15–30 min that careful 250-per-conn users report.

---

## Official API Reference

Polymarket exposes four WS channels (all from `docs.polymarket.com/market-data/websocket/overview.md`):

| Channel | URL | Auth |
|---|---|---|
| Market | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | No |
| User | `wss://ws-subscriptions-clob.polymarket.com/ws/user` | Yes |
| Sports | `wss://sports-api.polymarket.com/ws` | No |
| RTDS | `wss://ws-live-data.polymarket.com` | Optional |

This bot uses the **Market** channel. The canonical reference is `https://docs.polymarket.com/api-reference/wss/market.md` plus the AsyncAPI spec at `https://docs.polymarket.com/asyncapi.json` (29 KB JSON).

**Documented subscription fields** (from `api-reference/wss/market.md`):

- `assets_ids` (string[]) — token IDs to subscribe to
- `type` (string) — must be `"market"`
- `initial_dump` (bool, default `true`) — "Whether to send an initial orderbook snapshot on subscribe."
- `level` (int, default `2`, values 1/2/3) — subscription depth granularity
- `custom_feature_enabled` (bool, default `false`) — enables `best_bid_ask`, `new_market`, `market_resolved`

**Documented message types received**: `book`, `price_change`, `last_trade_price`, `tick_size_change`, plus the three custom-feature events and `PONG`.

---

## Subscription Mechanics

**Initial subscribe frame** (current format, unchanged across the docs and all observed clients):

```json
{"assets_ids": ["<token_id_1>", "<token_id_2>"], "type": "market"}
```

The bot uses exactly this; it is correct.

**Adding subscriptions after connect.** The docs state that after the initial subscribe, "clients can subscribe and unsubscribe to asset_ids by sending subscription messages." The TS reference client `poly-websockets` flushes pending subscription deltas every 100 ms (`DEFAULT_PENDING_FLUSH_INTERVAL_MS = 100ms`), so dynamic add/remove is supported on a live socket. NautilusTrader instead spins up additional sockets once the per-connection cap is reached.

**One condition_id = two token_ids.** As `api-reference/wss/market.md` warns for the User channel: "use condition IDs (market identifiers), not asset IDs. Each market has one condition ID but two asset IDs." For the Market channel you do want the asset/token IDs (YES and NO), which is what the bot does.

**Changelog history** (from `docs.polymarket.com/changelog`):
- **May 28, 2025**: "The 100 token subscription limit has been removed for the Markets channel. You can now subscribe to as many token IDs as needed for your use case." (Note: one community guide misdates this as "January 2026" — the official changelog says May 28, 2025.)
- **May 28, 2025**: `initial_dump` field added.
- **Sep 15, 2025**: `price_change` payload structure changed.

---

## Heartbeat & Connection Lifecycle

**Officially documented protocol** (`market-data/websocket/overview.md` and the AsyncAPI spec):

> "Send `PING` every 10 seconds. The server responds with `PONG`."

The AsyncAPI spec is explicit that the payload is the literal string `"PING"` (text frame), not a WebSocket-protocol-level ping frame:

```json
"ping":  { "payload": { "type": "string", "const": "PING" } },
"pong":  { "payload": { "type": "string", "const": "PONG" } }
```

So **the bot's current "send text `PING` every 10 s, expect text `PONG`" is correct and matches the spec.** Using `websockets.connect(..., ping_interval=None, ping_timeout=None)` to suppress the library's protocol pings (as the bot does, per the prior research notes) is the right call.

**Server-initiated closes — documented behavior:** "Send a valid subscription message immediately after connecting. The server may close connections that don't subscribe within a timeout period." No other formal timeout is documented.

**Undocumented but well-attested:** the endpoint is fronted by Cloudflare, which enforces a **~100 second idle timeout** on WebSockets — this is the mechanical reason a 10 s heartbeat is necessary. (Cloudflare community confirmation, e.g. `community.cloudflare.com/t/websockets-keep-disconnecting-for-sites-proxied-via-cloudflare/639858`.)

---

## Limits (Documented and Undocumented)

| Limit | Status | Value | Source |
|---|---|---|---|
| Assets per subscription (formal) | Documented | None / unlimited (since May 28 2025) | `docs.polymarket.com/changelog` |
| Assets per *connection* (practical) | **Undocumented, community-confirmed** | **~500 instruments**, after which initial snapshots stop arriving | NautilusTrader docs; `agentbets.ai/guides/polymarket-websocket-guide/` |
| Connections per IP | Not documented | Unknown — no reports of an explicit cap | — |
| Payload size | Not documented | Unknown | — |
| Inbound message rate | **Developer confirms no rate limit** | n/a | (project context) |
| REST rate limits (CLOB) | Documented | 9,000 req / 10 s general | `docs.polymarket.com/quickstart/introduction/rate-limits` |
| Idle timeout (front door) | Inferred (Cloudflare default) | ~100 s | Cloudflare community |
| Heartbeat | Documented | text `PING` every 10 s | AsyncAPI spec |
| Subscribe-timeout grace | Documented vaguely | "may close" if no subscribe sent | overview.md |

**The single most consequential undocumented limit is the 500-instrument practical cap per connection.** Above it, subscriptions succeed (the server ACKs and returns `[]`) but `book` snapshots are silently dropped and the connection later closes 1006.

---

## The 1006 Disconnect Problem

This is the central question. Direct evidence:

### `Polymarket/py-clob-client` issue #292 — "CLOB WSS: Server accepts connection + subscription but sends no book data (silent freeze)" (opened Mar 5 2026)

Reporter setup: ~250 tokens per connection across 6 connections (~1,268 tokens total). Symptoms:

- TCP connects, subscribe is ACKed, app-level PING/PONG keeps working.
- Server replies to subscribe with `[]`, then sends zero `book` or `price_change` events.
- Connection drops with **code 1006** after 15–30 minutes of silence.
- Reporter implemented a 120-second silence watchdog as a workaround.
- The reporter explicitly asks: "Is there a maximum number of `assets_ids` per connection that is officially supported?"
- **No maintainer response.** The repo was archived May 11 2026.

### `Polymarket/real-time-data-client` issue #26 — "WebSocket data stream stops after some time"

Even on the RTDS endpoint (different host, `ws-live-data.polymarket.com`):
- 14–30 msg/s for 18–22 minutes, then complete silence.
- Connection state remains `OPEN`, ping/pong continues (99.6 % success).
- 37.5-minute session: 31,754 messages then nothing.
- Tried `ws`, `isomorphic-ws`, TCP `setNoDelay` / `setKeepAlive` — all behave the same.
- Workaround: 30-s data-inactivity check with 5-min threshold → force reconnect.
- No maintainer response visible.

### `Polymarket/rs-clob-client` issue #185 — "Websocket reconnection mechanism isn't working" (Jan 13 2026)

User reports the built-in reconnect path is broken. No maintainer reply; repo archived May 11 2026.

### NautilusTrader issue #3403 — "Duplicate PolymarketWebsocketClient asset subscriptions on user channel"

Different bug (race on startup creating duplicate subscriptions), but useful confirmation that production adapters work around Polymarket WS quirks rather than rely on the server behaving well.

### Consensus on cause

Across all sources, the converging diagnosis is:
1. **No documented WS rate limit exists**, and the Polymarket developer has confirmed there is none on inbound messages — so "too many msg/s" is the wrong hypothesis (matches what we were told).
2. The **practical cap is ~500 instruments per connection**; above it the server silently degrades (no snapshot, no updates) and ultimately closes 1006.
3. A second, **distinct silent-freeze bug** can hit even compliant connections — the symptom is healthy PING/PONG plus zero data — and the only known mitigation is a data-inactivity watchdog with forced reconnect.
4. **No public maintainer post-mortem exists** for either issue.

This matches the bot's observations: 100 tokens (well below cap) is rock-solid; 1000–4000 tokens on **one socket** is well above the 500 ceiling and drops in <60 s.

---

## How Official Clients Handle WS

- **`Polymarket/py-clob-client` (Python)** — does **not** implement WebSocket at all. Issue #116 ("Websocket support? Is that on the roadmap?", Jan 2025) received no maintainer reply. The repo was archived May 11 2026. Anything calling itself "official Python WS" is community.
- **`Polymarket/clob-client` (TypeScript)** — also **no WebSocket support**, also archived May 11 2026. Users are directed to V2 (`clob-client-v2`).
- **`Polymarket/rs-clob-client-v2` (Rust)** — has WS via the `ws` Cargo feature, plus a `heartbeats` feature: *"automatically sends heartbeat messages to the Polymarket server, if the client disconnects all open orders will be cancelled."* Reconnect logic is internal, not documented; the prior `rs-clob-client` (v1) had #185 reporting reconnects broken before being archived.
- **`Polymarket/real-time-data-client` (TypeScript, RTDS endpoint)** — has `connect()` / `disconnect()` / `subscribe()` but its README documents **no** heartbeat, reconnect strategy, or limits. Issue #26 shows it doesn't survive ~20 min without an external watchdog.

**Net:** the only official client with first-class CLOB WS is the new Rust v2, and even it doesn't expose what it does about 1006. The Python ecosystem has no official option — every Polymarket Python bot is rolling its own WS layer.

---

## Community Patterns

The two most-cited production patterns:

### NautilusTrader (Rust + Python adapter)
Source: `nautilustrader.io/docs/latest/integrations/polymarket/`.

- Per-connection cap: **default 200**, configurable via `ws_max_subscriptions_per_connection` (Python) / `ws_max_subscriptions` (Rust), tunable up to **500**.
- Above the threshold, **additional WebSocket connections are spun up automatically** — i.e. it shards.
- Stated rationale: at >500 per conn, "you will not receive the initial order book snapshot for each instrument and will only receive subsequent order book updates."
- Resubscribes after reconnect.

### `@nevuamarkets/poly-websockets` (TypeScript, npm)
Source: GitHub `nevuamarkets/poly-websockets`, file `src/WSSubscriptionManager.ts`.

Hardcoded constants in the manager:
- `CLOB_WSS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'`
- Base ping interval: **20 s** with **±5 s jitter** (note: **looser than Polymarket's 10 s recommendation**, but apparently sufficient)
- `DEFAULT_RECONNECT_INTERVAL_MS = 5 s` (reconnect check cadence)
- `DEFAULT_PENDING_FLUSH_INTERVAL_MS = 100 ms` (batch subscribe-delta flush)
- Connection timeout: **30 s**
- One manager = one socket, **unlimited subs per manager** — user must instantiate multiple managers to shard. (No automatic sharding, unlike NautilusTrader.)

### `caiovicentino/polymarket-mcp-server`
- Exponential backoff: initial 1 s, max 60 s, multiplier 2×.
- Message buffer capped at 1000.
- Auto-resubscribes after reconnect.

### Cross-cutting patterns the bot should adopt
1. **Shard at ≤ 500 assets per socket** (target 200 to match NautilusTrader's default and stay well clear).
2. **Data-inactivity watchdog**: if no inbound non-PONG frame in ~120 s, force reconnect even when PING/PONG is succeeding.
3. **Exponential backoff on reconnect** (1 s → 60 s).
4. **Re-fetch a fresh REST snapshot before resuming**, since you may have missed `price_change` deltas during the gap (issue #292 reporter's mitigation).
5. **Persistent retry** — both issues #292 and #26 are intermittent server-side; transient `[]` responses or silent freezes recover on their own after a few minutes.

---

## Open Questions

- **Why does the silent-freeze (open socket, zero data) happen at all?** No maintainer comment on issues #292 or #26.
- **Exact per-IP concurrent connection cap.** No public source quantifies it; NautilusTrader users routinely run dozens. Worth empirically probing if shard count > ~20.
- **Whether the 500 cap counts in-flight subscribes or current subscribers**, and whether unsubscribing decrements toward the cap as expected — undocumented.
- **Why a single oversubscribed socket fails in <60 s** for this bot when issue #292's user gets 15–30 min at 250-per-conn — likely non-linear degradation past ~500, but unproven.
- **Whether `level: 1` (less granular) reduces server load enough to shift the practical cap** — undocumented, not reported anywhere.
- **Server-side fix ETA for the silent freeze** — no public commitment.

---

## Sources

1. `https://docs.polymarket.com/market-data/websocket/overview.md` — canonical heartbeat (`PING` every 10 s, server `PONG`), endpoint list, subscribe-timeout warning.
2. `https://docs.polymarket.com/api-reference/wss/market.md` — Market-channel subscribe schema (`assets_ids`, `type`, `initial_dump`, `level`, `custom_feature_enabled`), six message types.
3. `https://docs.polymarket.com/market-data/websocket/market-channel.md` — Market-channel field semantics and event triggers.
4. `https://docs.polymarket.com/asyncapi.json` — Machine-readable spec confirming text `"PING"` / `"PONG"` payloads (not protocol pings).
5. `https://docs.polymarket.com/llms.txt` — Doc index used to enumerate all WS-related pages.
6. `https://docs.polymarket.com/changelog` — May 28 2025 removal of the 100-token sub limit; `initial_dump` added same day; Sep 15 2025 `price_change` schema change.
7. `https://docs.polymarket.com/quickstart/introduction/rate-limits` — Confirms REST rate limits are Cloudflare-enforced; explicitly silent on WS limits.
8. `https://github.com/Polymarket/py-clob-client/issues/292` — Silent-freeze bug at 250 tokens/conn × 6 conns; 1006 after 15–30 min; reporter's 120 s watchdog workaround; no maintainer reply; repo archived.
9. `https://github.com/Polymarket/py-clob-client/issues/116` — "Websocket support?" — no reply; py-clob-client never had WS.
10. `https://github.com/Polymarket/real-time-data-client/issues/26` — RTDS stream dies after ~20 min with ping/pong still green; multiple library swaps tried; workaround = inactivity timer.
11. `https://github.com/Polymarket/rs-clob-client/issues/185` — Rust client's reconnect mechanism reported broken before archival.
12. `https://github.com/Polymarket/rs-clob-client-v2` — Current official Rust client with `ws` + `heartbeats` Cargo features; internal reconnect, undocumented.
13. `https://github.com/Polymarket/real-time-data-client` — Official RTDS TS client; README documents no heartbeat / limits / reconnect.
14. `https://github.com/nautechsystems/nautilus_trader/issues/3403` — NautilusTrader duplicate-subscription bug; demonstrates official adapters work around Polymarket WS quirks.
15. `https://nautilustrader.io/docs/latest/integrations/polymarket/` — Source for the **500 instruments / connection** practical cap and the `ws_max_subscriptions_per_connection` default of **200** with automatic sharding.
16. `https://github.com/nevuamarkets/poly-websockets` and `src/WSSubscriptionManager.ts` — Constants: 20 s±5 s ping, 5 s reconnect-check, 100 ms flush, 30 s connect timeout; no automatic sharding (manual via multiple manager instances).
17. `https://github.com/caiovicentino/polymarket-mcp-server/blob/main/WEBSOCKET_INTEGRATION.md` — Exponential backoff 1→60 s ×2, 1000-message buffer, auto-resubscribe after reconnect.
18. `https://agentbets.ai/guides/polymarket-websocket-guide/` — Independent confirmation of the 500-instrument practical cap and the "subscribe to 501+ → no initial snapshot" failure mode.
19. `https://community.cloudflare.com/t/websockets-keep-disconnecting-for-sites-proxied-via-cloudflare/639858` — Background on the ~100 s Cloudflare WS idle timeout that mandates the 10 s heartbeat.
20. `/Users/johnseter/Desktop/Odin/polymarket-arbitrage/2026-05-21-222636-WS-Summary.txt` — Project's prior research transcript documenting the existing PING/PONG-every-10 s implementation, `ping_interval=None` choice for the `websockets` library, and the supervisor/reconnect skeleton (lines 226–1015).
