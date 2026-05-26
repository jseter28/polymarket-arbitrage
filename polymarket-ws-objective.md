# Objective: Polymarket WebSocket Listener

## In one sentence

**Provide a continuously-available, real-time feed of every active market on Polymarket, delivered as a single stream that any downstream consumer can read without having to know how it's produced.**

## What that means concretely

1. **Continuously available.** The feed never goes dark from the consumer's point of view. Individual connections, network paths, or components may fail underneath at any moment — but the data stream the consumer reads stays uninterrupted. If the consumer is reading, the consumer is receiving updates.

2. **Real-time.** Updates land in the consumer's hands within seconds of the event happening on Polymarket. Not minutes. Not "the next polling cycle." Within the time it would take a human to refresh a browser.

3. **Every active market.** Coverage is the entire active Polymarket universe, not a top-N subset. As Polymarket lists new markets or resolves existing ones, the listener keeps up — coverage doesn't drift over time.

4. **One stream.** The consumer never has to think about how the listener works. No knowledge of shards, reconnects, queues, or WebSocket details should be required to use it. The interface is simply: "give me the next update," repeated.

5. **Self-healing.** When something fails — a network blip, a server restart, a transient connection drop — the listener fixes it on its own. No human in the loop. No external monitoring poking it back to life.

## What the listener is NOT responsible for

- **Trading decisions.** The listener delivers market data. What to do with that data — arbitrage detection, order placement, risk control — belongs to whoever consumes the feed.
- **Authentication.** This is the public market channel only. User-specific data (orders, fills, positions) needs separate plumbing.
- **Cross-platform aggregation.** Kalshi, other prediction-market venues, and the matching between platforms live elsewhere in the system.
- **Persistence or history.** The listener serves *live* data. Recording, replay, and historical analysis are separate concerns.

## The bar for "working"

The listener is meeting its objective when, over any continuous 24-hour period:

- A consumer reading the stream never observes a visible pause longer than a few hundred milliseconds.
- Coverage of the active universe stays at or above 99% — accounting for markets added or resolved during the window.
- No human intervention was required to keep it running.
- The listener's own health view doesn't show silent degradation (queue chronically full, shards stuck quarantined, drop counter climbing without bound).

If any of those conditions break, the listener is not meeting its objective, regardless of how much data it happens to be delivering at the moment.

## Why this matters

The bot's edge depends on knowing about price changes the moment they happen and acting before other traders do. Anything slower or less reliable than real-time puts the bot behind the market and erases that edge.

The listener is the foundation. Nothing downstream of it can be better than what it delivers. If the listener is broken, everything is broken. If the listener is solid, every other piece — arbitrage detection, risk control, execution, monitoring — can be built on top of it with confidence.

That is its job, and that is the only thing its job is.
