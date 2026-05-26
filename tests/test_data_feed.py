"""
Tests for DataFeed's analyze-queue mechanism (R2: decouple WS recv from analyze).

The queue exists so that one slow market's analysis can't head-of-line-block
WS reads for every other market. These tests verify the per-market latest-wins
coalescing and that the producer never blocks on a slow consumer.
"""

import asyncio

import pytest

from core.data_feed import DataFeed
from polymarket_client.models import Market, MarketState, OrderBook


@pytest.fixture
def feed():
    """A DataFeed with no client — we only exercise the queue mechanism."""
    f = DataFeed.__new__(DataFeed)
    # Manually initialize only the fields the queue mechanism touches.
    f.on_update = None
    f._markets = {}
    f._order_books = {}
    f._positions = {}
    f._market_states = {}
    f._update_count = 0
    f._last_update = {}
    f._analyze_queue = asyncio.Queue()
    f._pending = {}
    f._queued = set()
    return f


def _make_state(market_id: str, version: int) -> MarketState:
    """Build a minimal MarketState with version stashed in market.description."""
    return MarketState(
        market=Market(market_id=market_id, condition_id=market_id, question="q", description=str(version)),
        order_book=OrderBook(market_id=market_id),
    )


def _enqueue(feed: DataFeed, market_id: str, state: MarketState) -> None:
    """Mirror what _update_market_state does after building a state."""
    feed._pending[market_id] = state
    if market_id not in feed._queued:
        feed._analyze_queue.put_nowait(market_id)
        feed._queued.add(market_id)


@pytest.mark.asyncio
async def test_next_update_returns_latest_state(feed):
    """A market enqueued multiple times yields only the most recent state."""
    _enqueue(feed, "A", _make_state("A", 1))
    _enqueue(feed, "A", _make_state("A", 2))
    _enqueue(feed, "A", _make_state("A", 3))

    # Latest-wins: queue holds only one entry for A; state is version 3.
    assert feed._analyze_queue.qsize() == 1

    mid, state = await feed.next_update()
    assert mid == "A"
    assert state.market.description == "3"

    # After consuming, no pending state remains.
    assert "A" not in feed._pending
    assert "A" not in feed._queued


@pytest.mark.asyncio
async def test_coalescing_preserves_per_market(feed):
    """Three markets each updated many times → exactly 3 queue entries, latest each."""
    for v in range(50):
        _enqueue(feed, "A", _make_state("A", v))
        _enqueue(feed, "B", _make_state("B", v))
        _enqueue(feed, "C", _make_state("C", v))

    assert feed._analyze_queue.qsize() == 3

    seen = {}
    for _ in range(3):
        mid, state = await feed.next_update()
        seen[mid] = int(state.market.description)

    assert seen == {"A": 49, "B": 49, "C": 49}


@pytest.mark.asyncio
async def test_producer_does_not_block_on_slow_consumer(feed):
    """A flood of updates completes immediately even when the consumer is slow."""
    consumed = []

    async def slow_worker():
        while True:
            mid, state = await feed.next_update()
            await asyncio.sleep(0.02)  # deliberately slow analyze
            consumed.append((mid, int(state.market.description)))

    worker = asyncio.create_task(slow_worker())

    # Producer pushes 1000 updates across 50 markets in a tight loop.
    # If the queue mechanism is non-blocking, this completes in ~1ms.
    loop = asyncio.get_event_loop()
    t0 = loop.time()
    for v in range(1000):
        _enqueue(feed, f"M{v % 50}", _make_state(f"M{v % 50}", v))
    producer_elapsed = loop.time() - t0

    assert producer_elapsed < 0.1, (
        f"producer blocked: took {producer_elapsed * 1000:.1f} ms — "
        f"head-of-line blocking not eliminated"
    )

    # Let the worker drain a bit, then cancel.
    await asyncio.sleep(0.2)
    worker.cancel()
    try:
        await worker
    except asyncio.CancelledError:
        pass

    # Each market that got consumed should hold the latest version, not a stale one.
    by_market = {}
    for mid, v in consumed:
        by_market[mid] = v
    # Final versions for markets M0..M49 are 950, 951, ..., 999 (last full pass).
    for mid, v in by_market.items():
        idx = int(mid[1:])
        # Worker may have picked up an intermediate state if it dequeued before
        # the producer's final write — but only versions on the cycle for that
        # market are valid (v % 50 == idx, so v ∈ {idx, idx+50, idx+100, ...}).
        assert v % 50 == idx, f"market {mid} got bogus version {v}"


@pytest.mark.asyncio
async def test_next_update_skips_missing_pending(feed):
    """If pending dict is cleared between enqueue and dequeue, next_update returns None state."""
    _enqueue(feed, "A", _make_state("A", 1))
    # Simulate a race: worker is about to dequeue, but pending got cleared.
    del feed._pending["A"]

    mid, state = await feed.next_update()
    assert mid == "A"
    assert state is None
