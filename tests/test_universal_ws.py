"""
Tests for polymarket_client.universal_ws

Pure unit tests — no network. Exercise the partition helper, the book/delta
handlers, the snapshot helper, and the drop-oldest queue policy. The
network-bound paths (_open_shard_ws, _run_shard_session, _shard_supervisor)
are validated by probe_universal_ws.py.
"""

from __future__ import annotations

import asyncio

import pytest

from polymarket_client.models import Market, OrderBook, OrderBookSide, PriceLevel, TokenOrderBook, TokenType
from polymarket_client.universal_ws import PolymarketUniversalWS, _partition_round_robin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mk_market(i: int) -> Market:
    return Market(
        market_id=f"mkt_{i}",
        condition_id=f"cond_{i}",
        question=f"q{i}",
        yes_token_id=f"yes_{i}",
        no_token_id=f"no_{i}",
        volume_24h=float(1000 - i),
    )


def _seed_universal_ws(markets: list[Market]) -> PolymarketUniversalWS:
    """Construct a UniversalWS and pre-register the token map as if shards opened."""
    ws = PolymarketUniversalWS()
    for m in markets:
        ws._token_to_market[m.yes_token_id] = (m.market_id, TokenType.YES)
        ws._token_to_market[m.no_token_id] = (m.market_id, TokenType.NO)
    return ws


# ---------------------------------------------------------------------------
# Partition helper
# ---------------------------------------------------------------------------


class TestPartitionRoundRobin:
    def test_partition_round_robin_basic(self) -> None:
        """250 markets, shard_size=100 -> 3 shards of sizes [84, 83, 83], interleaved."""
        markets = [_mk_market(i) for i in range(250)]
        shards = _partition_round_robin(markets, shard_size=100)
        assert len(shards) == 3
        assert [len(s) for s in shards] == [84, 83, 83]

        # Interleaved means: shard 0 gets indices 0, 3, 6, ... etc.
        assert [m.market_id for m in shards[0][:4]] == ["mkt_0", "mkt_3", "mkt_6", "mkt_9"]
        assert [m.market_id for m in shards[1][:4]] == ["mkt_1", "mkt_4", "mkt_7", "mkt_10"]
        assert [m.market_id for m in shards[2][:4]] == ["mkt_2", "mkt_5", "mkt_8", "mkt_11"]

        # All markets accounted for exactly once.
        assigned = {m.market_id for s in shards for m in s}
        assert assigned == {f"mkt_{i}" for i in range(250)}

    def test_partition_below_shard_size(self) -> None:
        """50 markets with shard_size=100 -> 1 shard."""
        markets = [_mk_market(i) for i in range(50)]
        shards = _partition_round_robin(markets, shard_size=100)
        assert len(shards) == 1
        assert len(shards[0]) == 50
        assert [m.market_id for m in shards[0]] == [f"mkt_{i}" for i in range(50)]

    def test_partition_empty(self) -> None:
        """Empty input -> empty list."""
        assert _partition_round_robin([], shard_size=100) == []


# ---------------------------------------------------------------------------
# Book snapshot handler
# ---------------------------------------------------------------------------


class TestApplyBookSnapshot:
    def test_apply_book_snapshot_known_asset_writes_state(self) -> None:
        """Feed a canned snapshot dict; assert self._books[mid].yes matches."""
        markets = [_mk_market(0)]
        ws = _seed_universal_ws(markets)
        msg = {
            "event_type": "book",
            "asset_id": "yes_0",
            "bids": [{"price": "0.45", "size": "100"}, {"price": "0.40", "size": "50"}],
            "asks": [{"price": "0.55", "size": "75"}, {"price": "0.60", "size": "25"}],
        }
        mid = ws._apply_book_snapshot(msg)

        assert mid == "mkt_0"
        book = ws._books["mkt_0"]
        # Bids sorted desc, asks sorted asc.
        assert [(l.price, l.size) for l in book.yes.bids.levels] == [(0.45, 100.0), (0.40, 50.0)]
        assert [(l.price, l.size) for l in book.yes.asks.levels] == [(0.55, 75.0), (0.60, 25.0)]
        # NO side untouched (default empty book).
        assert book.no.bids.levels == []
        assert book.no.asks.levels == []

    def test_apply_book_snapshot_unknown_asset_returns_none(self) -> None:
        """Cross-shard isolation defense: unknown asset_id returns None and writes nothing."""
        ws = _seed_universal_ws([_mk_market(0)])
        msg = {
            "event_type": "book",
            "asset_id": "unknown_asset_xyz",
            "bids": [{"price": "0.5", "size": "1"}],
            "asks": [],
        }
        assert ws._apply_book_snapshot(msg) is None
        assert ws._books == {}


# ---------------------------------------------------------------------------
# Price-change delta handler
# ---------------------------------------------------------------------------


class TestApplyPriceChange:
    def test_apply_price_change_upsert_and_remove(self) -> None:
        """Upsert a new level, modify an existing level, and remove via size=0."""
        ws = _seed_universal_ws([_mk_market(0)])

        # Seed an initial snapshot so deltas can apply.
        ws._apply_book_snapshot({
            "event_type": "book",
            "asset_id": "yes_0",
            "bids": [{"price": "0.45", "size": "100"}, {"price": "0.40", "size": "50"}],
            "asks": [{"price": "0.55", "size": "75"}],
        })

        # 1) Upsert a new bid level above the top of book.
        touched = ws._apply_price_change({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "yes_0", "side": "BUY", "price": "0.48", "size": "20"}
            ],
        })
        assert touched == {"mkt_0"}
        bids = ws._books["mkt_0"].yes.bids.levels
        # Sorted desc after insert.
        assert [(l.price, l.size) for l in bids] == [(0.48, 20.0), (0.45, 100.0), (0.40, 50.0)]

        # 2) Modify an existing bid (same price, new size).
        ws._apply_price_change({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "yes_0", "side": "BUY", "price": "0.45", "size": "999"}
            ],
        })
        bids = ws._books["mkt_0"].yes.bids.levels
        assert next(l.size for l in bids if l.price == 0.45) == 999.0
        # Still sorted desc.
        assert [l.price for l in bids] == [0.48, 0.45, 0.40]

        # 3) Remove a bid level via size=0.
        ws._apply_price_change({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "yes_0", "side": "BUY", "price": "0.40", "size": "0"}
            ],
        })
        bids = ws._books["mkt_0"].yes.bids.levels
        assert [l.price for l in bids] == [0.48, 0.45]


# ---------------------------------------------------------------------------
# Snapshot deep-copy
# ---------------------------------------------------------------------------


class TestSnapshotDeepCopy:
    def test_snapshot_is_deep_copy(self) -> None:
        """Mutating a returned book must not affect internal state."""
        ws = _seed_universal_ws([_mk_market(0)])
        ws._apply_book_snapshot({
            "event_type": "book",
            "asset_id": "yes_0",
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "75"}],
        })

        snap = ws._snapshot_book("mkt_0")
        assert isinstance(snap, OrderBook)

        # Mutate the snapshot.
        snap.yes.bids.levels.clear()
        snap.yes.asks.levels.append(PriceLevel(price=0.99, size=1.0))

        # Internal state untouched.
        internal_bids = ws._books["mkt_0"].yes.bids.levels
        internal_asks = ws._books["mkt_0"].yes.asks.levels
        assert [(l.price, l.size) for l in internal_bids] == [(0.45, 100.0)]
        assert [(l.price, l.size) for l in internal_asks] == [(0.55, 75.0)]


# ---------------------------------------------------------------------------
# Drop-oldest queue policy
# ---------------------------------------------------------------------------


class TestDropOldestQueue:
    def test_drop_oldest_on_full_queue(self) -> None:
        """When the bounded queue is full, oldest id is evicted and _drops increments."""
        ws = PolymarketUniversalWS(queue_maxsize=3)
        assert ws._drops == 0

        # Fill the queue.
        ws._enqueue_market_id("a")
        ws._enqueue_market_id("b")
        ws._enqueue_market_id("c")
        assert ws._queue.qsize() == 3
        assert ws._drops == 0

        # Now push a 4th — oldest ("a") should be evicted.
        ws._enqueue_market_id("d")
        assert ws._queue.qsize() == 3
        assert ws._drops == 1

        # Drain and confirm order: b, c, d.
        drained = []
        while not ws._queue.empty():
            drained.append(ws._queue.get_nowait())
        assert drained == ["b", "c", "d"]
