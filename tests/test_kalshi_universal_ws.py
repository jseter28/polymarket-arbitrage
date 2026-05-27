"""
Tests for kalshi_client.universal_ws — KalshiUniversalWS.

Pure unit tests, no network. Mirror the structure of test_universal_ws.py.
The live network paths (handshake, recv loop, reconnect) are validated by
the probe scripts and the smoke verification in the Phase 4 plan.
"""

from __future__ import annotations

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from polymarket_client.models import OrderBook, PriceLevel
from kalshi_client.models import KalshiOrderBook
from kalshi_client.universal_ws import (
    KalshiUniversalWS,
    _partition_by_hash,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_key():
    """Throwaway RSA key for KalshiUniversalWS construction — never used to sign."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _mk_ws(
    rsa_key, conn_count: int = 3, queue_maxsize: int = 10000
) -> KalshiUniversalWS:
    return KalshiUniversalWS(
        api_key_id="test-key",
        private_key=rsa_key,
        conn_count=conn_count,
        queue_maxsize=queue_maxsize,
    )


def _seed_assignment(ws: KalshiUniversalWS, tickers: list[str]) -> None:
    """Pretend start() ran: assign tickers to conns deterministically."""
    shards = _partition_by_hash(tickers, ws._conn_count)
    for cid, sub in enumerate(shards):
        for t in sub:
            ws._ticker_to_conn[t] = cid
        ws._conn_tickers[cid] = list(sub)


# ---------------------------------------------------------------------------
# Partition helper
# ---------------------------------------------------------------------------


class TestPartitionByHash:
    def test_partition_each_ticker_appears_exactly_once(self) -> None:
        tickers = [f"TICKER_{i}" for i in range(250)]
        shards = _partition_by_hash(tickers, conn_count=3)
        assert len(shards) == 3
        assigned = [t for s in shards for t in s]
        assert sorted(assigned) == sorted(tickers)

    def test_partition_deterministic_across_calls(self) -> None:
        """Same input → same assignment. Stable assignment across process restarts."""
        tickers = [f"TICKER_{i}" for i in range(100)]
        a = _partition_by_hash(tickers, conn_count=3)
        b = _partition_by_hash(tickers, conn_count=3)
        assert a == b

    def test_partition_input_order_independence_for_a_given_ticker(self) -> None:
        """A ticker lands on the same conn regardless of input order."""
        a = _partition_by_hash(["A", "B", "C", "D"], 3)
        b = _partition_by_hash(["D", "C", "B", "A"], 3)
        loc_a = {t: i for i, s in enumerate(a) for t in s}
        loc_b = {t: i for i, s in enumerate(b) for t in s}
        assert loc_a == loc_b

    def test_partition_single_conn(self) -> None:
        tickers = ["A", "B", "C"]
        shards = _partition_by_hash(tickers, conn_count=1)
        assert len(shards) == 1
        assert sorted(shards[0]) == ["A", "B", "C"]

    def test_partition_empty(self) -> None:
        assert _partition_by_hash([], conn_count=3) == [[], [], []]

    def test_partition_roughly_balanced(self) -> None:
        """With many tickers, no shard hoards everything."""
        tickers = [f"T{i}" for i in range(1000)]
        shards = _partition_by_hash(tickers, conn_count=4)
        sizes = [len(s) for s in shards]
        # Generous bound — none should be empty, none more than ~2x the mean.
        assert all(s > 0 for s in sizes)
        mean = sum(sizes) / len(sizes)
        assert all(s < mean * 2 for s in sizes)


# ---------------------------------------------------------------------------
# Constructor guardrails
# ---------------------------------------------------------------------------


class TestConstructor:
    def test_refuses_conn_count_above_9(self, rsa_key) -> None:
        with pytest.raises(ValueError):
            KalshiUniversalWS(api_key_id="k", private_key=rsa_key, conn_count=10)

    def test_refuses_conn_count_below_1(self, rsa_key) -> None:
        with pytest.raises(ValueError):
            KalshiUniversalWS(api_key_id="k", private_key=rsa_key, conn_count=0)


# ---------------------------------------------------------------------------
# Snapshot apply
# ---------------------------------------------------------------------------


class TestApplySnapshot:
    def test_apply_snapshot_builds_book_from_both_sides(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-A"])
        msg = {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KX-A",
                "yes_dollars_fp": [["0.45", "100.00"], ["0.40", "50.00"]],
                "no_dollars_fp": [["0.55", "75.00"]],
            },
        }
        ticker = ws._apply_snapshot(conn_id=0, msg=msg)
        assert ticker == "KX-A"
        book = ws._books["KX-A"]
        assert isinstance(book, KalshiOrderBook)
        # Sorted desc by price.
        assert [(l.price, l.size) for l in book.yes_bids] == [
            (0.45, 100.0),
            (0.40, 50.0),
        ]
        assert [(l.price, l.size) for l in book.no_bids] == [(0.55, 75.0)]

    def test_apply_snapshot_with_yes_side_omitted(self, rsa_key) -> None:
        """Kalshi omits yes_dollars_fp / no_dollars_fp entirely when empty — not [], absent."""
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-B"])
        msg = {
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KX-B",
                "no_dollars_fp": [["0.60", "20.00"]],
                # yes_dollars_fp absent entirely
            },
        }
        ws._apply_snapshot(conn_id=0, msg=msg)
        book = ws._books["KX-B"]
        assert book.yes_bids == []
        assert [(l.price, l.size) for l in book.no_bids] == [(0.60, 20.0)]

    def test_apply_snapshot_replaces_existing_bids(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-C"])
        ws._apply_snapshot(
            conn_id=0,
            msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KX-C",
                    "yes_dollars_fp": [["0.20", "10.00"]],
                },
            },
        )
        # Second snapshot must replace, not merge.
        ws._apply_snapshot(
            conn_id=0,
            msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 5,
                "msg": {
                    "market_ticker": "KX-C",
                    "yes_dollars_fp": [["0.30", "5.00"]],
                },
            },
        )
        book = ws._books["KX-C"]
        assert [(l.price, l.size) for l in book.yes_bids] == [(0.30, 5.0)]


# ---------------------------------------------------------------------------
# Delta apply
# ---------------------------------------------------------------------------


class TestApplyDelta:
    def _seed(self, ws: KalshiUniversalWS, ticker: str) -> None:
        ws._apply_snapshot(
            conn_id=0,
            msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": ticker,
                    "yes_dollars_fp": [["0.45", "100.00"], ["0.40", "50.00"]],
                    "no_dollars_fp": [["0.55", "75.00"]],
                },
            },
        )

    def test_delta_upsert_new_yes_level(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-D"])
        self._seed(ws, "KX-D")
        ws._apply_delta(
            conn_id=0,
            msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KX-D",
                    "price_dollars": "0.48",
                    "delta_fp": "20.00",
                    "side": "yes",
                },
            },
        )
        bids = ws._books["KX-D"].yes_bids
        # Sorted desc; new level inserted at top.
        assert [(l.price, l.size) for l in bids] == [
            (0.48, 20.0),
            (0.45, 100.0),
            (0.40, 50.0),
        ]

    def test_delta_modify_existing_yes_level_by_summing(self, rsa_key) -> None:
        """A positive delta on an existing price increases the level size."""
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-E"])
        self._seed(ws, "KX-E")
        ws._apply_delta(
            conn_id=0,
            msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KX-E",
                    "price_dollars": "0.45",
                    "delta_fp": "15.00",
                    "side": "yes",
                },
            },
        )
        bids = ws._books["KX-E"].yes_bids
        assert next(l.size for l in bids if l.price == 0.45) == 115.0

    def test_delta_removes_level_when_size_zeroes(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-F"])
        self._seed(ws, "KX-F")
        ws._apply_delta(
            conn_id=0,
            msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KX-F",
                    "price_dollars": "0.40",
                    "delta_fp": "-50.00",
                    "side": "yes",
                },
            },
        )
        bids = ws._books["KX-F"].yes_bids
        assert [l.price for l in bids] == [0.45]

    def test_delta_side_routing_no(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-G"])
        self._seed(ws, "KX-G")
        ws._apply_delta(
            conn_id=0,
            msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KX-G",
                    "price_dollars": "0.62",
                    "delta_fp": "5.00",
                    "side": "no",
                },
            },
        )
        book = ws._books["KX-G"]
        # YES side untouched.
        assert [(l.price, l.size) for l in book.yes_bids] == [
            (0.45, 100.0),
            (0.40, 50.0),
        ]
        # NO side has the new level on top.
        assert [(l.price, l.size) for l in book.no_bids] == [(0.62, 5.0), (0.55, 75.0)]

    def test_delta_before_snapshot_is_tolerated(self, rsa_key) -> None:
        """Receiving a delta before any snapshot must not crash; the bookcreates fresh."""
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-H"])
        # No prior snapshot.
        ws._apply_delta(
            conn_id=0,
            msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KX-H",
                    "price_dollars": "0.50",
                    "delta_fp": "10.00",
                    "side": "yes",
                },
            },
        )
        # Either book is empty or has the single level; not a crash either way.
        book = ws._books.get("KX-H")
        if book is not None:
            assert isinstance(book, KalshiOrderBook)


# ---------------------------------------------------------------------------
# Yield: unified OrderBook conversion at consumer time
# ---------------------------------------------------------------------------


class TestGetBook:
    def test_get_book_returns_unified_orderbook(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-Y"])
        ws._apply_snapshot(
            conn_id=0,
            msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KX-Y",
                    "yes_dollars_fp": [["0.30", "100.00"]],
                    "no_dollars_fp": [["0.65", "50.00"]],
                },
            },
        )
        book = ws.get_book("kalshi:KX-Y")
        assert isinstance(book, OrderBook)
        assert book.market_id == "kalshi:KX-Y"
        # YES bids come straight through; YES asks derived from NO bids (1 - price).
        assert book.yes.bids.levels[0].price == 0.30
        # 1 - 0.65 = 0.35
        assert abs(book.yes.asks.levels[0].price - 0.35) < 1e-9

    def test_get_book_missing_ticker_returns_none(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        assert ws.get_book("kalshi:NOT-THERE") is None


# ---------------------------------------------------------------------------
# Drop-oldest queue
# ---------------------------------------------------------------------------


class TestDropOldestQueue:
    def test_drop_oldest_on_full_queue(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, queue_maxsize=3)
        assert ws._drops == 0
        ws._enqueue_ticker("a")
        ws._enqueue_ticker("b")
        ws._enqueue_ticker("c")
        assert ws._queue.qsize() == 3
        assert ws._drops == 0

        ws._enqueue_ticker("d")
        assert ws._queue.qsize() == 3
        assert ws._drops == 1

        drained = []
        while not ws._queue.empty():
            drained.append(ws._queue.get_nowait())
        assert drained == ["b", "c", "d"]


# ---------------------------------------------------------------------------
# Status surface
# ---------------------------------------------------------------------------


class TestStatus:
    def test_status_shape_matches_polymarket(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=3)
        s = ws.status()
        # Compat keys.
        for key in (
            "started",
            "uptime_s",
            "shard_count",
            "market_count",
            "token_count",
            "queue_depth",
            "queue_maxsize",
            "drops",
            "shards",
        ):
            assert key in s, f"missing status key: {key}"
        assert s["shard_count"] == 3
        assert s["started"] is False
        assert isinstance(s["shards"], list)
        assert len(s["shards"]) == 3

    def test_status_shard_dicts_have_kalshi_specific_keys(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=2)
        s = ws.status()
        for shard in s["shards"]:
            for key in (
                "sid",
                "last_seq",
                "snapshot_count",
                "delta_count",
                "seq_gap_count",
                "snapshot_resync_count",
            ):
                assert key in shard, f"missing per-shard key: {key}"


# ---------------------------------------------------------------------------
# Sequence tracking + gap detection
# ---------------------------------------------------------------------------


class TestSeqTracking:
    def test_first_message_sets_last_seq_no_gap(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        gap = ws._track_seq(conn_id=0, seq=1)
        assert gap is False
        assert ws._conn_state[0].last_seq == 1
        assert ws._conn_state[0].seq_gap_count == 0

    def test_monotonic_seq_advances_without_gap(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        for s in range(1, 6):
            assert ws._track_seq(conn_id=0, seq=s) is False
        assert ws._conn_state[0].last_seq == 5
        assert ws._conn_state[0].seq_gap_count == 0

    def test_gap_detected_when_seq_skips_forward(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        ws._track_seq(conn_id=0, seq=1)
        ws._track_seq(conn_id=0, seq=2)
        gap = ws._track_seq(conn_id=0, seq=5)  # 3 and 4 missing
        assert gap is True
        assert ws._conn_state[0].seq_gap_count == 1
        # last_seq advances to the new value despite the gap.
        assert ws._conn_state[0].last_seq == 5

    def test_seq_resets_to_none_on_disconnect_reset(self, rsa_key) -> None:
        """When a conn is reset (disconnect), last_seq → None; next seq accepted."""
        ws = _mk_ws(rsa_key, conn_count=1)
        ws._track_seq(conn_id=0, seq=10)
        ws._reset_conn_state(conn_id=0, reason="test")
        assert ws._conn_state[0].last_seq is None
        # Fresh sub starts at seq=1; not a gap.
        assert ws._track_seq(conn_id=0, seq=1) is False


# ---------------------------------------------------------------------------
# Message routing
# ---------------------------------------------------------------------------


class TestRouteMessage:
    def test_subscribed_ack_extracts_sid_from_nested_msg(self, rsa_key) -> None:
        """subscribed ack puts sid inside `msg.sid`, not at the top level."""
        ws = _mk_ws(rsa_key, conn_count=1)
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "subscribed",
                "id": 1,
                "msg": {"channel": "orderbook_delta", "sid": 7},
            },
        )
        assert ws._conn_state[0].sid == 7

    def test_snapshot_routed_to_apply_snapshot(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-R1"])
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KX-R1",
                    "yes_dollars_fp": [["0.50", "10.00"]],
                },
            },
        )
        assert "KX-R1" in ws._books
        assert ws._conn_state[0].snapshot_count == 1

    def test_delta_routed_to_apply_delta(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-R2"])
        # Seed snapshot first.
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KX-R2",
                    "yes_dollars_fp": [["0.50", "10.00"]],
                },
            },
        )
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KX-R2",
                    "price_dollars": "0.50",
                    "delta_fp": "5.00",
                    "side": "yes",
                },
            },
        )
        assert ws._conn_state[0].delta_count == 1
        assert ws._books["KX-R2"].yes_bids[0].size == 15.0

    def test_error_message_logged_and_stored(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "error",
                "id": 99,
                "msg": {"code": 7, "msg": "Unknown subscription ID"},
            },
        )
        assert ws._conn_state[0].last_disconnect_reason is not None
        assert "Unknown subscription ID" in ws._conn_state[0].last_disconnect_reason

    def test_routed_message_enqueues_ticker(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-Q"])
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KX-Q",
                    "yes_dollars_fp": [["0.30", "1.00"]],
                },
            },
        )
        # Queue should contain the ticker for iter_updates consumption.
        assert ws._queue.qsize() == 1

    def test_gap_in_routed_messages_increments_gap_count(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_assignment(ws, ["KX-G"])
        # First message ok.
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {"market_ticker": "KX-G", "yes_dollars_fp": [["0.5", "1"]]},
            },
        )
        # Skip seq 2-4, jump to 5.
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 5,
                "msg": {
                    "market_ticker": "KX-G",
                    "price_dollars": "0.5",
                    "delta_fp": "1",
                    "side": "yes",
                },
            },
        )
        assert ws._conn_state[0].seq_gap_count == 1


# ---------------------------------------------------------------------------
# Backoff schedule (offline)
# ---------------------------------------------------------------------------


class TestBackoff:
    def test_open_failure_streak_exponential_capped_at_30(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        # streak 1 → 1s, 2 → 2s, 3 → 4s, 4 → 8s, 5 → 16s, 6 → 30s (capped)
        assert ws._compute_open_failed_backoff(streak=1) == 1
        assert ws._compute_open_failed_backoff(streak=2) == 2
        assert ws._compute_open_failed_backoff(streak=3) == 4
        assert ws._compute_open_failed_backoff(streak=4) == 8
        assert ws._compute_open_failed_backoff(streak=5) == 16
        assert ws._compute_open_failed_backoff(streak=6) == 30
        assert ws._compute_open_failed_backoff(streak=10) == 30
