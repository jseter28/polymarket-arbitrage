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


# ---------------------------------------------------------------------------
# Phase 5: Lifecycle + Reconcile
# ---------------------------------------------------------------------------


class _FakeWS:
    """Recording stub for self._conn_ws[cid] in unit tests."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self) -> None:
        self.closed = True


def _seed_conn_ready(ws: KalshiUniversalWS, conn_id: int, sid: int = 1) -> _FakeWS:
    """Pretend conn `conn_id` is open and subscribed with `sid`."""
    fake = _FakeWS()
    ws._conn_ws[conn_id] = fake
    ws._conn_state[conn_id].sid = sid
    ws._conn_state[conn_id].state = "connected"
    return fake


class TestPrepareAddRemove:
    def test_prepare_add_returns_none_when_conn_not_ready(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        # conn 0 has no ws/sid
        assert ws._prepare_add(conn_id=0, ticker="KX-NEW") is None
        assert "KX-NEW" not in ws._ticker_to_conn

    def test_prepare_add_updates_state_and_returns_cmd(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_conn_ready(ws, 0, sid=7)
        cmd = ws._prepare_add(conn_id=0, ticker="KX-NEW")
        assert cmd is not None
        assert cmd["cmd"] == "update_subscription"
        assert cmd["params"]["sids"] == [7]
        assert cmd["params"]["market_tickers"] == ["KX-NEW"]
        assert cmd["params"]["action"] == "add_markets"
        assert ws._ticker_to_conn["KX-NEW"] == 0
        assert "KX-NEW" in ws._conn_tickers[0]
        assert ws._conn_state[0].tickers == 1

    def test_prepare_add_is_idempotent_for_already_subscribed(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_conn_ready(ws, 0, sid=1)
        ws._prepare_add(0, "KX-X")
        # second call returns None and does NOT re-add
        assert ws._prepare_add(0, "KX-X") is None
        assert ws._conn_state[0].tickers == 1

    def test_prepare_remove_returns_none_for_unknown_ticker(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_conn_ready(ws, 0, sid=1)
        assert ws._prepare_remove(0, "KX-NOPE") is None

    def test_prepare_remove_drops_state_and_book(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_conn_ready(ws, 0, sid=4)
        ws._prepare_add(0, "KX-R")
        # seed a book for that ticker
        ws._apply_snapshot(
            conn_id=0,
            msg={
                "type": "orderbook_snapshot",
                "sid": 4,
                "seq": 1,
                "msg": {"market_ticker": "KX-R", "yes_dollars_fp": [["0.5", "1"]]},
            },
        )
        assert "KX-R" in ws._books

        cmd = ws._prepare_remove(0, "KX-R")
        assert cmd is not None
        assert cmd["params"]["action"] == "delete_markets"
        assert cmd["params"]["market_tickers"] == ["KX-R"]
        assert "KX-R" not in ws._ticker_to_conn
        assert "KX-R" not in ws._books
        assert "KX-R" not in ws._conn_tickers[0]


class TestLifecycleEventHandling:
    def test_handle_lifecycle_activated_dispatches_add(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=3)
        # All 3 conns ready so any hash target works.
        for cid in range(3):
            _seed_conn_ready(ws, cid, sid=1)
        ws._handle_lifecycle_event(
            {
                "type": "market_activated",
                "sid": 99,
                "seq": 1,
                "msg": {"market_ticker": "KXNBA-NEW"},
            }
        )
        # ticker assigned to its hash-target conn
        assert "KXNBA-NEW" in ws._ticker_to_conn
        target = ws._ticker_to_conn["KXNBA-NEW"]
        assert ws._conn_state[target].lifecycle_add_count == 1
        assert ws._conn_state[0].lifecycle_event_count == 1

    def test_handle_lifecycle_settled_dispatches_remove(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=2)
        for cid in range(2):
            _seed_conn_ready(ws, cid, sid=1)
        # Pre-add a ticker that will then be settled.
        target = zlib_crc32_mod("KXOLD", 2)
        ws._prepare_add(target, "KXOLD")
        ws._apply_snapshot(
            conn_id=target,
            msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {"market_ticker": "KXOLD", "yes_dollars_fp": [["0.5", "1"]]},
            },
        )
        assert "KXOLD" in ws._books

        ws._handle_lifecycle_event(
            {
                "type": "market_settled",
                "sid": 99,
                "seq": 5,
                "msg": {"market_ticker": "KXOLD"},
            }
        )
        assert "KXOLD" not in ws._ticker_to_conn
        assert "KXOLD" not in ws._books
        assert ws._conn_state[target].lifecycle_remove_count == 1

    def test_handle_lifecycle_metadata_update_telemetry_only(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_conn_ready(ws, 0, sid=1)
        ws._prepare_add(0, "KX-A")
        ws._handle_lifecycle_event(
            {
                "type": "market_metadata_updated",
                "sid": 99,
                "seq": 3,
                "msg": {"market_ticker": "KX-A"},
            }
        )
        # No add/remove side-effects.
        assert ws._conn_state[0].lifecycle_event_count == 1
        assert ws._conn_state[0].lifecycle_add_count == 0
        assert ws._conn_state[0].lifecycle_remove_count == 0
        # ticker still subscribed
        assert "KX-A" in ws._ticker_to_conn

    def test_handle_lifecycle_unknown_event_ignored(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_conn_ready(ws, 0, sid=1)
        # Made-up event type. Should not crash; should count as one event.
        ws._handle_lifecycle_event(
            {
                "type": "market_made_up_event",
                "sid": 99,
                "seq": 1,
                "msg": {"market_ticker": "KXNEW"},
            }
        )
        assert ws._conn_state[0].lifecycle_event_count == 1
        assert "KXNEW" not in ws._ticker_to_conn

    def test_route_message_dispatches_market_prefixed_to_lifecycle(
        self, rsa_key
    ) -> None:
        """Any type that starts with 'market_' goes to the lifecycle handler."""
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_conn_ready(ws, 0, sid=1)
        ws._route_message(
            conn_id=0,
            raw_msg={
                "type": "market_activated",
                "sid": 99,
                "seq": 1,
                "msg": {"market_ticker": "KXLIFE"},
            },
        )
        assert ws._conn_state[0].lifecycle_event_count == 1


class TestReconcileDiff:
    def test_reconcile_diff_adds_and_removes(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=2)
        for cid in range(2):
            _seed_conn_ready(ws, cid, sid=1)
        # Currently subscribed: A, B, C
        for t in ["A", "B", "C"]:
            ws._prepare_add(conn_id=zlib_crc32_mod(t, 2), ticker=t)
        # REST says we should have: B, C, D, E
        rest_tickers = ["B", "C", "D", "E"]
        adds, removes = ws._compute_reconcile_diff(rest_tickers)
        assert sorted(adds) == ["D", "E"]
        assert removes == ["A"]


class TestLifecycleTelemetry:
    def test_status_includes_lifecycle_and_reconcile_keys(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=2)
        s = ws.status()
        assert "reconcile_count" in s
        for shard in s["shards"]:
            for key in (
                "lifecycle_sid",
                "lifecycle_event_count",
                "lifecycle_add_count",
                "lifecycle_remove_count",
            ):
                assert key in shard, f"missing per-shard key: {key}"


def zlib_crc32_mod(ticker: str, n: int) -> int:
    """Mirror of the partition function so tests can predict assignments."""
    import zlib

    return zlib.crc32(ticker.encode("utf-8")) % n


# ---------------------------------------------------------------------------
# Phase 6a: markets-browser surface
# ---------------------------------------------------------------------------


def _seed_market_meta(
    ws: KalshiUniversalWS, ticker: str, series: str, title: str = ""
) -> None:
    """Pretend the REST market discovery step ran for this ticker."""
    from kalshi_client.models import KalshiMarket

    m = KalshiMarket(
        ticker=ticker,
        event_ticker=ticker.split("-")[0] if "-" in ticker else ticker,
        series_ticker=series,
        title=title or f"Mkt {ticker}",
    )
    ws._market_meta[ticker] = m
    # Also register in the series_registry for category counts.
    ws._note_market_added(ticker)


class TestCategories:
    def test_categories_groups_by_series_ticker(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        for i in range(5):
            _seed_market_meta(ws, f"KXNBA-{i}", "KXNBA")
        for i in range(3):
            _seed_market_meta(ws, f"KXFED-{i}", "KXFED")

        cats = ws.categories()
        by_slug = {c["slug"]: c for c in cats}
        assert by_slug["KXNBA"]["count"] == 5
        assert by_slug["KXFED"]["count"] == 3
        # Each category has required keys.
        for c in cats:
            for key in ("slug", "label", "class", "count", "msg_rate_sum"):
                assert key in c, f"missing category key: {key}"

    def test_categories_sorted_by_count_desc(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        for i in range(2):
            _seed_market_meta(ws, f"KXA-{i}", "KXA")
        for i in range(7):
            _seed_market_meta(ws, f"KXB-{i}", "KXB")
        for i in range(4):
            _seed_market_meta(ws, f"KXC-{i}", "KXC")
        cats = ws.categories()
        counts = [c["count"] for c in cats]
        assert counts == sorted(counts, reverse=True)

    def test_categories_kxmve_classified_as_programs(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_market_meta(ws, "KXMVE-ABC-1", "KXMVECROSSCATEGORY")
        cats = ws.categories()
        kxmve = next(c for c in cats if c["slug"] == "KXMVECROSSCATEGORY")
        assert kxmve["class"] == "programs"

    def test_categories_empty_when_no_markets(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        assert ws.categories() == []


class TestMarketsByTag:
    def test_filters_to_series(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        for i in range(3):
            _seed_market_meta(ws, f"KXNBA-{i}", "KXNBA")
        for i in range(2):
            _seed_market_meta(ws, f"KXFED-{i}", "KXFED")
        nba = ws.markets_by_tag("KXNBA")
        assert len(nba) == 3
        assert all(m["ticker"].startswith("KXNBA-") for m in nba)

    def test_respects_limit(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        for i in range(50):
            _seed_market_meta(ws, f"KXNBA-{i:02}", "KXNBA")
        out = ws.markets_by_tag("KXNBA", limit=10)
        assert len(out) == 10

    def test_unknown_tag_returns_empty(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        assert ws.markets_by_tag("KXNOPE") == []


class TestMarketDetail:
    def test_market_detail_returns_snapshot_with_book(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_market_meta(ws, "KXNBA-1", "KXNBA", title="NBA Test")
        _seed_assignment(ws, ["KXNBA-1"])
        ws._apply_snapshot(
            conn_id=0,
            msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXNBA-1",
                    "yes_dollars_fp": [["0.45", "100"]],
                    "no_dollars_fp": [["0.55", "75"]],
                },
            },
        )
        d = ws.market_detail("kalshi:KXNBA-1")
        assert d is not None
        assert d["ticker"] == "KXNBA-1"
        assert d["series_ticker"] == "KXNBA"
        assert d["title"] == "NBA Test"
        assert len(d["yes_bids"]) == 1
        assert d["yes_bids"][0]["price"] == 0.45
        assert len(d["no_bids"]) == 1

    def test_market_detail_unknown_returns_none(self, rsa_key) -> None:
        ws = _mk_ws(rsa_key, conn_count=1)
        assert ws.market_detail("kalshi:NOPE") is None
        # Without venue prefix → None.
        assert ws.market_detail("polymarket:0xabc") is None


class TestPerMarketSubscriber:
    def test_subscribe_unsubscribe_market_fanout(self, rsa_key) -> None:
        import asyncio

        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_market_meta(ws, "KXNBA-9", "KXNBA")
        _seed_assignment(ws, ["KXNBA-9"])
        q: asyncio.Queue = asyncio.Queue(maxsize=10)

        ws.subscribe_market("kalshi:KXNBA-9", q)
        # Apply a snapshot → fanout must push at least one event.
        ws._apply_snapshot(
            conn_id=0,
            msg={
                "type": "orderbook_snapshot",
                "sid": 1,
                "seq": 1,
                "msg": {
                    "market_ticker": "KXNBA-9",
                    "yes_dollars_fp": [["0.5", "10"]],
                },
            },
        )
        assert not q.empty()
        evt = q.get_nowait()
        assert evt["ticker"] == "KXNBA-9"
        assert evt["kind"] == "snapshot"

        ws.unsubscribe_market("kalshi:KXNBA-9", q)
        # Drain anything left.
        while not q.empty():
            q.get_nowait()
        # Next apply must NOT enqueue.
        ws._apply_delta(
            conn_id=0,
            msg={
                "type": "orderbook_delta",
                "sid": 1,
                "seq": 2,
                "msg": {
                    "market_ticker": "KXNBA-9",
                    "price_dollars": "0.5",
                    "delta_fp": "1",
                    "side": "yes",
                },
            },
        )
        assert q.empty()

    def test_per_market_subscriber_drop_oldest_on_full(self, rsa_key) -> None:
        import asyncio

        ws = _mk_ws(rsa_key, conn_count=1)
        _seed_market_meta(ws, "KXNBA-X", "KXNBA")
        _seed_assignment(ws, ["KXNBA-X"])
        q: asyncio.Queue = asyncio.Queue(maxsize=2)
        ws.subscribe_market("kalshi:KXNBA-X", q)

        # Push three apply events; queue maxsize 2 → oldest evicted.
        for i in range(3):
            ws._apply_delta(
                conn_id=0,
                msg={
                    "type": "orderbook_delta",
                    "sid": 1,
                    "seq": i + 1,
                    "msg": {
                        "market_ticker": "KXNBA-X",
                        "price_dollars": f"0.{40 + i}",
                        "delta_fp": "1",
                        "side": "yes",
                    },
                },
            )
        # Queue has at most 2 items.
        assert q.qsize() <= 2
