"""
Kalshi Universal WebSocket ingest layer.

Production-grade always-on sharded ingest for Kalshi's `orderbook_delta`
channel. Mirrors the public surface of `polymarket_client.universal_ws.
PolymarketUniversalWS` so consumers (probe dashboard, future bot
integration) are venue-agnostic.

Phase 4 of the Kalshi UniversalWS workstream
(plan: tasks/kalshi-universal-ws.md). Phase 0–3 empirics:
  * Auth = signed RSA-PSS handshake headers (kalshi_client/auth.py).
  * Demo conn cap = 10 (HTTP 429 on 11+); use ≤5 in production budget.
  * One auth conn cleanly sustains 10k subscribed tickers.
  * One sid per (channel, conn); seq is per-sid, monotonic, gap-free.
  * Gap recovery via `update_subscription get_snapshot` with explicit
    market_tickers; seq continues, no reset.

This module implements the offline data path (partition, snapshot apply,
delta apply, queue, status). The live conn supervisor + reconnect +
seq-gap recovery is layered in subsequent work.
"""

from __future__ import annotations

import asyncio
import logging
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from kalshi_client.models import KalshiOrderBook
from polymarket_client.models import OrderBook, PriceLevel

logger = logging.getLogger(__name__)

DEFAULT_BASE_WS = "wss://demo-api.kalshi.co/trade-api/ws/v2"
DEFAULT_BASE_REST = "https://demo-api.kalshi.co/trade-api/v2"

# Demo cap is 10; reserve headroom for ad-hoc dev sessions.
MAX_CONN_COUNT = 9


def _partition_by_hash(tickers: list[str], conn_count: int) -> list[list[str]]:
    """
    Deterministically partition tickers across `conn_count` connections.

    Uses zlib.crc32 (stable across processes, unlike Python's built-in hash
    for strings under PYTHONHASHSEED randomization). Same ticker always lands
    on the same conn regardless of input order or process invocation. New
    tickers added later (Phase 5 lifecycle) don't reshuffle existing ones.
    """
    shards: list[list[str]] = [[] for _ in range(conn_count)]
    for t in tickers:
        idx = zlib.crc32(t.encode("utf-8")) % conn_count
        shards[idx].append(t)
    return shards


@dataclass
class KalshiConnState:
    """Per-conn lifecycle + telemetry. Exposed via `status().shards`."""

    conn_id: int
    tickers: int = 0
    state: str = "init"  # init|connecting|connected|reconnecting|quarantined|stopped
    sid: int | None = None
    last_seq: int | None = None
    session_count: int = 0
    message_count: int = 0
    bytes_received: int = 0
    snapshot_count: int = 0
    delta_count: int = 0
    seq_gap_count: int = 0
    snapshot_resync_count: int = 0
    last_msg_monotonic: float = 0.0
    last_disconnect_reason: str | None = None
    last_open_failure: str | None = None
    short_session_streak: int = 0
    connect_attempts: int = 0
    connect_successes: int = 0
    first_msg_received: bool = False

    def to_dict(self) -> dict:
        last_msg_age_s: float | None
        if self.last_msg_monotonic:
            last_msg_age_s = round(time.monotonic() - self.last_msg_monotonic, 3)
        else:
            last_msg_age_s = None
        return {
            "conn_id": self.conn_id,
            "tickers": self.tickers,
            "state": self.state,
            "sid": self.sid,
            "last_seq": self.last_seq,
            "session_count": self.session_count,
            "message_count": self.message_count,
            "bytes_received": self.bytes_received,
            "snapshot_count": self.snapshot_count,
            "delta_count": self.delta_count,
            "seq_gap_count": self.seq_gap_count,
            "snapshot_resync_count": self.snapshot_resync_count,
            "last_msg_age_s": last_msg_age_s,
            "last_disconnect_reason": self.last_disconnect_reason,
            "last_open_failure": self.last_open_failure,
            "short_session_streak": self.short_session_streak,
            "connect_attempts": self.connect_attempts,
            "connect_successes": self.connect_successes,
            "first_msg_received": self.first_msg_received,
        }


def _parse_levels(raw: list[list[str]] | None) -> list[PriceLevel]:
    """Convert Kalshi's [['price', 'size'], ...] fixed-point string pairs to PriceLevels."""
    if not raw:
        return []
    return [PriceLevel(price=float(p), size=float(s)) for p, s in raw]


class KalshiUniversalWS:
    """
    Always-on sharded ingest for Kalshi orderbook_delta. Public surface:

        ws = KalshiUniversalWS(api_key_id, private_key, conn_count=3)
        await ws.start(market_tickers)              # not yet implemented in Phase 4 part A
        async for market_id, book in ws.iter_updates(): ...
        book = ws.get_book("kalshi:TICKER")
        status = ws.status()
        await ws.stop()
    """

    def __init__(
        self,
        api_key_id: str,
        private_key: RSAPrivateKey,
        base_ws: str = DEFAULT_BASE_WS,
        base_rest: str = DEFAULT_BASE_REST,
        conn_count: int = 3,
        queue_maxsize: int = 10000,
    ) -> None:
        if conn_count < 1 or conn_count > MAX_CONN_COUNT:
            raise ValueError(
                f"conn_count must be 1..{MAX_CONN_COUNT} "
                f"(demo cap is 10; reserve headroom). got {conn_count}"
            )

        self._api_key_id = api_key_id
        self._private_key = private_key
        self._base_ws = base_ws
        self._base_rest = base_rest
        self._conn_count = conn_count

        # Book storage keyed by raw market_ticker (NOT "kalshi:" prefixed).
        self._books: dict[str, KalshiOrderBook] = {}
        # ticker → owning conn_id; set up by start() via _partition_by_hash.
        self._ticker_to_conn: dict[str, int] = {}
        # conn_id → list of tickers it owns.
        self._conn_tickers: list[list[str]] = [[] for _ in range(conn_count)]
        # Per-conn lifecycle telemetry.
        self._conn_state: list[KalshiConnState] = [
            KalshiConnState(conn_id=i) for i in range(conn_count)
        ]

        # Update queue: drop-oldest, latest-wins coalesce via dedup at yield.
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_maxsize)
        self._queue_maxsize = queue_maxsize
        self._drops = 0

        # Lifecycle.
        self._stop_event = asyncio.Event()
        self._supervisor_tasks: list[asyncio.Task] = []
        self._started = False
        self._start_monotonic: float = 0.0

    # ------------------------------------------------------------------
    # Internal data path — exercised by unit tests.
    # ------------------------------------------------------------------

    def _apply_snapshot(self, conn_id: int, msg: dict) -> Optional[str]:
        """
        Apply an orderbook_snapshot frame to internal book state.

        Returns the affected market_ticker, or None if the frame was
        malformed. Caller is responsible for enqueueing the ticker.
        """
        body = msg.get("msg") or {}
        ticker = body.get("market_ticker")
        if not ticker:
            return None

        # Kalshi omits yes_dollars_fp / no_dollars_fp keys entirely when
        # that side has no resting offers — not [], absent.
        yes_bids = _parse_levels(body.get("yes_dollars_fp"))
        no_bids = _parse_levels(body.get("no_dollars_fp"))

        # Kalshi already returns descending; re-sort defensively.
        yes_bids.sort(key=lambda l: l.price, reverse=True)
        no_bids.sort(key=lambda l: l.price, reverse=True)

        book = self._books.get(ticker)
        if book is None:
            book = KalshiOrderBook(ticker=ticker)
            self._books[ticker] = book
        book.yes_bids = yes_bids
        book.no_bids = no_bids
        book.timestamp = datetime.utcnow()

        state = self._conn_state[conn_id]
        state.snapshot_count += 1
        return ticker

    def _apply_delta(self, conn_id: int, msg: dict) -> Optional[str]:
        """
        Apply an orderbook_delta frame to internal book state.

        Returns the affected market_ticker, or None if the frame was
        malformed. Caller is responsible for enqueueing the ticker.
        """
        body = msg.get("msg") or {}
        ticker = body.get("market_ticker")
        if not ticker:
            return None

        side = body.get("side")
        if side not in ("yes", "no"):
            logger.warning(f"orderbook_delta with unknown side={side!r} on {ticker}")
            return None

        try:
            price = float(body["price_dollars"])
            delta = float(body["delta_fp"])
        except (KeyError, TypeError, ValueError):
            logger.warning(f"orderbook_delta malformed price/delta on {ticker}")
            return None

        book = self._books.get(ticker)
        if book is None:
            # Delta arrived before any snapshot — start a fresh book.
            # The seq tracker will detect this as gap-during-warmup; resync
            # will be triggered by the supervisor when seq tracking is wired.
            book = KalshiOrderBook(ticker=ticker)
            self._books[ticker] = book

        levels = book.yes_bids if side == "yes" else book.no_bids

        # Find existing level at this price.
        existing_idx = next((i for i, l in enumerate(levels) if l.price == price), None)
        if existing_idx is not None:
            new_size = levels[existing_idx].size + delta
            if new_size <= 0:
                levels.pop(existing_idx)
            else:
                # Slot-replace: never mutate a PriceLevel in place.
                levels[existing_idx] = PriceLevel(price=price, size=new_size)
        else:
            if delta > 0:
                levels.append(PriceLevel(price=price, size=delta))
            # delta <= 0 on a missing level: ignore (no negative book entries).

        levels.sort(key=lambda l: l.price, reverse=True)
        if side == "yes":
            book.yes_bids = levels
        else:
            book.no_bids = levels
        book.timestamp = datetime.utcnow()

        state = self._conn_state[conn_id]
        state.delta_count += 1
        return ticker

    def _enqueue_ticker(self, ticker: str) -> None:
        """
        Non-blocking enqueue with drop-oldest semantics on overflow.

        Latest-wins coalescing is naturally handled at consumer time:
        the consumer dedups by always reading the current book via
        `to_unified_orderbook()` on yield, so multiple enqueues of the
        same ticker before the consumer wakes collapse to one delivery
        of the most recent state.
        """
        try:
            self._queue.put_nowait(ticker)
            return
        except asyncio.QueueFull:
            pass

        try:
            self._queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        self._drops += 1
        try:
            self._queue.put_nowait(ticker)
        except asyncio.QueueFull:
            self._drops += 1

    # ------------------------------------------------------------------
    # Public surface — read paths.
    # ------------------------------------------------------------------

    def get_book(self, market_id: str) -> Optional[OrderBook]:
        """
        Return a unified OrderBook for `market_id` ("kalshi:TICKER" form),
        or None if no book is known.

        Returns a fresh `OrderBook` produced by KalshiOrderBook.
        to_unified_orderbook(); the internal state is untouched.
        """
        if not market_id.startswith("kalshi:"):
            return None
        ticker = market_id[len("kalshi:") :]
        book = self._books.get(ticker)
        if book is None:
            return None
        return book.to_unified_orderbook()

    def status(self) -> dict:
        uptime_s = (
            round(time.monotonic() - self._start_monotonic, 2) if self._started else 0.0
        )
        return {
            "started": self._started,
            "uptime_s": uptime_s,
            "shard_count": self._conn_count,
            "market_count": len(self._books),
            "token_count": len(self._books),  # Kalshi: no separate YES/NO at WS layer
            "queue_depth": self._queue.qsize(),
            "queue_maxsize": self._queue_maxsize,
            "drops": self._drops,
            "shards": [s.to_dict() for s in self._conn_state],
        }
