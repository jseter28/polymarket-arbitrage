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
import json
import logging
import random
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import httpx
import websockets
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from websockets.exceptions import ConnectionClosed

from kalshi_client.auth import build_headers
from kalshi_client.models import KalshiOrderBook
from polymarket_client.models import OrderBook, PriceLevel

logger = logging.getLogger(__name__)

DEFAULT_BASE_WS = "wss://demo-api.kalshi.co/trade-api/ws/v2"
DEFAULT_BASE_REST = "https://demo-api.kalshi.co/trade-api/v2"
WS_PATH = "/trade-api/ws/v2"  # used in signing string

# Demo cap is 10; reserve headroom for ad-hoc dev sessions.
MAX_CONN_COUNT = 9

# Subscribe / update_subscription batching size (Phase 2 confirmed 500 works).
SUBSCRIBE_BATCH_SIZE = 500

# Quarantine threshold: 3 consecutive short sessions (< 5s) triggers 60s sleep.
QUARANTINE_THRESHOLD = 3
QUARANTINE_BACKOFF_S = 60.0
SHORT_SESSION_THRESHOLD_S = 5.0
OPEN_FAILED_BACKOFF_CAP_S = 30
DEFAULT_RECONNECT_BACKOFF_S = 1.0

# market_lifecycle_v2 events: which trigger add/remove on our side.
# (Exact strings per Kalshi docs; live smoke will confirm.)
LIFECYCLE_ADD_EVENTS = frozenset({"market_created", "market_activated"})
LIFECYCLE_REMOVE_EVENTS = frozenset(
    {"market_deactivated", "market_settled", "market_determined"}
)

# REST safety-net reconcile cadence.
DEFAULT_RECONCILE_INTERVAL_S = 900.0


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
    lifecycle_sid: int | None = None  # set on conn 0 after lifecycle subscribe
    session_count: int = 0
    message_count: int = 0
    bytes_received: int = 0
    snapshot_count: int = 0
    delta_count: int = 0
    seq_gap_count: int = 0
    snapshot_resync_count: int = 0
    lifecycle_event_count: int = 0
    lifecycle_add_count: int = 0
    lifecycle_remove_count: int = 0
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
            "lifecycle_sid": self.lifecycle_sid,
            "session_count": self.session_count,
            "message_count": self.message_count,
            "bytes_received": self.bytes_received,
            "snapshot_count": self.snapshot_count,
            "delta_count": self.delta_count,
            "seq_gap_count": self.seq_gap_count,
            "snapshot_resync_count": self.snapshot_resync_count,
            "lifecycle_event_count": self.lifecycle_event_count,
            "lifecycle_add_count": self.lifecycle_add_count,
            "lifecycle_remove_count": self.lifecycle_remove_count,
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
        reconcile_interval_s: float = DEFAULT_RECONCILE_INTERVAL_S,
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

        # Per-conn live WS handle. Set by _run_conn_session when conn is open;
        # cleared on disconnect. Used by _resync_conn to send recovery commands.
        self._conn_ws: list[Any] = [None] * conn_count

        # Reconcile loop config + telemetry.
        self._reconcile_interval_s = reconcile_interval_s
        self._reconcile_task: Optional[asyncio.Task] = None
        self._reconcile_count = 0
        self._reconcile_last_added = 0
        self._reconcile_last_removed = 0
        # Monotonically increasing correlation id for outbound commands.
        # 0 is reserved by Kalshi ("treated as no id"); start at 1.
        self._cmd_id_seq = 1

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

    def _track_seq(self, conn_id: int, seq: int) -> bool:
        """
        Advance the per-conn seq tracker. Returns True if a gap was detected.

        Per Phase 3 findings (tasks/kalshi-seq-semantics.md): seq is per-sid,
        monotonic, gap-free under normal flow. First message on a fresh sub
        sets the baseline; subsequent messages must be `last + 1`.
        """
        state = self._conn_state[conn_id]
        if state.last_seq is None:
            state.last_seq = seq
            return False
        if seq == state.last_seq + 1:
            state.last_seq = seq
            return False
        # Gap (forward jump or out-of-order). Record and advance.
        logger.warning(
            f"conn#{conn_id} seq gap: prev={state.last_seq} got={seq} "
            f"(missing {seq - state.last_seq - 1} messages)"
        )
        state.seq_gap_count += 1
        state.last_seq = seq
        return True

    def _reset_conn_state(self, conn_id: int, reason: str) -> None:
        """Drop the live state for a conn on disconnect. Book state is preserved."""
        state = self._conn_state[conn_id]
        state.sid = None
        state.last_seq = None
        state.lifecycle_sid = None
        state.first_msg_received = False
        state.last_disconnect_reason = reason
        self._conn_ws[conn_id] = None

    def _next_cmd_id(self) -> int:
        self._cmd_id_seq += 1
        return self._cmd_id_seq

    def _prepare_add(self, conn_id: int, ticker: str) -> Optional[dict]:
        """
        Synchronously update internal state to reflect a new subscription on
        `conn_id`. Returns the JSON command to send to that conn's WS, or
        None if the conn isn't ready (or the ticker is already subscribed).
        """
        if conn_id < 0 or conn_id >= self._conn_count:
            return None
        ws = self._conn_ws[conn_id]
        state = self._conn_state[conn_id]
        if ws is None or state.sid is None:
            return None
        if ticker in self._ticker_to_conn:
            return None
        self._ticker_to_conn[ticker] = conn_id
        self._conn_tickers[conn_id].append(ticker)
        state.tickers += 1
        return {
            "id": self._next_cmd_id(),
            "cmd": "update_subscription",
            "params": {
                "sids": [state.sid],
                "market_tickers": [ticker],
                "action": "add_markets",
            },
        }

    def _prepare_remove(self, conn_id: int, ticker: str) -> Optional[dict]:
        """
        Synchronously remove `ticker` from `conn_id`'s subscription state and
        drop its book. Returns the JSON command to send, or None if the conn
        isn't ready or the ticker isn't currently subscribed.
        """
        if conn_id < 0 or conn_id >= self._conn_count:
            return None
        ws = self._conn_ws[conn_id]
        state = self._conn_state[conn_id]
        if ws is None or state.sid is None:
            return None
        if ticker not in self._ticker_to_conn:
            return None
        del self._ticker_to_conn[ticker]
        try:
            self._conn_tickers[conn_id].remove(ticker)
        except ValueError:
            pass
        self._books.pop(ticker, None)
        state.tickers = max(0, state.tickers - 1)
        return {
            "id": self._next_cmd_id(),
            "cmd": "update_subscription",
            "params": {
                "sids": [state.sid],
                "market_tickers": [ticker],
                "action": "delete_markets",
            },
        }

    def _schedule_send(self, conn_id: int, cmd: dict) -> None:
        """Fire-and-forget async send. No-op if no running loop (unit tests)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._send_cmd(conn_id, cmd))

    async def _send_cmd(self, conn_id: int, cmd: dict) -> None:
        ws = self._conn_ws[conn_id]
        if ws is None:
            return
        try:
            await ws.send(json.dumps(cmd))
        except Exception as e:
            logger.warning(f"conn#{conn_id} _send_cmd failed: {e}")

    def _handle_lifecycle_event(self, raw_msg: dict) -> None:
        """
        Dispatch a market_lifecycle_v2 event. Called synchronously from
        _route_message. Updates per-conn telemetry and, for add/remove
        events, schedules the corresponding update_subscription command.

        Lifecycle events arrive on conn 0 (the conn that holds the
        lifecycle subscription); the affected ticker lives on whichever
        conn hash(ticker) % conn_count selects.
        """
        state0 = self._conn_state[0]
        state0.lifecycle_event_count += 1

        body = raw_msg.get("msg") or {}
        ticker = body.get("market_ticker")
        event_type = raw_msg.get("type", "")
        if not ticker:
            return

        if event_type in LIFECYCLE_ADD_EVENTS:
            target_cid = zlib.crc32(ticker.encode("utf-8")) % self._conn_count
            cmd = self._prepare_add(target_cid, ticker)
            if cmd is not None:
                self._conn_state[target_cid].lifecycle_add_count += 1
                self._schedule_send(target_cid, cmd)
            return

        if event_type in LIFECYCLE_REMOVE_EVENTS:
            owner_cid = self._ticker_to_conn.get(ticker)
            if owner_cid is None:
                return
            cmd = self._prepare_remove(owner_cid, ticker)
            if cmd is not None:
                self._conn_state[owner_cid].lifecycle_remove_count += 1
                self._schedule_send(owner_cid, cmd)
            return

        # Other event types (metadata_updated, close_date_updated, etc.):
        # telemetry only; no subscription change.

    def _compute_reconcile_diff(
        self, rest_tickers: list[str]
    ) -> tuple[list[str], list[str]]:
        """Given the REST-truth set of open tickers, return (adds, removes)."""
        current = set(self._ticker_to_conn.keys())
        truth = set(rest_tickers)
        adds = sorted(truth - current)
        removes = sorted(current - truth)
        return adds, removes

    def _route_message(self, conn_id: int, raw_msg: dict) -> None:
        """
        Dispatch a parsed WS frame to the right handler. Single entry point
        from the recv loop so unit tests and the live supervisor agree.
        """
        mtype = raw_msg.get("type")
        state = self._conn_state[conn_id]
        state.message_count += 1
        state.last_msg_monotonic = time.monotonic()
        if not state.first_msg_received:
            state.first_msg_received = True

        if mtype == "subscribed":
            # sid nests under msg.sid (Phase 2 found this the hard way).
            sub_msg = raw_msg.get("msg") or {}
            sid = sub_msg.get("sid")
            channel = sub_msg.get("channel")
            if channel == "market_lifecycle_v2":
                state.lifecycle_sid = sid
                logger.info(f"conn#{conn_id} lifecycle subscribed sid={sid}")
            else:
                state.sid = sid
                logger.info(f"conn#{conn_id} subscribed sid={sid}")
            return

        if mtype == "ok" or mtype == "subscription_updated":
            # Acknowledgement of a subscribe/update_subscription. Already
            # bumped message_count above.
            return

        if mtype == "error":
            err_body = raw_msg.get("msg") or {}
            reason = f"server error {err_body.get('code')}: {err_body.get('msg')}"
            logger.warning(f"conn#{conn_id} {reason}")
            state.last_disconnect_reason = reason
            return

        if mtype == "orderbook_snapshot":
            seq = raw_msg.get("seq")
            if seq is not None:
                self._track_seq(conn_id, int(seq))
            ticker = self._apply_snapshot(conn_id, raw_msg)
            if ticker:
                self._enqueue_ticker(ticker)
            return

        if mtype == "orderbook_delta":
            seq = raw_msg.get("seq")
            gap_detected = False
            if seq is not None:
                gap_detected = self._track_seq(conn_id, int(seq))
            ticker = self._apply_delta(conn_id, raw_msg)
            if ticker:
                self._enqueue_ticker(ticker)
            if gap_detected:
                # Fire and forget — resync is non-destructive (seq continues).
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # No running loop (e.g., in unit tests). Skip; the
                    # supervisor will catch up on next live frame.
                    pass
                else:
                    loop.create_task(self._resync_conn(conn_id))
            return

        if isinstance(mtype, str) and mtype.startswith("market_"):
            self._handle_lifecycle_event(raw_msg)
            return

        # Unknown / unhandled message type.
        logger.debug(f"conn#{conn_id} ignored message type={mtype!r}")

    @staticmethod
    def _compute_open_failed_backoff(streak: int) -> int:
        """Exponential backoff capped at 30s. streak=1 → 1s, 2→2, ..., 6+→30."""
        return min(2 ** max(0, streak - 1), OPEN_FAILED_BACKOFF_CAP_S)

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

    async def iter_updates(self):
        """
        Yield (market_id, OrderBook) tuples as books change. Latest-wins
        coalescing happens implicitly: the consumer always sees the current
        book state at yield time, not the per-frame state.

        On consumer cancellation, the stop_event is set so supervisors wind
        down cleanly.
        """
        try:
            while not self._stop_event.is_set():
                try:
                    ticker = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                book = self._books.get(ticker)
                if book is None:
                    # Dequeued before any snapshot landed; skip.
                    continue
                yield (f"kalshi:{ticker}", book.to_unified_orderbook())
        finally:
            self._stop_event.set()

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
            "reconcile_count": self._reconcile_count,
            "reconcile_last_added": self._reconcile_last_added,
            "reconcile_last_removed": self._reconcile_last_removed,
            "shards": [s.to_dict() for s in self._conn_state],
        }

    # ------------------------------------------------------------------
    # Lifecycle — public.
    # ------------------------------------------------------------------

    async def start(self, market_tickers: Optional[list[str]] = None) -> None:
        """
        Open `conn_count` sharded WS connections, subscribe each to its
        share of `market_tickers`, and begin streaming snapshots + deltas.

        If `market_tickers` is None, fetches all currently-open markets
        from the REST API.
        """
        if self._started:
            raise RuntimeError("KalshiUniversalWS.start() called twice")

        if market_tickers is None:
            market_tickers = await self._fetch_open_market_tickers()
        if not market_tickers:
            raise RuntimeError("No market_tickers to subscribe — universe is empty")

        shards = _partition_by_hash(market_tickers, self._conn_count)
        for cid, sub in enumerate(shards):
            self._conn_tickers[cid] = list(sub)
            for t in sub:
                self._ticker_to_conn[t] = cid
            self._conn_state[cid].tickers = len(sub)

        self._started = True
        self._start_monotonic = time.monotonic()
        self._stop_event.clear()

        for cid in range(self._conn_count):
            tickers = self._conn_tickers[cid]
            if not tickers:
                continue
            task = asyncio.create_task(self._conn_supervisor(cid, tickers))
            self._supervisor_tasks.append(task)

        # REST safety-net reconcile loop. Keeps the subscribed set in sync
        # with /markets?status=open when lifecycle events are missed
        # (notably KXMVE-prefixed markets — they are excluded from the
        # market_lifecycle_v2 channel per Kalshi docs).
        self._reconcile_task = asyncio.create_task(self._reconcile_loop())

        logger.info(
            f"KalshiUniversalWS started: {self._conn_count} conns, "
            f"{len(market_tickers)} tickers total"
        )

    async def stop(self) -> None:
        """Signal supervisors to shut down, await their exit."""
        if not self._started:
            return
        self._stop_event.set()
        for t in self._supervisor_tasks:
            t.cancel()
        if self._reconcile_task is not None:
            self._reconcile_task.cancel()
        if self._supervisor_tasks:
            await asyncio.gather(*self._supervisor_tasks, return_exceptions=True)
        if self._reconcile_task is not None:
            await asyncio.gather(self._reconcile_task, return_exceptions=True)
            self._reconcile_task = None
        self._supervisor_tasks.clear()
        self._started = False

    async def _reconcile_loop(self) -> None:
        """Periodic REST reconcile. Exits cleanly when stop_event is set."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._reconcile_interval_s
                )
                return  # stop signaled
            except asyncio.TimeoutError:
                pass
            try:
                await self._reconcile_loop_once()
            except Exception as e:
                logger.exception(f"reconcile cycle failed: {e}")

    async def _reconcile_loop_once(self) -> None:
        """One cycle of REST reconcile. Public for smoke verification."""
        rest_tickers = await self._fetch_open_market_tickers()
        adds, removes = self._compute_reconcile_diff(rest_tickers)
        self._reconcile_count += 1
        self._reconcile_last_added = 0
        self._reconcile_last_removed = 0
        for ticker in adds:
            target_cid = zlib.crc32(ticker.encode("utf-8")) % self._conn_count
            cmd = self._prepare_add(target_cid, ticker)
            if cmd is not None:
                self._reconcile_last_added += 1
                self._schedule_send(target_cid, cmd)
        for ticker in removes:
            owner_cid = self._ticker_to_conn.get(ticker)
            if owner_cid is None:
                continue
            cmd = self._prepare_remove(owner_cid, ticker)
            if cmd is not None:
                self._reconcile_last_removed += 1
                self._schedule_send(owner_cid, cmd)
        logger.info(
            f"reconcile #{self._reconcile_count}: "
            f"added={self._reconcile_last_added} "
            f"removed={self._reconcile_last_removed}"
        )

    # ------------------------------------------------------------------
    # Lifecycle — internal.
    # ------------------------------------------------------------------

    async def _fetch_open_market_tickers(self) -> list[str]:
        """Page through /markets?status=open and return every active ticker."""
        tickers: list[str] = []
        cursor = ""
        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                params: dict = {"status": "open", "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                r = await client.get(
                    f"{self._base_rest}/markets",
                    params=params,
                    headers={"Accept": "application/json"},
                )
                r.raise_for_status()
                d = r.json()
                markets = d.get("markets", [])
                if not markets:
                    break
                tickers.extend(m["ticker"] for m in markets if m.get("ticker"))
                cursor = d.get("cursor", "")
                if not cursor:
                    break
        return tickers

    async def _conn_supervisor(self, conn_id: int, tickers: list[str]) -> None:
        """Per-conn reconnect loop: jitter, session, backoff, quarantine."""
        state = self._conn_state[conn_id]
        # Spread initial reconnect storms across conns.
        initial_jitter = random.uniform(0, conn_id * 0.05)
        if initial_jitter:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=initial_jitter)
                return
            except asyncio.TimeoutError:
                pass

        open_failure_streak = 0
        try:
            while not self._stop_event.is_set():
                state.session_count += 1
                logger.info(
                    f"conn#{conn_id} session #{state.session_count} connecting "
                    f"({len(tickers)} tickers)"
                )
                try:
                    reason, uptime = await self._run_conn_session(conn_id, tickers)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception(f"conn#{conn_id} session crashed: {e}")
                    reason = f"crash: {type(e).__name__}: {e}"
                    uptime = 0.0
                    state.last_disconnect_reason = reason

                self._reset_conn_state(conn_id, reason)

                if self._stop_event.is_set():
                    state.state = "stopped"
                    return

                logger.info(
                    f"conn#{conn_id} session #{state.session_count} "
                    f"ended after {uptime:.1f}s: {reason}"
                )

                # Quarantine guard.
                if uptime < SHORT_SESSION_THRESHOLD_S:
                    state.short_session_streak += 1
                else:
                    state.short_session_streak = 0

                # Backoff schedule.
                backoff: float
                if reason.startswith("open_failed"):
                    open_failure_streak += 1
                    backoff = float(
                        self._compute_open_failed_backoff(open_failure_streak)
                    )
                else:
                    open_failure_streak = 0
                    backoff = DEFAULT_RECONNECT_BACKOFF_S

                if state.short_session_streak >= QUARANTINE_THRESHOLD:
                    logger.error(
                        f"conn#{conn_id}: {QUARANTINE_THRESHOLD} consecutive short "
                        f"sessions; quarantining for {QUARANTINE_BACKOFF_S}s"
                    )
                    state.state = "quarantined"
                    backoff = QUARANTINE_BACKOFF_S
                    state.short_session_streak = 0
                else:
                    state.state = "reconnecting"

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                    return
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            state.state = "stopped"
            raise
        except Exception as e:
            logger.exception(f"conn#{conn_id} supervisor crashed fatally: {e}")
            state.state = "stopped"

    async def _run_conn_session(
        self, conn_id: int, tickers: list[str]
    ) -> tuple[str, float]:
        """One conn lifetime: handshake → subscribe → recv loop → return."""
        state = self._conn_state[conn_id]
        state.state = "connecting"
        state.connect_attempts += 1
        started_at = time.monotonic()

        try:
            headers = build_headers(self._api_key_id, self._private_key, "GET", WS_PATH)
            ws = await websockets.connect(
                self._base_ws,
                extra_headers=headers,
                open_timeout=20,
                ping_interval=20,
                max_size=16 * 1024 * 1024,
            )
        except Exception as e:
            failure = f"open_failed: {type(e).__name__}: {e}"
            state.last_open_failure = failure
            logger.warning(f"conn#{conn_id} {failure}")
            return failure, 0.0

        state.state = "connected"
        state.connect_successes += 1
        self._conn_ws[conn_id] = ws

        try:
            # Initial subscribe — batch tickers if huge.
            if len(tickers) <= SUBSCRIBE_BATCH_SIZE:
                await ws.send(
                    json.dumps(
                        {
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_tickers": tickers,
                            },
                        }
                    )
                )
            else:
                # First batch creates the sid; remaining batches use
                # update_subscription add_markets once sid is known.
                first = tickers[:SUBSCRIBE_BATCH_SIZE]
                await ws.send(
                    json.dumps(
                        {
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_tickers": first,
                            },
                        }
                    )
                )
                # The rest are appended once subscribed ack arrives; do it
                # eagerly without waiting — Kalshi merges into the existing
                # sid via update_subscription's `subscribe` semantics. To
                # keep this simple and correct, we wait briefly for the sid
                # then add the rest.
                deadline = time.monotonic() + 5.0
                while state.sid is None and time.monotonic() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    state.bytes_received += len(raw)
                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        continue
                    self._route_message(conn_id, parsed)
                if state.sid is not None:
                    for i in range(
                        SUBSCRIBE_BATCH_SIZE, len(tickers), SUBSCRIBE_BATCH_SIZE
                    ):
                        batch = tickers[i : i + SUBSCRIBE_BATCH_SIZE]
                        await ws.send(
                            json.dumps(
                                {
                                    "id": 100 + i // SUBSCRIBE_BATCH_SIZE,
                                    "cmd": "update_subscription",
                                    "params": {
                                        "sids": [state.sid],
                                        "market_tickers": batch,
                                        "action": "add_markets",
                                    },
                                }
                            )
                        )

            # On conn 0, also subscribe to market_lifecycle_v2 so we receive
            # activated/deactivated/settled events. Separate channel = separate
            # sid (captured into state.lifecycle_sid by _route_message on the
            # subscribed ack).
            if conn_id == 0:
                await ws.send(
                    json.dumps(
                        {
                            "id": self._next_cmd_id(),
                            "cmd": "subscribe",
                            "params": {"channels": ["market_lifecycle_v2"]},
                        }
                    )
                )

            # Main recv loop.
            async for raw in ws:
                if self._stop_event.is_set():
                    break
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                state.bytes_received += len(raw)
                try:
                    parsed = json.loads(raw)
                except Exception:
                    logger.warning(f"conn#{conn_id} non-JSON frame; skipping")
                    continue
                self._route_message(conn_id, parsed)
        except ConnectionClosed as e:
            uptime = time.monotonic() - started_at
            return f"ConnectionClosed code={e.code} reason={e.reason!r}", uptime
        except asyncio.CancelledError:
            raise
        finally:
            self._conn_ws[conn_id] = None
            try:
                await ws.close()
            except Exception:
                pass

        return "loop_exit", time.monotonic() - started_at

    async def _resync_conn(self, conn_id: int) -> None:
        """
        Request a fresh snapshot for every ticker on this conn via
        `update_subscription get_snapshot`. Phase 3 confirmed seq continues
        (no reset), so this is safe to issue mid-stream.
        """
        state = self._conn_state[conn_id]
        ws = self._conn_ws[conn_id]
        if ws is None or state.sid is None:
            return  # Conn not live; supervisor will rebuild from scratch on reconnect.

        tickers = list(self._conn_tickers[conn_id])
        if not tickers:
            return

        state.snapshot_resync_count += 1
        logger.warning(
            f"conn#{conn_id} requesting resync for {len(tickers)} tickers "
            f"(gap_count={state.seq_gap_count})"
        )
        for i in range(0, len(tickers), SUBSCRIBE_BATCH_SIZE):
            batch = tickers[i : i + SUBSCRIBE_BATCH_SIZE]
            cmd = {
                "id": 900 + i // SUBSCRIBE_BATCH_SIZE,
                "cmd": "update_subscription",
                "params": {
                    "sids": [state.sid],
                    "market_tickers": batch,
                    "action": "get_snapshot",
                },
            }
            try:
                await ws.send(json.dumps(cmd))
            except Exception as e:
                logger.warning(f"conn#{conn_id} resync send failed: {e}")
                return
