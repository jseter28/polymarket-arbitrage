"""
Universal Polymarket WebSocket Ingest
=====================================

A standalone, sustained-connection ingest layer for the full active Polymarket
universe (~5000 markets / ~10000 tokens). Solves Polymarket's undocumented
per-connection instrument cap (~500) by sharding the subscription pool across
many concurrent WebSocket connections (default 100 markets / 200 tokens per
shard).

Public surface:

    ws = PolymarketUniversalWS()
    await ws.start(market_ids=None)        # full universe by default
    async for market_id, book in ws.iter_updates():
        ...
    snapshot = ws.get_book(market_id)
    info = ws.status()
    await ws.stop()

This module is intentionally decoupled from `PolymarketClient` / `DataFeed`:
the bot's existing single-connection WS continues working; integration is a
separate downstream task.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import AsyncIterator, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from polymarket_client.models import (
    Market,
    OrderBook,
    OrderBookSide,
    PriceLevel,
    TokenOrderBook,
    TokenType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shard observability state
# ---------------------------------------------------------------------------


@dataclass
class ShardState:
    """Per-shard observability container — exposed via PolymarketUniversalWS.status()."""

    shard_id: int
    tokens: int = 0
    markets: int = 0
    state: str = "init"  # init | connecting | connected | reconnecting | quarantined | stopped
    session_count: int = 0
    message_count: int = 0
    bytes_received: int = 0
    last_msg_monotonic: float = 0.0
    last_disconnect_reason: Optional[str] = None
    last_open_failure: Optional[str] = None
    short_session_streak: int = 0  # consecutive sessions ending in <5s (quarantine trigger)
    connect_attempts: int = 0
    connect_successes: int = 0
    first_msg_received: bool = False

    def to_dict(self) -> dict:
        now = time.monotonic()
        last_msg_age = (now - self.last_msg_monotonic) if self.last_msg_monotonic else None
        return {
            "shard_id": self.shard_id,
            "state": self.state,
            "tokens": self.tokens,
            "markets": self.markets,
            "session_count": self.session_count,
            "message_count": self.message_count,
            "bytes_received": self.bytes_received,
            "last_msg_age_s": round(last_msg_age, 1) if last_msg_age is not None else None,
            "last_disconnect_reason": self.last_disconnect_reason,
            "last_open_failure": self.last_open_failure,
            "short_session_streak": self.short_session_streak,
            "connect_attempts": self.connect_attempts,
            "connect_successes": self.connect_successes,
            "first_msg_received": self.first_msg_received,
        }


# ---------------------------------------------------------------------------
# Module-level helpers (pure, easy to unit-test)
# ---------------------------------------------------------------------------


def _partition_round_robin(markets: list[Market], shard_size: int) -> list[list[Market]]:
    """
    Split markets across shards in a round-robin (interleaved) fashion so each
    shard sees a mixed volume profile (since input is sorted by 24h volume desc,
    plain chunking would put all the loudest markets on one socket).

    Number of shards = ceil(len(markets) / shard_size). Each shard receives at
    most ceil(N / shards) markets.
    """
    if not markets:
        return []
    if shard_size <= 0:
        raise ValueError("shard_size must be positive")

    n_shards = (len(markets) + shard_size - 1) // shard_size
    shards: list[list[Market]] = [[] for _ in range(n_shards)]
    for i, m in enumerate(markets):
        shards[i % n_shards].append(m)
    return shards


async def _fetch_active_markets(
    gamma_url: str,
    max_n: int,
    page_size: int = 100,
    inter_page_delay_s: float = 0.15,
) -> list[Market]:
    """
    Paginate Gamma for active markets, sorted by 24h volume desc, returning up
    to ``max_n`` markets that have valid YES/NO token ids.

    Mirrors the validated logic in `polymarket_client/api.py::list_markets`,
    but returns `Market` dataclasses without dragging in the full client.
    """
    out: list[Market] = []
    offset = 0
    async with httpx.AsyncClient(timeout=30.0) as client:
        while len(out) < max_n:
            params = {
                "closed": "false",
                "active": "true",
                "order": "volume24hr",
                "ascending": "false",
                "limit": page_size,
                "offset": offset,
            }
            try:
                resp = await client.get(f"{gamma_url}/markets", params=params)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Gamma fetch failed at offset={offset}: {e}")
                raise
            data = resp.json()
            if not data:
                break

            batch_valid = 0
            for item in data:
                clob_ids_raw = item.get("clobTokenIds") or ""
                if not clob_ids_raw:
                    continue
                try:
                    ids = json.loads(clob_ids_raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if len(ids) < 2 or not ids[0] or not ids[1]:
                    continue
                yes_tok = str(ids[0]).strip()
                no_tok = str(ids[1]).strip()
                if not yes_tok or not no_tok:
                    continue
                m = Market(
                    market_id=str(item.get("id", "")),
                    condition_id=str(item.get("conditionId", "") or ""),
                    question=str(item.get("question", "") or ""),
                    description=str(item.get("description", "") or ""),
                    yes_token_id=yes_tok,
                    no_token_id=no_tok,
                    active=bool(item.get("active", True)),
                    closed=bool(item.get("closed", False)),
                    volume_24h=float(item.get("volume24hr") or 0.0),
                    liquidity=float(item.get("liquidity") or 0.0),
                )
                if not m.market_id:
                    continue
                out.append(m)
                batch_valid += 1
                if len(out) >= max_n:
                    break

            logger.info(
                f"Gamma fetch: offset={offset} got={len(data)} valid={batch_valid} total={len(out)}"
            )
            if len(data) < page_size:
                break
            offset += page_size
            await asyncio.sleep(inter_page_delay_s)

    return out[:max_n]


# ---------------------------------------------------------------------------
# Universal WS
# ---------------------------------------------------------------------------


class PolymarketUniversalWS:
    """
    Sharded WebSocket pool covering the entire active Polymarket universe.

    See module docstring for usage. Construction does no I/O; ``start()``
    fetches markets (if ``market_ids`` is None), partitions them into shards,
    and spawns one supervisor task per shard. Each supervisor maintains its
    own WS connection, reconnects on disconnect, and writes into the shared
    book state.
    """

    def __init__(
        self,
        gamma_url: str = "https://gamma-api.polymarket.com",
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        shard_size: int = 100,
        max_markets: int = 5000,
        queue_maxsize: int = 10000,
        stale_timeout_s: float = 45.0,
    ) -> None:
        self.gamma_url = gamma_url
        self.ws_url = ws_url
        self.shard_size = shard_size
        self.max_markets = max_markets
        self.queue_maxsize = queue_maxsize
        self.stale_timeout_s = stale_timeout_s

        # asset_id -> (market_id, TokenType). Written disjointly per shard at
        # subscribe time; never cleared during a run.
        self._token_to_market: dict[str, tuple[str, TokenType]] = {}

        # market_id -> OrderBook. Each market is owned by exactly one shard
        # (its YES + NO tokens go together), so writes are disjoint per market.
        self._books: dict[str, OrderBook] = {}

        self._shard_state: list[ShardState] = []
        self._shard_markets: list[list[Market]] = []

        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_maxsize)
        self._stop_event: asyncio.Event = asyncio.Event()
        self._drops: int = 0

        self._supervisor_tasks: list[asyncio.Task] = []
        self._started: bool = False
        self._start_monotonic: float = 0.0

    # -----------------------------------------------------------------------
    # Public surface
    # -----------------------------------------------------------------------

    async def start(self, market_ids: Optional[list[str]] = None) -> None:
        """
        Fetch (or use provided) markets, partition into shards, spawn one
        supervisor task per shard. Returns once supervisors are scheduled —
        first message from each shard may arrive a few seconds later.
        """
        if self._started:
            raise RuntimeError("PolymarketUniversalWS.start() called twice")

        if market_ids is None:
            logger.info(
                f"Fetching active Polymarket universe from Gamma (max_markets={self.max_markets})..."
            )
            markets = await _fetch_active_markets(self.gamma_url, self.max_markets)
        else:
            logger.info(f"Fetching metadata for {len(market_ids)} caller-supplied market ids")
            all_active = await _fetch_active_markets(self.gamma_url, self.max_markets)
            by_id = {m.market_id: m for m in all_active}
            markets = [by_id[mid] for mid in market_ids if mid in by_id]
            missing = len(market_ids) - len(markets)
            if missing:
                logger.warning(
                    f"{missing} of {len(market_ids)} requested market ids not found in active universe"
                )

        if not markets:
            raise RuntimeError("PolymarketUniversalWS.start(): zero valid markets to subscribe")

        self._shard_markets = _partition_round_robin(markets, self.shard_size)
        self._shard_state = [
            ShardState(
                shard_id=i,
                tokens=len(s) * 2,
                markets=len(s),
            )
            for i, s in enumerate(self._shard_markets)
        ]

        logger.info(
            f"WS pool: {len(self._shard_markets)} shards, "
            f"{sum(len(s) for s in self._shard_markets)} markets total, "
            f"~{self.shard_size} markets/shard"
        )

        self._stop_event.clear()
        self._start_monotonic = time.monotonic()
        self._supervisor_tasks = [
            asyncio.create_task(
                self._shard_supervisor(i, ms), name=f"ws_shard_{i}"
            )
            for i, ms in enumerate(self._shard_markets)
        ]
        self._started = True

    async def stop(self) -> None:
        """Signal supervisors to stop, cancel pending tasks, await cleanup."""
        if not self._started:
            return
        self._stop_event.set()
        for t in self._supervisor_tasks:
            if not t.done():
                t.cancel()
        for t in self._supervisor_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._supervisor_tasks = []
        self._started = False
        logger.info("PolymarketUniversalWS stopped")

    async def iter_updates(self) -> AsyncIterator[tuple[str, OrderBook]]:
        """
        Async generator yielding ``(market_id, OrderBook)`` per change.

        The returned OrderBook is a deep copy of internal state — mutate it
        freely. Under sustained load, consumer wake-ups may be dropped (oldest
        first) but the OrderBook yielded always reflects current state.

        If the consumer cancels iteration, the stop event is set so the pool
        cleans up.
        """
        try:
            while not self._stop_event.is_set():
                try:
                    mid = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if mid not in self._books:
                    # Race: dequeued an id whose book hasn't materialized yet.
                    # Skip — next update for the same market will re-enqueue.
                    continue
                yield mid, self._snapshot_book(mid)
        finally:
            # Consumer cancellation propagates a stop signal so the pool can
            # tear down rather than spinning forever with no reader.
            self._stop_event.set()

    def get_book(self, market_id: str) -> Optional[OrderBook]:
        """Return a deep copy of the current combined book, or None if unknown."""
        if market_id not in self._books:
            return None
        return self._snapshot_book(market_id)

    def status(self) -> dict:
        """Live observability snapshot — shape stable for log/dashboard consumption."""
        now = time.monotonic()
        return {
            "started": self._started,
            "uptime_s": round(now - self._start_monotonic, 1) if self._start_monotonic else 0.0,
            "shard_count": len(self._shard_state),
            "market_count": len(self._books),
            "token_count": len(self._token_to_market),
            "queue_depth": self._queue.qsize(),
            "queue_maxsize": self.queue_maxsize,
            "drops": self._drops,
            "shards": [s.to_dict() for s in self._shard_state],
        }

    # -----------------------------------------------------------------------
    # Internal: book state mutations (lifted from PolymarketClient)
    # -----------------------------------------------------------------------

    def _apply_book_snapshot(self, msg: dict) -> Optional[str]:
        """Apply a 'book' snapshot. Returns market_id if known asset, else None."""
        asset_id = msg.get("asset_id")
        if not asset_id or asset_id not in self._token_to_market:
            return None
        market_id, token_type = self._token_to_market[asset_id]

        def to_levels(raw) -> list[PriceLevel]:
            out: list[PriceLevel] = []
            for item in raw or []:
                try:
                    out.append(PriceLevel(price=float(item["price"]), size=float(item["size"])))
                except (KeyError, ValueError, TypeError):
                    continue
            return out

        bids = to_levels(msg.get("bids"))
        asks = to_levels(msg.get("asks"))
        bids.sort(key=lambda l: l.price, reverse=True)
        asks.sort(key=lambda l: l.price)

        token_book = TokenOrderBook(
            token_type=token_type,
            bids=OrderBookSide(levels=bids),
            asks=OrderBookSide(levels=asks),
            last_update=datetime.utcnow(),
        )

        combined = self._books.get(market_id)
        if combined is None:
            combined = OrderBook(market_id=market_id)
            self._books[market_id] = combined
        if token_type == TokenType.YES:
            combined.yes = token_book
        else:
            combined.no = token_book
        combined.timestamp = datetime.utcnow()
        return market_id

    def _apply_price_change(self, msg: dict) -> set[str]:
        """Apply a 'price_change' delta. Returns set of touched market_ids."""
        changes = msg.get("price_changes") or msg.get("changes") or []
        touched: set[str] = set()
        for entry in changes:
            asset_id = entry.get("asset_id")
            if not asset_id or asset_id not in self._token_to_market:
                continue
            market_id, token_type = self._token_to_market[asset_id]
            combined = self._books.get(market_id)
            if combined is None:
                # Delta arrived before snapshot — ignore until snapshot lands.
                continue
            token_book = combined.yes if token_type == TokenType.YES else combined.no
            side_str = (entry.get("side") or "").upper()
            is_bid = side_str == "BUY"
            levels = token_book.bids.levels if is_bid else token_book.asks.levels
            try:
                price = float(entry["price"])
                size = float(entry["size"])
            except (KeyError, ValueError, TypeError):
                continue

            for i, lvl in enumerate(levels):
                if lvl.price == price:
                    if size == 0:
                        levels.pop(i)
                    else:
                        levels[i] = PriceLevel(price=price, size=size)
                    break
            else:
                if size > 0:
                    levels.append(PriceLevel(price=price, size=size))

            if is_bid:
                levels.sort(key=lambda l: l.price, reverse=True)
            else:
                levels.sort(key=lambda l: l.price)
            token_book.last_update = datetime.utcnow()
            combined.timestamp = datetime.utcnow()
            touched.add(market_id)
        return touched

    def _snapshot_book(self, market_id: str) -> OrderBook:
        """Deep copy so consumers can't mutate internal state."""
        return copy.deepcopy(self._books[market_id])

    # -----------------------------------------------------------------------
    # Internal: drop-oldest enqueue
    # -----------------------------------------------------------------------

    def _enqueue_market_id(self, market_id: str) -> None:
        """Non-blocking enqueue with drop-oldest semantics on full queue."""
        try:
            self._queue.put_nowait(market_id)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self._drops += 1
            try:
                self._queue.put_nowait(market_id)
            except asyncio.QueueFull:
                # Extremely unlikely — would require concurrent producers + a
                # nearly-instantly-refilled queue. Just count as a drop.
                self._drops += 1

    # -----------------------------------------------------------------------
    # Internal: per-shard WS lifecycle
    # -----------------------------------------------------------------------

    async def _shard_heartbeat(self, ws, stop: asyncio.Event) -> None:
        """Send app-level 'PING' every 10s until stop or send failure."""
        try:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=10.0)
                    return
                except asyncio.TimeoutError:
                    pass
                try:
                    await ws.send("PING")
                except Exception:
                    return
        except asyncio.CancelledError:
            raise

    async def _shard_recv(
        self,
        shard_id: int,
        ws,
        last_msg_box: list[float],
    ) -> None:
        """Parse messages from one shard, route to handlers, enqueue mids."""
        state = self._shard_state[shard_id]
        async for raw in ws:
            last_msg_box[0] = time.monotonic()
            state.last_msg_monotonic = last_msg_box[0]
            if not state.first_msg_received:
                state.first_msg_received = True
                logger.info(f"shard #{shard_id}: first message received")

            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            state.message_count += 1
            state.bytes_received += len(raw)

            if raw.strip() == "PONG":
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            messages = data if isinstance(data, list) else [data]
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                et = msg.get("event_type")
                if et == "book":
                    mid = self._apply_book_snapshot(msg)
                    if mid:
                        self._enqueue_market_id(mid)
                elif et == "price_change":
                    for mid in self._apply_price_change(msg):
                        self._enqueue_market_id(mid)
                # Other event types (last_trade_price, tick_size_change) ignored

    async def _shard_stale_watchdog(
        self,
        ws,
        last_msg_box: list[float],
        stop: asyncio.Event,
        timeout_s: float,
    ) -> Optional[str]:
        """Force-close the socket if silent for `timeout_s`."""
        try:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=5.0)
                    return None
                except asyncio.TimeoutError:
                    pass
                silence = time.monotonic() - last_msg_box[0]
                if silence > timeout_s:
                    try:
                        await ws.close(code=1000, reason="stale watchdog")
                    except Exception:
                        pass
                    return f"stale_watchdog: {silence:.1f}s of silence"
            return None
        except asyncio.CancelledError:
            raise

    async def _open_shard_ws(self, shard_id: int, markets: list[Market]):
        """Register tokens for this shard, connect, send subscribe frame."""
        assets_ids: list[str] = []
        for m in markets:
            if not m.yes_token_id or not m.no_token_id:
                continue
            assets_ids.append(m.yes_token_id)
            assets_ids.append(m.no_token_id)
            # Token map registration is idempotent across reconnects.
            self._token_to_market[m.yes_token_id] = (m.market_id, TokenType.YES)
            self._token_to_market[m.no_token_id] = (m.market_id, TokenType.NO)

        if not assets_ids:
            raise RuntimeError(f"shard #{shard_id}: no valid tokens to subscribe")

        ws = await websockets.connect(
            self.ws_url,
            ping_interval=None,  # Polymarket uses app-level "PING" strings
            ping_timeout=None,
            close_timeout=5,
            max_size=None,
            open_timeout=15,
        )
        await ws.send(json.dumps({"assets_ids": assets_ids, "type": "market"}))
        logger.info(
            f"shard #{shard_id}: connected, subscribed to {len(assets_ids)} tokens "
            f"({len(markets)} markets)"
        )
        return ws

    async def _run_shard_session(
        self, shard_id: int, markets: list[Market]
    ) -> tuple[str, float]:
        """One connection lifetime. Returns (disconnect_reason, uptime_s)."""
        state = self._shard_state[shard_id]
        state.connect_attempts += 1
        state.state = "connecting"
        connect_mono = time.monotonic()

        try:
            ws = await self._open_shard_ws(shard_id, markets)
        except Exception as e:
            reason = f"open_failed: {type(e).__name__}: {e}"
            state.last_open_failure = reason
            return reason, 0.0

        state.connect_successes += 1
        state.state = "connected"
        last_msg_box = [time.monotonic()]
        stop = asyncio.Event()
        hb = asyncio.create_task(self._shard_heartbeat(ws, stop))
        rx = asyncio.create_task(self._shard_recv(shard_id, ws, last_msg_box))
        wd = asyncio.create_task(
            self._shard_stale_watchdog(ws, last_msg_box, stop, self.stale_timeout_s)
        )

        try:
            stop_waiter = asyncio.create_task(self._stop_event.wait())
            try:
                done, _pending = await asyncio.wait(
                    {rx, wd, stop_waiter},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                if not stop_waiter.done():
                    stop_waiter.cancel()
                    try:
                        await stop_waiter
                    except (asyncio.CancelledError, Exception):
                        pass

            stop.set()
            for t in (hb, rx, wd):
                if not t.done():
                    t.cancel()
            for t in (hb, rx, wd):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

            uptime = time.monotonic() - connect_mono

            if self._stop_event.is_set():
                reason = "stop_event"
            elif rx in done:
                exc = rx.exception() if not rx.cancelled() else None
                if exc is None:
                    reason = "recv_clean_exit"
                elif isinstance(exc, ConnectionClosed):
                    code = getattr(exc, "code", "?")
                    reason = f"ConnectionClosed code={code}: {exc}"
                else:
                    reason = f"recv_exception: {type(exc).__name__}: {exc}"
            elif wd in done:
                try:
                    wd_reason = wd.result()
                except Exception as e:
                    wd_reason = f"watchdog_err: {e}"
                reason = wd_reason or "stale_watchdog"
            else:
                reason = "unknown"

            state.last_disconnect_reason = reason
            return reason, uptime
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    async def _shard_supervisor(self, shard_id: int, markets: list[Market]) -> None:
        """
        Reconnect loop for one shard with jittered backoff and a quarantine
        guard against tight reconnect loops. Exits cleanly when stop_event
        is set; never raises out.
        """
        state = self._shard_state[shard_id]
        # Spread initial reconnect storms across shards.
        initial_jitter = random.uniform(0, shard_id * 0.05)
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
                    f"shard #{shard_id} session #{state.session_count} connecting "
                    f"({len(markets)} markets, {len(markets) * 2} tokens)..."
                )
                try:
                    reason, uptime = await self._run_shard_session(shard_id, markets)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception(
                        f"shard #{shard_id} session #{state.session_count} crashed: {e}"
                    )
                    reason = f"crash: {type(e).__name__}: {e}"
                    uptime = 0.0
                    state.last_disconnect_reason = reason

                if self._stop_event.is_set():
                    state.state = "stopped"
                    logger.info(
                        f"shard #{shard_id} session #{state.session_count} "
                        f"ended after {uptime:.1f}s: {reason} (stop signaled)"
                    )
                    return

                logger.info(
                    f"shard #{shard_id} session #{state.session_count} "
                    f"ended after {uptime:.1f}s: {reason}"
                )

                # Quarantine guard: 3 consecutive short sessions → long sleep.
                if uptime < 5.0:
                    state.short_session_streak += 1
                else:
                    state.short_session_streak = 0

                # Backoff schedule.
                if reason.startswith("open_failed"):
                    open_failure_streak += 1
                    backoff = min(2 ** (open_failure_streak - 1), 30)
                else:
                    open_failure_streak = 0
                    backoff = 1.0

                if state.short_session_streak >= 3:
                    logger.error(
                        f"shard #{shard_id}: 3 consecutive short sessions; "
                        f"quarantining for 60s"
                    )
                    state.state = "quarantined"
                    backoff = 60.0
                    state.short_session_streak = 0
                else:
                    state.state = "reconnecting"

                # Sleep with stop-event responsiveness.
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                    return
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            state.state = "stopped"
            raise
        except Exception as e:
            logger.exception(f"shard #{shard_id} supervisor crashed fatally: {e}")
            state.state = "stopped"
