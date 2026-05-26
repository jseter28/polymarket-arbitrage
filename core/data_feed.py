"""
Data Feed Module
=================

Maintains real-time in-memory state of order books and positions
for all monitored markets.
"""

import asyncio
import logging
from datetime import datetime
from typing import Callable, Optional

from polymarket_client.api import PolymarketClient
from polymarket_client.models import (
    Market,
    MarketState,
    OrderBook,
    Position,
    TokenType,
)


logger = logging.getLogger(__name__)


class DataFeed:
    """
    Real-time data feed manager.
    
    Subscribes to order book updates via WebSocket and periodically
    refreshes positions via REST API. Provides a unified view of
    market state for the trading engine.
    """
    
    def __init__(
        self,
        client: PolymarketClient,
        market_ids: list[str],
        position_refresh_interval: float = 5.0,
        on_update: Optional[Callable[[str, MarketState], None]] = None,
        config = None,
    ):
        self.client = client
        self.market_ids = market_ids
        self.position_refresh_interval = position_refresh_interval
        self.on_update = on_update
        self.config = config

        # In-memory state
        self._markets: dict[str, Market] = {}
        self._order_books: dict[str, OrderBook] = {}
        self._positions: dict[str, dict[TokenType, Position]] = {}
        self._market_states: dict[str, MarketState] = {}

        # Tasks
        self._orderbook_task: Optional[asyncio.Task] = None
        self._position_task: Optional[asyncio.Task] = None
        self._running = False

        # Analyze queue: producer (_update_market_state) enqueues market_ids;
        # consumer (await next_update()) dequeues the latest state per market.
        # Latest-wins coalescing: _pending holds at most one state per market,
        # _queued tracks which markets currently have an entry in the queue.
        # With per-market coalescing the queue size is bounded by len(market_ids),
        # so we leave it unbounded (set() membership keeps it tight).
        self._analyze_queue: asyncio.Queue[str] = asyncio.Queue()
        self._pending: dict[str, MarketState] = {}
        self._queued: set[str] = set()

        # Statistics
        self._update_count = 0
        self._last_update: dict[str, datetime] = {}
    
    async def start(self) -> None:
        """
        Start the data feed.
        
        Connects to order book streams and starts position refresh loop.
        """
        if self._running:
            logger.warning("DataFeed already running")
            return
        
        self._running = True
        logger.info(f"Starting DataFeed for {len(self.market_ids)} markets")
        
        # Fetch initial market info
        await self._fetch_markets()
        
        # Fetch initial positions
        await self._refresh_positions()
        
        # Start streaming order books
        self._orderbook_task = asyncio.create_task(
            self._stream_orderbooks(),
            name="orderbook_stream"
        )
        
        # Start position refresh loop
        self._position_task = asyncio.create_task(
            self._position_refresh_loop(),
            name="position_refresh"
        )
        
        logger.info("DataFeed started successfully")
    
    async def stop(self) -> None:
        """Stop the data feed."""
        self._running = False
        
        if self._orderbook_task:
            self._orderbook_task.cancel()
            try:
                await self._orderbook_task
            except asyncio.CancelledError:
                pass
        
        if self._position_task:
            self._position_task.cancel()
            try:
                await self._position_task
            except asyncio.CancelledError:
                pass
        
        logger.info("DataFeed stopped")
    
    async def _fetch_markets(self) -> None:
        """Fetch market information for all monitored markets."""
        try:
            if not self.market_ids:
                # Discover markets if none specified - list_markets returns full Market objects!
                markets = await self.client.list_markets({"active": True})
                
                # Store markets directly from the list - no need to re-fetch!
                for market in markets:
                    self._markets[market.market_id] = market
                
                self.market_ids = [m.market_id for m in markets]
                logger.info(f"Discovered and loaded {len(self.market_ids)} active markets (no re-fetch needed!)")
            else:
                # Only fetch if specific market_ids were provided
                for market_id in self.market_ids:
                    market = await self.client.get_market(market_id)
                    self._markets[market_id] = market
                
        except Exception as e:
            logger.error(f"Failed to fetch markets: {e}")
            raise
    
    async def _stream_orderbooks(self) -> None:
        """Stream order book updates."""
        # Use simulation for demo/screenshots, real data for production
        # Check config.mode.data_mode (set in config.yaml)
        use_simulation = getattr(self.config, 'use_simulation', False)
        
        while self._running:
            try:
                async for market_id, orderbook in self.client.stream_orderbook(self.market_ids, use_simulation=use_simulation):
                    if not self._running:
                        break
                    
                    self._order_books[market_id] = orderbook
                    self._last_update[market_id] = datetime.utcnow()
                    self._update_count += 1
                    
                    # Update market state
                    self._update_market_state(market_id)
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Order book stream error: {e}")
                if self._running:
                    await asyncio.sleep(1)  # Brief delay before reconnecting
    
    async def _position_refresh_loop(self) -> None:
        """Periodically refresh positions."""
        while self._running:
            try:
                await asyncio.sleep(self.position_refresh_interval)
                await self._refresh_positions()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Position refresh error: {e}")
    
    async def _refresh_positions(self) -> None:
        """Fetch current positions from API."""
        try:
            self._positions = await self.client.get_positions()
            logger.debug(f"Refreshed positions for {len(self._positions)} markets")
            
            # Update market states with new positions
            for market_id in self._positions:
                if market_id in self._order_books:
                    self._update_market_state(market_id)
                    
        except Exception as e:
            logger.warning(f"Failed to refresh positions: {e}")
    
    def _update_market_state(self, market_id: str) -> None:
        """Update the complete market state for a market."""
        if market_id not in self._markets:
            return
        
        state = MarketState(
            market=self._markets.get(market_id, Market(market_id=market_id, condition_id=market_id, question="")),
            order_book=self._order_books.get(market_id, OrderBook(market_id=market_id)),
            positions=self._positions.get(market_id, {}),
            open_orders=[],  # Will be populated by execution engine
            timestamp=datetime.utcnow(),
        )
        
        self._market_states[market_id] = state

        # Latest-wins per-market enqueue. A market already in the queue is not
        # re-enqueued; the consumer picks up whatever is in _pending when it
        # dequeues the market_id. Stale intermediate states are dropped — fine
        # for arb detection (only the latest book matters).
        self._pending[market_id] = state
        if market_id not in self._queued:
            try:
                self._analyze_queue.put_nowait(market_id)
                self._queued.add(market_id)
            except asyncio.QueueFull:
                logger.error(f"Analyze queue full for {market_id} — dropping update")

        # Notify callback if set (legacy, synchronous — not used by the bot
        # entry points after R2; kept for backward-compat).
        if self.on_update:
            try:
                self.on_update(market_id, state)
            except Exception as e:
                logger.error(f"Update callback error for {market_id}: {e}")

    async def next_update(self) -> tuple[str, Optional[MarketState]]:
        """
        Wait for the next market update.

        Returns (market_id, state). state is None if the pending entry was
        cleared between enqueue and dequeue (race — consumer should skip).
        """
        market_id = await self._analyze_queue.get()
        self._queued.discard(market_id)
        state = self._pending.pop(market_id, None)
        return market_id, state
    
    def get_market_state(self, market_id: str) -> Optional[MarketState]:
        """
        Get the latest state snapshot for a market.
        
        Returns None if the market hasn't been loaded yet.
        """
        return self._market_states.get(market_id)
    
    def get_all_market_states(self) -> dict[str, MarketState]:
        """Get all current market states."""
        return self._market_states.copy()
    
    def get_order_book(self, market_id: str) -> Optional[OrderBook]:
        """Get the latest order book for a market."""
        return self._order_books.get(market_id)
    
    def get_position(self, market_id: str, token_type: TokenType) -> Optional[Position]:
        """Get position for a specific market and token."""
        market_positions = self._positions.get(market_id, {})
        return market_positions.get(token_type)
    
    def get_positions(self, market_id: str) -> dict[TokenType, Position]:
        """Get all positions for a market."""
        return self._positions.get(market_id, {})
    
    def get_market(self, market_id: str) -> Optional[Market]:
        """Get market information."""
        return self._markets.get(market_id)
    
    @property
    def update_count(self) -> int:
        """Get total number of order book updates received."""
        return self._update_count
    
    @property
    def is_running(self) -> bool:
        """Check if the data feed is running."""
        return self._running
    
    def get_staleness(self, market_id: str) -> Optional[float]:
        """
        Get time since last update for a market (in seconds).
        Returns None if never updated.
        """
        if market_id not in self._last_update:
            return None
        return (datetime.utcnow() - self._last_update[market_id]).total_seconds()
    
    async def wait_for_data(self, timeout: float = 10.0) -> bool:
        """
        Wait until data is available for all markets.
        
        Returns True if data is available, False on timeout.
        """
        start = datetime.utcnow()
        while (datetime.utcnow() - start).total_seconds() < timeout:
            if all(m in self._order_books for m in self.market_ids):
                return True
            await asyncio.sleep(0.1)
        return False

