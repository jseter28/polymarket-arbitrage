"""
Data Models for Polymarket Trading Bot
=======================================

Defines core data structures used throughout the trading system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class OrderSide(Enum):
    """Order side enumeration."""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order status enumeration."""
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REJECTED = "rejected"


class TokenType(Enum):
    """Token type in a binary market."""
    YES = "yes"
    NO = "no"


class OpportunityType(Enum):
    """Type of trading opportunity detected."""
    BUNDLE_LONG = "bundle_long"    # Buy YES + NO when sum < 1
    BUNDLE_SHORT = "bundle_short"  # Sell YES + NO when sum > 1
    MM_BID = "mm_bid"              # Market-making bid placement
    MM_ASK = "mm_ask"              # Market-making ask placement


@dataclass
class PriceLevel:
    """Single price level in an order book."""
    price: float
    size: float
    
    def __post_init__(self) -> None:
        self.price = float(self.price)
        self.size = float(self.size)


@dataclass
class OrderBookSide:
    """One side of an order book (bids or asks)."""
    levels: list[PriceLevel] = field(default_factory=list)
    
    @property
    def best_price(self) -> Optional[float]:
        """Get the best price on this side."""
        if not self.levels:
            return None
        return self.levels[0].price
    
    @property
    def best_size(self) -> Optional[float]:
        """Get the size at the best price."""
        if not self.levels:
            return None
        return self.levels[0].size
    
    def get_depth(self, levels: int = 5) -> list[PriceLevel]:
        """Get top N levels of depth."""
        return self.levels[:levels]
    
    def total_size(self, levels: int = 5) -> float:
        """Get total size in top N levels."""
        return sum(level.size for level in self.levels[:levels])


@dataclass
class TokenOrderBook:
    """Order book for a single token (YES or NO)."""
    token_type: TokenType
    bids: OrderBookSide = field(default_factory=OrderBookSide)
    asks: OrderBookSide = field(default_factory=OrderBookSide)
    last_update: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def best_bid(self) -> Optional[float]:
        return self.bids.best_price
    
    @property
    def best_ask(self) -> Optional[float]:
        return self.asks.best_price
    
    @property
    def best_bid_size(self) -> Optional[float]:
        return self.bids.best_size
    
    @property
    def best_ask_size(self) -> Optional[float]:
        return self.asks.best_size
    
    @property
    def spread(self) -> Optional[float]:
        """Calculate bid-ask spread."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid
    
    @property
    def mid_price(self) -> Optional[float]:
        """Calculate mid price."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2


@dataclass
class OrderBook:
    """Complete order book for a market (YES and NO tokens)."""
    market_id: str
    yes: TokenOrderBook = field(default_factory=lambda: TokenOrderBook(TokenType.YES))
    no: TokenOrderBook = field(default_factory=lambda: TokenOrderBook(TokenType.NO))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def best_bid_yes(self) -> Optional[float]:
        return self.yes.best_bid
    
    @property
    def best_ask_yes(self) -> Optional[float]:
        return self.yes.best_ask
    
    @property
    def best_bid_no(self) -> Optional[float]:
        return self.no.best_bid
    
    @property
    def best_ask_no(self) -> Optional[float]:
        return self.no.best_ask
    
    @property
    def total_ask(self) -> Optional[float]:
        """Sum of best ask prices (YES + NO)."""
        if self.best_ask_yes is None or self.best_ask_no is None:
            return None
        return self.best_ask_yes + self.best_ask_no
    
    @property
    def total_bid(self) -> Optional[float]:
        """Sum of best bid prices (YES + NO)."""
        if self.best_bid_yes is None or self.best_bid_no is None:
            return None
        return self.best_bid_yes + self.best_bid_no


@dataclass
class Market:
    """Polymarket market information."""
    market_id: str
    condition_id: str
    question: str
    description: str = ""
    
    # Token IDs
    yes_token_id: str = ""
    no_token_id: str = ""
    
    # Market state
    active: bool = True
    closed: bool = False
    resolved: bool = False
    resolution: Optional[str] = None  # "YES", "NO", or None
    
    # Volume and liquidity
    volume_24h: float = 0.0
    liquidity: float = 0.0
    
    # Timestamps
    created_at: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    # Metadata
    category: str = ""
    tags: list[str] = field(default_factory=list)

    # Tag join data (populated by PolymarketUniversalWS via Gamma /events)
    event_id: str = ""
    tag_ids: list[int] = field(default_factory=list)
    tag_slugs: list[str] = field(default_factory=list)


@dataclass
class Order:
    """Trading order."""
    order_id: str
    market_id: str
    token_type: TokenType
    side: OrderSide
    price: float
    size: float
    filled_size: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    
    # Metadata
    strategy_tag: str = ""  # e.g., "bundle_arb", "mm"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def remaining_size(self) -> float:
        """Get unfilled size."""
        return self.size - self.filled_size
    
    @property
    def is_filled(self) -> bool:
        """Check if order is fully filled."""
        return self.status == OrderStatus.FILLED
    
    @property
    def is_open(self) -> bool:
        """Check if order is still open."""
        return self.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING)
    
    @property
    def notional(self) -> float:
        """Calculate notional value."""
        return self.price * self.size


@dataclass
class Position:
    """Position in a market."""
    market_id: str
    token_type: TokenType
    size: float  # Positive for long, negative for short
    avg_entry_price: float = 0.0
    realized_pnl: float = 0.0
    
    @property
    def notional(self) -> float:
        """Calculate position notional value."""
        return abs(self.size) * self.avg_entry_price
    
    @property
    def is_long(self) -> bool:
        return self.size > 0
    
    @property
    def is_short(self) -> bool:
        return self.size < 0
    
    def unrealized_pnl(self, current_price: float) -> float:
        """Calculate unrealized PnL at current price."""
        if self.size == 0:
            return 0.0
        return self.size * (current_price - self.avg_entry_price)


@dataclass
class Trade:
    """Executed trade."""
    trade_id: str
    order_id: str
    market_id: str
    token_type: TokenType
    side: OrderSide
    price: float
    size: float
    fee: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def notional(self) -> float:
        """Calculate trade notional."""
        return self.price * self.size
    
    @property
    def net_cost(self) -> float:
        """Calculate net cost including fees."""
        return self.notional + self.fee


@dataclass
class Opportunity:
    """Trading opportunity detected by the arbitrage engine."""
    opportunity_id: str
    opportunity_type: OpportunityType
    market_id: str
    edge: float  # Expected profit margin
    
    # Pricing snapshot
    best_bid_yes: Optional[float] = None
    best_ask_yes: Optional[float] = None
    best_bid_no: Optional[float] = None
    best_ask_no: Optional[float] = None
    
    # Sizing
    suggested_size: float = 0.0
    max_size: float = 0.0  # Limited by liquidity
    
    # Metadata
    detected_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    acted_upon: bool = False
    
    @property
    def is_bundle_arb(self) -> bool:
        return self.opportunity_type in (OpportunityType.BUNDLE_LONG, OpportunityType.BUNDLE_SHORT)
    
    @property
    def is_market_making(self) -> bool:
        return self.opportunity_type in (OpportunityType.MM_BID, OpportunityType.MM_ASK)


@dataclass
class Signal:
    """Trading signal from arbitrage engine to execution layer."""
    signal_id: str
    action: str  # "place_orders", "cancel_orders", "modify_orders"
    market_id: str
    opportunity: Optional[Opportunity] = None
    orders: list[dict] = field(default_factory=list)  # Desired order specs
    cancel_order_ids: list[str] = field(default_factory=list)
    priority: int = 0  # Higher = more urgent
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def is_place(self) -> bool:
        return self.action == "place_orders"
    
    @property
    def is_cancel(self) -> bool:
        return self.action == "cancel_orders"


@dataclass 
class MarketState:
    """Complete state snapshot for a market."""
    market: Market
    order_book: OrderBook
    positions: dict[TokenType, Position] = field(default_factory=dict)
    open_orders: list[Order] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def yes_position(self) -> Optional[Position]:
        return self.positions.get(TokenType.YES)
    
    @property
    def no_position(self) -> Optional[Position]:
        return self.positions.get(TokenType.NO)
    
    @property
    def net_exposure(self) -> float:
        """Calculate net exposure in this market."""
        yes_notional = self.yes_position.notional if self.yes_position else 0
        no_notional = self.no_position.notional if self.no_position else 0
        return yes_notional + no_notional

