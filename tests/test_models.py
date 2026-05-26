"""
Tests for OrderBook.clone() — R1: shallow clone to replace copy.deepcopy.

The clone contract:
  * new OrderBook, new TokenOrderBook(yes), new TokenOrderBook(no)
  * new OrderBookSide(bids/asks) on each side
  * new levels list per side (the producer mutates these in place with
    pop/append/sort/slot-assign, so consumers must have independent lists)
  * PriceLevel references are SHARED — codebase invariant is that levels are
    treated immutably (every "update" replaces the slot, never mutates the
    existing PriceLevel). Tests guard that invariant by checking identity.
"""

import pytest

from polymarket_client.models import (
    OrderBook,
    OrderBookSide,
    PriceLevel,
    TokenOrderBook,
    TokenType,
)


def _make_book() -> OrderBook:
    return OrderBook(
        market_id="m1",
        yes=TokenOrderBook(
            token_type=TokenType.YES,
            bids=OrderBookSide(levels=[
                PriceLevel(price=0.52, size=100),
                PriceLevel(price=0.51, size=200),
            ]),
            asks=OrderBookSide(levels=[
                PriceLevel(price=0.54, size=150),
                PriceLevel(price=0.55, size=250),
            ]),
        ),
        no=TokenOrderBook(
            token_type=TokenType.NO,
            bids=OrderBookSide(levels=[
                PriceLevel(price=0.46, size=80),
            ]),
            asks=OrderBookSide(levels=[
                PriceLevel(price=0.48, size=90),
            ]),
        ),
        recv_mono_ns=123_456_789,
    )


def test_clone_returns_new_orderbook():
    ob = _make_book()
    c = ob.clone()
    assert c is not ob
    assert c.market_id == ob.market_id
    assert c.recv_mono_ns == ob.recv_mono_ns


def test_clone_new_token_books_and_sides():
    ob = _make_book()
    c = ob.clone()
    assert c.yes is not ob.yes
    assert c.no is not ob.no
    assert c.yes.bids is not ob.yes.bids
    assert c.yes.asks is not ob.yes.asks
    assert c.no.bids is not ob.no.bids
    assert c.no.asks is not ob.no.asks


def test_clone_levels_list_independence():
    """Producer mutates internal levels with pop/append/sort/slot-assign.
    Clone's lists must be independent so consumers aren't corrupted mid-read."""
    ob = _make_book()
    c = ob.clone()

    # Mutate producer-side lists in every mutation pattern the WS code uses.
    ob.yes.bids.levels.append(PriceLevel(price=0.50, size=999))
    ob.yes.asks.levels.pop(0)
    ob.yes.asks.levels[0] = PriceLevel(price=9.99, size=9.99)
    ob.no.bids.levels.clear()
    ob.no.asks.levels.sort(key=lambda l: l.size)

    # Clone is unaffected.
    assert len(c.yes.bids.levels) == 2
    assert c.yes.bids.levels[0].price == 0.52
    assert len(c.yes.asks.levels) == 2
    assert c.yes.asks.levels[0].price == 0.54
    assert len(c.no.bids.levels) == 1
    assert c.no.bids.levels[0].price == 0.46


def test_clone_pricelevels_shared_by_reference():
    """PriceLevels are de-facto immutable — codebase replaces slots, never mutates.
    Sharing refs is the speed win; this test asserts the invariant explicitly."""
    ob = _make_book()
    c = ob.clone()

    for side_name in ("bids", "asks"):
        for orig_lvl, clone_lvl in zip(
            getattr(ob.yes, side_name).levels,
            getattr(c.yes, side_name).levels,
        ):
            assert orig_lvl is clone_lvl, (
                f"{side_name} PriceLevel should be shared by ref — "
                f"contract is that levels are treated immutably"
            )


def test_clone_replacing_yes_no_does_not_leak():
    """_apply_book_snapshot does combined.yes = TokenOrderBook(...) — slot replace
    on the producer side. The clone's .yes must be a separate object so this
    doesn't reach into consumers."""
    ob = _make_book()
    c = ob.clone()

    ob.yes = TokenOrderBook(token_type=TokenType.YES)  # mimic _apply_book_snapshot
    assert c.yes.token_type == TokenType.YES
    assert len(c.yes.bids.levels) == 2  # untouched


def test_clone_preserves_recv_mono_ns_for_latency_math():
    ob = _make_book()
    c = ob.clone()
    assert c.recv_mono_ns == 123_456_789

    ob2 = OrderBook(market_id="m2")  # recv_mono_ns = None
    c2 = ob2.clone()
    assert c2.recv_mono_ns is None
