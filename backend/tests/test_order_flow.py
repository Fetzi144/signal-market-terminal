"""Tests for OrderFlowImbalanceDetector."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_strong_buy_imbalance_generates_up_signal(session):
    """Strong increase in bid depth vs ask depth -> signal with direction 'up'."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Three orderbook snapshots: bid depth increasing, ask depth stable
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=5000, captured_at=now - timedelta(minutes=20))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=7000, depth_ask=5000, captured_at=now - timedelta(minutes=10))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=10000, depth_ask=5000, captured_at=now - timedelta(minutes=1))

    # Flat price over last 30 minutes
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    detector = OrderFlowImbalanceDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    c = candidates[0]
    assert c.signal_type == "order_flow_imbalance"
    assert c.details["direction"] == "up"
    assert float(c.details["ofi_value"]) > 0
    assert c.signal_score > Decimal("0")


@pytest.mark.asyncio
async def test_strong_sell_imbalance_generates_down_signal(session):
    """Strong increase in ask depth vs bid depth -> signal with direction 'down'."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Ask depth increasing, bid depth stable
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=5000, captured_at=now - timedelta(minutes=20))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=7000, captured_at=now - timedelta(minutes=10))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=10000, captured_at=now - timedelta(minutes=1))

    # Flat price
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    detector = OrderFlowImbalanceDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    c = candidates[0]
    assert c.signal_type == "order_flow_imbalance"
    assert c.details["direction"] == "down"
    assert float(c.details["ofi_value"]) < 0


@pytest.mark.asyncio
async def test_balanced_book_no_signal(session):
    """When bid and ask depth change equally -> OFI near zero -> no signal."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Both sides increase equally
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=5000, captured_at=now - timedelta(minutes=20))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=7000, depth_ask=7000, captured_at=now - timedelta(minutes=10))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=8000, depth_ask=8000, captured_at=now - timedelta(minutes=1))

    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    detector = OrderFlowImbalanceDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_price_already_moved_no_signal(session):
    """If price moved significantly in the flat window, OFI should not fire."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Strong bid imbalance
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=5000, captured_at=now - timedelta(minutes=20))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=8000, depth_ask=5000, captured_at=now - timedelta(minutes=10))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=10000, depth_ask=5000, captured_at=now - timedelta(minutes=1))

    # Price moved 10% -> not flat
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=30))
    make_price_snapshot(session, o.id, price=0.55, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    detector = OrderFlowImbalanceDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_thin_orderbook_confidence_penalty(session):
    """Thin order books should result in reduced confidence."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Very thin book with strong imbalance
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=100, depth_ask=100, captured_at=now - timedelta(minutes=20))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=300, depth_ask=100, captured_at=now - timedelta(minutes=10))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=500, depth_ask=100, captured_at=now - timedelta(minutes=1))

    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    detector = OrderFlowImbalanceDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    c = candidates[0]
    # Total depth = 500 + 100 = 600 < 1000 -> confidence *= 0.4
    assert c.confidence <= Decimal("0.4")


@pytest.mark.asyncio
async def test_insufficient_snapshots_no_signal(session):
    """Fewer than ofi_min_snapshots (2) orderbook snapshots -> no signal."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Only 1 snapshot (default min is 2)
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=10000, depth_ask=5000, captured_at=now - timedelta(minutes=1))

    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    detector = OrderFlowImbalanceDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_ofi_disabled_no_signal(session):
    """When ofi_enabled is False, detector should return empty list."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=5000, captured_at=now - timedelta(minutes=20))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=8000, depth_ask=5000, captured_at=now - timedelta(minutes=10))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=10000, depth_ask=5000, captured_at=now - timedelta(minutes=1))

    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    with patch("app.signals.order_flow.settings") as mock_settings:
        mock_settings.ofi_enabled = False
        detector = OrderFlowImbalanceDetector()
        candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_ofi_score_scaling(session):
    """OFI of 0.6 should produce max score of 1.0."""
    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Extreme imbalance: bid goes from 1000 to 10000, ask stays at 1000
    # bid_change = 9000, ask_change = 0
    # ofi = (9000 - 0) / (9000 + 0) = 1.0
    # score = min(1.0, 1.0 / 0.6) = 1.0
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=1000, depth_ask=5000, captured_at=now - timedelta(minutes=20))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=5000, depth_ask=5000, captured_at=now - timedelta(minutes=10))
    make_orderbook_snapshot(session, o.id, spread=0.02, depth_bid=10000, depth_ask=5000, captured_at=now - timedelta(minutes=1))

    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=25))
    make_price_snapshot(session, o.id, price=0.50, captured_at=now - timedelta(minutes=1))
    await session.flush()

    from app.signals.order_flow import OrderFlowImbalanceDetector
    detector = OrderFlowImbalanceDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    assert candidates[0].signal_score == Decimal("1.000")
