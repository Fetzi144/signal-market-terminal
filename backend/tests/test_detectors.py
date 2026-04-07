"""Tests for signal detectors."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import pytest_asyncio

from app.signals.price_move import PriceMoveDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_price_move_fires_on_large_change(session):
    """PriceMoveDetector should fire when price changes exceed threshold."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Old price: 0.50, new price: 0.60 = 20% change (exceeds default 5%)
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.60", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    assert c.signal_type == "price_move"
    assert c.details["direction"] == "up"
    assert float(c.signal_score) > 0


@pytest.mark.asyncio
async def test_price_move_no_fire_on_small_change(session):
    """PriceMoveDetector should NOT fire when price change is below threshold."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Old price: 0.50, new price: 0.51 = 2% change (below 5% threshold)
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.51", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_price_move_confidence_penalty_thin_market(session):
    """PriceMoveDetector should penalize confidence for thin markets."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Big price move but very low volume
    make_price_snapshot(
        session, outcome.id, "0.50",
        captured_at=now - timedelta(minutes=20),
        volume_24h=Decimal("100"),  # Very low
        liquidity=Decimal("100"),   # Very low
    )
    make_price_snapshot(
        session, outcome.id, "0.70",
        captured_at=now - timedelta(minutes=1),
        volume_24h=Decimal("100"),
        liquidity=Decimal("100"),
    )
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    # Confidence should be penalized (0.5 * 0.5 = 0.25)
    assert float(c.confidence) <= 0.25


@pytest.mark.asyncio
async def test_price_move_ignores_near_zero_prices(session):
    """PriceMoveDetector should skip outcomes with near-zero old prices."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, outcome.id, "0.005", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.05", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0
