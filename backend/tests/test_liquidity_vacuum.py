"""Tests for LiquidityVacuumDetector."""
from datetime import datetime, timedelta, timezone

import pytest

from app.signals.liquidity_vacuum import LiquidityVacuumDetector
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_bid_side_vacuum_fires(session):
    """Should fire when bid depth drops below 30% of baseline."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Baseline: 8 snapshots with healthy depth
    for i in range(8):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.02",
            depth_bid="1000",
            depth_ask="1000",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    # Current: bid depth collapsed to 200 (20% of 1000 baseline), ask still healthy
    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.02",
        depth_bid="200",
        depth_ask="900",
        captured_at=now - timedelta(minutes=5),
    )

    # Need a price snapshot for the detector to look up latest price
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = LiquidityVacuumDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    assert c.signal_type == "liquidity_vacuum"
    assert c.details["vacuum_side"] == "bid"


@pytest.mark.asyncio
async def test_both_sides_vacuum_fires_with_boost(session):
    """Should fire with higher score when both bid and ask depth collapse."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    for i in range(8):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.02",
            depth_bid="1000",
            depth_ask="1000",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    # Both sides collapsed
    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.02",
        depth_bid="100",
        depth_ask="100",
        captured_at=now - timedelta(minutes=5),
    )
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = LiquidityVacuumDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    assert c.details["vacuum_side"] == "both"
    # Both-side vacuum should have higher score due to 1.3x boost
    assert float(c.signal_score) > 0.5


@pytest.mark.asyncio
async def test_no_vacuum_when_depth_healthy(session):
    """Should NOT fire when depth is above threshold ratio."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    for i in range(8):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.02",
            depth_bid="1000",
            depth_ask="1000",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    # Current: depth at 50% of baseline (above 30% threshold)
    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.02",
        depth_bid="500",
        depth_ask="500",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = LiquidityVacuumDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_vacuum_insufficient_baseline(session):
    """Should NOT fire when baseline has too few snapshots."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Only 3 baseline snapshots
    for i in range(3):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.02",
            depth_bid="1000",
            depth_ask="1000",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.02",
        depth_bid="50",
        depth_ask="50",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = LiquidityVacuumDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0
