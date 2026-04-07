"""Tests for DeadlineNearDetector."""
from datetime import datetime, timedelta, timezone

import pytest

from app.signals.deadline_near import DeadlineNearDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_deadline_near_fires_on_price_move(session):
    """Should fire when a market near its deadline shows significant price movement."""
    now = datetime.now(timezone.utc)
    # Market ending in 12 hours
    market = make_market(session, end_date=now + timedelta(hours=12))
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    # Price moved from 0.50 to 0.55 in last 2 hours (10% > 3% threshold)
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(hours=1, minutes=30))
    make_price_snapshot(session, outcome.id, "0.55", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = DeadlineNearDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    assert c.signal_type == "deadline_near"
    assert c.details["direction"] == "up"
    assert float(c.details["hours_until_deadline"]) < 13


@pytest.mark.asyncio
async def test_deadline_near_no_fire_far_deadline(session):
    """Should NOT fire when market deadline is far away (>48h)."""
    now = datetime.now(timezone.utc)
    # Market ending in 7 days — outside the 48h window
    market = make_market(session, end_date=now + timedelta(days=7))
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    # Big price move
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(hours=1, minutes=30))
    make_price_snapshot(session, outcome.id, "0.70", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = DeadlineNearDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_deadline_near_no_fire_small_move(session):
    """Should NOT fire when price change is below threshold."""
    now = datetime.now(timezone.utc)
    market = make_market(session, end_date=now + timedelta(hours=12))
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    # Price barely moved: 0.50 to 0.505 (1% < 3% threshold)
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(hours=1, minutes=30))
    make_price_snapshot(session, outcome.id, "0.505", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = DeadlineNearDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_deadline_near_no_end_date(session):
    """Should skip markets with no end_date."""
    now = datetime.now(timezone.utc)
    market = make_market(session, end_date=None)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(hours=1, minutes=30))
    make_price_snapshot(session, outcome.id, "0.70", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = DeadlineNearDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_deadline_near_urgency_increases_closer_to_deadline(session):
    """Markets closer to deadline should have higher urgency in details."""
    now = datetime.now(timezone.utc)
    # Very close: 2 hours remaining
    market = make_market(session, end_date=now + timedelta(hours=2))
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(hours=1, minutes=30))
    make_price_snapshot(session, outcome.id, "0.60", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = DeadlineNearDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    # Urgency should be high (close to 1.0) since only 2h remain
    urgency = float(c.details["urgency"])
    assert urgency >= 0.9
