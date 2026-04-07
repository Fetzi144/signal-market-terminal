"""Tests for PriceMoveDetector."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.signals.price_move import PriceMoveDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


def _setup_market(session, **kwargs):
    market = make_market(session, **kwargs)
    return market


@pytest.mark.asyncio
async def test_price_increase_above_threshold(session):
    """Price increase >5% in window → signal generated with direction=up."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Old price: 0.50, new price: 0.56 → 12% change (above 5% threshold)
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.56", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    c = candidates[0]
    assert c.signal_type == "price_move"
    assert c.details["direction"] == "up"
    assert float(c.signal_score) > 0
    assert float(c.confidence) > 0


@pytest.mark.asyncio
async def test_price_decrease_above_threshold(session):
    """Price decrease >5% in window → signal generated with direction=down."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Old price: 0.60, new price: 0.50 → ~16.7% decrease
    make_price_snapshot(session, outcome.id, "0.60", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    assert candidates[0].details["direction"] == "down"


@pytest.mark.asyncio
async def test_price_change_below_threshold(session):
    """Price change <5% → no signal."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Old price: 0.50, new price: 0.51 → 2% (below 5% threshold)
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.51", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_low_volume_confidence_penalty(session):
    """Low volume market → confidence < 1.0."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # 10% move with low volume
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("5000"), liquidity=Decimal("3000"),
        captured_at=now - timedelta(minutes=20),
    )
    make_price_snapshot(
        session, outcome.id, "0.55",
        volume_24h=Decimal("5000"), liquidity=Decimal("3000"),
        captured_at=now - timedelta(minutes=1),
    )
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    # volume < 10000 → 0.5, liquidity < 5000 → 0.5, so confidence = 0.25
    assert float(candidates[0].confidence) < 1.0


@pytest.mark.asyncio
async def test_sub_penny_price_skipped(session):
    """Price < 0.01 → skipped (floor behavior)."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, outcome.id, "0.005", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.010", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    # old_price < 0.01 → skipped
    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_insufficient_snapshots(session):
    """Only one snapshot in window → no signal (need 2 distinct timestamps)."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(minutes=5))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_signal_score_capped_at_one(session):
    """Extremely large price move → signal_score capped at 1.0."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # 0.10 → 0.80 = 700% change, raw score = 7.0/0.3 >> 1.0
    make_price_snapshot(session, outcome.id, "0.10", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome.id, "0.80", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    assert float(candidates[0].signal_score) == 1.0


@pytest.mark.asyncio
async def test_score_proportional_to_change(session):
    """Larger price move → higher signal score."""
    market = make_market(session)
    await session.flush()
    outcome1 = make_outcome(session, market.id, name="Small")
    outcome2 = make_outcome(session, market.id, name="Large")
    await session.flush()

    now = datetime.now(timezone.utc)
    # Small move: 6%
    make_price_snapshot(session, outcome1.id, "0.50", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome1.id, "0.53", captured_at=now - timedelta(minutes=1))
    # Large move: 20%
    make_price_snapshot(session, outcome2.id, "0.50", captured_at=now - timedelta(minutes=20))
    make_price_snapshot(session, outcome2.id, "0.60", captured_at=now - timedelta(minutes=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 2
    scores = {c.details["outcome_name"]: float(c.signal_score) for c in candidates}
    assert scores["Large"] > scores["Small"]


@pytest.mark.asyncio
async def test_snapshots_outside_window_ignored(session):
    """Snapshots older than window → not considered."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Both snapshots are outside the 30-min window
    make_price_snapshot(session, outcome.id, "0.50", captured_at=now - timedelta(hours=2))
    make_price_snapshot(session, outcome.id, "0.80", captured_at=now - timedelta(hours=1))
    await session.commit()

    detector = PriceMoveDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0
