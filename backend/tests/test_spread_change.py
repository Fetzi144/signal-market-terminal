"""Tests for SpreadChangeDetector."""
from datetime import datetime, timedelta, timezone

import pytest

from app.signals.spread_change import SpreadChangeDetector
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome


@pytest.mark.asyncio
async def test_spread_widening_fires(session):
    """Should fire when current spread is >=2x baseline average."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Baseline: 8 snapshots with spread=0.02, all >1h ago (within 12h window)
    for i in range(8):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.02",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    # Current: spread=0.06 (3x baseline), within last hour
    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.06",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = SpreadChangeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    assert c.signal_type == "spread_change"
    assert c.details["direction"] == "widening"
    assert float(c.signal_score) > 0


@pytest.mark.asyncio
async def test_spread_narrowing_fires(session):
    """Should fire when baseline spread is >=2x current (narrowing)."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Baseline: spread=0.08
    for i in range(8):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.08",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    # Current: spread=0.03 (baseline/current ~2.67x)
    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.03",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = SpreadChangeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    assert candidates[0].details["direction"] == "narrowing"


@pytest.mark.asyncio
async def test_spread_no_fire_below_threshold(session):
    """Should NOT fire when spread ratio is below threshold."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Baseline: spread=0.02
    for i in range(8):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.02",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    # Current: spread=0.03 (1.5x, below 2.0 threshold)
    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.03",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = SpreadChangeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_spread_insufficient_baseline(session):
    """Should NOT fire when baseline has fewer than MIN_BASELINE_SNAPSHOTS."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Only 3 baseline snapshots (below MIN_BASELINE_SNAPSHOTS=6)
    for i in range(3):
        make_orderbook_snapshot(
            session, outcome.id,
            spread="0.02",
            captured_at=now - timedelta(hours=3, minutes=i * 10),
        )

    # Current: big widening
    make_orderbook_snapshot(
        session, outcome.id,
        spread="0.10",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = SpreadChangeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0
