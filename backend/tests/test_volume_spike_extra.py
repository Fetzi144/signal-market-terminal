"""Additional edge-case tests for VolumeSpikeDetector."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.signals.volume_spike import VolumeSpikeDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_volume_spike_fires(session):
    """Should fire when current volume is >=3x baseline average."""
    market = make_market(session, last_volume_24h=Decimal("50000"))
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Baseline: 14 snapshots with volume=10000, >1h ago
    for i in range(14):
        make_price_snapshot(
            session, outcome.id, "0.50",
            volume_24h=Decimal("10000"),
            captured_at=now - timedelta(hours=6, minutes=i * 20),
        )

    # Current: volume=40000 (4x baseline), within last hour
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("40000"),
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    c = candidates[0]
    assert c.signal_type == "volume_spike"
    assert float(c.signal_score) > 0


@pytest.mark.asyncio
async def test_volume_spike_insufficient_baseline(session):
    """Should NOT fire when baseline has fewer than MIN_BASELINE_SNAPSHOTS (12)."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    # Only 5 baseline snapshots (below 12 minimum)
    for i in range(5):
        make_price_snapshot(
            session, outcome.id, "0.50",
            volume_24h=Decimal("10000"),
            captured_at=now - timedelta(hours=6, minutes=i * 20),
        )

    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("50000"),
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_volume_spike_no_fire_below_multiplier(session):
    """Should NOT fire when volume is below 3x multiplier threshold."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    for i in range(14):
        make_price_snapshot(
            session, outcome.id, "0.50",
            volume_24h=Decimal("10000"),
            captured_at=now - timedelta(hours=6, minutes=i * 20),
        )

    # Current: 2x baseline (below 3x threshold)
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("20000"),
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_volume_spike_null_volume_skipped(session):
    """Should skip outcomes where volume_24h is NULL."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)

    for i in range(14):
        make_price_snapshot(
            session, outcome.id, "0.50",
            volume_24h=None,
            captured_at=now - timedelta(hours=6, minutes=i * 20),
        )

    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=None,
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0
