"""Tests for VolumeSpikeDetector — comprehensive coverage."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.signals.volume_spike import VolumeSpikeDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


def _build_baseline(session, outcome_id, count, volume, start_offset_hours=6):
    """Create baseline snapshots >1h ago."""
    now = datetime.now(timezone.utc)
    for i in range(count):
        make_price_snapshot(
            session, outcome_id, "0.50",
            volume_24h=Decimal(str(volume)),
            captured_at=now - timedelta(hours=start_offset_hours, minutes=i * 20),
        )


@pytest.mark.asyncio
async def test_volume_above_3x_fires(session):
    """Volume > 3x baseline → signal generated."""
    market = make_market(session, last_volume_24h=Decimal("50000"))
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    _build_baseline(session, outcome.id, 14, 10000)
    now = datetime.now(timezone.utc)
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("40000"),
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    assert candidates[0].signal_type == "volume_spike"


@pytest.mark.asyncio
async def test_volume_below_3x_no_fire(session):
    """Volume < 3x baseline → no signal."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    _build_baseline(session, outcome.id, 14, 10000)
    now = datetime.now(timezone.utc)
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("20000"),  # 2x, below 3x threshold
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_insufficient_baseline_no_fire(session):
    """Fewer than 12 baseline snapshots → no signal."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    _build_baseline(session, outcome.id, 5, 10000)  # Only 5 < 12
    now = datetime.now(timezone.utc)
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
async def test_low_baseline_confidence_penalty(session):
    """Low baseline avg volume → confidence < 1.0."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    # Baseline with low volume (avg=800 < 1000)
    _build_baseline(session, outcome.id, 14, 800)
    now = datetime.now(timezone.utc)
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("5000"),  # ~6.25x
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    # avg_vol < 1000 → confidence *= 0.3
    assert float(candidates[0].confidence) <= Decimal("0.3")


@pytest.mark.asyncio
async def test_medium_baseline_confidence_penalty(session):
    """Medium baseline avg volume (1000-5000) → confidence *= 0.6."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    _build_baseline(session, outcome.id, 14, 3000)
    now = datetime.now(timezone.utc)
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("15000"),  # 5x
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    assert float(candidates[0].confidence) <= 0.6


@pytest.mark.asyncio
async def test_score_capped_at_one(session):
    """Very high spike → score capped at 1.0."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    _build_baseline(session, outcome.id, 14, 1000)
    now = datetime.now(timezone.utc)
    # 100x spike → log10(100)/1.5 = 1.33 → capped at 1.0
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=Decimal("100000"),
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) >= 1
    assert float(candidates[0].signal_score) == 1.0


@pytest.mark.asyncio
async def test_log_scaling_higher_spike_higher_score(session):
    """10x spike scores higher than 3x spike (log scaling)."""
    market = make_market(session)
    await session.flush()
    outcome_low = make_outcome(session, market.id, name="Low")
    outcome_high = make_outcome(session, market.id, name="High")
    await session.flush()

    _build_baseline(session, outcome_low.id, 14, 10000)
    _build_baseline(session, outcome_high.id, 14, 10000)

    now = datetime.now(timezone.utc)
    # 3.5x spike
    make_price_snapshot(
        session, outcome_low.id, "0.50",
        volume_24h=Decimal("35000"),
        captured_at=now - timedelta(minutes=5),
    )
    # 10x spike
    make_price_snapshot(
        session, outcome_high.id, "0.50",
        volume_24h=Decimal("100000"),
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    scores = {c.details["outcome_name"]: float(c.signal_score) for c in candidates}
    assert scores.get("High", 0) > scores.get("Low", 0)


@pytest.mark.asyncio
async def test_null_volume_current_no_fire(session):
    """Current snapshot with NULL volume → no signal."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    _build_baseline(session, outcome.id, 14, 10000)
    now = datetime.now(timezone.utc)
    make_price_snapshot(
        session, outcome.id, "0.50",
        volume_24h=None,
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    detector = VolumeSpikeDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0
