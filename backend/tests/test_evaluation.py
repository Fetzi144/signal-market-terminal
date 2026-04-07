"""Tests for signal evaluation."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.evaluation.evaluator import evaluate_signals
from app.models.signal import SignalEvaluation
from sqlalchemy import select
from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal


@pytest.mark.asyncio
async def test_evaluator_creates_evaluation_at_horizon(session):
    """Evaluator should create an evaluation when a snapshot exists at the target horizon."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Signal fired 20 minutes ago, price was 0.50
    signal = make_signal(
        session, market.id, outcome.id,
        fired_at=now - timedelta(minutes=20),
        price_at_fire=Decimal("0.500000"),
    )
    await session.flush()

    # Snapshot at the 15m horizon (5 minutes ago) showing price moved to 0.55
    make_price_snapshot(
        session, outcome.id, "0.55",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    created = await evaluate_signals(session)
    assert created >= 1

    # Check the evaluation was stored
    result = await session.execute(
        select(SignalEvaluation).where(
            SignalEvaluation.signal_id == signal.id,
            SignalEvaluation.horizon == "15m",
        )
    )
    ev = result.scalar_one_or_none()
    assert ev is not None
    assert ev.price_at_eval == Decimal("0.550000")
    assert float(ev.price_change_pct) == pytest.approx(10.0, abs=0.1)


@pytest.mark.asyncio
async def test_evaluator_marks_resolved_when_all_horizons_done(session):
    """Signal should be marked resolved after all 4 horizons are evaluated."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Signal fired 25 hours ago — all horizons should be past due
    signal = make_signal(
        session, market.id, outcome.id,
        fired_at=now - timedelta(hours=25),
        price_at_fire=Decimal("0.500000"),
    )
    await session.flush()

    # Create snapshots at all horizon points
    for offset in [timedelta(minutes=15), timedelta(hours=1), timedelta(hours=4), timedelta(hours=24)]:
        target = signal.fired_at + offset
        make_price_snapshot(session, outcome.id, "0.55", captured_at=target)

    await session.commit()

    await evaluate_signals(session)
    await session.refresh(signal)

    assert signal.resolved is True


@pytest.mark.asyncio
async def test_evaluator_skips_when_no_snapshot(session):
    """Evaluator should not create an evaluation when no snapshot exists near the horizon."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    # Signal fired 20 minutes ago, but NO snapshots at the 15m mark
    make_signal(
        session, market.id, outcome.id,
        fired_at=now - timedelta(minutes=20),
        price_at_fire=Decimal("0.500000"),
    )
    await session.commit()

    created = await evaluate_signals(session)
    assert created == 0
