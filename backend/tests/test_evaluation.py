"""Tests for signal evaluation."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

import app.evaluation.evaluator as evaluator_module
from app.evaluation.evaluator import _bounded_price_change_pct, evaluate_signals
from app.models.signal import SignalEvaluation
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


@pytest.mark.asyncio
async def test_evaluator_clamps_extreme_price_change_pct(session):
    """Tiny entry prices should not overflow the stored evaluation percentage."""
    market = make_market(session)
    await session.flush()
    outcome = make_outcome(session, market.id)
    await session.flush()

    now = datetime.now(timezone.utc)
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        fired_at=now - timedelta(minutes=20),
        price_at_fire=Decimal("0.006000"),
    )
    await session.flush()

    make_price_snapshot(
        session,
        outcome.id,
        "1.00",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    created = await evaluate_signals(session)
    assert created >= 1

    result = await session.execute(
        select(SignalEvaluation).where(
            SignalEvaluation.signal_id == signal.id,
            SignalEvaluation.horizon == "15m",
        )
    )
    evaluation = result.scalar_one()
    assert evaluation.price_change_pct == Decimal("9999.9999")


@pytest.mark.parametrize(
    ("price_change", "price_at_fire", "expected"),
    [
        (Decimal("0.994000"), Decimal("0.006000"), Decimal("9999.9999")),
        (Decimal("-1.006000"), Decimal("0.006000"), Decimal("-9999.9999")),
    ],
)
def test_bounded_price_change_pct_clamps_extreme_values(price_change, price_at_fire, expected):
    assert _bounded_price_change_pct(
        price_change=price_change,
        price_at_fire=price_at_fire,
        signal_id=uuid.uuid4(),
        horizon="15m",
    ) == expected


@pytest.mark.asyncio
async def test_evaluator_isolates_failed_horizons_and_continues(session, monkeypatch):
    market = make_market(session)
    await session.flush()
    bad_outcome = make_outcome(session, market.id, name="Bad")
    good_outcome = make_outcome(session, market.id, name="Good")
    await session.flush()

    now = datetime.now(timezone.utc)
    bad_signal = make_signal(
        session,
        market.id,
        bad_outcome.id,
        fired_at=now - timedelta(minutes=20),
        price_at_fire=Decimal("0.500000"),
    )
    good_signal = make_signal(
        session,
        market.id,
        good_outcome.id,
        fired_at=now - timedelta(minutes=20),
        price_at_fire=Decimal("0.500000"),
    )
    await session.flush()

    make_price_snapshot(
        session,
        bad_outcome.id,
        "0.55",
        captured_at=now - timedelta(minutes=5),
    )
    make_price_snapshot(
        session,
        good_outcome.id,
        "0.55",
        captured_at=now - timedelta(minutes=5),
    )
    await session.commit()

    original_persist = evaluator_module._persist_signal_evaluation
    failure_state = {"raised": False}

    async def flaky_persist(session_obj, evaluation):
        if not failure_state["raised"] and evaluation.signal_id == bad_signal.id:
            failure_state["raised"] = True
            raise RuntimeError("simulated evaluation persistence failure")
        await original_persist(session_obj, evaluation)

    monkeypatch.setattr(evaluator_module, "_persist_signal_evaluation", flaky_persist)

    created = await evaluate_signals(session)

    assert created == 1
    assert session.sync_session.info["signal_evaluation_stats"]["failed"] == 1

    result = await session.execute(
        select(SignalEvaluation).order_by(SignalEvaluation.signal_id.asc())
    )
    evaluations = result.scalars().all()

    assert len(evaluations) == 1
    assert evaluations[0].signal_id == good_signal.id
