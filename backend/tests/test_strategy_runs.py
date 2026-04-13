from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.exc import IntegrityError

from app.models.strategy_run import StrategyRun
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_outcome, make_signal


@pytest.mark.asyncio
async def test_active_run_bootstraps_once_and_ignores_later_env_changes(session, monkeypatch):
    from app.config import settings

    first_start = datetime(2026, 4, 13, tzinfo=timezone.utc)
    second_start = first_start + timedelta(days=7)

    monkeypatch.setattr(settings, "default_strategy_start_at", first_start)
    first_run = await ensure_active_default_strategy_run(session)
    await session.commit()

    monkeypatch.setattr(settings, "default_strategy_start_at", second_start)
    second_run = await ensure_active_default_strategy_run(session)

    assert second_run.id == first_run.id
    assert second_run.started_at == first_start


@pytest.mark.asyncio
async def test_active_run_bootstraps_from_earliest_signal_when_env_unset(session):
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    fired_at = datetime.now(timezone.utc) - timedelta(days=1)
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=fired_at,
        dedupe_bucket=fired_at.replace(minute=0, second=0, microsecond=0),
    )
    await session.commit()

    strategy_run = await ensure_active_default_strategy_run(session)

    assert strategy_run.started_at == fired_at


@pytest.mark.asyncio
async def test_only_one_active_run_per_strategy(session):
    strategy_run = StrategyRun(
        strategy_name="prove_the_edge_default",
        status="active",
        started_at=datetime(2026, 4, 13, tzinfo=timezone.utc),
        contract_snapshot={"name": "prove_the_edge_default"},
    )
    duplicate_run = StrategyRun(
        strategy_name="prove_the_edge_default",
        status="active",
        started_at=datetime(2026, 4, 20, tzinfo=timezone.utc),
        contract_snapshot={"name": "prove_the_edge_default"},
    )
    session.add(strategy_run)
    await session.flush()
    session.add(duplicate_run)

    with pytest.raises(IntegrityError):
        await session.flush()
