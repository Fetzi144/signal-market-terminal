from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.exc import IntegrityError

from app.models.strategy_run import StrategyRun
from app.strategy_runs.service import (
    ActiveStrategyRunExistsError,
    close_active_default_strategy_run,
    ensure_active_default_strategy_run,
    open_default_strategy_run,
)
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
    assert second_run.strategy_family == "default_strategy"
    assert second_run.strategy_version_id is not None
    assert second_run.contract_snapshot["strategy_version_key"] == "default_strategy_benchmark_v1"
    assert second_run.contract_snapshot["bootstrap_source"] == "DEFAULT_STRATEGY_START_AT"
    assert second_run.contract_snapshot["bootstrap_anchor_at"] == first_start.isoformat()


@pytest.mark.asyncio
async def test_active_run_prefers_configured_launch_boundary_over_bootstrap_candidate(session, monkeypatch):
    from app.config import settings

    launch_at = datetime(2026, 4, 13, tzinfo=timezone.utc)
    bootstrap_started_at = launch_at + timedelta(days=2)

    monkeypatch.setattr(settings, "default_strategy_start_at", launch_at)

    strategy_run = await ensure_active_default_strategy_run(
        session,
        bootstrap_started_at=bootstrap_started_at,
    )

    assert strategy_run.started_at == launch_at
    assert strategy_run.contract_snapshot["baseline_start_at"] == launch_at.isoformat()
    assert strategy_run.contract_snapshot["bootstrap_source"] == "DEFAULT_STRATEGY_START_AT"
    assert strategy_run.contract_snapshot["bootstrap_anchor_at"] == launch_at.isoformat()


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
    assert strategy_run.contract_snapshot["bootstrap_source"] == "EARLIEST_SIGNAL_FIRED_AT"
    assert strategy_run.contract_snapshot["bootstrap_anchor_at"] == fired_at.isoformat()


@pytest.mark.asyncio
async def test_strategy_run_open_close_lifecycle_requires_explicit_rollover(session):
    first_launch_at = datetime(2026, 4, 13, tzinfo=timezone.utc)
    second_launch_at = first_launch_at + timedelta(days=14)
    first_end_at = first_launch_at + timedelta(days=7)

    first_run = await open_default_strategy_run(session, launch_boundary_at=first_launch_at)

    with pytest.raises(ActiveStrategyRunExistsError):
        await open_default_strategy_run(session, launch_boundary_at=second_launch_at)

    closed_run = await close_active_default_strategy_run(session, ended_at=first_end_at)
    second_run = await open_default_strategy_run(session, launch_boundary_at=second_launch_at)

    assert closed_run is not None
    assert closed_run.id == first_run.id
    assert closed_run.status == "closed"
    assert closed_run.ended_at == first_end_at
    assert second_run.id != first_run.id
    assert second_run.status == "active"
    assert second_run.started_at == second_launch_at
    assert second_run.strategy_family == "default_strategy"
    assert second_run.strategy_version_id is not None
    assert second_run.contract_snapshot["bootstrap_source"] == "EXPLICIT_LAUNCH_BOUNDARY"
    assert second_run.contract_snapshot["bootstrap_anchor_at"] == second_launch_at.isoformat()


@pytest.mark.asyncio
async def test_strategy_run_persists_evidence_boundary_metadata_in_contract_snapshot(session):
    launch_at = datetime(2026, 4, 15, tzinfo=timezone.utc)
    strategy_run = await open_default_strategy_run(
        session,
        launch_boundary_at=launch_at,
        contract_metadata={
            "contract_version": "default_strategy_v0.4.1",
            "evidence_boundary": {
                "boundary_id": "v0.4.1",
                "release_tag": "v0.4.1",
                "commit_sha": "87a4315b81b81365d9ee974aff5b130813757897",
                "migration_revision": "038",
            },
            "evidence_gate": {
                "min_resolved_trades": 20,
                "execution_adjusted_pnl_rule": "positive",
            },
        },
    )

    assert strategy_run.contract_snapshot["contract_version"] == "default_strategy_v0.4.1"
    assert strategy_run.contract_snapshot["evidence_boundary"]["release_tag"] == "v0.4.1"
    assert strategy_run.contract_snapshot["evidence_boundary"]["migration_revision"] == "038"
    assert strategy_run.contract_snapshot["evidence_gate"]["min_resolved_trades"] == 20


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
