from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.strategy_registry import AUTONOMY_TIER_SHADOW_ONLY, VERSION_STATUS_CANDIDATE
from app.models.strategy_run import StrategyRun
from app.reports.kalshi_cheap_yes_follow import (
    build_kalshi_cheap_yes_follow_snapshot,
    kalshi_cheap_yes_follow_lane_payload,
)
from app.strategies.kalshi_cheap_yes_follow import (
    STRATEGY_FAMILY,
    STRATEGY_NAME,
    STRATEGY_VERSION_KEY,
    evaluate_kalshi_cheap_yes_follow_signal,
    run_kalshi_cheap_yes_follow_paper_lane,
)
from app.strategies.registry import get_current_strategy_version, sync_strategy_registry
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_signal


@pytest.mark.asyncio
async def test_kalshi_cheap_yes_follow_evaluator_matches_only_fixed_rule(session):
    now = datetime.now(timezone.utc)
    market = make_market(session, platform="kalshi", end_date=now + timedelta(days=1))
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now,
        details={"direction": "down", "market_question": "Test market?"},
        price_at_fire=Decimal("0.040000"),
        expected_value=Decimal("0.009000"),
        estimated_probability=Decimal("0.0490"),
    )

    evaluation = evaluate_kalshi_cheap_yes_follow_signal(signal, market_platform="kalshi")

    assert evaluation.in_scope is True
    assert evaluation.eligible is True
    assert evaluation.diagnostics["intended_direction"] == "buy_yes"
    assert evaluation.diagnostics["edge_per_share"] == "0.009000"

    signal.price_at_fire = Decimal("0.060000")
    evaluation = evaluate_kalshi_cheap_yes_follow_signal(signal, market_platform="kalshi")

    assert evaluation.in_scope is False
    assert evaluation.eligible is False
    assert evaluation.reason_code == "not_kalshi_cheap_yes_follow_price_bucket"


@pytest.mark.asyncio
async def test_kalshi_cheap_yes_follow_registry_seeds_shadow_candidate(session):
    await sync_strategy_registry(session)

    version = await get_current_strategy_version(session, STRATEGY_FAMILY)

    assert version is not None
    assert version.version_key == STRATEGY_VERSION_KEY
    assert version.strategy_name == STRATEGY_NAME
    assert version.version_status == VERSION_STATUS_CANDIDATE
    assert version.autonomy_tier == AUTONOMY_TIER_SHADOW_ONLY
    assert version.config_json["live_orders_enabled"] is False
    assert version.config_json["rule"]["trade_direction"] == "buy_yes"


@pytest.mark.asyncio
async def test_kalshi_cheap_yes_follow_paper_lane_opens_buy_yes_trade_under_candidate_run(session):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        question="Will the cheap test contract rebound?",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("5000.00"),
        active=True,
    )
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now,
        details={"direction": "down", "market_question": market.question},
        price_at_fire=Decimal("0.040000"),
        expected_value=Decimal("0.009000"),
        estimated_probability=Decimal("0.0490"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread=Decimal("0.010000"),
        bids=[["0.040000", "100000"]],
        asks=[["0.045000", "100000"]],
        captured_at=now,
    )
    await session.commit()

    result = await run_kalshi_cheap_yes_follow_paper_lane(
        session,
        [signal],
        pending_retry_limit=0,
        backlog_limit=0,
        pending_expiry_limit=100,
    )

    assert result["candidate_count"] == 1
    assert result["opened_count"] == 1
    trade = (await session.execute(select(PaperTrade))).scalars().one()
    strategy_run = (
        await session.execute(select(StrategyRun).where(StrategyRun.strategy_name == STRATEGY_NAME))
    ).scalars().one()
    decision = (await session.execute(select(ExecutionDecision))).scalars().one()
    version = await get_current_strategy_version(session, STRATEGY_FAMILY)

    assert strategy_run.strategy_name == STRATEGY_NAME
    assert strategy_run.strategy_family == STRATEGY_FAMILY
    assert strategy_run.strategy_version_id == version.id
    assert trade.strategy_run_id == strategy_run.id
    assert trade.strategy_version_id == version.id
    assert trade.direction == "buy_yes"
    assert trade.details["strategy_run_id"] == str(strategy_run.id)
    assert decision.decision_status == "opened"
    assert decision.direction == "buy_yes"


@pytest.mark.asyncio
async def test_scheduler_runs_kalshi_lane_without_default_strategy_run(session):
    from app.jobs.scheduler import _run_paper_trading

    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        question="Will scheduler cheap test rebound?",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("5000.00"),
        active=True,
    )
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now,
        details={"direction": "down", "market_question": market.question},
        price_at_fire=Decimal("0.040000"),
        expected_value=Decimal("0.009000"),
        estimated_probability=Decimal("0.0490"),
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
        spread=Decimal("0.010000"),
        bids=[["0.040000", "100000"]],
        asks=[["0.045000", "100000"]],
        captured_at=now,
    )
    await session.commit()

    await _run_paper_trading(session, [signal])

    trade = (await session.execute(select(PaperTrade))).scalars().one()
    strategy_run = (
        await session.execute(select(StrategyRun).where(StrategyRun.strategy_name == STRATEGY_NAME))
    ).scalars().one()
    assert strategy_run.strategy_name == STRATEGY_NAME
    assert trade.strategy_run_id == strategy_run.id
    assert trade.direction == "buy_yes"


@pytest.mark.asyncio
async def test_kalshi_cheap_yes_follow_snapshot_normalizes_to_research_lane(session):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(session, platform="kalshi", end_date=now + timedelta(days=1))
    outcome = make_outcome(session, market.id, name="Yes")
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now - timedelta(hours=1),
        details={"direction": "down", "market_question": market.question},
        price_at_fire=Decimal("0.040000"),
        expected_value=Decimal("0.009000"),
        estimated_probability=Decimal("0.0490"),
        resolved_correctly=True,
        profit_loss=Decimal("0.850000"),
        clv=Decimal("0.020000"),
    )
    await session.commit()

    snapshot = await build_kalshi_cheap_yes_follow_snapshot(
        session,
        window_days=30,
        max_signals=5000,
        as_of=now,
    )
    payload = kalshi_cheap_yes_follow_lane_payload(snapshot)

    assert snapshot["historical"]["matching_signals"] == 1
    assert snapshot["verdict"] == "research_ready"
    assert "no_active_candidate_run" in snapshot["blockers"]
    assert payload["family"] == STRATEGY_FAMILY
    assert payload["strategy_version"] == STRATEGY_VERSION_KEY
    assert payload["source_kind"] == "kalshi_cheap_yes_follow_snapshot"
    assert payload["details_json"]["next_best_actions"]
