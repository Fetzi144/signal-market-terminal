from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.connectors.base import RawOrderbook
from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.snapshot import OrderbookSnapshot
from app.models.strategy_registry import AUTONOMY_TIER_SHADOW_ONLY, VERSION_STATUS_CANDIDATE
from app.models.strategy_run import StrategyRun
from app.reports.kalshi_down_yes_fade import build_kalshi_down_yes_fade_snapshot, kalshi_down_yes_fade_lane_payload
from app.strategies.kalshi_down_yes_fade import (
    STRATEGY_FAMILY,
    STRATEGY_NAME,
    STRATEGY_VERSION_KEY,
    evaluate_kalshi_down_yes_fade_signal,
    run_kalshi_down_yes_fade_paper_lane,
)
from app.strategies.registry import get_current_strategy_version, sync_strategy_registry
from tests.conftest import make_market, make_outcome, make_signal


class _FakeKalshiConnector:
    def __init__(self) -> None:
        self.orderbook_tokens: list[str] = []
        self.midpoint_batches: list[list[str]] = []
        self.closed = False

    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        self.orderbook_tokens.append(token_id)
        return RawOrderbook(
            token_id=token_id,
            bids=[["0.350000", "100000"]],
            asks=[["0.360000", "100000"]],
            spread=Decimal("0.010000"),
        )

    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, Decimal]:
        self.midpoint_batches.append(list(token_ids))
        return {token_id: Decimal("0.355000") for token_id in token_ids}

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_kalshi_down_yes_fade_evaluator_matches_only_frozen_v2_rule(session):
    now = datetime.now(timezone.utc)
    market = make_market(session, platform="kalshi", end_date=now + timedelta(days=1))
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now - timedelta(minutes=5),
        details={"direction": "down", "market_question": "Test market?"},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("-0.050000"),
        estimated_probability=Decimal("0.220000"),
    )

    evaluation = evaluate_kalshi_down_yes_fade_signal(signal, market_platform="kalshi")

    assert evaluation.in_scope is True
    assert evaluation.eligible is True
    assert evaluation.diagnostics["intended_direction"] == "buy_no"
    assert evaluation.diagnostics["no_entry_price"] == "0.650000"
    assert evaluation.diagnostics["edge_per_share"] == "0.130000"

    signal.price_at_fire = Decimal("0.150000")
    evaluation = evaluate_kalshi_down_yes_fade_signal(signal, market_platform="kalshi")

    assert evaluation.in_scope is False
    assert evaluation.eligible is False
    assert evaluation.reason_code == "not_kalshi_down_yes_fade_price_bucket"


@pytest.mark.asyncio
async def test_kalshi_down_yes_fade_registry_seeds_shadow_candidate(session):
    await sync_strategy_registry(session)

    version = await get_current_strategy_version(session, STRATEGY_FAMILY)

    assert version is not None
    assert version.version_key == STRATEGY_VERSION_KEY
    assert version.strategy_name == STRATEGY_NAME
    assert version.version_status == VERSION_STATUS_CANDIDATE
    assert version.autonomy_tier == AUTONOMY_TIER_SHADOW_ONLY
    assert version.config_json["live_orders_enabled"] is False
    assert version.config_json["rule"]["trade_direction"] == "buy_no"
    assert version.config_json["rule"]["targeted_orderbook_capture"] is True


@pytest.mark.asyncio
async def test_kalshi_down_yes_fade_paper_lane_captures_fresh_book_and_opens_buy_no(
    session,
    monkeypatch,
):
    import app.strategies.kalshi_down_yes_fade as lane_module

    fake_connector = _FakeKalshiConnector()
    monkeypatch.setattr(lane_module, "get_connector", lambda _platform: fake_connector)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        question="Will the mid-priced test contract fade?",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("5000.00"),
        active=True,
    )
    outcome = make_outcome(session, market.id, name="Yes", token_id="KTEST-FADE:yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now - timedelta(minutes=5),
        details={"direction": "down", "market_question": market.question},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("-0.130000"),
        estimated_probability=Decimal("0.220000"),
    )
    await session.commit()

    result = await run_kalshi_down_yes_fade_paper_lane(
        session,
        [signal],
        pending_retry_limit=0,
        backlog_limit=0,
        pending_expiry_limit=100,
    )

    assert result["candidate_count"] == 1
    assert result["targeted_orderbook_captures"] == 1
    assert result["opened_count"] == 1
    assert fake_connector.orderbook_tokens == ["KTEST-FADE:yes"]
    assert fake_connector.closed is True

    orderbook = (await session.execute(select(OrderbookSnapshot))).scalars().one()
    trade = (await session.execute(select(PaperTrade))).scalars().one()
    strategy_run = (await session.execute(select(StrategyRun))).scalars().one()
    decision = (await session.execute(select(ExecutionDecision))).scalars().one()
    version = await get_current_strategy_version(session, STRATEGY_FAMILY)

    assert orderbook.outcome_id == outcome.id
    assert strategy_run.strategy_name == STRATEGY_NAME
    assert strategy_run.strategy_family == STRATEGY_FAMILY
    assert strategy_run.strategy_version_id == version.id
    assert trade.strategy_run_id == strategy_run.id
    assert trade.strategy_version_id == version.id
    assert trade.direction == "buy_no"
    assert trade.details["strategy_run_id"] == str(strategy_run.id)
    assert decision.decision_status == "opened"
    assert decision.direction == "buy_no"
    decision_at = decision.decision_at if decision.decision_at.tzinfo else decision.decision_at.replace(tzinfo=timezone.utc)
    signal_fired_at = signal.fired_at if signal.fired_at.tzinfo else signal.fired_at.replace(tzinfo=timezone.utc)
    assert decision_at > signal_fired_at
    assert decision.details["market_price"] == "0.355000"
    assert decision.details["shadow_execution"]["missing_orderbook_context"] is False


@pytest.mark.asyncio
async def test_scheduler_runs_kalshi_down_lane_without_default_strategy_run(session, monkeypatch):
    import app.strategies.kalshi_down_yes_fade as lane_module
    from app.jobs.scheduler import _run_paper_trading

    fake_connector = _FakeKalshiConnector()
    monkeypatch.setattr(lane_module, "get_connector", lambda _platform: fake_connector)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        question="Will scheduler v2 test fade?",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("5000.00"),
        active=True,
    )
    outcome = make_outcome(session, market.id, name="Yes", token_id="KTEST-SCHED:yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now - timedelta(minutes=5),
        details={"direction": "down", "market_question": market.question},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("-0.130000"),
        estimated_probability=Decimal("0.220000"),
    )
    await session.commit()

    await _run_paper_trading(session, [signal])

    trade = (await session.execute(select(PaperTrade))).scalars().one()
    strategy_run = (
        await session.execute(select(StrategyRun).where(StrategyRun.strategy_name == STRATEGY_NAME))
    ).scalars().one()
    assert strategy_run.strategy_name == STRATEGY_NAME
    assert trade.strategy_run_id == strategy_run.id
    assert trade.direction == "buy_no"


@pytest.mark.asyncio
async def test_kalshi_down_yes_fade_snapshot_normalizes_to_research_lane(session):
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
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("-0.130000"),
        estimated_probability=Decimal("0.220000"),
        resolved_correctly=True,
        profit_loss=Decimal("0.650000"),
        clv=Decimal("0.020000"),
    )
    await session.commit()

    snapshot = await build_kalshi_down_yes_fade_snapshot(
        session,
        window_days=30,
        max_signals=5000,
        as_of=now,
    )
    payload = kalshi_down_yes_fade_lane_payload(snapshot)

    assert snapshot["historical"]["matching_signals"] == 1
    assert snapshot["verdict"] == "research_ready"
    assert "no_active_candidate_run" in snapshot["blockers"]
    assert payload["family"] == STRATEGY_FAMILY
    assert payload["strategy_version"] == STRATEGY_VERSION_KEY
    assert payload["source_kind"] == "kalshi_down_yes_fade_snapshot"
    assert payload["details_json"]["next_best_actions"]
