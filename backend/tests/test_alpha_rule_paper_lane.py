from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.alpha_rule_specs import (
    ALPHA_KALSHI_4237F81367_FAMILY,
    ALPHA_KALSHI_4237F81367_V1,
    ALPHA_KALSHI_4237F81367_VERSION,
)
from app.connectors.base import RawOrderbook
from app.models.execution_decision import ExecutionDecision
from app.models.paper_trade import PaperTrade
from app.models.snapshot import OrderbookSnapshot
from app.models.strategy_registry import AUTONOMY_TIER_SHADOW_ONLY, VERSION_STATUS_CANDIDATE
from app.models.strategy_run import StrategyRun
from app.reports.alpha_rule_paper_lane import (
    alpha_rule_paper_lane_payload,
    build_alpha_rule_paper_lane_snapshot,
)
from app.strategies.alpha_rule_paper_lane import (
    evaluate_alpha_rule_signal,
    run_alpha_rule_paper_lane,
)
from app.strategies.registry import get_current_strategy_version, sync_strategy_registry
from tests.conftest import make_market, make_outcome, make_signal


class _FakeKalshiConnector:
    def __init__(self, *, midpoint: Decimal = Decimal("0.360000")) -> None:
        self.midpoint = midpoint
        self.orderbook_tokens: list[str] = []
        self.midpoint_batches: list[list[str]] = []
        self.closed = False

    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        self.orderbook_tokens.append(token_id)
        return RawOrderbook(
            token_id=token_id,
            bids=[[str(self.midpoint - Decimal("0.005000")), "100000"]],
            asks=[[str(self.midpoint + Decimal("0.005000")), "100000"]],
            spread=Decimal("0.010000"),
        )

    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, Decimal]:
        self.midpoint_batches.append(list(token_ids))
        return {token_id: self.midpoint for token_id in token_ids}

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_alpha_rule_evaluator_matches_frozen_blueprint(session):
    now = datetime.now(timezone.utc)
    market = make_market(
        session,
        platform="kalshi",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("50000.00"),
    )
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now,
        details={"direction": "up", "market_question": "Test market?"},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("0.080000"),
        estimated_probability=Decimal("0.4400"),
    )

    evaluation = evaluate_alpha_rule_signal(
        signal,
        blueprint=ALPHA_KALSHI_4237F81367_V1,
        market_platform="kalshi",
        market=market,
    )

    assert evaluation.in_scope is True
    assert evaluation.eligible is True
    assert evaluation.diagnostics["intended_direction"] == "buy_yes"
    assert evaluation.diagnostics["edge_per_share"] == "0.090000"

    signal.price_at_fire = Decimal("0.550000")
    evaluation = evaluate_alpha_rule_signal(
        signal,
        blueprint=ALPHA_KALSHI_4237F81367_V1,
        market_platform="kalshi",
        market=market,
    )

    assert evaluation.in_scope is False
    assert evaluation.eligible is False
    assert evaluation.reason_code == "not_alpha_rule_4237f81367_price_bucket"


@pytest.mark.asyncio
async def test_alpha_rule_registry_seeds_frozen_shadow_candidate(session):
    await sync_strategy_registry(session)

    version = await get_current_strategy_version(session, ALPHA_KALSHI_4237F81367_FAMILY)

    assert version is not None
    assert version.version_key == ALPHA_KALSHI_4237F81367_VERSION
    assert version.strategy_name == ALPHA_KALSHI_4237F81367_VERSION
    assert version.version_status == VERSION_STATUS_CANDIDATE
    assert version.autonomy_tier == AUTONOMY_TIER_SHADOW_ONLY
    assert version.is_frozen is True
    assert version.config_json["live_orders_enabled"] is False
    assert version.config_json["thresholds_frozen"] is True
    assert version.config_json["trade_direction"] == "buy_yes"
    assert version.config_json["rule_digest"] == "4237f81367"


@pytest.mark.asyncio
async def test_alpha_rule_paper_lane_captures_fresh_book_and_opens_buy_yes(session, monkeypatch):
    import app.strategies.kalshi_orderbook_capture as capture_module

    fake_connector = _FakeKalshiConnector(midpoint=Decimal("0.360000"))
    monkeypatch.setattr(capture_module, "get_connector", lambda _platform: fake_connector)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        question="Will the alpha-rule test follow up?",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("50000.00"),
        active=True,
    )
    outcome = make_outcome(session, market.id, name="Yes", token_id="KTEST-ALPHA:yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now,
        details={"direction": "up", "market_question": market.question},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("0.080000"),
        estimated_probability=Decimal("0.4400"),
    )
    await session.commit()

    result = await run_alpha_rule_paper_lane(
        session,
        [signal],
        blueprint=ALPHA_KALSHI_4237F81367_V1,
        pending_retry_limit=0,
        backlog_limit=0,
        pending_expiry_limit=100,
    )

    assert result["candidate_count"] == 1
    assert result["targeted_orderbook_captures"] == 1
    assert result["opened_count"] == 1
    assert fake_connector.orderbook_tokens == ["KTEST-ALPHA:yes"]
    assert fake_connector.closed is True

    orderbook = (await session.execute(select(OrderbookSnapshot))).scalars().one()
    trade = (await session.execute(select(PaperTrade))).scalars().one()
    strategy_run = (
        await session.execute(
            select(StrategyRun).where(StrategyRun.strategy_name == ALPHA_KALSHI_4237F81367_VERSION)
        )
    ).scalars().one()
    decision = (await session.execute(select(ExecutionDecision))).scalars().one()
    version = await get_current_strategy_version(session, ALPHA_KALSHI_4237F81367_FAMILY)

    assert orderbook.outcome_id == outcome.id
    assert strategy_run.strategy_family == ALPHA_KALSHI_4237F81367_FAMILY
    assert strategy_run.strategy_version_id == version.id
    assert strategy_run.contract_snapshot["thresholds_frozen"] is True
    assert strategy_run.contract_snapshot["rule_digest"] == "4237f81367"
    assert trade.strategy_run_id == strategy_run.id
    assert trade.strategy_version_id == version.id
    assert trade.direction == "buy_yes"
    assert decision.decision_status == "opened"
    assert decision.direction == "buy_yes"
    assert decision.details["market_price"] == "0.360000"
    assert signal.details[ALPHA_KALSHI_4237F81367_FAMILY]["trade_id"] == str(trade.id)


@pytest.mark.asyncio
async def test_alpha_rule_current_precheck_blocks_price_outside_bucket(session, monkeypatch):
    import app.strategies.kalshi_orderbook_capture as capture_module

    fake_connector = _FakeKalshiConnector(midpoint=Decimal("0.610000"))
    monkeypatch.setattr(capture_module, "get_connector", lambda _platform: fake_connector)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("50000.00"),
        active=True,
    )
    outcome = make_outcome(session, market.id, name="Yes", token_id="KTEST-ALPHA-SKIP:yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now,
        details={"direction": "up", "market_question": market.question},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("0.080000"),
        estimated_probability=Decimal("0.7000"),
    )
    await session.commit()

    result = await run_alpha_rule_paper_lane(
        session,
        [signal],
        blueprint=ALPHA_KALSHI_4237F81367_V1,
        pending_retry_limit=0,
        backlog_limit=0,
        pending_expiry_limit=100,
    )

    assert result["candidate_count"] == 1
    assert result["opened_count"] == 0
    assert result["skip_counts"] == {"alpha_rule_4237f81367_current_price_outside_bucket": 1}
    decision = (await session.execute(select(ExecutionDecision))).scalars().one()
    assert decision.decision_status == "skipped"
    assert decision.reason_code == "alpha_rule_4237f81367_current_price_outside_bucket"


@pytest.mark.asyncio
async def test_scheduler_runs_alpha_rule_lane_without_default_strategy_run(session, monkeypatch):
    import app.strategies.kalshi_orderbook_capture as capture_module
    from app.jobs.scheduler import _run_paper_trading

    fake_connector = _FakeKalshiConnector(midpoint=Decimal("0.360000"))
    monkeypatch.setattr(capture_module, "get_connector", lambda _platform: fake_connector)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        question="Will scheduler alpha-rule test follow?",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("50000.00"),
        active=True,
    )
    outcome = make_outcome(session, market.id, name="Yes", token_id="KTEST-ALPHA-SCHED:yes")
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now,
        details={"direction": "up", "market_question": market.question},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("0.080000"),
        estimated_probability=Decimal("0.4400"),
    )
    await session.commit()

    await _run_paper_trading(session, [signal])

    trade = (await session.execute(select(PaperTrade))).scalars().one()
    strategy_run = (
        await session.execute(
            select(StrategyRun).where(StrategyRun.strategy_name == ALPHA_KALSHI_4237F81367_VERSION)
        )
    ).scalars().one()
    assert strategy_run.strategy_family == ALPHA_KALSHI_4237F81367_FAMILY
    assert trade.strategy_run_id == strategy_run.id
    assert trade.direction == "buy_yes"


@pytest.mark.asyncio
async def test_alpha_rule_snapshot_normalizes_to_research_lane(session):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    market = make_market(
        session,
        platform="kalshi",
        end_date=now + timedelta(days=1),
        last_liquidity=Decimal("50000.00"),
    )
    outcome = make_outcome(session, market.id, name="Yes")
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        source_platform="kalshi",
        fired_at=now - timedelta(hours=1),
        details={"direction": "up", "market_question": market.question},
        price_at_fire=Decimal("0.350000"),
        expected_value=Decimal("0.080000"),
        estimated_probability=Decimal("0.4400"),
        resolved_correctly=True,
        profit_loss=Decimal("0.650000"),
        clv=Decimal("0.020000"),
    )
    await session.commit()

    snapshot = await build_alpha_rule_paper_lane_snapshot(
        session,
        window_days=30,
        max_signals=5000,
        as_of=now,
    )
    payload = alpha_rule_paper_lane_payload(snapshot)

    assert snapshot["historical"]["matching_signals"] == 1
    assert snapshot["verdict"] == "research_ready"
    assert "no_active_candidate_run" in snapshot["blockers"]
    assert payload["family"] == ALPHA_KALSHI_4237F81367_FAMILY
    assert payload["strategy_version"] == ALPHA_KALSHI_4237F81367_VERSION
    assert payload["source_kind"] == "alpha_rule_paper_lane_snapshot"
    assert payload["details_json"]["next_best_actions"]


@pytest.mark.asyncio
async def test_strategy_profitability_reports_alpha_rule_snapshot(client):
    response = await client.get("/api/v1/strategies/profitability")

    assert response.status_code == 200
    snapshots = response.json()["snapshots"]
    alpha_snapshot = next(
        row for row in snapshots if row["family"] == ALPHA_KALSHI_4237F81367_FAMILY
    )
    assert alpha_snapshot["strategy_version"] == ALPHA_KALSHI_4237F81367_VERSION
    assert alpha_snapshot["source_kind"] == "alpha_rule_paper_lane_snapshot"
    assert alpha_snapshot["paper_only"] is True
    assert alpha_snapshot["live_submission_permitted"] is False
    assert "paper_lane_not_populated" not in alpha_snapshot["profitability_blockers"]
