from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_microstructure import (
    AssetContext,
    ReplayData,
    ReplayMarker,
    SnapshotBoundary,
    TradeObservation,
)
from app.ingestion.polymarket_replay_simulator import (
    ORDER_STATUS_FILLED,
    ORDER_STATUS_PARTIAL,
    PolymarketReplaySimulatorService,
    ReplayScenarioBlueprint,
    list_polymarket_replay_decision_traces,
    list_polymarket_replay_metrics,
)
from app.ingestion.structure_engine import PolymarketStructureEngineService
from app.models.market_structure import MarketStructureOpportunity
from app.models.polymarket_maker import PolymarketMakerEconomicsSnapshot, PolymarketQuoteRecommendation
from app.models.polymarket_raw import PolymarketBookDelta
from app.models.polymarket_risk import PortfolioOptimizerRecommendation, RiskGraphRun
from app.models.strategy_registry import PromotionEvaluation
from app.paper_trading.engine import attempt_open_trade
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_polymarket_market_event
from tests.test_polymarket_execution_policy import _make_context, _seed_polymarket_execution_fixture
from tests.test_structure_engine import FIXED_NOW, _seed_executable_neg_risk_setup, _set_structure_defaults


ZERO = Decimal("0")


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _set_phase11_defaults(monkeypatch, **overrides):
    values = {
        "polymarket_replay_enabled": True,
        "polymarket_replay_on_startup": False,
        "polymarket_replay_interval_seconds": 1800,
        "polymarket_replay_default_window_minutes": 5,
        "polymarket_replay_max_scenarios_per_run": 50,
        "polymarket_replay_enable_structure": True,
        "polymarket_replay_enable_maker": True,
        "polymarket_replay_enable_risk_adjustments": True,
        "polymarket_replay_require_complete_book_coverage": True,
        "polymarket_replay_passive_fill_timeout_seconds": 30,
        "polymarket_execution_policy_enabled": True,
        "polymarket_execution_policy_step_ahead_enabled": True,
        "polymarket_execution_policy_passive_lookback_hours": 24,
        "polymarket_execution_policy_passive_min_label_rows": 5,
        "polymarket_execution_policy_default_horizon_ms": 1000,
        "polymarket_execution_policy_max_cross_slippage_bps": 500.0,
        "polymarket_execution_policy_min_net_ev_bps": 0.0,
        "polymarket_live_trading_enabled": False,
    }
    values.update(overrides)
    for key, value in values.items():
        monkeypatch.setattr(settings, key, value)


def _manual_replay(
    *,
    asset_id: str,
    condition_id: str,
    observed_at: datetime,
    best_bid: str,
    best_ask: str,
    bid_levels: list[tuple[str, str]] | None = None,
    ask_levels: list[tuple[str, str]] | None = None,
    trades: list[TradeObservation] | None = None,
    latest_observed_time: datetime | None = None,
) -> ReplayData:
    bid_pairs = [(Decimal(price), Decimal(size)) for price, size in (bid_levels or [(best_bid, "200")])]
    ask_pairs = [(Decimal(price), Decimal(size)) for price, size in (ask_levels or [(best_ask, "200")])]
    best_bid_decimal = Decimal(best_bid)
    best_ask_decimal = Decimal(best_ask)
    marker = ReplayMarker(
        exchange_time=observed_at,
        observed_at_local=observed_at,
        best_bid=best_bid_decimal,
        best_ask=best_ask_decimal,
        spread=(best_ask_decimal - best_bid_decimal).quantize(Decimal("0.00000001")),
        mid=((best_bid_decimal + best_ask_decimal) / Decimal("2")).quantize(Decimal("0.00000001")),
        microprice=((best_bid_decimal + best_ask_decimal) / Decimal("2")).quantize(Decimal("0.00000001")),
        tick_size=Decimal("0.01"),
        bid_levels=bid_pairs,
        ask_levels=ask_pairs,
        bid_depth_top1=bid_pairs[0][1],
        bid_depth_top3=sum((size for _price, size in bid_pairs[:3]), ZERO),
        bid_depth_top5=sum((size for _price, size in bid_pairs[:5]), ZERO),
        ask_depth_top1=ask_pairs[0][1],
        ask_depth_top3=sum((size for _price, size in ask_pairs[:3]), ZERO),
        ask_depth_top5=sum((size for _price, size in ask_pairs[:5]), ZERO),
        imbalance_top1=Decimal("0.50"),
        imbalance_top3=Decimal("0.50"),
        imbalance_top5=Decimal("0.50"),
        trustworthy_seed=True,
        affected_by_drift=False,
        last_snapshot_id=1,
        last_snapshot_hash="manual-seed",
        last_applied_raw_event_id=1,
    )
    resolved_latest = latest_observed_time or observed_at
    return ReplayData(
        context=AssetContext(
            asset_id=asset_id,
            condition_id=condition_id,
            market_dim_id=1,
            asset_dim_id=1,
            recon_state_id=1,
        ),
        markers=[marker],
        marker_times=[marker.exchange_time],
        delta_flows=[],
        trades=trades or [],
        bbo_events=[],
        snapshot_boundaries=[SnapshotBoundary(exchange_time=observed_at, source_kind="ws_book")],
        drift_times=[],
        partial_event_times=[],
        latest_observed_time=resolved_latest,
    )


async def _seed_policy_replay_decision(
    session: AsyncSession,
    *,
    decision_at: datetime,
    condition_id: str,
    asset_id: str,
):
    seeded = await _seed_polymarket_execution_fixture(
        session,
        condition_id=condition_id,
        asset_id=asset_id,
        decision_at=decision_at,
        estimated_probability="0.65",
        price_at_fire="0.40",
        expected_value="0.25",
        best_bid="0.40",
        best_ask="0.41",
        bids=[("0.40", "500"), ("0.39", "500")],
        asks=[("0.41", "300"), ("0.42", "300")],
        label_rows=0,
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=decision_at)
    result = await attempt_open_trade(
        session=session,
        signal_id=seeded["signal"].id,
        outcome_id=seeded["outcome"].id,
        market_id=seeded["market"].id,
        estimated_probability=Decimal("0.65"),
        market_price=Decimal("0.40"),
        market_question=seeded["market"].question,
        fired_at=decision_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()
    return {
        **seeded,
        "execution_decision": result.execution_decision,
        "paper_trade": result.trade,
    }


async def _seed_replay_truth_fixture(
    session: AsyncSession,
    *,
    base: datetime,
    condition_id: str,
    asset_id: str,
):
    seeded = await _seed_polymarket_execution_fixture(
        session,
        condition_id=condition_id,
        asset_id=asset_id,
        decision_at=base,
        estimated_probability="0.62",
        price_at_fire="0.50",
        expected_value="0.12",
        best_bid="0.40",
        best_ask="0.60",
        bids=[("0.40", "100"), ("0.39", "50")],
        asks=[("0.60", "80"), ("0.61", "60")],
        label_rows=0,
    )
    raw_event_1 = make_polymarket_market_event(
        session,
        market_id=condition_id,
        asset_id=asset_id,
        message_type="price_change",
        event_time=base + timedelta(milliseconds=100),
        received_at_local=base + timedelta(milliseconds=100),
        payload={"asset_id": asset_id, "event": "delta-1"},
    )
    raw_event_2 = make_polymarket_market_event(
        session,
        market_id=condition_id,
        asset_id=asset_id,
        message_type="price_change",
        event_time=base + timedelta(milliseconds=200),
        received_at_local=base + timedelta(milliseconds=200),
        payload={"asset_id": asset_id, "event": "delta-2"},
    )
    await session.flush()
    session.add_all(
        [
            PolymarketBookDelta(
                market_dim_id=seeded["market_dim"].id,
                asset_dim_id=seeded["asset_dim"].id,
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=raw_event_1.id,
                delta_index=0,
                price=Decimal("0.40"),
                size=Decimal("120"),
                side="BUY",
                event_ts_exchange=base + timedelta(milliseconds=100),
                recv_ts_local=base + timedelta(milliseconds=100),
                ingest_ts_db=base + timedelta(milliseconds=100),
            ),
            PolymarketBookDelta(
                market_dim_id=seeded["market_dim"].id,
                asset_dim_id=seeded["asset_dim"].id,
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=raw_event_2.id,
                delta_index=0,
                price=Decimal("0.60"),
                size=Decimal("0"),
                side="SELL",
                event_ts_exchange=base + timedelta(milliseconds=200),
                recv_ts_local=base + timedelta(milliseconds=200),
                ingest_ts_db=base + timedelta(milliseconds=200),
            ),
            PolymarketBookDelta(
                market_dim_id=seeded["market_dim"].id,
                asset_dim_id=seeded["asset_dim"].id,
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=raw_event_2.id,
                delta_index=1,
                price=Decimal("0.58"),
                size=Decimal("70"),
                side="SELL",
                event_ts_exchange=base + timedelta(milliseconds=201),
                recv_ts_local=base + timedelta(milliseconds=201),
                ingest_ts_db=base + timedelta(milliseconds=201),
            ),
        ]
    )
    await session.commit()
    return seeded


@pytest.mark.asyncio
async def test_replay_rebuild_is_deterministic_from_stored_snapshot_and_deltas(engine, monkeypatch):
    _set_phase11_defaults(monkeypatch)
    session_factory = _session_factory(engine)
    base = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        await _seed_replay_truth_fixture(
            session,
            base=base,
            condition_id="cond-phase11-replay",
            asset_id="token-phase11-replay",
        )

    service = PolymarketReplaySimulatorService(session_factory)
    async with session_factory() as session:
        asset_context, _asset_dim, _recon_state = await service._load_asset_dependencies(
            session,
            asset_id="token-phase11-replay",
            condition_id="cond-phase11-replay",
        )
        replay_one = await service._get_replay(
            session,
            asset_context=asset_context,
            start=base,
            end=base + timedelta(seconds=2),
            replay_cache={},
        )
        replay_two = await service._get_replay(
            session,
            asset_context=asset_context,
            start=base,
            end=base + timedelta(seconds=2),
            replay_cache={},
        )
    await service.close()

    marker_projection = lambda replay: [
        (
            marker.exchange_time,
            marker.best_bid,
            marker.best_ask,
            marker.last_applied_raw_event_id,
        )
        for marker in replay.markers
    ]

    assert marker_projection(replay_one) == marker_projection(replay_two)
    assert replay_one.marker_times == sorted(replay_one.marker_times)
    assert replay_one.context.asset_id == "token-phase11-replay"


@pytest.mark.asyncio
async def test_replay_aggressive_book_walk_fill_math_is_deterministic(engine, monkeypatch):
    _set_phase11_defaults(monkeypatch)
    service = PolymarketReplaySimulatorService(_session_factory(engine))
    decision_at = datetime(2026, 4, 14, 11, 0, 0, tzinfo=timezone.utc)
    context = _make_context(
        asset_id="token-phase11-bookwalk",
        condition_id="cond-phase11-bookwalk",
        decision_at=decision_at,
        estimated_probability="0.80",
        market_price="0.52",
        baseline_target_size="133.00",
        asks=[("0.52", "100"), ("0.54", "150")],
        bids=[("0.48", "200"), ("0.47", "200")],
    )
    replay = _manual_replay(
        asset_id=context.asset_id,
        condition_id=context.condition_id,
        observed_at=decision_at,
        best_bid="0.48",
        best_ask="0.52",
        bid_levels=[("0.48", "200"), ("0.47", "200")],
        ask_levels=[("0.52", "100"), ("0.54", "150")],
    )

    result = service._simulate_aggressive_order(
        context=context,
        replay=replay,
        decision_ts=decision_at,
        limit_price=Decimal("0.52"),
        requested_size=Decimal("133.00"),
    )
    await service.close()

    assert result["status"] == ORDER_STATUS_FILLED
    assert len(result["fills"]) == 2
    assert result["order_details"]["avg_entry_price"] == Decimal("0.53200000")
    assert result["order_details"]["fillable_notional"] == Decimal("133.0000")
    assert result["order_details"]["fillable_shares"] == Decimal("250.0000")
    assert float(result["order_details"]["slippage_bps"]) == pytest.approx(230.76923077, rel=1e-6)


@pytest.mark.asyncio
async def test_replay_passive_fill_stays_conservative_and_explainable(session, engine, monkeypatch):
    _set_phase11_defaults(monkeypatch, polymarket_execution_policy_passive_min_label_rows=5)
    service = PolymarketReplaySimulatorService(_session_factory(engine))
    decision_at = datetime(2026, 4, 14, 11, 15, 0, tzinfo=timezone.utc)
    context = _make_context(
        asset_id="token-phase11-passive",
        condition_id="cond-phase11-passive",
        decision_at=decision_at,
        baseline_target_size="100.00",
        best_bid="0.48",
        best_ask="0.52",
        bids=[("0.48", "200"), ("0.47", "200")],
        asks=[("0.52", "200"), ("0.53", "200")],
    )
    replay = _manual_replay(
        asset_id=context.asset_id,
        condition_id=context.condition_id,
        observed_at=decision_at,
        best_bid="0.48",
        best_ask="0.52",
        bid_levels=[("0.48", "200"), ("0.47", "200")],
        ask_levels=[("0.52", "200"), ("0.53", "200")],
        trades=[
            TradeObservation(
                exchange_time=decision_at + timedelta(seconds=5),
                observed_at_local=decision_at + timedelta(seconds=5),
                raw_event_id=1,
                side="sell",
                price=Decimal("0.48"),
                size=Decimal("10"),
            )
        ],
        latest_observed_time=decision_at + timedelta(seconds=10),
    )

    result = await service._simulate_passive_order(
        session,
        context=context,
        replay=replay,
        action_type="post_best",
        decision_ts=decision_at,
        expiry_ts=decision_at + timedelta(seconds=10),
        limit_price=Decimal("0.48"),
        requested_size=Decimal("100.00"),
        reward_total=ZERO,
    )
    await service.close()

    assert result["status"] == ORDER_STATUS_PARTIAL
    assert result["order_details"]["model_reason"] == "trade_touch_without_sufficient_label_history"
    assert len(result["fills"]) == 1
    assert result["fills"][0]["fill_source_kind"] == "trade_touch"
    assert result["fills"][0]["size"] == Decimal("5.0000")


@pytest.mark.asyncio
async def test_action_policy_replay_produces_stable_choice_traces(session, engine, monkeypatch):
    _set_phase11_defaults(monkeypatch)
    service = PolymarketReplaySimulatorService(_session_factory(engine))
    decision_at = datetime(2026, 4, 14, 11, 30, 0, tzinfo=timezone.utc)
    context = _make_context(
        asset_id="token-phase11-policy",
        condition_id="cond-phase11-policy",
        decision_at=decision_at,
        estimated_probability="0.80",
        market_price="0.40",
        baseline_target_size="100.00",
        best_bid="0.40",
        best_ask="0.41",
        bids=[("0.40", "500"), ("0.39", "500")],
        asks=[("0.41", "300"), ("0.42", "300")],
    )
    replay = _manual_replay(
        asset_id=context.asset_id,
        condition_id=context.condition_id,
        observed_at=decision_at,
        best_bid="0.40",
        best_ask="0.41",
        bid_levels=[("0.40", "500"), ("0.39", "500")],
        ask_levels=[("0.41", "300"), ("0.42", "300")],
        latest_observed_time=decision_at + timedelta(seconds=10),
    )
    blueprint = ReplayScenarioBlueprint(
        scenario_type="policy_comparison",
        scenario_key="policy-unit",
        window_start=decision_at,
        window_end=decision_at + timedelta(seconds=30),
        decision_at=decision_at,
        condition_id=context.condition_id,
        asset_id=context.asset_id,
        direction=context.direction,
        estimated_probability=context.estimated_probability,
        market_price=context.market_price,
        baseline_target_size=context.baseline_target_size,
    )

    result_one = await service._replay_execution_policy_variant(
        session,
        context=context,
        replay=replay,
        blueprint=blueprint,
        variant_name="exec_policy",
        apply_risk=False,
    )
    result_two = await service._replay_execution_policy_variant(
        session,
        context=context,
        replay=replay,
        blueprint=blueprint,
        variant_name="exec_policy",
        apply_risk=False,
    )
    await service.close()

    assert result_one.orders[0]["action_type"] == result_two.orders[0]["action_type"] == "cross_now"
    assert result_one.traces[0]["trace_type"] == "action_choice"
    assert result_one.traces[0]["reason_code"] == result_two.traces[0]["reason_code"]
    assert result_one.metric["details_json"]["chosen_reason"] == result_two.metric["details_json"]["chosen_reason"]


@pytest.mark.asyncio
async def test_replay_metrics_use_canonical_settlement_price(session, engine, monkeypatch):
    _set_phase11_defaults(monkeypatch)
    service = PolymarketReplaySimulatorService(_session_factory(engine))
    decision_at = datetime(2026, 4, 14, 11, 35, 0, tzinfo=timezone.utc)
    seeded = await _seed_polymarket_execution_fixture(
        session,
        condition_id="cond-phase11-settlement",
        asset_id="token-phase11-settlement",
        decision_at=decision_at,
        estimated_probability="0.65",
        price_at_fire="0.40",
        expected_value="0.25",
        best_bid="0.40",
        best_ask="0.41",
        bids=[("0.40", "500"), ("0.39", "500")],
        asks=[("0.41", "300"), ("0.42", "300")],
        label_rows=0,
    )
    seeded["market_dim"].resolved = True
    seeded["market_dim"].resolution_state = "resolved"
    seeded["market_dim"].winning_asset_id = seeded["asset_dim"].asset_id
    await session.commit()

    replay = _manual_replay(
        asset_id=seeded["asset_dim"].asset_id,
        condition_id=seeded["asset_dim"].condition_id,
        observed_at=decision_at,
        best_bid="0.40",
        best_ask="0.41",
        bid_levels=[("0.40", "500"), ("0.39", "500")],
        ask_levels=[("0.41", "300"), ("0.42", "300")],
        latest_observed_time=decision_at + timedelta(seconds=10),
    )

    metric = await service._metric_from_execution(
        session,
        variant_name="exec_policy",
        direction="buy_yes",
        orders=[
            {
                "requested_size": Decimal("40.0000"),
                "status": ORDER_STATUS_FILLED,
                "action_type": "cross_now",
                "details_json": {},
            }
        ],
        fills=[
            {
                "price": Decimal("0.40"),
                "size": Decimal("100.0000"),
                "fee_paid": ZERO,
                "reward_estimate": ZERO,
            }
        ],
        replay=replay,
        decision_ts=decision_at,
    )
    await service.close()

    assert metric["gross_pnl"] == Decimal("60.00000000")
    assert metric["net_pnl"] == Decimal("60.00000000")
    assert metric["details_json"]["exit_price"] == Decimal("1.00000000")
    assert metric["details_json"]["settlement_source_kind"] == "market_dim"
    assert metric["details_json"]["coverage_mode"] == "canonical_settlement"
    assert metric["details_json"]["coverage_limited"] is False


@pytest.mark.asyncio
async def test_maker_replay_accounts_for_rewards_and_realism_adjustments(session, engine, monkeypatch):
    _set_phase11_defaults(monkeypatch)
    service = PolymarketReplaySimulatorService(_session_factory(engine))
    decision_at = datetime(2026, 4, 14, 11, 45, 0, tzinfo=timezone.utc)
    context = _make_context(
        asset_id="token-phase11-maker",
        condition_id="cond-phase11-maker",
        decision_at=decision_at,
        baseline_target_size="24.00",
        best_bid="0.48",
        best_ask="0.52",
        bids=[("0.48", "300"), ("0.47", "300")],
        asks=[("0.52", "300"), ("0.53", "300")],
    )
    replay = _manual_replay(
        asset_id=context.asset_id,
        condition_id=context.condition_id,
        observed_at=decision_at,
        best_bid="0.48",
        best_ask="0.52",
        bid_levels=[("0.48", "300"), ("0.47", "300")],
        ask_levels=[("0.52", "300"), ("0.53", "300")],
        trades=[
            TradeObservation(
                exchange_time=decision_at + timedelta(seconds=3),
                observed_at_local=decision_at + timedelta(seconds=3),
                raw_event_id=10,
                side="sell",
                price=Decimal("0.48"),
                size=Decimal("60"),
            )
        ],
        latest_observed_time=decision_at + timedelta(seconds=15),
    )
    snapshot = PolymarketMakerEconomicsSnapshot(
        id=uuid.uuid4(),
        condition_id=context.condition_id,
        asset_id=context.asset_id,
        context_kind="opportunity",
        estimator_version="phase9-test",
        status="ok",
        preferred_action="maker",
        maker_action_type="post_best",
        side=context.direction,
        target_size=Decimal("50.0000"),
        target_notional=Decimal("24.0000"),
        maker_fill_probability=Decimal("0.400000"),
        maker_gross_edge_total=Decimal("4.00000000"),
        maker_fees_total=Decimal("0.00000000"),
        maker_rewards_total=Decimal("3.00000000"),
        maker_realism_adjustment_total=Decimal("1.00000000"),
        maker_net_total=Decimal("6.00000000"),
        taker_gross_edge_total=Decimal("2.00000000"),
        taker_fees_total=Decimal("0.50000000"),
        taker_rewards_total=Decimal("0.00000000"),
        taker_realism_adjustment_total=Decimal("0.50000000"),
        taker_net_total=Decimal("1.00000000"),
        maker_advantage_total=Decimal("5.00000000"),
        reason_codes_json=["maker_edge"],
        details_json={"selected_candidate": {"action_type": "post_best", "entry_price": "0.48", "target_notional": "24.00"}},
        input_fingerprint="phase11-maker-snapshot",
        evaluated_at=decision_at,
    )
    quote = PolymarketQuoteRecommendation(
        id=uuid.uuid4(),
        snapshot_id=snapshot.id,
        condition_id=context.condition_id,
        asset_id=context.asset_id,
        recommendation_kind="advisory_quote",
        status="ok",
        comparison_winner="maker",
        recommendation_action="recommend_quote",
        recommended_action_type="post_best",
        recommended_side=context.direction,
        recommended_yes_price=Decimal("0.48000000"),
        recommended_entry_price=Decimal("0.48000000"),
        recommended_size=Decimal("50.0000"),
        recommended_notional=Decimal("24.0000"),
        price_offset_ticks=0,
        reason_codes_json=["maker_edge"],
        summary_json={"source": "test"},
        details_json={"source": "test"},
        input_fingerprint="phase11-maker-quote",
        created_at=decision_at,
        updated_at=decision_at,
    )

    result = await service._replay_maker_variant(
        session,
        quote=quote,
        snapshot=snapshot,
        context=context,
        replay=replay,
        variant_name="maker_policy",
        use_quote_row=True,
    )
    await service.close()

    assert result.orders[0]["status"] in {ORDER_STATUS_FILLED, ORDER_STATUS_PARTIAL}
    assert Decimal(str(result.orders[0]["details_json"]["maker_advantage_total"])) == Decimal("5.00000000")
    assert result.metric["rewards_estimated"] > ZERO
    assert result.metric["drawdown_proxy"] == Decimal("1.00000000")


@pytest.mark.asyncio
async def test_risk_adjusted_replay_reduces_size_when_optimizer_cap_exists(session, engine, monkeypatch):
    _set_phase11_defaults(monkeypatch)
    service = PolymarketReplaySimulatorService(_session_factory(engine))
    decision_at = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
    context = _make_context(
        asset_id="token-phase11-risk",
        condition_id="cond-phase11-risk",
        decision_at=decision_at,
        estimated_probability="0.80",
        market_price="0.40",
        baseline_target_size="100.00",
        best_bid="0.40",
        best_ask="0.41",
        bids=[("0.40", "500"), ("0.39", "500")],
        asks=[("0.41", "300"), ("0.42", "300")],
    )
    replay = _manual_replay(
        asset_id=context.asset_id,
        condition_id=context.condition_id,
        observed_at=decision_at,
        best_bid="0.40",
        best_ask="0.41",
        bid_levels=[("0.40", "500"), ("0.39", "500")],
        ask_levels=[("0.41", "300"), ("0.42", "300")],
        latest_observed_time=decision_at + timedelta(seconds=15),
    )
    run = RiskGraphRun(
        run_type="optimizer",
        reason="manual",
        started_at=decision_at - timedelta(minutes=1),
        completed_at=decision_at - timedelta(minutes=1),
        status="completed",
    )
    session.add(run)
    await session.flush()
    session.add(
        PortfolioOptimizerRecommendation(
            run_id=run.id,
            recommendation_type="reduce_size",
            scope_kind="asset",
            condition_id=context.condition_id,
            asset_id=context.asset_id,
            target_size_cap_usd=Decimal("25.00000000"),
            inventory_penalty_bps=Decimal("15.00000000"),
            reservation_price_adjustment_bps=Decimal("10.00000000"),
            maker_budget_remaining_usd=Decimal("20.00000000"),
            taker_budget_remaining_usd=Decimal("30.00000000"),
            reason_code="inventory_cap",
            details_json={"source": "test"},
            observed_at_local=decision_at - timedelta(seconds=1),
        )
    )
    await session.commit()
    blueprint = ReplayScenarioBlueprint(
        scenario_type="policy_comparison",
        scenario_key="policy-risk",
        window_start=decision_at,
        window_end=decision_at + timedelta(seconds=30),
        decision_at=decision_at,
        condition_id=context.condition_id,
        asset_id=context.asset_id,
        direction=context.direction,
        estimated_probability=context.estimated_probability,
        market_price=context.market_price,
        baseline_target_size=context.baseline_target_size,
    )

    result = await service._replay_execution_policy_variant(
        session,
        context=context,
        replay=replay,
        blueprint=blueprint,
        variant_name="risk_adjusted",
        apply_risk=True,
    )
    await service.close()

    assert Decimal(str(result.orders[0]["submitted_size"])) == Decimal("25.0000")
    assert any(
        trace["trace_type"] == "risk_adjustment" and trace["reason_code"] == "inventory_cap"
        for trace in result.traces
    )


@pytest.mark.asyncio
async def test_structure_replay_run_persists_variant_metrics(engine, monkeypatch):
    _set_structure_defaults(monkeypatch, polymarket_structure_validation_enabled=True, polymarket_structure_run_lock_enabled=False)
    _set_phase11_defaults(
        monkeypatch,
        polymarket_replay_enable_structure=True,
        polymarket_replay_enable_maker=False,
        polymarket_replay_enable_risk_adjustments=False,
    )
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_executable_neg_risk_setup(
            session,
            slug="phase11-structure",
            title="Phase11 Structure",
            anchor_condition_id="phase11-structure-anchor",
            basket_condition_id="phase11-structure-basket",
        )
        await session.commit()

    structure_service = PolymarketStructureEngineService(session_factory)
    await structure_service.build_groups(reason="manual", event_slug="phase11-structure")
    await structure_service.scan_opportunities(reason="manual", event_slug="phase11-structure")
    await structure_service.validate_opportunities(reason="manual")
    await structure_service.close()

    async with session_factory() as session:
        opportunity = (
            await session.execute(
                select(MarketStructureOpportunity)
                .where(MarketStructureOpportunity.anchor_condition_id == "phase11-structure-anchor")
                .order_by(MarketStructureOpportunity.id.asc())
                .limit(1)
            )
        ).scalar_one()

    replay_service = PolymarketReplaySimulatorService(session_factory)
    result = await replay_service.run_once(
        reason="research",
        run_type="structure_replay",
        start=FIXED_NOW - timedelta(minutes=1),
        end=FIXED_NOW + timedelta(minutes=1),
        opportunity_ids=[opportunity.id],
        limit=1,
    )
    await replay_service.close()

    assert result["run"]["scenario_count"] == 1
    async with session_factory() as session:
        metrics = await list_polymarket_replay_metrics(
            session,
            run_type="structure_replay",
            metric_scope="scenario",
            limit=20,
        )
    variant_names = {row["variant_name"] for row in metrics}
    assert "midpoint_baseline" in variant_names
    assert "structure_policy" in variant_names


@pytest.mark.asyncio
async def test_replay_api_and_health_surfaces_are_idempotent(client, engine, monkeypatch):
    _set_phase11_defaults(
        monkeypatch,
        polymarket_replay_enable_structure=False,
        polymarket_replay_enable_maker=False,
        polymarket_replay_enable_risk_adjustments=True,
    )
    session_factory = _session_factory(engine)
    decision_at = datetime(2026, 4, 14, 12, 30, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        seeded = await _seed_policy_replay_decision(
            session,
            decision_at=decision_at,
            condition_id="cond-phase11-api",
            asset_id="token-phase11-api",
        )
        existing_trade_id = seeded["paper_trade"].id

    payload = {
        "reason": "manual",
        "run_type": "policy_compare",
        "start": (decision_at - timedelta(minutes=1)).isoformat(),
        "end": (decision_at + timedelta(minutes=1)).isoformat(),
        "asset_ids": ["token-phase11-api"],
        "limit": 5,
    }
    first = await client.post("/api/v1/ingest/polymarket/replay/trigger", json=payload)
    second = await client.post("/api/v1/ingest/polymarket/replay/trigger", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    first_payload = first.json()
    second_payload = second.json()
    assert first_payload["idempotent_hit"] is False
    assert second_payload["idempotent_hit"] is True
    assert first_payload["run"]["id"] == second_payload["run"]["id"]

    runs_response = await client.get("/api/v1/ingest/polymarket/replay/runs?run_type=policy_compare")
    scenarios_response = await client.get("/api/v1/ingest/polymarket/replay/scenarios?asset_id=token-phase11-api")
    metrics_response = await client.get("/api/v1/ingest/polymarket/replay/metrics?variant_name=exec_policy")
    traces_response = await client.get("/api/v1/ingest/polymarket/replay/decision-traces?variant_name=exec_policy")
    summary_response = await client.get("/api/v1/ingest/polymarket/replay/policy-summary")
    status_response = await client.get("/api/v1/ingest/polymarket/replay/status")
    health_response = await client.get("/api/v1/health")
    strategies_response = await client.get("/api/v1/strategies")

    assert runs_response.status_code == 200
    assert scenarios_response.status_code == 200
    assert metrics_response.status_code == 200
    assert traces_response.status_code == 200
    assert summary_response.status_code == 200
    assert status_response.status_code == 200
    assert health_response.status_code == 200
    assert strategies_response.status_code == 200

    scenarios = scenarios_response.json()["rows"]
    assert scenarios
    scenario_detail = await client.get(f"/api/v1/ingest/polymarket/replay/scenarios/{scenarios[0]['id']}")
    assert scenario_detail.status_code == 200
    assert scenario_detail.json()["metrics"]
    assert status_response.json()["recent_scenario_count_24h"] >= 1
    assert status_response.json()["coverage_mode"] == "supported_detectors_only"
    assert "exec_policy" in summary_response.json()["variants"]
    assert "polymarket_phase11" in health_response.json()
    assert health_response.json()["polymarket_phase11"]["last_replay_run"]["id"] == first_payload["run"]["id"]
    assert health_response.json()["polymarket_phase11"]["coverage_mode"] == "supported_detectors_only"
    assert health_response.json()["polymarket_phase11"]["supported_detectors"] == ["confluence"]
    assert first_payload["run"]["strategy_version_key"] == "exec_policy_infra_v1"
    assert first_payload["run"]["promotion_evaluation"]["evaluation_kind"] == "replay_gate"
    assert first_payload["run"]["promotion_evaluation"]["evaluation_status"] == "blocked"
    strategy_families = {row["family"]: row for row in strategies_response.json()["families"]}
    assert strategy_families["exec_policy"]["latest_promotion_evaluation"]["evaluation_kind"] == "replay_gate"
    assert strategy_families["exec_policy"]["latest_promotion_evaluation"]["summary_json"]["primary_variant"] == "exec_policy"
    assert strategy_families["exec_policy"]["current_version"]["evidence_alignment"]["latest_replay_run"]["run_key"] == first_payload["run"]["run_key"]
    assert strategy_families["exec_policy"]["current_version"]["evidence_alignment"]["latest_replay_run"]["promotion_evaluation"]["evaluation_kind"] == "replay_gate"

    async with session_factory() as session:
        traces = await list_polymarket_replay_decision_traces(session, variant_name="exec_policy", limit=20)
        assert traces
        paper_trade = await session.get(type(seeded["paper_trade"]), existing_trade_id)
        assert paper_trade is not None
        replay_evaluation = (
            await session.execute(
                select(PromotionEvaluation)
                .where(PromotionEvaluation.evaluation_kind == "replay_gate")
                .order_by(PromotionEvaluation.created_at.desc(), PromotionEvaluation.id.desc())
                .limit(1)
            )
        ).scalar_one()
        assert replay_evaluation.summary_json["primary_variant"] == "exec_policy"
        assert replay_evaluation.summary_json["variant_count"] >= 1
        assert replay_evaluation.provenance_json["promotion_gate_policy_key"] == "promotion_gate_policy_v1"
