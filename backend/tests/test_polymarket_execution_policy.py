from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_execution_policy import (
    PASSIVE_LABEL_BY_DIRECTION,
    POLICY_VERSION,
    BookLevel,
    PolymarketExecutionContext,
    _evaluate_cross_now,
    _evaluate_post_best,
    _evaluate_step_ahead,
    _walk_visible_book,
    evaluate_polymarket_execution_policy,
)
from app.models.execution_decision import ExecutionDecision
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketMarketDim,
    PolymarketMarketParamHistory,
)
from app.models.polymarket_microstructure import PolymarketAlphaLabel, PolymarketPassiveFillLabel
from app.models.polymarket_raw import PolymarketBookSnapshot
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.paper_trading.engine import attempt_open_trade
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_signal


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _book_levels(levels: list[tuple[str, str]] | None) -> list[BookLevel]:
    result: list[BookLevel] = []
    for price, size in levels or []:
        result.append(BookLevel(yes_price=Decimal(price), size_shares=Decimal(size)))
    return result


def _make_context(
    *,
    direction: str = "buy_yes",
    asset_id: str = "token-phase6-unit",
    condition_id: str = "cond-phase6-unit",
    decision_at: datetime | None = None,
    estimated_probability: str = "0.62",
    market_price: str = "0.50",
    baseline_target_size: str = "100.00",
    tick_size: str = "0.01",
    min_order_size: str = "1",
    fees_enabled: bool = True,
    taker_fee_rate: str = "0.02",
    maker_fee_rate: str = "0.00",
    reliable_book: bool = True,
    best_bid: str | None = "0.48",
    best_ask: str | None = "0.52",
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> PolymarketExecutionContext:
    anchor = decision_at or datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)
    best_bid_decimal = Decimal(best_bid) if best_bid is not None else None
    best_ask_decimal = Decimal(best_ask) if best_ask is not None else None
    spread = (
        (best_ask_decimal - best_bid_decimal).quantize(Decimal("0.00000001"))
        if best_bid_decimal is not None and best_ask_decimal is not None
        else None
    )
    return PolymarketExecutionContext(
        signal_id=uuid.uuid4(),
        market_id=uuid.uuid4(),
        outcome_id=uuid.uuid4(),
        direction=direction,
        estimated_probability=Decimal(estimated_probability),
        market_price=Decimal(market_price),
        baseline_target_size=Decimal(baseline_target_size),
        bankroll=Decimal("10000"),
        decision_at=anchor,
        asset_id=asset_id,
        condition_id=condition_id,
        market_dim_id=1,
        asset_dim_id=1,
        tick_size=Decimal(tick_size),
        min_order_size=Decimal(min_order_size),
        fees_enabled=fees_enabled,
        taker_fee_rate=Decimal(taker_fee_rate),
        maker_fee_rate=Decimal(maker_fee_rate),
        fee_schedule_json={"rate": taker_fee_rate} if fees_enabled else {},
        recon_state_id=1,
        recon_status="live",
        reliable_book=reliable_book,
        book_reason="live_book_rebuilt" if reliable_book else "book_unreliable",
        best_bid=best_bid_decimal,
        best_ask=best_ask_decimal,
        spread=spread,
        bids=_book_levels(bids or [("0.48", "200"), ("0.47", "200")]),
        asks=_book_levels(asks or [("0.52", "100"), ("0.54", "150")]),
        snapshot_id=1,
        snapshot_source_kind="ws_book",
        snapshot_observed_at=anchor,
        snapshot_age_seconds=0,
        horizon_ms=1000,
        lookback_start=anchor - timedelta(hours=24),
    )


async def _seed_label_history(
    session: AsyncSession,
    *,
    context: PolymarketExecutionContext,
    row_count: int,
    touch_observed: bool = True,
    trade_through_observed: bool = True,
    improved_against_order: bool = False,
    adverse_bps: str = "8",
    mid_return_bps: str = "15",
) -> None:
    passive_side = PASSIVE_LABEL_BY_DIRECTION[context.direction]
    posted_price = context.best_bid if context.direction == "buy_yes" else context.best_ask
    assert posted_price is not None
    directional_bps = Decimal(mid_return_bps)
    if context.direction == "buy_no":
        directional_bps = -directional_bps

    for index in range(row_count):
        anchor = context.decision_at - timedelta(minutes=index + 1)
        session.add(
            PolymarketPassiveFillLabel(
                condition_id=context.condition_id,
                asset_id=context.asset_id,
                anchor_bucket_start_exchange=anchor,
                horizon_ms=context.horizon_ms,
                side=passive_side,
                posted_price=posted_price,
                touch_observed=touch_observed,
                trade_through_observed=trade_through_observed,
                best_price_improved_against_order=improved_against_order,
                adverse_move_after_touch_bps=Decimal(adverse_bps),
                source_feature_table="polymarket_microstructure_features_1s",
                source_feature_row_id=index + 1,
                completeness_flags_json={"source": "test"},
            )
        )
        session.add(
            PolymarketAlphaLabel(
                condition_id=context.condition_id,
                asset_id=context.asset_id,
                anchor_bucket_start_exchange=anchor,
                horizon_ms=context.horizon_ms,
                source_feature_table="polymarket_microstructure_features_1s",
                source_feature_row_id=index + 1,
                start_mid=Decimal("0.50"),
                end_mid=(Decimal("0.50") + (directional_bps / Decimal("10000"))).quantize(Decimal("0.00000001")),
                mid_return_bps=directional_bps,
                mid_move_ticks=Decimal("1"),
                best_bid_change=Decimal("0.01"),
                best_ask_change=Decimal("0.01"),
                up_move=directional_bps > 0,
                down_move=directional_bps < 0,
                flat_move=directional_bps == 0,
                completeness_flags_json={"source": "test"},
            )
        )
    await session.flush()


async def _seed_polymarket_execution_fixture(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str,
    decision_at: datetime,
    estimated_probability: str,
    price_at_fire: str,
    expected_value: str | None,
    best_bid: str | None,
    best_ask: str | None,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    tick_size: str = "0.01",
    min_order_size: str = "5",
    fees_enabled: bool = True,
    taker_fee_rate: str = "0.02",
    create_registry: bool = True,
    label_rows: int = 0,
    improved_against_order: bool = False,
    mid_return_bps: str = "15",
):
    market = make_market(
        session,
        platform="polymarket",
        platform_id=f"pm-{condition_id}",
        question=f"Question for {condition_id}",
    )
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes", token_id=asset_id)
    await session.flush()
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        fired_at=decision_at,
        dedupe_bucket=decision_at.replace(
            minute=(decision_at.minute // 15) * 15,
            second=0,
            microsecond=0,
        ),
        estimated_probability=Decimal(estimated_probability),
        price_at_fire=Decimal(price_at_fire),
        expected_value=Decimal(expected_value) if expected_value is not None else None,
        source_platform="polymarket",
        source_token_id=asset_id,
        details={"market_question": market.question, "outcome_name": "Yes"},
    )
    await session.flush()

    if not create_registry:
        await session.commit()
        return {
            "market": market,
            "outcome": outcome,
            "signal": signal,
        }

    market_dim = PolymarketMarketDim(
        gamma_market_id=f"gamma-{condition_id}",
        condition_id=condition_id,
        market_slug=f"market-{condition_id}",
        question=market.question,
        active=True,
        fees_enabled=fees_enabled,
        fee_schedule_json={"rate": taker_fee_rate} if fees_enabled else {},
        taker_base_fee=Decimal(taker_fee_rate),
        maker_base_fee=Decimal("0"),
        source_payload_json={"conditionId": condition_id},
        last_gamma_sync_at=decision_at,
    )
    session.add(market_dim)
    await session.flush()

    asset_dim = PolymarketAssetDim(
        asset_id=asset_id,
        condition_id=condition_id,
        market_dim_id=market_dim.id,
        outcome_id=outcome.id,
        outcome_name=outcome.name,
        outcome_index=0,
        active=True,
        source_payload_json={"asset_id": asset_id},
        last_gamma_sync_at=decision_at,
    )
    session.add(asset_dim)
    await session.flush()

    session.add(
        PolymarketMarketParamHistory(
            market_dim_id=market_dim.id,
            asset_dim_id=asset_dim.id,
            condition_id=condition_id,
            asset_id=asset_id,
            source_kind="gamma_sync",
            effective_at_exchange=decision_at - timedelta(seconds=5),
            observed_at_local=decision_at - timedelta(seconds=5),
            tick_size=Decimal(tick_size),
            min_order_size=Decimal(min_order_size),
            fees_enabled=fees_enabled,
            fee_schedule_json={"rate": taker_fee_rate} if fees_enabled else {},
            maker_base_fee=Decimal("0"),
            taker_base_fee=Decimal(taker_fee_rate),
            fingerprint=f"{condition_id}-{asset_id}-params",
            details_json={"source": "test"},
        )
    )

    best_bid_decimal = Decimal(best_bid) if best_bid is not None else None
    best_ask_decimal = Decimal(best_ask) if best_ask is not None else None
    spread = (
        (best_ask_decimal - best_bid_decimal).quantize(Decimal("0.00000001"))
        if best_bid_decimal is not None and best_ask_decimal is not None
        else None
    )
    snapshot = PolymarketBookSnapshot(
        market_dim_id=market_dim.id,
        asset_dim_id=asset_dim.id,
        condition_id=condition_id,
        asset_id=asset_id,
        source_kind="ws_book",
        event_ts_exchange=decision_at,
        recv_ts_local=decision_at,
        ingest_ts_db=decision_at,
        observed_at_local=decision_at,
        bids_json=[[price, size] for price, size in bids],
        asks_json=[[price, size] for price, size in asks],
        min_order_size=Decimal(min_order_size),
        tick_size=Decimal(tick_size),
        best_bid=best_bid_decimal,
        best_ask=best_ask_decimal,
        spread=spread,
        source_payload_json={"source": "test"},
    )
    session.add(snapshot)
    await session.flush()

    session.add(
        PolymarketBookReconState(
            market_dim_id=market_dim.id,
            asset_dim_id=asset_dim.id,
            condition_id=condition_id,
            asset_id=asset_id,
            status="live",
            last_snapshot_id=snapshot.id,
            last_snapshot_source_kind=snapshot.source_kind,
            last_snapshot_hash="snapshot-test",
            last_snapshot_exchange_ts=decision_at,
            best_bid=best_bid_decimal,
            best_ask=best_ask_decimal,
            spread=spread,
            depth_levels_bid=len(bids),
            depth_levels_ask=len(asks),
            expected_tick_size=Decimal(tick_size),
            last_exchange_ts=decision_at,
            last_received_at_local=decision_at,
            last_reconciled_at=decision_at,
            last_resynced_at=decision_at,
            details_json={"source": "test"},
        )
    )
    await session.flush()

    if label_rows > 0:
        context = _make_context(
            direction="buy_yes",
            asset_id=asset_id,
            condition_id=condition_id,
            decision_at=decision_at,
            estimated_probability=estimated_probability,
            market_price=price_at_fire,
            baseline_target_size="100.00",
            tick_size=tick_size,
            min_order_size=min_order_size,
            best_bid=best_bid,
            best_ask=best_ask,
            bids=bids,
            asks=asks,
        )
        await _seed_label_history(
            session,
            context=context,
            row_count=label_rows,
            improved_against_order=improved_against_order,
            mid_return_bps=mid_return_bps,
        )

    await session.commit()
    return {
        "market": market,
        "outcome": outcome,
        "signal": signal,
        "market_dim": market_dim,
        "asset_dim": asset_dim,
        "snapshot": snapshot,
    }


def test_walk_visible_book_computes_fillable_size_avg_price_and_slippage():
    walk = _walk_visible_book(
        direction="buy_yes",
        levels=_book_levels([("0.52", "100"), ("0.54", "150")]),
        target_size=Decimal("133.00"),
        touch_entry_price=Decimal("0.52"),
    )

    assert walk.fillable_size == Decimal("133.0000")
    assert walk.fillable_shares == Decimal("250.0000")
    assert walk.avg_entry_price == Decimal("0.53200000")
    assert walk.worst_price == Decimal("0.54000000")
    assert walk.slippage_cost == Decimal("3.00000000")
    assert float(walk.slippage_bps) == pytest.approx(230.76923077, rel=1e-6)


@pytest.mark.asyncio
async def test_cross_now_includes_taker_fee_on_fee_enabled_market(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_execution_policy_max_cross_slippage_bps", 500.0)

    context = _make_context(
        estimated_probability="0.80",
        baseline_target_size="200.00",
        min_order_size="1",
        asks=[("0.52", "100"), ("0.54", "150")],
    )
    candidate = await _evaluate_cross_now(session, context)

    assert candidate.valid is True
    assert candidate.est_fillable_size == Decimal("133.0000")
    assert candidate.est_avg_entry_price == Decimal("0.53200000")
    assert candidate.est_taker_fee == Decimal("0.66227616")
    assert candidate.est_net_ev_total is not None
    assert candidate.details_json["taker_fee_rate"] == "0.02"


@pytest.mark.asyncio
async def test_passive_actions_respect_tick_size_min_order_size_and_non_crossing(session):
    misaligned_context = _make_context(best_bid="0.485", best_ask="0.52", min_order_size="1")
    misaligned = await _evaluate_post_best(session, misaligned_context)
    assert misaligned.valid is False
    assert misaligned.invalid_reason == "invalid_target_price"

    large_min_size_context = _make_context(best_bid="0.48", best_ask="0.52", min_order_size="2000")
    large_min_size = await _evaluate_post_best(session, large_min_size_context)
    assert large_min_size.valid is False
    assert large_min_size.invalid_reason == "below_min_order_size"

    tight_spread_context = _make_context(best_bid="0.49", best_ask="0.50", min_order_size="1")
    step_ahead = await _evaluate_step_ahead(session, tight_spread_context)
    assert step_ahead.valid is False
    assert step_ahead.invalid_reason == "step_ahead_would_cross"


@pytest.mark.asyncio
async def test_sparse_passive_label_coverage_invalidates_passive_actions(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_execution_policy_passive_min_label_rows", 3)

    context = _make_context(asset_id="token-phase6-sparse", condition_id="cond-phase6-sparse", min_order_size="1")
    await _seed_label_history(session, context=context, row_count=2)
    candidate = await _evaluate_post_best(session, context)

    assert candidate.valid is False
    assert candidate.invalid_reason == "passive_labels_insufficient"


@pytest.mark.asyncio
async def test_policy_chooses_skip_when_all_executable_actions_are_invalid(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_execution_policy_enabled", True)

    decision_at = datetime(2026, 4, 14, 11, 0, 0, tzinfo=timezone.utc)
    seeded = await _seed_polymarket_execution_fixture(
        session,
        condition_id="cond-phase6-skip",
        asset_id="token-phase6-skip",
        decision_at=decision_at,
        estimated_probability="0.49",
        price_at_fire="0.40",
        expected_value="0.09",
        best_bid="0.48",
        best_ask="0.52",
        bids=[("0.48", "200"), ("0.47", "200")],
        asks=[("0.52", "100"), ("0.54", "150")],
        label_rows=0,
    )

    result = await evaluate_polymarket_execution_policy(
        session,
        signal_id=seeded["signal"].id,
        outcome_id=seeded["outcome"].id,
        market_id=seeded["market"].id,
        direction="buy_yes",
        estimated_probability=Decimal("0.49"),
        market_price=Decimal("0.40"),
        decision_at=decision_at,
        baseline_target_size=Decimal("100.00"),
        bankroll=Decimal("10000"),
    )

    assert result.applicable is True
    assert result.chosen_candidate is not None
    assert result.chosen_candidate.action_type == "skip"
    assert result.chosen_reason == "all_non_skip_invalid"


@pytest.mark.asyncio
async def test_attempt_open_trade_persists_chosen_action_and_action_candidates(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_execution_policy_enabled", True)

    fired_at = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
    seeded = await _seed_polymarket_execution_fixture(
        session,
        condition_id="cond-phase6-open",
        asset_id="token-phase6-open",
        decision_at=fired_at,
        estimated_probability="0.65",
        price_at_fire="0.40",
        expected_value="0.25",
        best_bid="0.40",
        best_ask="0.41",
        bids=[("0.40", "500"), ("0.39", "500")],
        asks=[("0.41", "300"), ("0.42", "300")],
        label_rows=0,
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)

    result = await attempt_open_trade(
        session=session,
        signal_id=seeded["signal"].id,
        outcome_id=seeded["outcome"].id,
        market_id=seeded["market"].id,
        estimated_probability=Decimal("0.65"),
        market_price=Decimal("0.40"),
        market_question=seeded["market"].question,
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is not None
    assert result.execution_decision is not None
    assert result.execution_decision.chosen_action_type == "cross_now"
    assert result.execution_decision.chosen_policy_version == POLICY_VERSION
    assert result.execution_decision.chosen_action_candidate_id is not None
    assert result.trade.details["chosen_action_type"] == "cross_now"
    assert result.trade.details["shadow_execution"]["fill_status"] in {"full_fill", "partial_fill"}
    candidate_count = await session.scalar(
        select(func.count())
        .select_from(PolymarketExecutionActionCandidate)
        .where(PolymarketExecutionActionCandidate.execution_decision_id == result.execution_decision.id)
    )
    assert candidate_count == 4


@pytest.mark.asyncio
async def test_attempt_open_trade_falls_back_when_policy_not_applicable(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_execution_policy_enabled", True)

    fired_at = datetime(2026, 4, 14, 12, 30, 0, tzinfo=timezone.utc)
    seeded = await _seed_polymarket_execution_fixture(
        session,
        condition_id="cond-phase6-fallback",
        asset_id="token-phase6-fallback",
        decision_at=fired_at,
        estimated_probability="0.65",
        price_at_fire="0.40",
        expected_value="0.25",
        best_bid="0.40",
        best_ask="0.41",
        bids=[("0.40", "500"), ("0.39", "500")],
        asks=[("0.41", "300"), ("0.42", "300")],
        create_registry=False,
    )
    make_orderbook_snapshot(
        session,
        seeded["outcome"].id,
        spread="0.0200",
        depth_bid="900",
        depth_ask="600",
        captured_at=fired_at,
        bids=[["0.39", "500"], ["0.38", "400"]],
        asks=[["0.41", "300"], ["0.42", "300"]],
    )
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
    await session.commit()

    result = await attempt_open_trade(
        session=session,
        signal_id=seeded["signal"].id,
        outcome_id=seeded["outcome"].id,
        market_id=seeded["market"].id,
        estimated_probability=Decimal("0.65"),
        market_price=Decimal("0.40"),
        market_question=seeded["market"].question,
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )
    await session.commit()

    assert result.trade is not None
    assert result.execution_decision is not None
    assert result.execution_decision.chosen_action_type is None
    assert result.trade.entry_price == Decimal("0.410000")
    candidate_count = await session.scalar(
        select(func.count())
        .select_from(PolymarketExecutionActionCandidate)
        .where(PolymarketExecutionActionCandidate.execution_decision_id == result.execution_decision.id)
    )
    assert candidate_count == 0


@pytest.mark.asyncio
async def test_execution_policy_api_and_health_surfaces(client, engine, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_execution_policy_enabled", True)

    session_factory = _session_factory(engine)
    fired_at = datetime(2026, 4, 14, 13, 0, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        seeded = await _seed_polymarket_execution_fixture(
            session,
            condition_id="cond-phase6-api",
            asset_id="token-phase6-api",
            decision_at=fired_at,
            estimated_probability="0.65",
            price_at_fire="0.40",
            expected_value="0.25",
            best_bid="0.40",
            best_ask="0.41",
            bids=[("0.40", "500"), ("0.39", "500")],
            asks=[("0.41", "300"), ("0.42", "300")],
            label_rows=0,
        )
        strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=fired_at)
        result = await attempt_open_trade(
            session=session,
            signal_id=seeded["signal"].id,
            outcome_id=seeded["outcome"].id,
            market_id=seeded["market"].id,
            estimated_probability=Decimal("0.65"),
            market_price=Decimal("0.40"),
            market_question=seeded["market"].question,
            fired_at=fired_at,
            strategy_run_id=strategy_run.id,
        )
        await session.commit()
        signal_id = str(seeded["signal"].id)
        asset_id = seeded["asset_dim"].asset_id
        condition_id = seeded["asset_dim"].condition_id
        assert result.execution_decision is not None

    status_response = await client.get("/api/v1/ingest/polymarket/execution-policy/status")
    assert status_response.status_code == 200
    assert status_response.json()["enabled"] is True
    assert status_response.json()["recent_decisions_24h"] >= 1

    candidates_response = await client.get(
        f"/api/v1/ingest/polymarket/execution-policy/action-candidates?asset_id={asset_id}&limit=10"
    )
    assert candidates_response.status_code == 200
    assert len(candidates_response.json()["rows"]) == 4

    decisions_response = await client.get(
        f"/api/v1/ingest/polymarket/execution-policy/decisions?signal_id={signal_id}&limit=5"
    )
    assert decisions_response.status_code == 200
    assert decisions_response.json()["rows"][0]["chosen_action_type"] == "cross_now"

    mix_response = await client.get(
        f"/api/v1/ingest/polymarket/execution-policy/action-mix?condition_id={condition_id}"
    )
    assert mix_response.status_code == 200
    mix_by_action = {row["action_type"]: row["count"] for row in mix_response.json()["rows"]}
    assert mix_by_action["cross_now"] >= 1

    dry_run_response = await client.post(
        "/api/v1/ingest/polymarket/execution-policy/dry-run",
        json={"signal_id": signal_id},
    )
    assert dry_run_response.status_code == 200
    assert dry_run_response.json()["applicable"] is True
    assert dry_run_response.json()["chosen_candidate"]["action_type"] == "cross_now"

    health_response = await client.get("/api/v1/health")
    assert health_response.status_code == 200
    phase6 = health_response.json()["polymarket_phase6"]
    assert phase6["enabled"] is True
    assert phase6["recent_action_mix"]["cross_now"] >= 1
    assert phase6["recent_decisions_24h"] >= 1

