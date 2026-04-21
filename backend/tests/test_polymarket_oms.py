from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.polymarket_control_plane import arm_pilot, create_or_update_pilot_config
from app.execution.polymarket_gateway import (
    GatewayCancelResult,
    GatewayOrderRequest,
    GatewaySubmitResult,
)
from app.execution.polymarket_live_reconciler import PolymarketLiveReconciler
from app.execution.polymarket_live_state import (
    fetch_live_state_row,
    mark_reconcile_finished,
    set_gateway_status,
    set_kill_switch,
    set_user_stream_connection_state,
)
from app.execution.polymarket_order_manager import PolymarketOrderManager
from app.execution.polymarket_user_stream import PolymarketUserStreamService
from app.ingestion.polymarket_common import utcnow
from app.models.execution_decision import ExecutionDecision
from app.models.polymarket_execution_policy import PolymarketExecutionActionCandidate
from app.models.polymarket_live_execution import (
    CapitalReservation,
    LiveFill,
    LiveOrder,
    LiveOrderEvent,
    PolymarketUserEventRaw,
)
from app.models.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketEventDim,
    PolymarketMarketDim,
    PolymarketMarketParamHistory,
)
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.models.strategy_run import StrategyRun
from app.paper_trading.engine import attempt_open_trade
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_signal


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class FakeGateway:
    def __init__(
        self,
        *,
        venue_order_id: str = "venue-order-1",
        submit_status: str = "live",
        cancel_status: str = "canceled",
        order_snapshots: dict[str, dict] | None = None,
        trade_rows: list[dict] | None = None,
    ) -> None:
        self.venue_order_id = venue_order_id
        self.submit_status = submit_status
        self.cancel_status = cancel_status
        self.order_snapshots = order_snapshots or {}
        self.trade_rows = trade_rows or []
        self.submit_calls: list[GatewayOrderRequest] = []
        self.cancel_calls: list[str] = []
        self.healthcheck_calls = 0
        self.has_submit_credentials = True

    async def healthcheck(self) -> dict:
        self.healthcheck_calls += 1
        return {"reachable": True}

    async def submit_order(self, request: GatewayOrderRequest) -> GatewaySubmitResult:
        self.submit_calls.append(request)
        return GatewaySubmitResult(
            venue_order_id=self.venue_order_id,
            venue_status=self.submit_status,
            payload={
                "order_id": self.venue_order_id,
                "status": self.submit_status,
                "size": str(request.size),
                "asset_id": request.asset_id,
            },
            submitted_size=request.size,
            submitted_at=utcnow(),
        )

    async def cancel_order(self, venue_order_id: str) -> GatewayCancelResult:
        self.cancel_calls.append(venue_order_id)
        return GatewayCancelResult(
            venue_order_id=venue_order_id,
            venue_status=self.cancel_status,
            payload={"order_id": venue_order_id, "status": self.cancel_status},
        )

    async def fetch_order_status(self, venue_order_id: str) -> dict:
        return self.order_snapshots.get(
            venue_order_id,
            {"order_id": venue_order_id, "status": "live"},
        )

    async def fetch_user_orders(self, *, limit: int = 50) -> list[dict]:
        return []

    async def fetch_user_trades(self, *, limit: int = 100, asset_id: str | None = None) -> list[dict]:
        if asset_id is None:
            return list(self.trade_rows[:limit])
        return [row for row in self.trade_rows if row.get("asset_id") == asset_id][:limit]

    def user_stream_subscription_payload(self, markets: list[str]) -> dict:
        return {"type": "user", "markets": markets}


async def _seed_execution_fixture(
    session: AsyncSession,
    *,
    condition_id: str,
    action_type: str = "cross_now",
    candidate_side: str = "buy_yes",
    target_price: str = "0.55",
    target_size: str = "110.00",
    tick_size: str = "0.01",
    min_order_size: str = "1",
    category: str = "politics",
    decision_at: datetime | None = None,
    stale_orderbook_context: bool = False,
    missing_orderbook_context: bool = False,
    recon_status: str = "live",
) -> dict[str, object]:
    anchor = decision_at or utcnow()
    price = Decimal(target_price)
    notional = Decimal(target_size)
    tick = Decimal(tick_size)
    minimum = Decimal(min_order_size)
    yes_best_bid = max(price - Decimal("0.01"), Decimal("0.01"))
    yes_best_ask = min(price + Decimal("0.01"), Decimal("0.99"))
    no_best_bid = max(Decimal("1") - yes_best_ask, Decimal("0.01"))
    no_best_ask = min(Decimal("1") - yes_best_bid, Decimal("0.99"))

    market = make_market(
        session,
        platform="polymarket",
        platform_id=f"pm-{condition_id}",
        question=f"Question for {condition_id}",
    )
    await session.flush()

    yes_outcome = make_outcome(
        session,
        market.id,
        name=f"Yes-{condition_id}",
        token_id=f"asset-yes-{condition_id}",
    )
    no_outcome = make_outcome(
        session,
        market.id,
        name=f"No-{condition_id}",
        token_id=f"asset-no-{condition_id}",
    )
    await session.flush()

    signal = make_signal(
        session,
        market.id,
        yes_outcome.id,
        signal_type=f"phase7a-{condition_id}",
        fired_at=anchor,
        dedupe_bucket=anchor,
        estimated_probability=Decimal("0.62"),
        price_at_fire=price,
        expected_value=Decimal("0.15"),
        source_platform="polymarket",
        source_token_id=f"asset-yes-{condition_id}",
        details={"market_question": market.question, "outcome_name": yes_outcome.name},
    )

    strategy_run = StrategyRun(
        strategy_name=f"phase7a-{condition_id}",
        status="active",
        started_at=anchor - timedelta(minutes=5),
        contract_snapshot={"bootstrap_source": "test"},
    )
    session.add(strategy_run)

    event_dim = PolymarketEventDim(
        gamma_event_id=f"event-{condition_id}",
        title=f"Event {condition_id}",
        category=category,
        active=True,
        source_payload_json={"category": category},
        last_gamma_sync_at=anchor,
    )
    session.add(event_dim)
    await session.flush()

    market_dim = PolymarketMarketDim(
        gamma_market_id=f"gamma-{condition_id}",
        condition_id=condition_id,
        market_slug=f"market-{condition_id}",
        question=market.question,
        event_dim_id=event_dim.id,
        active=True,
        accepting_orders=True,
        fees_enabled=True,
        maker_base_fee=Decimal("0"),
        taker_base_fee=Decimal("0.02"),
        clob_token_ids_json=[f"asset-yes-{condition_id}", f"asset-no-{condition_id}"],
        outcomes_json=[yes_outcome.name, no_outcome.name],
        source_payload_json={"conditionId": condition_id},
        last_gamma_sync_at=anchor,
    )
    session.add(market_dim)
    await session.flush()

    yes_asset = PolymarketAssetDim(
        asset_id=f"asset-yes-{condition_id}",
        condition_id=condition_id,
        market_dim_id=market_dim.id,
        outcome_id=yes_outcome.id,
        outcome_name="Yes",
        outcome_index=0,
        active=True,
        source_payload_json={"asset_id": f"asset-yes-{condition_id}"},
        last_gamma_sync_at=anchor,
    )
    no_asset = PolymarketAssetDim(
        asset_id=f"asset-no-{condition_id}",
        condition_id=condition_id,
        market_dim_id=market_dim.id,
        outcome_id=no_outcome.id,
        outcome_name="No",
        outcome_index=1,
        active=True,
        source_payload_json={"asset_id": f"asset-no-{condition_id}"},
        last_gamma_sync_at=anchor,
    )
    session.add_all([yes_asset, no_asset])
    await session.flush()

    session.add_all(
        [
            PolymarketMarketParamHistory(
                market_dim_id=market_dim.id,
                asset_dim_id=yes_asset.id,
                condition_id=condition_id,
                asset_id=yes_asset.asset_id,
                source_kind="gamma_sync",
                effective_at_exchange=anchor - timedelta(seconds=5),
                observed_at_local=anchor - timedelta(seconds=5),
                tick_size=tick,
                min_order_size=minimum,
                fees_enabled=True,
                fee_schedule_json={"rate": "0.02"},
                maker_base_fee=Decimal("0"),
                taker_base_fee=Decimal("0.02"),
                fingerprint=f"{condition_id}-yes-params",
                details_json={"source": "test"},
            ),
            PolymarketMarketParamHistory(
                market_dim_id=market_dim.id,
                asset_dim_id=no_asset.id,
                condition_id=condition_id,
                asset_id=no_asset.asset_id,
                source_kind="gamma_sync",
                effective_at_exchange=anchor - timedelta(seconds=5),
                observed_at_local=anchor - timedelta(seconds=5),
                tick_size=tick,
                min_order_size=minimum,
                fees_enabled=True,
                fee_schedule_json={"rate": "0.02"},
                maker_base_fee=Decimal("0"),
                taker_base_fee=Decimal("0.02"),
                fingerprint=f"{condition_id}-no-params",
                details_json={"source": "test"},
            ),
        ]
    )

    session.add_all(
        [
            PolymarketBookReconState(
                market_dim_id=market_dim.id,
                asset_dim_id=yes_asset.id,
                condition_id=condition_id,
                asset_id=yes_asset.asset_id,
                status=recon_status,
                best_bid=yes_best_bid,
                best_ask=yes_best_ask,
                spread=yes_best_ask - yes_best_bid,
                expected_tick_size=tick,
                last_exchange_ts=anchor,
                last_received_at_local=anchor,
                last_reconciled_at=anchor,
                last_resynced_at=anchor,
                details_json={"source": "test"},
            ),
            PolymarketBookReconState(
                market_dim_id=market_dim.id,
                asset_dim_id=no_asset.id,
                condition_id=condition_id,
                asset_id=no_asset.asset_id,
                status=recon_status,
                best_bid=no_best_bid,
                best_ask=no_best_ask,
                spread=no_best_ask - no_best_bid,
                expected_tick_size=tick,
                last_exchange_ts=anchor,
                last_received_at_local=anchor,
                last_reconciled_at=anchor,
                last_resynced_at=anchor,
                details_json={"source": "test"},
            ),
        ]
    )
    await session.flush()

    order_type_hint = "post_only" if action_type in {"post_best", "step_ahead"} else "limit"
    decision = ExecutionDecision(
        signal_id=signal.id,
        strategy_run_id=strategy_run.id,
        decision_at=anchor,
        decision_status="opened",
        action="cross",
        direction=candidate_side,
        ideal_entry_price=price,
        executable_entry_price=price,
        requested_size_usd=notional,
        fillable_size_usd=notional,
        fill_probability=Decimal("0.800000"),
        net_ev_per_share=Decimal("0.05000000"),
        net_expected_pnl_usd=Decimal("10.00000000"),
        missing_orderbook_context=missing_orderbook_context,
        stale_orderbook_context=stale_orderbook_context,
        liquidity_constrained=False,
        fill_status="full_fill",
        reason_code="opened",
        chosen_action_type=action_type,
        chosen_order_type_hint=order_type_hint,
        chosen_target_price=price,
        chosen_target_size=notional,
        chosen_est_fillable_size=notional,
        chosen_est_fill_probability=Decimal("0.800000"),
        chosen_est_net_ev_bps=Decimal("25.00000000"),
        chosen_est_net_ev_total=Decimal("10.00000000"),
        chosen_est_fee=Decimal("0.50000000"),
        chosen_est_slippage=Decimal("0.25000000"),
        chosen_policy_version="phase7a-test",
        decision_reason_json={"source": "test"},
        details={"source": "test"},
    )
    session.add(decision)
    await session.flush()

    candidate = PolymarketExecutionActionCandidate(
        signal_id=signal.id,
        execution_decision_id=decision.id,
        market_dim_id=market_dim.id,
        asset_dim_id=yes_asset.id,
        condition_id=condition_id,
        asset_id=yes_asset.asset_id,
        outcome_id=yes_outcome.id,
        side=candidate_side,
        action_type=action_type,
        order_type_hint=order_type_hint,
        decision_horizon_ms=1000,
        target_size=notional,
        est_fillable_size=notional,
        est_fill_probability=Decimal("0.800000"),
        est_avg_entry_price=price,
        est_worst_price=price,
        est_tick_size=tick,
        est_min_order_size=minimum,
        est_taker_fee=Decimal("0.50000000"),
        est_maker_fee=Decimal("0"),
        est_slippage_cost=Decimal("0.25000000"),
        est_alpha_capture_bps=Decimal("30.00000000"),
        est_adverse_selection_bps=Decimal("5.00000000"),
        est_net_ev_bps=Decimal("25.00000000"),
        est_net_ev_total=Decimal("10.00000000"),
        valid=True,
        invalid_reason=None,
        policy_version="phase7a-test",
        decided_at=anchor,
        source_label_summary_json={"source": "test"},
        details_json={"source": "test"},
    )
    session.add(candidate)
    await session.flush()

    decision.chosen_action_candidate_id = candidate.id
    await session.flush()

    return {
        "market": market,
        "signal": signal,
        "strategy_run": strategy_run,
        "event_dim": event_dim,
        "market_dim": market_dim,
        "yes_asset": yes_asset,
        "no_asset": no_asset,
        "decision": decision,
        "candidate": candidate,
    }


async def _latest_reservation_for_order(session: AsyncSession, *, live_order_id) -> CapitalReservation | None:
    return (
        await session.execute(
            select(CapitalReservation)
            .where(CapitalReservation.live_order_id == live_order_id)
            .order_by(CapitalReservation.observed_at_local.desc(), CapitalReservation.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


async def _arm_exec_pilot(
    session: AsyncSession,
    *,
    live_enabled: bool = True,
    manual_approval_required: bool = False,
) -> dict[str, object]:
    config = await create_or_update_pilot_config(
        session,
        payload={
            "pilot_name": f"exec-pilot-{uuid.uuid4()}",
            "strategy_family": "exec_policy",
            "active": True,
            "live_enabled": live_enabled,
            "manual_approval_required": manual_approval_required,
            "max_open_orders": 5,
            "max_notional_per_day_usd": 500.0,
            "max_notional_per_order_usd": 250.0,
        },
    )
    return await arm_pilot(session, pilot_config_id=config["id"], operator_identity="test-operator")


@pytest.mark.asyncio
async def test_order_intent_creation_is_idempotent_and_resolves_buy_no(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", True)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_allowlist_markets", "")
    monkeypatch.setattr(settings, "polymarket_allowlist_categories", "")
    monkeypatch.setattr(settings, "polymarket_max_outstanding_notional_usd", 0.0)

    seeded = await _seed_execution_fixture(
        session,
        condition_id="cond-phase7a-idempotent",
        candidate_side="buy_no",
    )
    manager = PolymarketOrderManager(gateway=FakeGateway())

    first = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    second = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)

    live_orders = (await session.execute(select(LiveOrder))).scalars().all()
    events = (await session.execute(select(LiveOrderEvent))).scalars().all()

    assert first["id"] == second["id"]
    assert first["asset_id"] == seeded["no_asset"].asset_id
    assert first["condition_id"] == seeded["no_asset"].condition_id
    assert first["status"] == "approval_pending"
    assert len(live_orders) == 1
    assert len(events) == 2


@pytest.mark.asyncio
async def test_dry_run_submit_creates_auditable_events_without_gateway_submit(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", True)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase7a-dryrun")
    fake_gateway = FakeGateway()
    manager = PolymarketOrderManager(gateway=fake_gateway)

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    approved = await manager.approve_order(session, live_order_id=uuid.UUID(intent["id"]), approved_by="operator")
    submitted = await manager.submit_order(session, live_order_id=uuid.UUID(approved["id"]), operator="operator")

    event_types = (
        await session.execute(
            select(LiveOrderEvent.event_type).where(LiveOrderEvent.live_order_id == uuid.UUID(intent["id"]))
        )
    ).scalars().all()
    reservation = await _latest_reservation_for_order(session, live_order_id=uuid.UUID(intent["id"]))

    assert fake_gateway.submit_calls == []
    assert submitted["status"] == "submitted"
    assert submitted["submitted_at"] is not None
    assert reservation is not None
    assert reservation.status == "active"
    assert "dry_run_submit_simulated" in event_types


@pytest.mark.asyncio
async def test_live_submit_is_blocked_by_kill_switch(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase7a-killswitch")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    fake_gateway = FakeGateway()
    manager = PolymarketOrderManager(gateway=fake_gateway)

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    await set_kill_switch(session, enabled=True)
    blocked = await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    assert blocked["status"] == "submit_blocked"
    assert blocked["kill_switch_blocked"] is True
    assert fake_gateway.submit_calls == []


@pytest.mark.asyncio
async def test_allowlist_blocks_out_of_scope_markets(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", True)
    monkeypatch.setattr(settings, "polymarket_allowlist_markets", "cond-some-other-market")
    monkeypatch.setattr(settings, "polymarket_allowlist_categories", "")

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase7a-allowlist")
    manager = PolymarketOrderManager(gateway=FakeGateway())

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)

    assert intent["status"] == "submit_blocked"
    assert intent["allowlist_blocked"] is True


@pytest.mark.asyncio
async def test_tick_min_size_and_stale_book_validation_block_invalid_intents(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", True)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_live_decision_max_age_seconds", 300)

    misaligned = await _seed_execution_fixture(
        session,
        condition_id="cond-phase7a-bad-tick",
        target_price="0.555",
        tick_size="0.01",
    )
    too_small = await _seed_execution_fixture(
        session,
        condition_id="cond-phase7a-small",
        target_price="0.55",
        target_size="0.20",
        min_order_size="1",
    )
    stale = await _seed_execution_fixture(
        session,
        condition_id="cond-phase7a-stale",
        decision_at=utcnow() - timedelta(hours=2),
    )

    manager = PolymarketOrderManager(gateway=FakeGateway())
    misaligned_intent = await manager.create_order_intent(session, execution_decision_id=misaligned["decision"].id)
    too_small_intent = await manager.create_order_intent(session, execution_decision_id=too_small["decision"].id)
    stale_intent = await manager.create_order_intent(session, execution_decision_id=stale["decision"].id)

    assert misaligned_intent["status"] == "validation_failed"
    assert "tick_size_violation" in (misaligned_intent["validation_error"] or "")
    assert too_small_intent["status"] == "validation_failed"
    assert "below_min_order_size" in (too_small_intent["validation_error"] or "")
    assert stale_intent["status"] == "validation_failed"
    assert "stale_execution_decision" in (stale_intent["validation_error"] or "")


@pytest.mark.asyncio
async def test_reservations_block_oversubscription_and_release_on_cancel_and_fill(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_max_outstanding_notional_usd", 100.0)

    manager = PolymarketOrderManager(gateway=FakeGateway())
    first = await _seed_execution_fixture(
        session,
        condition_id="cond-phase7a-reserve-a",
        target_price="0.50",
        target_size="80.00",
    )
    second = await _seed_execution_fixture(
        session,
        condition_id="cond-phase7a-reserve-b",
        target_price="0.50",
        target_size="40.00",
    )
    third = await _seed_execution_fixture(
        session,
        condition_id="cond-phase7a-reserve-c",
        target_price="0.50",
        target_size="40.00",
    )

    first_intent = await manager.create_order_intent(session, execution_decision_id=first["decision"].id)
    second_intent = await manager.create_order_intent(session, execution_decision_id=second["decision"].id)
    canceled = await manager.cancel_order(session, live_order_id=uuid.UUID(first_intent["id"]), operator="operator")
    third_intent = await manager.create_order_intent(session, execution_decision_id=third["decision"].id)
    await manager.submit_order(session, live_order_id=uuid.UUID(third_intent["id"]), operator="operator")

    reconciler = PolymarketLiveReconciler(gateway=FakeGateway())
    fill_payload = {
        "event_type": "trade",
        "trade_id": "trade-phase7a-reserve-c",
        "market": third["market_dim"].condition_id,
        "asset_id": third["yes_asset"].asset_id,
        "price": "0.50",
        "size": "80",
        "side": "BUY",
        "status": "matched",
        "timestamp": "2026-04-14T12:05:00Z",
    }
    await reconciler.process_user_event_payload(
        session,
        payload=fill_payload,
        raw_user_event_id=None,
        source_kind="user_ws",
    )

    third_order = await session.get(LiveOrder, uuid.UUID(third_intent["id"]))
    third_reservation = await _latest_reservation_for_order(session, live_order_id=uuid.UUID(third_intent["id"]))

    assert second_intent["status"] == "submit_blocked"
    assert "max_outstanding_notional_exceeded" in (second_intent["validation_error"] or "")
    assert canceled["status"] == "canceled"
    assert third_order is not None
    assert third_order.filled_size == Decimal("80")
    assert third_reservation is not None
    assert third_reservation.strategy_family == "exec_policy"
    assert third_reservation.strategy_version_id is not None
    assert third_reservation.regime_label is not None
    assert isinstance(third_reservation.budget_metadata_json, dict)
    assert third_reservation.status == "released"
    assert third_reservation.open_amount == Decimal("0")


@pytest.mark.asyncio
async def test_user_stream_appends_raw_events_and_normalizes_updates(session, engine, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase7a-user-stream")
    manager = PolymarketOrderManager(gateway=FakeGateway())
    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    stream = PolymarketUserStreamService(
        _session_factory(engine),
        gateway=FakeGateway(),
        reconciler=PolymarketLiveReconciler(gateway=FakeGateway()),
    )
    payload = [
        {
            "event_type": "order",
            "type": "UPDATE",
            "order_id": "venue-user-stream-1",
            "market": seeded["market_dim"].condition_id,
            "asset_id": seeded["yes_asset"].asset_id,
            "original_size": "200",
            "size_matched": "50",
            "status": "live",
            "timestamp": "2026-04-14T12:00:10Z",
        },
        {
            "event_type": "trade",
            "trade_id": "trade-user-stream-1",
            "taker_order_id": "venue-user-stream-1",
            "market": seeded["market_dim"].condition_id,
            "asset_id": seeded["yes_asset"].asset_id,
            "price": "0.55",
            "size": "50",
            "side": "BUY",
            "status": "matched",
            "timestamp": "2026-04-14T12:00:12Z",
        },
    ]

    result = await stream.consume_message(session, payload=payload, stream_session_id="user-session-1")

    raw_rows = (await session.execute(select(PolymarketUserEventRaw))).scalars().all()
    order = await session.get(LiveOrder, uuid.UUID(intent["id"]))
    user_events = (
        await session.execute(
            select(LiveOrderEvent).where(LiveOrderEvent.source_kind == "user_ws")
        )
    ).scalars().all()
    fills = (await session.execute(select(LiveFill))).scalars().all()

    assert result["fills_created"] == 1
    assert len(raw_rows) == 2
    assert order is not None
    assert order.venue_order_id == "venue-user-stream-1"
    assert order.status == "partially_filled"
    assert order.filled_size == Decimal("50")
    assert len(user_events) == 2
    assert len(fills) == 1


@pytest.mark.asyncio
async def test_reconcile_recovers_backlog_and_dedupes_rest_trade_repair(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)

    trade_payload = {
        "event_type": "trade",
        "trade_id": "trade-phase7a-reconcile",
        "taker_order_id": "venue-reconcile-1",
        "market": "cond-phase7a-reconcile",
        "asset_id": "asset-yes-cond-phase7a-reconcile",
        "price": "0.55",
        "size": "50",
        "side": "BUY",
        "status": "matched",
        "timestamp": "2026-04-14T12:10:00Z",
    }
    fake_gateway = FakeGateway(
        venue_order_id="venue-reconcile-1",
        order_snapshots={
            "venue-reconcile-1": {
                "order_id": "venue-reconcile-1",
                "status": "live",
                "size": "200",
                "size_matched": "50",
            }
        },
        trade_rows=[trade_payload],
    )
    manager = PolymarketOrderManager(gateway=fake_gateway)
    seeded = await _seed_execution_fixture(session, condition_id="cond-phase7a-reconcile")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")
    raw_row = PolymarketUserEventRaw(
        stream_session_id="user-session-repair",
        condition_id=seeded["market_dim"].condition_id,
        asset_id=seeded["yes_asset"].asset_id,
        event_type="trade",
        recv_ts_local=utcnow(),
        source_payload_json=trade_payload,
    )
    session.add(raw_row)
    await session.flush()

    reconciler = PolymarketLiveReconciler(gateway=fake_gateway)
    result = await reconciler.reconcile_once(session, reason="test")

    order = await session.get(LiveOrder, uuid.UUID(intent["id"]))
    fills = (await session.execute(select(LiveFill))).scalars().all()
    state = await fetch_live_state_row(session)

    assert result["processed_fill_count"] == 1
    assert len(fills) == 1
    assert order is not None
    assert order.status == "partially_filled"
    assert order.filled_size == Decimal("50")
    assert state is not None
    assert state.last_reconciled_user_event_id == raw_row.id


@pytest.mark.asyncio
async def test_cancel_flow_is_idempotent(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)

    fake_gateway = FakeGateway(venue_order_id="venue-cancel-1")
    manager = PolymarketOrderManager(gateway=fake_gateway)
    seeded = await _seed_execution_fixture(session, condition_id="cond-phase7a-cancel")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")
    first_cancel = await manager.cancel_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")
    second_cancel = await manager.cancel_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    cancel_events = (
        await session.execute(
            select(LiveOrderEvent)
            .where(LiveOrderEvent.live_order_id == uuid.UUID(intent["id"]))
            .where(LiveOrderEvent.event_type == "cancel_ack")
        )
    ).scalars().all()

    assert first_cancel["status"] == "canceled"
    assert second_cancel["status"] == "canceled"
    assert fake_gateway.cancel_calls == ["venue-cancel-1"]
    assert len(cancel_events) == 1


@pytest.mark.asyncio
async def test_paper_trading_still_works_when_live_trading_disabled(session, monkeypatch):
    monkeypatch.setattr(settings, "paper_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_execution_policy_enabled", False)

    fired_at = datetime(2026, 4, 14, 13, 0, 0, tzinfo=timezone.utc)
    market = make_market(
        session,
        platform="polymarket",
        platform_id="pm-paper-phase7a",
        question="Will paper trading still work?",
    )
    await session.flush()
    outcome = make_outcome(
        session,
        market.id,
        name="Yes",
        token_id="asset-paper-phase7a",
    )
    await session.flush()
    signal = make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="paper-phase7a",
        fired_at=fired_at,
        dedupe_bucket=fired_at,
        estimated_probability=Decimal("0.65"),
        price_at_fire=Decimal("0.40"),
        expected_value=Decimal("0.20"),
        source_platform="polymarket",
        source_token_id="asset-paper-phase7a",
        details={"market_question": market.question, "outcome_name": "Yes"},
    )
    make_orderbook_snapshot(
        session,
        outcome.id,
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
        signal_id=signal.id,
        outcome_id=outcome.id,
        market_id=market.id,
        estimated_probability=Decimal("0.65"),
        market_price=Decimal("0.40"),
        market_question=market.question,
        fired_at=fired_at,
        strategy_run_id=strategy_run.id,
    )

    live_order_count = await session.scalar(select(func.count()).select_from(LiveOrder))

    assert result.trade is not None
    assert result.execution_decision is not None
    assert live_order_count == 0


@pytest.mark.asyncio
async def test_health_serialization_includes_phase7a_status(client, engine, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", False)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", True)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", True)
    monkeypatch.setattr(settings, "polymarket_user_stream_enabled", False)
    monkeypatch.setattr(settings, "polymarket_allowlist_markets", "")
    monkeypatch.setattr(settings, "polymarket_allowlist_categories", "")

    session_factory = _session_factory(engine)
    async with session_factory() as session:
        seeded = await _seed_execution_fixture(session, condition_id="cond-phase7a-health")
        manager = PolymarketOrderManager(gateway=FakeGateway())
        intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
        await set_gateway_status(session, reachable=True, error=None)
        await set_user_stream_connection_state(
            session,
            connected=True,
            session_id="health-user-session",
            started_at=utcnow(),
        )
        await mark_reconcile_finished(session, success=True, last_user_event_id=7)
        session.add(
            LiveFill(
                live_order_id=uuid.UUID(intent["id"]),
                condition_id=seeded["market_dim"].condition_id,
                asset_id=seeded["yes_asset"].asset_id,
                trade_id="trade-health-1",
                fill_status="matched",
                side="BUY",
                price=Decimal("0.55"),
                size=Decimal("10"),
                observed_at_local=utcnow(),
                fingerprint="fill-health-1",
                details_json={"source": "test"},
            )
        )
        await session.commit()

    response = await client.get("/api/v1/health")
    assert response.status_code == 200
    phase7a = response.json()["polymarket_phase7a"]
    assert phase7a["enabled"] is False
    assert phase7a["dry_run"] is True
    assert phase7a["manual_approval_required"] is True
    assert phase7a["gateway_reachable"] is True
    assert phase7a["user_stream_connected"] is True
    assert phase7a["outstanding_live_orders"] >= 1
    assert phase7a["outstanding_reservations"] > 0
    assert phase7a["recent_fills_24h"] == 1
