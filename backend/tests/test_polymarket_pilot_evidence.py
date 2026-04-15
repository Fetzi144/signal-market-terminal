from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.polymarket_control_plane import expire_stale_approvals
from app.execution.polymarket_order_manager import PolymarketOrderManager
from app.execution.polymarket_pilot_evidence import (
    PolymarketPilotEvidenceService,
    list_pilot_guardrail_events,
)
from app.ingestion.polymarket_common import utcnow
from app.models.polymarket_live_execution import LiveFill, LiveOrder, PositionLot, PositionLotEvent
from app.models.polymarket_pilot import (
    PolymarketControlPlaneIncident,
    PolymarketLiveShadowEvaluation,
    PolymarketPilotApprovalEvent,
)
from tests.test_polymarket_control_plane import _arm_exec_pilot
from tests.test_polymarket_oms import FakeGateway, _seed_execution_fixture


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _live_order(
    *,
    condition_id: str,
    asset_id: str,
    side: str,
    client_order_id: str,
    created_at: datetime,
    pilot_run_id=None,
) -> LiveOrder:
    return LiveOrder(
        condition_id=condition_id,
        asset_id=asset_id,
        client_order_id=client_order_id,
        side=side,
        action_type="cross_now",
        order_type="limit",
        post_only=False,
        requested_size=Decimal("10"),
        filled_size=Decimal("0"),
        status="matched",
        dry_run=False,
        manual_approval_required=False,
        strategy_family="exec_policy",
        approval_state="not_required",
        pilot_run_id=pilot_run_id,
        created_at=created_at,
    )


def _live_fill(
    *,
    order: LiveOrder,
    fill_id: str,
    side: str,
    price: str,
    size: str,
    fee_paid: str,
    observed_at: datetime,
) -> LiveFill:
    return LiveFill(
        live_order_id=order.id,
        condition_id=order.condition_id,
        asset_id=order.asset_id,
        trade_id=fill_id,
        transaction_hash=f"tx-{fill_id}",
        fill_status="matched",
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        fee_paid=Decimal(fee_paid),
        fee_currency="USDC",
        maker_taker="taker",
        event_ts_exchange=observed_at,
        observed_at_local=observed_at,
        fingerprint=f"fp-{fill_id}",
    )


@pytest.mark.asyncio
async def test_position_lots_open_close_and_fee_updates(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_default_strategy_family", "exec_policy")
    service = PolymarketPilotEvidenceService()
    now = datetime.now(timezone.utc)

    open_order = _live_order(
        condition_id="cond-lots",
        asset_id="asset-lots",
        side="BUY",
        client_order_id="buy-1",
        created_at=now - timedelta(minutes=10),
    )
    partial_close_order = _live_order(
        condition_id="cond-lots",
        asset_id="asset-lots",
        side="SELL",
        client_order_id="sell-1",
        created_at=now - timedelta(minutes=5),
    )
    final_close_order = _live_order(
        condition_id="cond-lots",
        asset_id="asset-lots",
        side="SELL",
        client_order_id="sell-2",
        created_at=now - timedelta(minutes=1),
    )
    session.add_all([open_order, partial_close_order, final_close_order])
    await session.flush()

    fill_open = _live_fill(
        order=open_order,
        fill_id="trade-open",
        side="BUY",
        price="0.40",
        size="10",
        fee_paid="0.10",
        observed_at=now - timedelta(minutes=10),
    )
    fill_partial = _live_fill(
        order=partial_close_order,
        fill_id="trade-partial",
        side="SELL",
        price="0.70",
        size="4",
        fee_paid="0.04",
        observed_at=now - timedelta(minutes=5),
    )
    fill_close = _live_fill(
        order=final_close_order,
        fill_id="trade-close",
        side="SELL",
        price="0.60",
        size="6",
        fee_paid="0.06",
        observed_at=now - timedelta(minutes=1),
    )
    session.add_all([fill_open, fill_partial, fill_close])
    await session.flush()

    result = await service.sync_position_lots(session)
    persisted_lots = (await session.execute(select(PositionLot).order_by(PositionLot.id.asc()))).scalars().all()
    persisted_events = (await session.execute(select(PositionLotEvent).order_by(PositionLotEvent.id.asc()))).scalars().all()

    assert result["fills_processed"] == 3
    assert len(persisted_lots) == 1
    assert len(persisted_events) == 3

    lot = persisted_lots[0]
    assert lot.status == "closed"
    assert lot.remaining_size == Decimal("0")
    assert lot.avg_close_price == Decimal("0.64000000")
    assert lot.realized_pnl == Decimal("2.40000000")
    assert lot.fee_paid == Decimal("0.20000000")

    fill_open.fee_paid = Decimal("0.15")
    await service.sync_position_lots(session)
    updated_lot = await session.get(PositionLot, lot.id)
    fee_update_events = (await session.execute(
        __import__("sqlalchemy").select(PositionLotEvent).where(PositionLotEvent.event_type == "fee_update")
    )).scalars().all()
    assert updated_lot is not None
    assert updated_lot.fee_paid == Decimal("0.25000000")
    assert fee_update_events


@pytest.mark.asyncio
async def test_default_config_keeps_live_disabled_and_manual_approval_required(session):
    seeded = await _seed_execution_fixture(session, condition_id="cond-default-safety")
    manager = PolymarketOrderManager(gateway=FakeGateway())

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)

    assert intent["dry_run"] is True
    assert intent["manual_approval_required"] is True
    assert intent["approval_state"] == "queued"
    assert intent["strategy_family"] == "exec_policy"


@pytest.mark.asyncio
async def test_max_daily_loss_guardrail_triggers(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_max_daily_loss_usd", 1.0)
    service = PolymarketPilotEvidenceService()
    now = datetime.now(timezone.utc)

    buy_order = _live_order(
        condition_id="cond-loss",
        asset_id="asset-loss",
        side="BUY",
        client_order_id="loss-buy",
        created_at=now - timedelta(minutes=6),
    )
    sell_order = _live_order(
        condition_id="cond-loss",
        asset_id="asset-loss",
        side="SELL",
        client_order_id="loss-sell",
        created_at=now - timedelta(minutes=2),
    )
    session.add_all([buy_order, sell_order])
    await session.flush()
    session.add_all([
        _live_fill(order=buy_order, fill_id="loss-open", side="BUY", price="0.80", size="5", fee_paid="0.00", observed_at=now - timedelta(minutes=6)),
        _live_fill(order=sell_order, fill_id="loss-close", side="SELL", price="0.10", size="5", fee_paid="0.00", observed_at=now - timedelta(minutes=2)),
    ])
    await session.flush()

    await service.sync_position_lots(session)
    guardrails = await service.enforce_periodic_guardrails(session, now=now)

    assert any(event["guardrail_type"] == "max_daily_loss" for event in guardrails)


@pytest.mark.asyncio
async def test_approval_ttl_expiration_creates_guardrail_and_audit(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_pilot_approval_ttl_seconds", 60)

    seeded = await _seed_execution_fixture(session, condition_id="cond-evidence-approval")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=True)
    manager = PolymarketOrderManager(gateway=FakeGateway())

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    order = await session.get(LiveOrder, uuid.UUID(intent["id"]))
    assert order is not None
    order.approval_expires_at = utcnow() - timedelta(seconds=1)

    expired = await expire_stale_approvals(session)
    guardrails = await list_pilot_guardrail_events(session, guardrail_type="approval_ttl", limit=10)
    approval_events = (await session.execute(
        select(PolymarketPilotApprovalEvent).where(PolymarketPilotApprovalEvent.live_order_id == order.id)
    )).scalars().all()

    assert expired == 1
    assert order.approval_state == "expired"
    assert any(event["guardrail_type"] == "approval_ttl" for event in guardrails)
    assert [event.action for event in approval_events] == ["queued", "expired"]


@pytest.mark.asyncio
async def test_scorecard_aggregates_shadow_and_readiness_stays_manual_only(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_semi_auto_candidate_min_days", 3)
    monkeypatch.setattr(settings, "polymarket_pilot_semi_auto_max_avg_shadow_gap_bps", 5.0)
    service = PolymarketPilotEvidenceService()
    now = datetime.now(timezone.utc)

    buy_order = _live_order(
        condition_id="cond-score",
        asset_id="asset-score",
        side="BUY",
        client_order_id="score-buy",
        created_at=now - timedelta(hours=2),
    )
    sell_order = _live_order(
        condition_id="cond-score",
        asset_id="asset-score",
        side="SELL",
        client_order_id="score-sell",
        created_at=now - timedelta(hours=1),
    )
    session.add_all([buy_order, sell_order])
    await session.flush()
    session.add_all([
        _live_fill(order=buy_order, fill_id="score-open", side="BUY", price="0.40", size="10", fee_paid="0.10", observed_at=now - timedelta(hours=2)),
        _live_fill(order=sell_order, fill_id="score-close", side="SELL", price="0.60", size="10", fee_paid="0.10", observed_at=now - timedelta(hours=1)),
    ])
    await session.flush()
    await service.sync_position_lots(session)

    session.add_all([
        PolymarketLiveShadowEvaluation(
            live_order_id=buy_order.id,
            execution_decision_id=None,
            variant_name="exec_policy",
            gap_bps=Decimal("5.0"),
            coverage_limited=False,
            created_at=now - timedelta(hours=1),
            updated_at=now - timedelta(hours=1),
        ),
        PolymarketLiveShadowEvaluation(
            live_order_id=sell_order.id,
            execution_decision_id=None,
            variant_name="exec_policy",
            gap_bps=Decimal("7.0"),
            coverage_limited=True,
            created_at=now - timedelta(minutes=30),
            updated_at=now - timedelta(minutes=30),
        ),
        PolymarketControlPlaneIncident(
            severity="warning",
            incident_type="submission_blocked",
            live_order_id=sell_order.id,
            condition_id="cond-score",
            asset_id="asset-score",
            details_json={"reason": "manual_review"},
            observed_at_local=now - timedelta(minutes=10),
        ),
    ])
    await session.flush()

    window_start = now - timedelta(hours=3)
    scorecard = await service.generate_scorecard(
        session,
        strategy_family="exec_policy",
        window_start=window_start,
        window_end=now,
        label="manual",
    )
    readiness = await service.generate_readiness_report(
        session,
        strategy_family="exec_policy",
        window_start=window_start,
        window_end=now,
    )

    assert scorecard["fills_count"] == 2
    assert scorecard["incident_count"] == 1
    assert scorecard["gross_pnl"] == pytest.approx(2.0)
    assert scorecard["avg_shadow_gap_bps"] == pytest.approx(5.0)
    assert scorecard["coverage_limited_count"] == 1
    assert readiness["status"] == "not_ready"


@pytest.mark.asyncio
async def test_evidence_api_endpoints_and_health(client, engine):
    service = PolymarketPilotEvidenceService()
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        buy_order = _live_order(
            condition_id="cond-api-evidence",
            asset_id="asset-api-evidence",
            side="BUY",
            client_order_id="api-buy",
            created_at=now - timedelta(hours=2),
        )
        sell_order = _live_order(
            condition_id="cond-api-evidence",
            asset_id="asset-api-evidence",
            side="SELL",
            client_order_id="api-sell",
            created_at=now - timedelta(hours=1),
        )
        session.add_all([buy_order, sell_order])
        await session.flush()
        session.add_all([
            _live_fill(order=buy_order, fill_id="api-open", side="BUY", price="0.45", size="10", fee_paid="0.05", observed_at=now - timedelta(hours=2)),
            _live_fill(order=sell_order, fill_id="api-close", side="SELL", price="0.55", size="10", fee_paid="0.05", observed_at=now - timedelta(hours=1)),
        ])
        await session.flush()
        await service.sync_position_lots(session)
        await service.record_guardrail_event(
            session,
            strategy_family="exec_policy",
            guardrail_type="decision_age",
            severity="warning",
            action_taken="block",
            live_order=sell_order,
            details={"reason": "manual_test"},
        )
        await service.generate_scorecard(
            session,
            strategy_family="exec_policy",
            window_start=now - timedelta(days=1),
            window_end=now,
            label="manual",
        )
        await service.generate_readiness_report(
            session,
            strategy_family="exec_policy",
            window_start=now - timedelta(days=1),
            window_end=now,
        )
        await session.commit()

    lots_response = await client.get("/api/v1/ingest/polymarket/live/position-lots?strategy_family=exec_policy")
    lot_events_response = await client.get("/api/v1/ingest/polymarket/live/position-lot-events?strategy_family=exec_policy")
    scorecards_response = await client.get("/api/v1/ingest/polymarket/live/pilot/scorecards?strategy_family=exec_policy")
    guardrails_response = await client.get("/api/v1/ingest/polymarket/live/pilot/guardrail-events?strategy_family=exec_policy")
    readiness_response = await client.get("/api/v1/ingest/polymarket/live/pilot/readiness-reports?strategy_family=exec_policy")
    generate_scorecard_response = await client.post(
        "/api/v1/ingest/polymarket/live/pilot/scorecards/generate",
        json={"strategy_family": "exec_policy", "window": "daily"},
    )
    generate_readiness_response = await client.post(
        "/api/v1/ingest/polymarket/live/pilot/readiness-reports/generate",
        json={"strategy_family": "exec_policy", "window": "daily"},
    )
    health_response = await client.get("/api/v1/health")

    assert lots_response.status_code == 200
    assert lot_events_response.status_code == 200
    assert scorecards_response.status_code == 200
    assert guardrails_response.status_code == 200
    assert readiness_response.status_code == 200
    assert generate_scorecard_response.status_code == 200
    assert generate_readiness_response.status_code == 200
    assert health_response.status_code == 200
    assert lots_response.json()["rows"]
    assert lot_events_response.json()["rows"]
    assert scorecards_response.json()["rows"]
    assert guardrails_response.json()["rows"]
    assert readiness_response.json()["rows"]
    assert health_response.json()["polymarket_phase12"]["daily_realized_pnl"]["net_realized_pnl"] is not None
    assert "recent_guardrail_triggers" in health_response.json()["polymarket_phase12"]
