from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.polymarket_control_plane import (
    arm_pilot,
    create_or_update_pilot_config,
    expire_stale_approvals,
    get_active_pilot_config,
    get_open_pilot_run,
    list_control_plane_incidents,
    list_pilot_runs,
    upsert_live_shadow_evaluation,
)
from app.execution.polymarket_heartbeat import PolymarketHeartbeatService
from app.execution.polymarket_live_state import set_kill_switch
from app.execution.polymarket_order_manager import PolymarketOrderManager
from app.execution.polymarket_pilot_supervisor import PolymarketPilotSupervisor
from app.ingestion.polymarket_common import utcnow
from app.models.polymarket_live_execution import LiveOrder
from app.models.polymarket_pilot import (
    PolymarketControlPlaneIncident,
    PolymarketLiveShadowEvaluation,
    PolymarketPilotApprovalEvent,
    PolymarketPilotConfig,
)
from app.models.polymarket_replay import (
    PolymarketReplayFill,
    PolymarketReplayMetric,
    PolymarketReplayOrder,
    PolymarketReplayRun,
    PolymarketReplayScenario,
)
from app.models.strategy_registry import DemotionEvent
from app.strategies.promotion import (
    record_demotion_event_from_promotion_evaluation,
    record_promotion_eligibility_evaluation,
)
from tests.test_polymarket_oms import FakeGateway, _seed_execution_fixture


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class RestartWindowError(RuntimeError):
    def __init__(self, message: str = "HTTP 425 restart window") -> None:
        super().__init__(message)
        self.status_code = 425


class RestartGateway(FakeGateway):
    async def submit_order(self, request):  # noqa: ARG002
        raise RestartWindowError()


class FailingHeartbeatGateway(FakeGateway):
    async def healthcheck(self) -> dict:
        raise RuntimeError("heartbeat timeout")


async def _arm_exec_pilot(
    session: AsyncSession,
    *,
    live_enabled: bool = True,
    manual_approval_required: bool = False,
    pilot_name: str | None = None,
):
    config = await create_or_update_pilot_config(
        session,
        payload={
            "pilot_name": pilot_name or f"phase12-exec-{uuid.uuid4()}",
            "strategy_family": "exec_policy",
            "active": True,
            "live_enabled": live_enabled,
            "manual_approval_required": manual_approval_required,
            "max_open_orders": 5,
            "max_notional_per_day_usd": 500.0,
            "max_notional_per_order_usd": 250.0,
            "max_decision_age_seconds": 300,
        },
    )
    return await arm_pilot(session, pilot_config_id=config["id"], operator_identity="test-operator")


@pytest.mark.asyncio
async def test_live_submit_requires_explicit_arming_before_pilot_submission(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase12-arming")
    manager = PolymarketOrderManager(gateway=FakeGateway(venue_order_id="venue-phase12-arming"))

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    blocked = await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    assert blocked["status"] == "submit_blocked"
    assert blocked["blocked_reason_code"] == "pilot_not_active"

    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    submitted = await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    assert submitted["status"] == "live"
    assert submitted["venue_order_id"] == "venue-phase12-arming"


@pytest.mark.asyncio
async def test_manual_approval_queue_is_durable_and_expires(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_pilot_approval_ttl_seconds", 60)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase12-approval")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=True)
    manager = PolymarketOrderManager(gateway=FakeGateway())

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    order = await session.get(LiveOrder, uuid.UUID(intent["id"]))
    assert order is not None
    assert order.approval_state == "queued"

    order.approval_expires_at = utcnow() - timedelta(seconds=1)
    expired = await expire_stale_approvals(session)

    approval_actions = (
        await session.execute(
            select(PolymarketPilotApprovalEvent.action).where(
                PolymarketPilotApprovalEvent.live_order_id == order.id
            )
        )
    ).scalars().all()
    incidents = (
        await session.execute(
            select(PolymarketControlPlaneIncident).where(
                PolymarketControlPlaneIncident.live_order_id == order.id
            )
        )
    ).scalars().all()

    assert expired == 1
    assert order.approval_state == "expired"
    assert order.status == "submit_blocked"
    assert approval_actions == ["queued", "expired"]
    assert [incident.incident_type for incident in incidents] == ["approval_timeout"]
    assert incidents[0].strategy_version_id == order.strategy_version_id


@pytest.mark.asyncio
async def test_heartbeat_runs_only_when_needed_and_failure_pauses_pilot(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_heartbeat_enabled", True)

    service = PolymarketHeartbeatService(gateway=FailingHeartbeatGateway())
    idle = await service.run_once(session)
    assert idle["status"] == "idle"

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase12-heartbeat")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    manager = PolymarketOrderManager(gateway=FakeGateway(venue_order_id="venue-phase12-heartbeat"))
    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    result = await service.run_once(session)
    run = await get_open_pilot_run(session, pilot_config_id=(await get_active_pilot_config(session)).id)
    incidents = await list_control_plane_incidents(session, incident_type="heartbeat_missed", limit=10)

    assert result["status"] == "degraded"
    assert run is not None
    assert run.status == "paused"
    assert incidents
    assert incidents[0]["strategy_version_id"] is not None
    assert incidents[0]["strategy_version"]["version_key"] == "exec_policy_infra_v1"


@pytest.mark.asyncio
async def test_restart_window_error_pauses_active_pilot(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase12-restart")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    manager = PolymarketOrderManager(gateway=RestartGateway())

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    blocked = await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")
    active_config = await get_active_pilot_config(session)
    run = await get_open_pilot_run(session, pilot_config_id=active_config.id)
    incidents = await list_control_plane_incidents(session, incident_type="restart_425", limit=10)

    assert blocked["status"] == "submit_blocked"
    assert blocked["blocked_reason_code"] == "restart_pause_active"
    assert run is not None
    assert run.status == "paused"
    assert incidents


@pytest.mark.asyncio
async def test_live_shadow_evaluations_persist_gap_metrics_conservatively(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)
    monkeypatch.setattr(settings, "polymarket_pilot_shadow_gap_breach_bps", 5000.0)
    monkeypatch.setattr(settings, "polymarket_pilot_pause_on_shadow_gap_breach", False)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase12-shadow")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    manager = PolymarketOrderManager(gateway=FakeGateway(venue_order_id="venue-phase12-shadow"))
    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    live_order = await session.get(LiveOrder, uuid.UUID(intent["id"]))
    assert live_order is not None
    live_order.status = "partially_filled"
    live_order.filled_size = Decimal("10")
    live_order.avg_fill_price = Decimal("0.55")

    replay_run = PolymarketReplayRun(
      run_key=f"phase12-shadow-{uuid.uuid4()}",
      run_type="policy_compare",
      reason="manual",
      status="completed",
      started_at=utcnow(),
      completed_at=utcnow(),
    )
    session.add(replay_run)
    await session.flush()

    scenario = PolymarketReplayScenario(
      run_id=replay_run.id,
      scenario_key=f"phase12-scenario-{uuid.uuid4()}",
      scenario_type="execution_decision",
      condition_id=live_order.condition_id,
      asset_id=live_order.asset_id,
      status="completed",
      window_start=utcnow() - timedelta(minutes=5),
      window_end=utcnow(),
    )
    session.add(scenario)
    await session.flush()

    replay_order = PolymarketReplayOrder(
      scenario_id=scenario.id,
      variant_name="exec_policy",
      sequence_no=1,
      side="BUY",
      action_type="cross_now",
      order_type_hint="limit",
      limit_price=Decimal("0.50"),
      requested_size=Decimal("10"),
      submitted_size=Decimal("10"),
      status="filled",
      decision_ts=utcnow(),
      source_execution_decision_id=live_order.execution_decision_id,
    )
    session.add(replay_order)
    await session.flush()

    session.add(
      PolymarketReplayFill(
        scenario_id=scenario.id,
        replay_order_id=replay_order.id,
        variant_name="exec_policy",
        fill_index=0,
        fill_ts=utcnow(),
        price=Decimal("0.50"),
        size=Decimal("10"),
        fill_source_kind="replay_fill",
      )
    )
    session.add(
      PolymarketReplayMetric(
        run_id=replay_run.id,
        scenario_id=scenario.id,
        metric_scope="scenario",
        variant_name="exec_policy",
        net_pnl=Decimal("1.50"),
        details_json={"coverage_limited": False},
      )
    )
    await session.flush()

    evaluation = await upsert_live_shadow_evaluation(session, live_order=live_order)
    row = (
      await session.execute(select(PolymarketLiveShadowEvaluation))
    ).scalar_one()

    assert evaluation is not None
    assert evaluation["coverage_limited"] is False
    assert evaluation["expected_fill_price"] == 0.5
    assert evaluation["actual_fill_price"] == 0.55
    assert evaluation["gap_bps"] == 1000.0
    assert row.reason_code == "replay_matched"


@pytest.mark.asyncio
async def test_kill_switch_blocks_even_with_armed_pilot(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase12-kill-switch")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    manager = PolymarketOrderManager(gateway=FakeGateway())

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    await set_kill_switch(session, enabled=True)
    blocked = await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    assert blocked["status"] == "submit_blocked"
    assert blocked["blocked_reason_code"] == "kill_switch_enabled"


@pytest.mark.asyncio
async def test_demotion_event_blocks_live_submission_even_with_armed_pilot(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)

    seeded = await _seed_execution_fixture(session, condition_id="cond-phase13-demoted")
    await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    manager = PolymarketOrderManager(gateway=FakeGateway())

    intent = await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
    order = await session.get(LiveOrder, uuid.UUID(intent["id"]))
    assert order is not None
    assert order.strategy_version_id is not None

    evaluation = await record_promotion_eligibility_evaluation(
        session,
        strategy_version_id=int(order.strategy_version_id),
        trigger_kind="test",
        trigger_ref=str(order.id),
        observed_at=utcnow(),
    )
    demotion = await record_demotion_event_from_promotion_evaluation(
        session,
        evaluation=evaluation,
        trigger_kind="test",
        trigger_ref=str(order.id),
        observed_at=utcnow(),
    )
    assert demotion is not None

    blocked = await manager.submit_order(session, live_order_id=uuid.UUID(intent["id"]), operator="operator")

    assert blocked["status"] == "submit_blocked"
    assert blocked["blocked_reason_code"] == "strategy_demoted"


@pytest.mark.asyncio
async def test_supervisor_records_demotion_and_pauses_on_blocked_eligibility(session, engine, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_pilot_scorecard_enabled", False)
    monkeypatch.setattr(settings, "polymarket_pilot_readiness_report_enabled", False)

    armed = await _arm_exec_pilot(session, live_enabled=True, manual_approval_required=False)
    run = await get_open_pilot_run(session, pilot_config_id=armed["pilot_config"]["id"])
    assert run is not None and run.status == "armed"

    supervisor = PolymarketPilotSupervisor(_session_factory(engine))
    result = await supervisor.tick_once(session)

    await session.refresh(run)
    demotions = (await session.execute(select(DemotionEvent))).scalars().all()
    incidents = await list_control_plane_incidents(session, limit=5)

    assert result["demotions_recorded"] == 1
    assert result["demotion_pauses"] == 1
    assert run.status == "paused"
    assert run.reason == "promotion_gate_demotion"
    assert len(demotions) == 1
    assert demotions[0].reason_code == "replay_missing"
    assert incidents[0]["incident_type"] == "promotion_gate_demotion"


@pytest.mark.asyncio
async def test_only_one_pilot_family_can_be_active_or_armed_at_once(session, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)

    first = await _arm_exec_pilot(
        session,
        live_enabled=False,
        manual_approval_required=True,
        pilot_name="phase12-first",
    )
    second_config = await create_or_update_pilot_config(
        session,
        payload={
            "pilot_name": "phase12-second",
            "strategy_family": "exec_policy",
            "active": True,
            "live_enabled": False,
            "manual_approval_required": True,
        },
    )
    second = await arm_pilot(session, pilot_config_id=second_config["id"], operator_identity="operator-2")

    first_row = await session.get(PolymarketPilotConfig, first["pilot_config"]["id"])
    second_row = await session.get(PolymarketPilotConfig, second["pilot_config"]["id"])
    runs = await list_pilot_runs(session, limit=10)

    assert first_row is not None and first_row.active is False and first_row.armed is False
    assert second_row is not None and second_row.active is True and second_row.armed is True
    assert sum(1 for run in runs if run["status"] in {"armed", "running", "paused"}) == 1


@pytest.mark.asyncio
async def test_control_plane_apis_and_health_include_phase12_state(client, engine, monkeypatch):
    monkeypatch.setattr(settings, "polymarket_pilot_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_trading_enabled", True)
    monkeypatch.setattr(settings, "polymarket_live_dry_run", False)
    monkeypatch.setattr(settings, "polymarket_live_manual_approval_required", False)
    monkeypatch.setattr(settings, "polymarket_execution_policy_require_live_book", True)

    create_response = await client.post(
        "/api/v1/ingest/polymarket/live/pilot/configs",
        json={
            "pilot_name": "phase12-api-pilot",
            "strategy_family": "exec_policy",
            "active": True,
            "live_enabled": True,
            "manual_approval_required": True,
            "max_open_orders": 2,
        },
    )
    assert create_response.status_code == 200
    pilot_config_id = create_response.json()["id"]

    arm_response = await client.post(
        "/api/v1/ingest/polymarket/live/pilot/arm",
        json={"pilot_config_id": pilot_config_id, "operator_identity": "operator"},
    )
    assert arm_response.status_code == 200

    session_factory = _session_factory(engine)
    async with session_factory() as session:
        seeded = await _seed_execution_fixture(session, condition_id="cond-phase12-api")
        manager = PolymarketOrderManager(gateway=FakeGateway())
        await manager.create_order_intent(session, execution_decision_id=seeded["decision"].id)
        await session.commit()

    status_response = await client.get("/api/v1/ingest/polymarket/live/pilot/status")
    live_status_response = await client.get("/api/v1/ingest/polymarket/live/status")
    console_response = await client.get("/api/v1/ingest/polymarket/live/console-summary")
    approvals_response = await client.get("/api/v1/ingest/polymarket/live/approvals?approval_state=queued")
    orders_response = await client.get("/api/v1/ingest/polymarket/live/orders?approval_state=queued")
    tape_response = await client.get("/api/v1/ingest/polymarket/live/tape?condition_id=cond-phase12-api")
    health_response = await client.get("/api/v1/health")

    assert status_response.status_code == 200
    assert live_status_response.status_code == 200
    assert console_response.status_code == 200
    assert approvals_response.status_code == 200
    assert orders_response.status_code == 200
    assert tape_response.status_code == 200
    assert health_response.status_code == 200
    assert status_response.json()["active_pilot"]["strategy_family"] == "exec_policy"
    assert status_response.json()["active_strategy_version"]["version_key"] == "exec_policy_infra_v1"
    assert status_response.json()["active_family_budget"]["strategy_family"] == "exec_policy"
    assert status_response.json()["active_family_budget"]["capacity_status"] in {"ok", "narrowed", "constrained", "breached"}
    assert status_response.json()["active_autonomy_state"]["effective_autonomy_tier"] == "assisted_live"
    assert status_response.json()["active_autonomy_state"]["submission_mode"] == "manual_approval"
    assert live_status_response.json()["active_autonomy_state"]["effective_autonomy_tier"] == "assisted_live"
    assert console_response.json()["approvals"]
    assert console_response.json()["pilot"]["active_strategy_version"]["version_key"] == "exec_policy_infra_v1"
    assert console_response.json()["active_family_budget"]["strategy_family"] == "exec_policy"
    assert console_response.json()["active_autonomy_state"]["submission_mode"] == "manual_approval"
    assert console_response.json()["recent_orders"][0]["strategy_version"]["version_key"] == "exec_policy_infra_v1"
    assert approvals_response.json()["rows"][0]["approval_state"] == "queued"
    assert orders_response.json()["rows"][0]["approval_state"] == "queued"
    assert orders_response.json()["rows"][0]["strategy_version"]["version_key"] == "exec_policy_infra_v1"
    assert tape_response.json()["selected_condition_id"] == "cond-phase12-api"
    assert health_response.json()["polymarket_phase12"]["approval_queue_count"] >= 1
    assert health_response.json()["polymarket_phase12"]["manual_approval_required"] is True
    assert health_response.json()["polymarket_phase12"]["autonomy_state"]["effective_autonomy_tier"] == "assisted_live"
