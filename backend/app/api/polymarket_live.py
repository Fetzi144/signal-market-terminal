from __future__ import annotations

import uuid
from datetime import datetime, time, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.execution.polymarket_control_plane import (
    arm_pilot,
    create_or_update_pilot_config,
    disarm_active_pilot,
    fetch_execution_console_summary,
    fetch_market_tape_view,
    fetch_pilot_status,
    get_pilot_config,
    list_approval_queue,
    list_control_plane_incidents,
    list_live_shadow_evaluations,
    list_pilot_configs,
    list_pilot_runs,
    pause_active_pilot,
    resume_active_pilot,
)
from app.execution.polymarket_gateway import GatewayUnavailableError, PolymarketGateway
from app.execution.polymarket_live_reconciler import PolymarketLiveReconciler
from app.execution.polymarket_live_state import (
    effective_category_allowlist,
    effective_market_allowlist,
    fetch_live_state_row,
    fetch_polymarket_live_status,
    list_current_reservations,
    list_live_fills,
    list_live_order_events,
    list_live_orders,
    set_allowlist_overrides,
    set_gateway_status,
    set_kill_switch,
)
from app.execution.polymarket_order_manager import PolymarketOrderManager
from app.execution.polymarket_pilot_evidence import (
    PolymarketPilotEvidenceService,
    list_pilot_guardrail_events,
    list_pilot_readiness_reports,
    list_pilot_scorecards,
    list_position_lot_events,
    list_position_lots,
)

router = APIRouter(prefix="/api/v1/ingest/polymarket/live", tags=["polymarket-live"])
_pilot_evidence = PolymarketPilotEvidenceService()


class RowsOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


class PolymarketLiveStatusOut(BaseModel):
    enabled: bool
    dry_run: bool
    manual_approval_required: bool
    decision_max_age_seconds: int
    user_stream_enabled: bool
    kill_switch_enabled: bool
    allowlist_markets: list[str]
    allowlist_categories: list[str]
    max_outstanding_notional_usd: float | None = None
    gateway_reachable: bool
    gateway_last_checked_at: Any | None = None
    gateway_last_error: str | None = None
    user_stream_connected: bool
    user_stream_session_id: str | None = None
    user_stream_connection_started_at: Any | None = None
    last_user_stream_message_at: Any | None = None
    last_user_stream_error: str | None = None
    last_user_stream_error_at: Any | None = None
    last_reconciled_user_event_id: int | None = None
    last_reconcile_started_at: Any | None = None
    last_reconcile_success_at: Any | None = None
    last_reconcile_error: str | None = None
    last_reconcile_error_at: Any | None = None
    heartbeat_healthy: bool | None = None
    heartbeat_last_checked_at: Any | None = None
    heartbeat_last_success_at: Any | None = None
    heartbeat_last_error: str | None = None
    outstanding_live_orders: int
    outstanding_reservations: float
    recent_fills_24h: int
    active_family_budget: dict[str, Any] | None = None
    live_submission_permitted: bool


class LiveMutationRequest(BaseModel):
    operator: str | None = Field(default=None, max_length=128)


class LiveIntentRequest(BaseModel):
    execution_decision_id: uuid.UUID


class LiveApprovalRequest(BaseModel):
    approved_by: str = Field(..., min_length=1, max_length=128)


class LiveRejectRequest(BaseModel):
    rejected_by: str = Field(..., min_length=1, max_length=128)
    reason: str = Field(..., min_length=1, max_length=255)


class KillSwitchRequest(BaseModel):
    enabled: bool


class AllowlistRequest(BaseModel):
    markets: list[str] | None = None
    categories: list[str] | None = None


class AllowlistOut(BaseModel):
    configured_markets: list[str]
    configured_categories: list[str]
    effective_markets: list[str]
    effective_categories: list[str]


class ReconcileRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)


class PilotConfigRequest(BaseModel):
    pilot_name: str | None = Field(default=None, max_length=128)
    strategy_family: str | None = Field(default=None, max_length=32)
    active: bool | None = None
    manual_approval_required: bool | None = None
    live_enabled: bool | None = None
    market_allowlist_json: list[str] | None = None
    category_allowlist_json: list[str] | None = None
    max_notional_per_order_usd: float | None = Field(default=None, ge=0)
    max_notional_per_day_usd: float | None = Field(default=None, ge=0)
    max_open_orders: int | None = Field(default=None, ge=1)
    max_plan_age_seconds: int | None = Field(default=None, ge=1)
    max_decision_age_seconds: int | None = Field(default=None, ge=1)
    max_slippage_bps: float | None = Field(default=None, ge=0)
    require_complete_replay_coverage: bool | None = None
    details_json: dict[str, Any] | list[Any] | str | None = None


class PilotArmRequest(BaseModel):
    pilot_config_id: int
    operator_identity: str | None = Field(default=None, max_length=128)


class EvidenceGenerationRequest(BaseModel):
    strategy_family: str | None = Field(default=None, max_length=32)
    start: datetime | None = None
    end: datetime | None = None
    window: str | None = Field(default=None, max_length=16)


def _resolve_evidence_window(
    *,
    start: datetime | None,
    end: datetime | None,
    window: str | None,
) -> tuple[datetime, datetime]:
    if start is not None and end is not None:
        return start, end
    observed = datetime.now(timezone.utc)
    normalized_window = str(window or "daily").strip().lower()
    if normalized_window == "weekly":
        resolved_end = end or observed
        resolved_start = start or (resolved_end - timedelta(days=7))
        return resolved_start, resolved_end
    day_start = datetime.combine(observed.date(), time.min, tzinfo=timezone.utc)
    return start or day_start, end or observed


@router.get("/status", response_model=PolymarketLiveStatusOut)
async def get_polymarket_live_status(
    probe_gateway: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
):
    if probe_gateway:
        gateway = PolymarketGateway()
        try:
            await gateway.healthcheck()
            await set_gateway_status(db, reachable=True, error=None)
        except GatewayUnavailableError as exc:
            await set_gateway_status(db, reachable=False, error=str(exc))
        except Exception as exc:
            await set_gateway_status(db, reachable=False, error=str(exc))
        await db.commit()
    return await fetch_polymarket_live_status(db)


@router.get("/console-summary")
async def get_polymarket_live_console_summary(db: AsyncSession = Depends(get_db)):
    return await fetch_execution_console_summary(db)


@router.get("/pilot/configs", response_model=RowsOut)
async def get_polymarket_pilot_configs(
    strategy_family: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_pilot_configs(db, strategy_family=strategy_family, active=active, limit=limit)
    return RowsOut(rows=rows, limit=limit)


@router.get("/pilot/configs/{pilot_config_id}")
async def get_polymarket_pilot_config(
    pilot_config_id: int,
    db: AsyncSession = Depends(get_db),
):
    row = await get_pilot_config(db, pilot_config_id=pilot_config_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Pilot config not found")
    from app.execution.polymarket_control_plane import serialize_pilot_config

    return serialize_pilot_config(row)


@router.post("/pilot/configs")
async def post_polymarket_pilot_config(
    body: PilotConfigRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await create_or_update_pilot_config(db, payload=body.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await db.commit()
    return result


@router.put("/pilot/configs/{pilot_config_id}")
async def put_polymarket_pilot_config(
    pilot_config_id: int,
    body: PilotConfigRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await create_or_update_pilot_config(
            db,
            payload=body.model_dump(exclude_none=True),
            pilot_config_id=pilot_config_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await db.commit()
    return result


@router.post("/pilot/arm")
async def arm_polymarket_pilot(
    body: PilotArmRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await arm_pilot(db, pilot_config_id=body.pilot_config_id, operator_identity=body.operator_identity)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await db.commit()
    return result


@router.post("/pilot/disarm")
async def disarm_polymarket_pilot(
    body: LiveMutationRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await disarm_active_pilot(db, operator_identity=body.operator)
    await db.commit()
    return result or {"pilot_config": None, "pilot_run": None}


@router.post("/pilot/pause")
async def pause_polymarket_pilot(
    body: LiveMutationRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await pause_active_pilot(db, reason="manual", operator_identity=body.operator)
    await db.commit()
    return result or {"pilot_config": None, "pilot_run": None}


@router.post("/pilot/resume")
async def resume_polymarket_pilot(
    body: LiveMutationRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await resume_active_pilot(db, operator_identity=body.operator)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    return result


@router.post("/pilot/scorecards/generate")
async def post_polymarket_pilot_scorecard_generation(
    body: EvidenceGenerationRequest,
    db: AsyncSession = Depends(get_db),
):
    start, end = _resolve_evidence_window(start=body.start, end=body.end, window=body.window)
    try:
        result = await _pilot_evidence.generate_scorecard(
            db,
            strategy_family=body.strategy_family or "exec_policy",
            window_start=start,
            window_end=end,
            label=str(body.window or "manual").lower(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await db.commit()
    return result


@router.post("/pilot/readiness-reports/generate")
async def post_polymarket_pilot_readiness_generation(
    body: EvidenceGenerationRequest,
    db: AsyncSession = Depends(get_db),
):
    start, end = _resolve_evidence_window(start=body.start, end=body.end, window=body.window)
    try:
        result = await _pilot_evidence.generate_readiness_report(
            db,
            strategy_family=body.strategy_family or "exec_policy",
            window_start=start,
            window_end=end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await db.commit()
    return result


@router.get("/pilot/status")
async def get_polymarket_pilot_status(db: AsyncSession = Depends(get_db)):
    return await fetch_pilot_status(db)


@router.get("/pilot/runs", response_model=RowsOut)
async def get_polymarket_pilot_runs(
    status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_pilot_runs(db, status=status, limit=limit)
    return RowsOut(rows=rows, limit=limit)


@router.get("/approvals", response_model=RowsOut)
async def get_polymarket_pilot_approvals(
    strategy_family: str | None = Query(default=None),
    approval_state: str | None = Query(default=None),
    status: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_approval_queue(
        db,
        strategy_family=strategy_family,
        approval_state=approval_state,
        status=status,
        condition_id=condition_id,
        asset_id=asset_id,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/incidents", response_model=RowsOut)
async def get_polymarket_control_plane_incidents(
    incident_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_control_plane_incidents(
        db,
        incident_type=incident_type,
        condition_id=condition_id,
        asset_id=asset_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/shadow-evaluations", response_model=RowsOut)
async def get_polymarket_shadow_evaluations(
    variant_name: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_live_shadow_evaluations(
        db,
        variant_name=variant_name,
        condition_id=condition_id,
        asset_id=asset_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/position-lots", response_model=RowsOut)
async def get_polymarket_position_lots(
    strategy_family: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_position_lots(
        db,
        strategy_family=strategy_family,
        condition_id=condition_id,
        asset_id=asset_id,
        status=status,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/position-lot-events", response_model=RowsOut)
async def get_polymarket_position_lot_events(
    strategy_family: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_position_lot_events(
        db,
        strategy_family=strategy_family,
        condition_id=condition_id,
        asset_id=asset_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/pilot/scorecards", response_model=RowsOut)
async def get_polymarket_pilot_scorecards(
    strategy_family: str | None = Query(default=None),
    status: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_pilot_scorecards(
        db,
        strategy_family=strategy_family,
        status=status,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/pilot/guardrail-events", response_model=RowsOut)
async def get_polymarket_pilot_guardrail_events(
    strategy_family: str | None = Query(default=None),
    guardrail_type: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_pilot_guardrail_events(
        db,
        strategy_family=strategy_family,
        guardrail_type=guardrail_type,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/pilot/readiness-reports", response_model=RowsOut)
async def get_polymarket_pilot_readiness_reports(
    strategy_family: str | None = Query(default=None),
    status: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_pilot_readiness_reports(
        db,
        strategy_family=strategy_family,
        status=status,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/tape")
async def get_polymarket_market_tape(
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    return await fetch_market_tape_view(db, condition_id=condition_id, asset_id=asset_id, limit=limit)


@router.post("/orders/intents", response_model=dict[str, Any])
async def create_polymarket_live_order_intent(
    body: LiveIntentRequest,
    db: AsyncSession = Depends(get_db),
):
    manager = PolymarketOrderManager()
    try:
        result = await manager.create_order_intent(db, execution_decision_id=body.execution_decision_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    return result


@router.get("/orders", response_model=RowsOut)
async def get_polymarket_live_orders(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    strategy_family: str | None = Query(default=None),
    approval_state: str | None = Query(default=None),
    client_order_id: str | None = Query(default=None),
    venue_order_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_live_orders(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        strategy_family=strategy_family,
        approval_state=approval_state,
        client_order_id=client_order_id,
        venue_order_id=venue_order_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/orders/events", response_model=RowsOut)
async def get_polymarket_live_order_events(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    strategy_family: str | None = Query(default=None),
    approval_state: str | None = Query(default=None),
    client_order_id: str | None = Query(default=None),
    venue_order_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_live_order_events(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        strategy_family=strategy_family,
        approval_state=approval_state,
        client_order_id=client_order_id,
        venue_order_id=venue_order_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/fills", response_model=RowsOut)
async def get_polymarket_live_fills(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    strategy_family: str | None = Query(default=None),
    approval_state: str | None = Query(default=None),
    client_order_id: str | None = Query(default=None),
    venue_order_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_live_fills(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        strategy_family=strategy_family,
        approval_state=approval_state,
        client_order_id=client_order_id,
        venue_order_id=venue_order_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.get("/reservations", response_model=RowsOut)
async def get_polymarket_live_reservations(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    strategy_family: str | None = Query(default=None),
    strategy_version_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_current_reservations(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        strategy_family=strategy_family,
        strategy_version_id=strategy_version_id,
        limit=limit,
    )
    return RowsOut(rows=rows, limit=limit)


@router.post("/orders/{live_order_id}/approve", response_model=dict[str, Any])
async def approve_polymarket_live_order(
    live_order_id: uuid.UUID,
    body: LiveApprovalRequest,
    db: AsyncSession = Depends(get_db),
):
    manager = PolymarketOrderManager()
    try:
        result = await manager.approve_order(db, live_order_id=live_order_id, approved_by=body.approved_by)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    return result


@router.post("/orders/{live_order_id}/reject", response_model=dict[str, Any])
async def reject_polymarket_live_order(
    live_order_id: uuid.UUID,
    body: LiveRejectRequest,
    db: AsyncSession = Depends(get_db),
):
    manager = PolymarketOrderManager()
    try:
        result = await manager.reject_order(
            db,
            live_order_id=live_order_id,
            rejected_by=body.rejected_by,
            reason=body.reason,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    return result


@router.post("/orders/{live_order_id}/submit", response_model=dict[str, Any])
async def submit_polymarket_live_order(
    live_order_id: uuid.UUID,
    body: LiveMutationRequest,
    db: AsyncSession = Depends(get_db),
):
    manager = PolymarketOrderManager()
    try:
        result = await manager.submit_order(db, live_order_id=live_order_id, operator=body.operator)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    return result


@router.post("/orders/{live_order_id}/cancel", response_model=dict[str, Any])
async def cancel_polymarket_live_order(
    live_order_id: uuid.UUID,
    body: LiveMutationRequest,
    db: AsyncSession = Depends(get_db),
):
    manager = PolymarketOrderManager()
    try:
        result = await manager.cancel_order(db, live_order_id=live_order_id, operator=body.operator)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    return result


@router.get("/kill-switch", response_model=dict[str, bool])
async def get_polymarket_live_kill_switch(db: AsyncSession = Depends(get_db)):
    status = await fetch_polymarket_live_status(db)
    return {"enabled": status["kill_switch_enabled"]}


@router.post("/kill-switch", response_model=dict[str, bool])
async def set_polymarket_live_kill_switch(
    body: KillSwitchRequest,
    db: AsyncSession = Depends(get_db),
):
    await set_kill_switch(db, enabled=body.enabled)
    await db.commit()
    return {"enabled": body.enabled}


@router.get("/allowlist", response_model=AllowlistOut)
async def get_polymarket_live_allowlist(db: AsyncSession = Depends(get_db)):
    state = await fetch_live_state_row(db)
    return AllowlistOut(
        configured_markets=settings.polymarket_allowlist_market_values,
        configured_categories=settings.polymarket_allowlist_category_values,
        effective_markets=effective_market_allowlist(state),
        effective_categories=effective_category_allowlist(state),
    )


@router.put("/allowlist", response_model=AllowlistOut)
async def put_polymarket_live_allowlist(
    body: AllowlistRequest,
    db: AsyncSession = Depends(get_db),
):
    state = await set_allowlist_overrides(
        db,
        markets=body.markets,
        categories=body.categories,
    )
    await db.commit()
    return AllowlistOut(
        configured_markets=settings.polymarket_allowlist_market_values,
        configured_categories=settings.polymarket_allowlist_category_values,
        effective_markets=effective_market_allowlist(state),
        effective_categories=effective_category_allowlist(state),
    )


@router.post("/reconcile", response_model=dict[str, Any])
async def trigger_polymarket_live_reconcile(
    body: ReconcileRequest,
    db: AsyncSession = Depends(get_db),
):
    reconciler = PolymarketLiveReconciler()
    result = await reconciler.reconcile_once(db, reason=body.reason)
    await db.commit()
    return result


@router.get("/user-stream/status", response_model=PolymarketLiveStatusOut)
async def get_polymarket_live_user_stream_status(db: AsyncSession = Depends(get_db)):
    return await fetch_polymarket_live_status(db)
