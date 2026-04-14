from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
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

router = APIRouter(prefix="/api/v1/ingest/polymarket/live", tags=["polymarket-live"])


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
    outstanding_live_orders: int
    outstanding_reservations: float
    recent_fills_24h: int
    live_submission_permitted: bool


class LiveOrderListOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


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


@router.get("/orders", response_model=LiveOrderListOut)
async def get_polymarket_live_orders(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    client_order_id: str | None = Query(default=None),
    venue_order_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_live_orders(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        client_order_id=client_order_id,
        venue_order_id=venue_order_id,
        limit=limit,
    )
    return LiveOrderListOut(rows=rows, limit=limit)


@router.get("/orders/events", response_model=LiveOrderListOut)
async def get_polymarket_live_order_events(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    client_order_id: str | None = Query(default=None),
    venue_order_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_live_order_events(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        client_order_id=client_order_id,
        venue_order_id=venue_order_id,
        limit=limit,
    )
    return LiveOrderListOut(rows=rows, limit=limit)


@router.get("/fills", response_model=LiveOrderListOut)
async def get_polymarket_live_fills(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    client_order_id: str | None = Query(default=None),
    venue_order_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_live_fills(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        client_order_id=client_order_id,
        venue_order_id=venue_order_id,
        limit=limit,
    )
    return LiveOrderListOut(rows=rows, limit=limit)


@router.get("/reservations", response_model=LiveOrderListOut)
async def get_polymarket_live_reservations(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_current_reservations(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        limit=limit,
    )
    return LiveOrderListOut(rows=rows, limit=limit)


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
