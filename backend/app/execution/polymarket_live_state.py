from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.ingestion.polymarket_common import utcnow
from app.strategies.registry import (
    get_latest_promotion_evaluation_by_version,
    get_strategy_version_snapshot_map,
)
from app.metrics import (
    polymarket_live_kill_switch,
    polymarket_live_last_reconcile_success_timestamp,
    polymarket_live_last_user_stream_message_timestamp,
    polymarket_live_outstanding_reservations,
)
from app.models.polymarket_live_execution import (
    CapitalReservation,
    LiveFill,
    LiveOrder,
    LiveOrderEvent,
    PolymarketLiveState,
)

LIVE_ORDER_TERMINAL_STATUSES = {
    "matched",
    "mined",
    "confirmed",
    "canceled",
    "expired",
    "rejected",
    "failed",
    "validation_failed",
}


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _serialize_decimal(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    return value


async def _strategy_lifecycle_maps(
    session: AsyncSession,
    *,
    version_ids: list[int] | set[int] | tuple[int, ...],
) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
    version_map = await get_strategy_version_snapshot_map(session, version_ids=version_ids)
    evaluation_map = await get_latest_promotion_evaluation_by_version(session, version_ids=version_ids)
    return version_map, evaluation_map


async def fetch_live_state_row(session: AsyncSession) -> PolymarketLiveState | None:
    return await session.get(PolymarketLiveState, 1)


async def ensure_live_state_row(session: AsyncSession) -> PolymarketLiveState:
    state = await fetch_live_state_row(session)
    if state is None:
        state = PolymarketLiveState(
            id=1,
            kill_switch_enabled=settings.polymarket_kill_switch_enabled,
        )
        session.add(state)
        await session.flush()
    return state


def effective_market_allowlist(state: PolymarketLiveState | None) -> list[str]:
    if state is not None and state.allowlist_markets_json is not None:
        return [str(value) for value in state.allowlist_markets_json if value]
    return settings.polymarket_allowlist_market_values


def effective_category_allowlist(state: PolymarketLiveState | None) -> list[str]:
    if state is not None and state.allowlist_categories_json is not None:
        return [str(value) for value in state.allowlist_categories_json if value]
    return settings.polymarket_allowlist_category_values


def effective_kill_switch_enabled(state: PolymarketLiveState | None) -> bool:
    if state is not None:
        return bool(state.kill_switch_enabled)
    return bool(settings.polymarket_kill_switch_enabled)


async def set_gateway_status(
    session: AsyncSession,
    *,
    reachable: bool,
    error: str | None = None,
    checked_at: datetime | None = None,
) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    state.gateway_reachable = reachable
    state.gateway_last_checked_at = _ensure_utc(checked_at) or utcnow()
    state.gateway_last_error = error
    await session.flush()
    return state


async def set_kill_switch(
    session: AsyncSession,
    *,
    enabled: bool,
) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    state.kill_switch_enabled = enabled
    await session.flush()
    polymarket_live_kill_switch.set(1 if enabled else 0)
    return state


async def set_allowlist_overrides(
    session: AsyncSession,
    *,
    markets: list[str] | None,
    categories: list[str] | None,
) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    state.allowlist_markets_json = markets
    state.allowlist_categories_json = categories
    await session.flush()
    return state


async def set_user_stream_connection_state(
    session: AsyncSession,
    *,
    connected: bool,
    session_id: str | None = None,
    started_at: datetime | None = None,
    error: str | None = None,
) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    state.user_stream_connected = connected
    state.user_stream_session_id = session_id
    state.user_stream_connection_started_at = _ensure_utc(started_at) if connected else None
    if error:
        state.last_user_stream_error = error
        state.last_user_stream_error_at = utcnow()
    await session.flush()
    return state


async def mark_user_stream_message(
    session: AsyncSession,
    *,
    session_id: str | None,
    message_at: datetime | None,
) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    state.user_stream_connected = True
    state.user_stream_session_id = session_id
    state.last_user_stream_message_at = _ensure_utc(message_at) or utcnow()
    state.last_user_stream_error = None
    state.last_user_stream_error_at = None
    await session.flush()
    polymarket_live_last_user_stream_message_timestamp.set(state.last_user_stream_message_at.timestamp())
    return state


async def mark_reconcile_started(session: AsyncSession) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    state.last_reconcile_started_at = utcnow()
    await session.flush()
    return state


async def mark_reconcile_finished(
    session: AsyncSession,
    *,
    success: bool,
    error: str | None = None,
    last_user_event_id: int | None = None,
) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    if success:
        state.last_reconcile_success_at = utcnow()
        state.last_reconcile_error = None
        state.last_reconcile_error_at = None
        if last_user_event_id is not None:
            state.last_reconciled_user_event_id = last_user_event_id
        polymarket_live_last_reconcile_success_timestamp.set(state.last_reconcile_success_at.timestamp())
    else:
        state.last_reconcile_error = error
        state.last_reconcile_error_at = utcnow()
    await session.flush()
    return state


def serialize_live_order(
    order: LiveOrder,
    *,
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": str(order.id),
        "execution_decision_id": str(order.execution_decision_id) if order.execution_decision_id is not None else None,
        "signal_id": str(order.signal_id) if order.signal_id is not None else None,
        "market_dim_id": order.market_dim_id,
        "asset_dim_id": order.asset_dim_id,
        "condition_id": order.condition_id,
        "asset_id": order.asset_id,
        "outcome_id": str(order.outcome_id) if order.outcome_id is not None else None,
        "client_order_id": order.client_order_id,
        "venue_order_id": order.venue_order_id,
        "side": order.side,
        "action_type": order.action_type,
        "order_type": order.order_type,
        "post_only": order.post_only,
        "limit_price": _serialize_decimal(order.limit_price),
        "target_price": _serialize_decimal(order.target_price),
        "requested_size": _serialize_decimal(order.requested_size),
        "submitted_size": _serialize_decimal(order.submitted_size),
        "filled_size": _serialize_decimal(order.filled_size),
        "avg_fill_price": _serialize_decimal(order.avg_fill_price),
        "status": order.status,
        "dry_run": order.dry_run,
        "strategy_family": order.strategy_family,
        "strategy_version_id": order.strategy_version_id,
        "strategy_version": strategy_version,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "pilot_config_id": order.pilot_config_id,
        "pilot_run_id": str(order.pilot_run_id) if order.pilot_run_id is not None else None,
        "manual_approval_required": order.manual_approval_required,
        "approval_state": order.approval_state,
        "approval_requested_at": order.approval_requested_at,
        "approval_expires_at": order.approval_expires_at,
        "approved_by": order.approved_by,
        "approved_at": order.approved_at,
        "blocked_reason_code": order.blocked_reason_code,
        "kill_switch_blocked": order.kill_switch_blocked,
        "allowlist_blocked": order.allowlist_blocked,
        "validation_error": order.validation_error,
        "submission_error": order.submission_error,
        "policy_version": order.policy_version,
        "decision_reason_json": _json_safe(order.decision_reason_json),
        "created_at": order.created_at,
        "submitted_at": order.submitted_at,
        "last_event_at": order.last_event_at,
        "completed_at": order.completed_at,
        "updated_at": order.updated_at,
    }


def serialize_live_order_event(
    event: LiveOrderEvent,
    *,
    live_order: LiveOrder | None = None,
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": event.id,
        "live_order_id": str(event.live_order_id),
        "strategy_family": live_order.strategy_family if live_order is not None else None,
        "strategy_version_id": live_order.strategy_version_id if live_order is not None else None,
        "strategy_version": strategy_version,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "raw_user_event_id": event.raw_user_event_id,
        "source_kind": event.source_kind,
        "event_type": event.event_type,
        "venue_status": event.venue_status,
        "event_ts_exchange": event.event_ts_exchange,
        "observed_at_local": event.observed_at_local,
        "payload_json": _json_safe(event.payload_json),
        "details_json": _json_safe(event.details_json),
        "fingerprint": event.fingerprint,
        "created_at": event.created_at,
    }


def serialize_live_fill(
    fill: LiveFill,
    *,
    live_order: LiveOrder | None = None,
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": fill.id,
        "live_order_id": str(fill.live_order_id) if fill.live_order_id is not None else None,
        "condition_id": fill.condition_id,
        "asset_id": fill.asset_id,
        "trade_id": fill.trade_id,
        "transaction_hash": fill.transaction_hash,
        "fill_status": fill.fill_status,
        "side": fill.side,
        "price": _serialize_decimal(fill.price),
        "size": _serialize_decimal(fill.size),
        "fee_paid": _serialize_decimal(fill.fee_paid),
        "fee_currency": fill.fee_currency,
        "maker_taker": fill.maker_taker,
        "strategy_family": live_order.strategy_family if live_order is not None else None,
        "strategy_version_id": live_order.strategy_version_id if live_order is not None else None,
        "strategy_version": strategy_version,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "event_ts_exchange": fill.event_ts_exchange,
        "observed_at_local": fill.observed_at_local,
        "raw_user_event_id": fill.raw_user_event_id,
        "details_json": _json_safe(fill.details_json),
        "fingerprint": fill.fingerprint,
        "created_at": fill.created_at,
    }


async def serialize_live_orders_with_lifecycle(
    session: AsyncSession,
    rows: list[LiveOrder],
) -> list[dict[str, Any]]:
    version_ids = {int(row.strategy_version_id) for row in rows if row.strategy_version_id is not None}
    version_map, evaluation_map = await _strategy_lifecycle_maps(session, version_ids=version_ids)
    return [
        serialize_live_order(
            row,
            strategy_version=version_map.get(int(row.strategy_version_id)) if row.strategy_version_id is not None else None,
            latest_promotion_evaluation=evaluation_map.get(int(row.strategy_version_id)) if row.strategy_version_id is not None else None,
        )
        for row in rows
    ]


async def serialize_live_order_events_with_lifecycle(
    session: AsyncSession,
    rows: list[tuple[LiveOrderEvent, LiveOrder | None]],
) -> list[dict[str, Any]]:
    version_ids = {
        int(order.strategy_version_id)
        for _event, order in rows
        if order is not None and order.strategy_version_id is not None
    }
    version_map, evaluation_map = await _strategy_lifecycle_maps(session, version_ids=version_ids)
    return [
        serialize_live_order_event(
            event,
            live_order=order,
            strategy_version=version_map.get(int(order.strategy_version_id)) if order is not None and order.strategy_version_id is not None else None,
            latest_promotion_evaluation=evaluation_map.get(int(order.strategy_version_id)) if order is not None and order.strategy_version_id is not None else None,
        )
        for event, order in rows
    ]


async def serialize_live_fills_with_lifecycle(
    session: AsyncSession,
    rows: list[tuple[LiveFill, LiveOrder | None]],
) -> list[dict[str, Any]]:
    version_ids = {
        int(order.strategy_version_id)
        for _fill, order in rows
        if order is not None and order.strategy_version_id is not None
    }
    version_map, evaluation_map = await _strategy_lifecycle_maps(session, version_ids=version_ids)
    return [
        serialize_live_fill(
            fill,
            live_order=order,
            strategy_version=version_map.get(int(order.strategy_version_id)) if order is not None and order.strategy_version_id is not None else None,
            latest_promotion_evaluation=evaluation_map.get(int(order.strategy_version_id)) if order is not None and order.strategy_version_id is not None else None,
        )
        for fill, order in rows
    ]


def serialize_capital_reservation(reservation: CapitalReservation) -> dict[str, Any]:
    return {
        "id": reservation.id,
        "live_order_id": str(reservation.live_order_id) if reservation.live_order_id is not None else None,
        "condition_id": reservation.condition_id,
        "asset_id": reservation.asset_id,
        "reservation_kind": reservation.reservation_kind,
        "requested_amount": _serialize_decimal(reservation.requested_amount),
        "reserved_amount": _serialize_decimal(reservation.reserved_amount),
        "released_amount": _serialize_decimal(reservation.released_amount),
        "open_amount": _serialize_decimal(reservation.open_amount),
        "status": reservation.status,
        "source_kind": reservation.source_kind,
        "details_json": _json_safe(reservation.details_json),
        "observed_at_local": reservation.observed_at_local,
        "fingerprint": reservation.fingerprint,
        "created_at": reservation.created_at,
    }


async def list_live_orders(
    session: AsyncSession,
    *,
    asset_id: str | None = None,
    condition_id: str | None = None,
    status: str | None = None,
    strategy_family: str | None = None,
    approval_state: str | None = None,
    client_order_id: str | None = None,
    venue_order_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = select(LiveOrder).order_by(LiveOrder.created_at.desc())
    if asset_id:
        query = query.where(LiveOrder.asset_id == asset_id)
    if condition_id:
        query = query.where(LiveOrder.condition_id == condition_id)
    if status:
        query = query.where(LiveOrder.status == status)
    if strategy_family:
        query = query.where(LiveOrder.strategy_family == strategy_family)
    if approval_state:
        query = query.where(LiveOrder.approval_state == approval_state)
    if client_order_id:
        query = query.where(LiveOrder.client_order_id == client_order_id)
    if venue_order_id:
        query = query.where(LiveOrder.venue_order_id == venue_order_id)
    if start is not None:
        query = query.where(LiveOrder.created_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(LiveOrder.created_at <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return await serialize_live_orders_with_lifecycle(session, rows)


async def list_live_order_events(
    session: AsyncSession,
    *,
    asset_id: str | None = None,
    condition_id: str | None = None,
    status: str | None = None,
    strategy_family: str | None = None,
    approval_state: str | None = None,
    client_order_id: str | None = None,
    venue_order_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = (
        select(LiveOrderEvent, LiveOrder)
        .join(LiveOrder, LiveOrder.id == LiveOrderEvent.live_order_id)
        .order_by(LiveOrderEvent.observed_at_local.desc(), LiveOrderEvent.id.desc())
    )
    if asset_id:
        query = query.where(LiveOrder.asset_id == asset_id)
    if condition_id:
        query = query.where(LiveOrder.condition_id == condition_id)
    if status:
        query = query.where(LiveOrder.status == status)
    if strategy_family:
        query = query.where(LiveOrder.strategy_family == strategy_family)
    if approval_state:
        query = query.where(LiveOrder.approval_state == approval_state)
    if client_order_id:
        query = query.where(LiveOrder.client_order_id == client_order_id)
    if venue_order_id:
        query = query.where(LiveOrder.venue_order_id == venue_order_id)
    if start is not None:
        query = query.where(LiveOrderEvent.observed_at_local >= _ensure_utc(start))
    if end is not None:
        query = query.where(LiveOrderEvent.observed_at_local <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).all()
    return await serialize_live_order_events_with_lifecycle(session, rows)


async def list_live_fills(
    session: AsyncSession,
    *,
    asset_id: str | None = None,
    condition_id: str | None = None,
    status: str | None = None,
    strategy_family: str | None = None,
    approval_state: str | None = None,
    client_order_id: str | None = None,
    venue_order_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = (
        select(LiveFill, LiveOrder)
        .join(LiveOrder, LiveOrder.id == LiveFill.live_order_id, isouter=True)
        .order_by(LiveFill.observed_at_local.desc(), LiveFill.id.desc())
    )
    if asset_id:
        query = query.where(LiveFill.asset_id == asset_id)
    if condition_id:
        query = query.where(LiveFill.condition_id == condition_id)
    if status:
        query = query.where(LiveFill.fill_status == status)
    if client_order_id:
        query = query.where(LiveOrder.client_order_id == client_order_id)
    if venue_order_id:
        query = query.where(LiveOrder.venue_order_id == venue_order_id)
    if strategy_family:
        query = query.where(LiveOrder.strategy_family == strategy_family)
    if approval_state:
        query = query.where(LiveOrder.approval_state == approval_state)
    if start is not None:
        query = query.where(LiveFill.observed_at_local >= _ensure_utc(start))
    if end is not None:
        query = query.where(LiveFill.observed_at_local <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).all()
    return await serialize_live_fills_with_lifecycle(session, rows)


async def list_current_reservations(
    session: AsyncSession,
    *,
    asset_id: str | None = None,
    condition_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(CapitalReservation).order_by(
        CapitalReservation.observed_at_local.desc(),
        CapitalReservation.id.desc(),
    )
    if asset_id:
        query = query.where(CapitalReservation.asset_id == asset_id)
    if condition_id:
        query = query.where(CapitalReservation.condition_id == condition_id)
    rows = (await session.execute(query.limit(max(limit * 4, limit)))).scalars().all()
    latest_by_key: dict[str, CapitalReservation] = {}
    for row in rows:
        key = str(row.live_order_id) if row.live_order_id is not None else f"orphan:{row.id}"
        if key not in latest_by_key:
            latest_by_key[key] = row
    filtered = [row for row in latest_by_key.values() if row.open_amount > 0 and row.status not in {"released", "failed"}]
    filtered.sort(key=lambda row: (row.observed_at_local, row.id), reverse=True)
    return [serialize_capital_reservation(row) for row in filtered[:limit]]


async def compute_outstanding_reservations(session: AsyncSession) -> Decimal:
    rows = (await session.execute(
        select(CapitalReservation).order_by(CapitalReservation.observed_at_local.desc(), CapitalReservation.id.desc())
    )).scalars().all()
    latest_by_key: dict[str, CapitalReservation] = {}
    for row in rows:
        key = str(row.live_order_id) if row.live_order_id is not None else f"orphan:{row.id}"
        if key not in latest_by_key:
            latest_by_key[key] = row
    total = Decimal("0")
    for row in latest_by_key.values():
        if row.status in {"released", "failed"}:
            continue
        if row.open_amount > 0:
            total += row.open_amount
    polymarket_live_outstanding_reservations.set(float(total))
    return total


async def fetch_polymarket_live_status(session: AsyncSession) -> dict[str, Any]:
    from app.execution.polymarket_control_plane import get_active_pilot_config, get_open_pilot_run

    state = await fetch_live_state_row(session)
    outstanding_reservations = await compute_outstanding_reservations(session)
    outstanding_live_orders = int(
        len(
            (
                await session.execute(
                    select(LiveOrder).where(LiveOrder.status.not_in(LIVE_ORDER_TERMINAL_STATUSES))
                )
            ).scalars().all()
        )
    )
    recent_fill_cutoff = utcnow() - timedelta(hours=24)
    recent_fills_24h = int(
        len(
            (
                await session.execute(
                    select(LiveFill).where(LiveFill.observed_at_local >= recent_fill_cutoff)
                )
            ).scalars().all()
        )
    )
    kill_switch = effective_kill_switch_enabled(state)
    active_pilot = await get_active_pilot_config(session)
    active_pilot_run = await get_open_pilot_run(session, pilot_config_id=active_pilot.id) if active_pilot is not None else None
    polymarket_live_kill_switch.set(1 if kill_switch else 0)
    if state is not None and state.last_user_stream_message_at is not None:
        polymarket_live_last_user_stream_message_timestamp.set(state.last_user_stream_message_at.timestamp())
    if state is not None and state.last_reconcile_success_at is not None:
        polymarket_live_last_reconcile_success_timestamp.set(state.last_reconcile_success_at.timestamp())
    return {
        "enabled": settings.polymarket_live_trading_enabled,
        "dry_run": settings.polymarket_live_dry_run,
        "manual_approval_required": settings.polymarket_live_manual_approval_required,
        "decision_max_age_seconds": settings.polymarket_live_decision_max_age_seconds,
        "user_stream_enabled": settings.polymarket_user_stream_enabled,
        "kill_switch_enabled": kill_switch,
        "allowlist_markets": effective_market_allowlist(state),
        "allowlist_categories": effective_category_allowlist(state),
        "max_outstanding_notional_usd": (
            settings.polymarket_max_outstanding_notional_usd
            if settings.polymarket_max_outstanding_notional_usd > 0
            else None
        ),
        "gateway_reachable": bool(state.gateway_reachable) if state is not None and state.gateway_reachable is not None else False,
        "gateway_last_checked_at": state.gateway_last_checked_at if state is not None else None,
        "gateway_last_error": state.gateway_last_error if state is not None else None,
        "user_stream_connected": state.user_stream_connected if state is not None else False,
        "user_stream_session_id": state.user_stream_session_id if state is not None else None,
        "user_stream_connection_started_at": state.user_stream_connection_started_at if state is not None else None,
        "last_user_stream_message_at": state.last_user_stream_message_at if state is not None else None,
        "last_user_stream_error": state.last_user_stream_error if state is not None else None,
        "last_user_stream_error_at": state.last_user_stream_error_at if state is not None else None,
        "last_reconciled_user_event_id": state.last_reconciled_user_event_id if state is not None else None,
        "last_reconcile_started_at": state.last_reconcile_started_at if state is not None else None,
        "last_reconcile_success_at": state.last_reconcile_success_at if state is not None else None,
        "last_reconcile_error": state.last_reconcile_error if state is not None else None,
        "last_reconcile_error_at": state.last_reconcile_error_at if state is not None else None,
        "heartbeat_healthy": state.heartbeat_healthy if state is not None else None,
        "heartbeat_last_checked_at": state.heartbeat_last_checked_at if state is not None else None,
        "heartbeat_last_success_at": state.heartbeat_last_success_at if state is not None else None,
        "heartbeat_last_error": state.heartbeat_last_error if state is not None else None,
        "outstanding_live_orders": outstanding_live_orders,
        "outstanding_reservations": float(outstanding_reservations),
        "recent_fills_24h": recent_fills_24h,
        "live_submission_permitted": (
            settings.polymarket_live_trading_enabled
            and not settings.polymarket_live_dry_run
            and not kill_switch
            and active_pilot is not None
            and active_pilot.armed
            and active_pilot.live_enabled
            and active_pilot_run is not None
            and active_pilot_run.status != "paused"
        ),
    }
