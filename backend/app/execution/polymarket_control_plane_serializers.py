from __future__ import annotations

from typing import Any

from app.execution.polymarket_control_plane_utils import json_safe, serialize_decimal, to_decimal
from app.models.polymarket_pilot import (
    PolymarketControlPlaneIncident,
    PolymarketLiveShadowEvaluation,
    PolymarketPilotApprovalEvent,
    PolymarketPilotConfig,
    PolymarketPilotRun,
)


def serialize_pilot_config(row: PolymarketPilotConfig) -> dict[str, Any]:
    return {
        "id": row.id,
        "pilot_name": row.pilot_name,
        "strategy_family": row.strategy_family,
        "active": row.active,
        "armed": row.armed,
        "manual_approval_required": row.manual_approval_required,
        "live_enabled": row.live_enabled,
        "market_allowlist_json": row.market_allowlist_json,
        "category_allowlist_json": row.category_allowlist_json,
        "max_notional_per_order_usd": serialize_decimal(to_decimal(row.max_notional_per_order_usd)),
        "max_notional_per_day_usd": serialize_decimal(to_decimal(row.max_notional_per_day_usd)),
        "max_open_orders": row.max_open_orders,
        "max_plan_age_seconds": row.max_plan_age_seconds,
        "max_decision_age_seconds": row.max_decision_age_seconds,
        "max_slippage_bps": serialize_decimal(to_decimal(row.max_slippage_bps)),
        "require_complete_replay_coverage": row.require_complete_replay_coverage,
        "details_json": json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_pilot_run(row: PolymarketPilotRun) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "pilot_config_id": row.pilot_config_id,
        "status": row.status,
        "reason": row.reason,
        "started_at": row.started_at,
        "ended_at": row.ended_at,
        "details_json": json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_pilot_approval_event(row: PolymarketPilotApprovalEvent) -> dict[str, Any]:
    return {
        "id": row.id,
        "live_order_id": str(row.live_order_id) if row.live_order_id is not None else None,
        "execution_decision_id": str(row.execution_decision_id) if row.execution_decision_id is not None else None,
        "pilot_run_id": str(row.pilot_run_id) if row.pilot_run_id is not None else None,
        "action": row.action,
        "operator_identity": row.operator_identity,
        "reason_code": row.reason_code,
        "details_json": json_safe(row.details_json),
        "observed_at_local": row.observed_at_local,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_control_plane_incident(
    row: PolymarketControlPlaneIncident,
    *,
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": row.id,
        "pilot_run_id": str(row.pilot_run_id) if row.pilot_run_id is not None else None,
        "strategy_version_id": row.strategy_version_id,
        "strategy_version": strategy_version,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "severity": row.severity,
        "incident_type": row.incident_type,
        "live_order_id": str(row.live_order_id) if row.live_order_id is not None else None,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "details_json": json_safe(row.details_json),
        "observed_at_local": row.observed_at_local,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_shadow_evaluation(row: PolymarketLiveShadowEvaluation) -> dict[str, Any]:
    return {
        "id": row.id,
        "live_order_id": str(row.live_order_id) if row.live_order_id is not None else None,
        "execution_decision_id": str(row.execution_decision_id) if row.execution_decision_id is not None else None,
        "replay_run_id": str(row.replay_run_id) if row.replay_run_id is not None else None,
        "variant_name": row.variant_name,
        "expected_fill_price": serialize_decimal(to_decimal(row.expected_fill_price)),
        "actual_fill_price": serialize_decimal(to_decimal(row.actual_fill_price)),
        "expected_fill_size": serialize_decimal(to_decimal(row.expected_fill_size)),
        "actual_fill_size": serialize_decimal(to_decimal(row.actual_fill_size)),
        "expected_net_ev_bps": serialize_decimal(to_decimal(row.expected_net_ev_bps)),
        "realized_net_bps": serialize_decimal(to_decimal(row.realized_net_bps)),
        "gap_bps": serialize_decimal(to_decimal(row.gap_bps)),
        "reason_code": row.reason_code,
        "coverage_limited": row.coverage_limited,
        "details_json": json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


__all__ = [
    "serialize_control_plane_incident",
    "serialize_pilot_approval_event",
    "serialize_pilot_config",
    "serialize_pilot_run",
    "serialize_shadow_evaluation",
]
