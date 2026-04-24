from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.execution.polymarket_autonomy_state import build_active_autonomy_state
from app.execution.polymarket_control_plane_serializers import (
    serialize_control_plane_incident,
    serialize_pilot_config,
    serialize_pilot_run,
    serialize_shadow_evaluation,
)
from app.execution.polymarket_control_plane_utils import (
    BPS_Q,
    PRICE_Q,
    SUPPORTED_PHASE12_FAMILY,
    ZERO,
    effective_kill_switch_enabled,
)
from app.execution.polymarket_control_plane_utils import (
    approval_required as _approval_required,
)
from app.execution.polymarket_control_plane_utils import (
    details_with as _details_with,
)
from app.execution.polymarket_control_plane_utils import (
    ensure_utc as _ensure_utc,
)
from app.execution.polymarket_control_plane_utils import (
    guardrail_from_submission_reason as _guardrail_from_submission_reason,
)
from app.execution.polymarket_control_plane_utils import (
    heartbeat_status as _heartbeat_status,
)
from app.execution.polymarket_control_plane_utils import (
    json_safe as _json_safe,
)
from app.execution.polymarket_control_plane_utils import (
    live_order_notional as _live_order_notional,
)
from app.execution.polymarket_control_plane_utils import (
    normalize_strategy_family as _normalize_strategy_family,
)
from app.execution.polymarket_control_plane_utils import (
    pilot_limit_decimal as _pilot_limit_decimal,
)
from app.execution.polymarket_control_plane_utils import (
    pilot_limit_int as _pilot_limit_int,
)
from app.execution.polymarket_control_plane_utils import (
    price_gap_bps as _price_gap_bps,
)
from app.execution.polymarket_control_plane_utils import (
    serialize_decimal as _serialize_decimal,
)
from app.execution.polymarket_control_plane_utils import (
    stable_hash as _stable_hash,
)
from app.execution.polymarket_control_plane_utils import (
    to_decimal as _to_decimal,
)
from app.execution.polymarket_live_state import (
    LIVE_ORDER_TERMINAL_STATUSES,
    ensure_live_state_row,
    fetch_live_state_row,
    serialize_live_fills_with_lifecycle,
    serialize_live_order,
    serialize_live_order_events_with_lifecycle,
    serialize_live_orders_with_lifecycle,
)
from app.execution.polymarket_pilot_evidence import (
    PolymarketPilotEvidenceService,
    list_pilot_guardrail_events,
    list_pilot_readiness_reports,
    list_pilot_scorecards,
    resolve_pilot_strategy_family,
    resolve_pilot_strategy_version_id,
)
from app.ingestion.polymarket_common import utcnow
from app.metrics import (
    polymarket_control_plane_incidents_total,
    polymarket_heartbeat_healthy,
    polymarket_live_last_successful_fill_timestamp,
    polymarket_live_shadow_evaluations_total,
    polymarket_live_submissions_blocked_by_pilot_total,
    polymarket_pilot_failures_total,
    polymarket_pilot_manual_approvals_total,
    polymarket_pilot_runs_total,
    polymarket_restart_pauses_total,
    polymarket_shadow_gap_breaches_total,
)
from app.models.execution_decision import ExecutionDecision
from app.models.market_structure import MarketStructureOpportunity
from app.models.polymarket_live_execution import LiveFill, LiveOrder, LiveOrderEvent, PolymarketLiveState
from app.models.polymarket_maker import PolymarketQuoteRecommendation
from app.models.polymarket_metadata import PolymarketEventDim, PolymarketMarketDim
from app.models.polymarket_pilot import (
    PolymarketControlPlaneIncident,
    PolymarketLiveShadowEvaluation,
    PolymarketPilotApprovalEvent,
    PolymarketPilotConfig,
    PolymarketPilotRun,
)
from app.models.polymarket_raw import PolymarketBboEvent, PolymarketTradeTape
from app.models.polymarket_reconstruction import PolymarketBookReconState
from app.models.polymarket_replay import (
    PolymarketReplayFill,
    PolymarketReplayMetric,
    PolymarketReplayOrder,
    PolymarketReplayRun,
    PolymarketReplayScenario,
)
from app.models.strategy_registry import StrategyFamilyRegistry, StrategyVersion
from app.risk.budgets import build_strategy_budget_status, serialize_risk_budget_status
from app.strategies.promotion import (
    PROMOTION_EVALUATION_KIND_INCIDENT,
    hash_json_payload,
    map_incident_summary_to_promotion_verdict,
    record_promotion_eligibility_evaluation,
    rolling_promotion_window_bounds,
    upsert_promotion_evaluation,
)
from app.strategies.registry import (
    PROMOTION_GATE_POLICY_V1,
    get_current_strategy_version,
    get_latest_promotion_evaluation_by_version,
    get_strategy_version_snapshot_map,
    serialize_strategy_version_snapshot,
    sync_strategy_registry,
)

RUN_ACTIVE_STATUSES = {"armed", "running", "paused"}
REPLAY_SUCCESS_STATUSES = {"completed", "completed_warnings"}
_pilot_evidence = PolymarketPilotEvidenceService()


async def _serialize_control_plane_incidents_with_lifecycle(
    session: AsyncSession,
    rows: list[PolymarketControlPlaneIncident],
) -> list[dict[str, Any]]:
    version_ids = {int(row.strategy_version_id) for row in rows if row.strategy_version_id is not None}
    version_map = await get_strategy_version_snapshot_map(session, version_ids=version_ids)
    evaluation_map = await get_latest_promotion_evaluation_by_version(
        session,
        version_ids=version_ids,
        include_supporting=True,
    )
    return [
        serialize_control_plane_incident(
            row,
            strategy_version=version_map.get(int(row.strategy_version_id)) if row.strategy_version_id is not None else None,
            latest_promotion_evaluation=(
                evaluation_map.get(int(row.strategy_version_id))
                if row.strategy_version_id is not None
                else None
            ),
        )
        for row in rows
    ]


def _count_labels(values: list[str | None]) -> dict[str, int]:
    normalized = [
        str(value).strip()
        for value in values
        if value not in (None, "")
    ]
    return {
        label: sum(1 for value in normalized if value == label)
        for label in sorted(set(normalized))
    }


async def _record_phase13a_incident_evaluation(
    session: AsyncSession,
    *,
    row: PolymarketControlPlaneIncident,
    strategy_family: str | None,
) -> None:
    if row.strategy_version_id is None:
        return
    resolved_family = str(strategy_family or "").strip().lower() or None
    if resolved_family is None:
        family_match = (
            await session.execute(
                select(StrategyFamilyRegistry.family)
                .join(StrategyVersion, StrategyVersion.family_id == StrategyFamilyRegistry.id)
                .where(StrategyVersion.id == int(row.strategy_version_id))
                .limit(1)
            )
        ).scalar_one_or_none()
        resolved_family = str(family_match or "").strip().lower() or None
    if resolved_family is None:
        return

    registry_state = await sync_strategy_registry(session)
    family_row = registry_state["family_rows"].get(resolved_family)
    gate_policy = registry_state["gate_policy_rows"].get(PROMOTION_GATE_POLICY_V1)
    version_snapshot = (
        await get_strategy_version_snapshot_map(session, version_ids=[int(row.strategy_version_id)])
    ).get(int(row.strategy_version_id))
    if family_row is None or version_snapshot is None:
        return

    window_start, window_end = rolling_promotion_window_bounds(row.observed_at_local)
    if window_start is None or window_end is None:
        return

    incident_rows = (
        await session.execute(
            select(PolymarketControlPlaneIncident)
            .where(
                PolymarketControlPlaneIncident.strategy_version_id == row.strategy_version_id,
                PolymarketControlPlaneIncident.observed_at_local >= window_start,
                PolymarketControlPlaneIncident.observed_at_local <= window_end,
            )
            .order_by(PolymarketControlPlaneIncident.observed_at_local.desc(), PolymarketControlPlaneIncident.id.desc())
        )
    ).scalars().all()
    evaluation_status, recommended_tier = map_incident_summary_to_promotion_verdict(
        incident_count=len(incident_rows),
    )
    market_universe = sorted({
        str(value)
        for incident in incident_rows
        for value in (incident.condition_id, incident.asset_id)
        if value
    })
    summary = {
        "incident_count_24h": len(incident_rows),
        "incident_type_counts_24h": _count_labels([incident.incident_type for incident in incident_rows]),
        "severity_counts_24h": _count_labels([incident.severity for incident in incident_rows]),
        "latest_incident_type": row.incident_type,
        "latest_severity": row.severity,
    }
    provenance = {
        "source": "polymarket_control_plane_incident",
        "strategy_family": resolved_family,
        "strategy_version_key": version_snapshot["version_key"],
        "strategy_version_status": version_snapshot["version_status"],
        "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
        "promotion_gate_policy_label": gate_policy.label if gate_policy is not None else None,
        "incident_id": row.id,
        "live_order_id": str(row.live_order_id) if row.live_order_id is not None else None,
        "pilot_run_id": str(row.pilot_run_id) if row.pilot_run_id is not None else None,
        "rolling_window_hours": 24,
        "market_universe_hash": hash_json_payload(market_universe),
        "config_hash": hash_json_payload(
            {
                "rolling_window_hours": 24,
                "strategy_version_key": version_snapshot["version_key"],
                "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
            }
        ),
    }
    await upsert_promotion_evaluation(
        session,
        family_id=family_row.id,
        strategy_version_id=int(row.strategy_version_id),
        gate_policy_id=gate_policy.id if gate_policy is not None else None,
        evaluation_kind=PROMOTION_EVALUATION_KIND_INCIDENT,
        evaluation_status=evaluation_status,
        autonomy_tier=recommended_tier,
        evaluation_window_start=window_start,
        evaluation_window_end=window_end,
        provenance_json=provenance,
        summary_json=summary,
    )
    await record_promotion_eligibility_evaluation(
        session,
        strategy_version_id=int(row.strategy_version_id),
        trigger_kind=PROMOTION_EVALUATION_KIND_INCIDENT,
        trigger_ref=str(row.id),
        observed_at=row.observed_at_local,
    )


async def list_pilot_configs(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    active: bool | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    query = select(PolymarketPilotConfig).order_by(PolymarketPilotConfig.updated_at.desc(), PolymarketPilotConfig.id.desc())
    if strategy_family:
        query = query.where(PolymarketPilotConfig.strategy_family == _normalize_strategy_family(strategy_family))
    if active is not None:
        query = query.where(PolymarketPilotConfig.active.is_(active))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return [serialize_pilot_config(row) for row in rows]


async def get_pilot_config(session: AsyncSession, *, pilot_config_id: int) -> PolymarketPilotConfig | None:
    return await session.get(PolymarketPilotConfig, pilot_config_id)


async def get_active_pilot_config(session: AsyncSession) -> PolymarketPilotConfig | None:
    return (
        await session.execute(
            select(PolymarketPilotConfig)
            .where(PolymarketPilotConfig.active.is_(True))
            .order_by(PolymarketPilotConfig.updated_at.desc(), PolymarketPilotConfig.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


async def get_open_pilot_run(
    session: AsyncSession,
    *,
    pilot_config_id: int | None = None,
) -> PolymarketPilotRun | None:
    query = select(PolymarketPilotRun).where(
        PolymarketPilotRun.ended_at.is_(None),
        PolymarketPilotRun.status.in_(tuple(RUN_ACTIVE_STATUSES)),
    )
    if pilot_config_id is not None:
        query = query.where(PolymarketPilotRun.pilot_config_id == pilot_config_id)
    query = query.order_by(PolymarketPilotRun.started_at.desc(), PolymarketPilotRun.created_at.desc()).limit(1)
    return (await session.execute(query)).scalar_one_or_none()


async def _close_other_active_runs(
    session: AsyncSession,
    *,
    except_config_id: int | None = None,
    ended_status: str,
    reason: str,
    operator_identity: str | None,
) -> None:
    rows = (
        await session.execute(
            select(PolymarketPilotRun)
            .where(
                PolymarketPilotRun.ended_at.is_(None),
                PolymarketPilotRun.status.in_(tuple(RUN_ACTIVE_STATUSES)),
            )
            .order_by(PolymarketPilotRun.started_at.desc())
        )
    ).scalars().all()
    now = utcnow()
    for row in rows:
        if except_config_id is not None and row.pilot_config_id == except_config_id:
            continue
        row.status = ended_status
        row.reason = reason
        row.ended_at = now
        row.details_json = _details_with(
            row.details_json,
            ended_by=operator_identity,
            ended_reason=reason,
        )
        config = await session.get(PolymarketPilotConfig, row.pilot_config_id)
        if config is not None:
            config.active = False
            config.armed = False


async def create_or_update_pilot_config(
    session: AsyncSession,
    *,
    payload: dict[str, Any],
    pilot_config_id: int | None = None,
) -> dict[str, Any]:
    row = await session.get(PolymarketPilotConfig, pilot_config_id) if pilot_config_id is not None else None
    if row is None:
        pilot_name = str(payload.get("pilot_name") or "").strip()
        if not pilot_name:
            raise ValueError("pilot_name is required")
        row = PolymarketPilotConfig(
            pilot_name=pilot_name,
            strategy_family=_normalize_strategy_family(payload.get("strategy_family")),
            active=False,
            armed=False,
            manual_approval_required=bool(payload.get("manual_approval_required", settings.polymarket_pilot_require_manual_approval)),
            live_enabled=bool(payload.get("live_enabled", False)),
        )
        session.add(row)
    if "pilot_name" in payload and payload["pilot_name"] is not None:
        row.pilot_name = str(payload["pilot_name"]).strip()
    if "strategy_family" in payload and payload["strategy_family"] is not None:
        row.strategy_family = _normalize_strategy_family(payload["strategy_family"])
    if "active" in payload:
        row.active = bool(payload["active"])
    if "manual_approval_required" in payload:
        row.manual_approval_required = bool(payload["manual_approval_required"])
    if "live_enabled" in payload:
        row.live_enabled = bool(payload["live_enabled"])
    if "market_allowlist_json" in payload:
        row.market_allowlist_json = payload["market_allowlist_json"]
    if "category_allowlist_json" in payload:
        row.category_allowlist_json = payload["category_allowlist_json"]
    if "max_notional_per_order_usd" in payload:
        row.max_notional_per_order_usd = _to_decimal(payload["max_notional_per_order_usd"])
    if "max_notional_per_day_usd" in payload:
        row.max_notional_per_day_usd = _to_decimal(payload["max_notional_per_day_usd"])
    if "max_open_orders" in payload:
        row.max_open_orders = int(payload["max_open_orders"]) if payload["max_open_orders"] is not None else None
    if "max_plan_age_seconds" in payload:
        row.max_plan_age_seconds = int(payload["max_plan_age_seconds"]) if payload["max_plan_age_seconds"] is not None else None
    if "max_decision_age_seconds" in payload:
        row.max_decision_age_seconds = int(payload["max_decision_age_seconds"]) if payload["max_decision_age_seconds"] is not None else None
    if "max_slippage_bps" in payload:
        row.max_slippage_bps = _to_decimal(payload["max_slippage_bps"])
    if "require_complete_replay_coverage" in payload:
        row.require_complete_replay_coverage = bool(payload["require_complete_replay_coverage"])
    if "details_json" in payload:
        row.details_json = payload["details_json"]
    await session.flush()
    if row.active:
        active_rows = (
            await session.execute(
                select(PolymarketPilotConfig).where(
                    PolymarketPilotConfig.active.is_(True),
                    PolymarketPilotConfig.id != row.id,
                )
            )
        ).scalars().all()
        for other in active_rows:
            other.active = False
            other.armed = False
    return serialize_pilot_config(row)


async def list_pilot_runs(
    session: AsyncSession,
    *,
    status: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    query = select(PolymarketPilotRun).order_by(PolymarketPilotRun.started_at.desc(), PolymarketPilotRun.created_at.desc())
    if status:
        query = query.where(PolymarketPilotRun.status == status)
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return [serialize_pilot_run(row) for row in rows]


async def record_approval_event(
    session: AsyncSession,
    *,
    live_order: LiveOrder | None,
    pilot_run_id: uuid.UUID | None,
    action: str,
    operator_identity: str | None = None,
    reason_code: str | None = None,
    details: dict[str, Any] | None = None,
) -> PolymarketPilotApprovalEvent:
    row = PolymarketPilotApprovalEvent(
        live_order_id=live_order.id if live_order is not None else None,
        execution_decision_id=live_order.execution_decision_id if live_order is not None else None,
        pilot_run_id=pilot_run_id,
        action=action,
        operator_identity=operator_identity,
        reason_code=reason_code,
        details_json=_json_safe(details or {}),
        observed_at_local=utcnow(),
    )
    session.add(row)
    await session.flush()
    polymarket_pilot_manual_approvals_total.labels(action=action).inc()
    return row


async def append_live_order_event(
    session: AsyncSession,
    *,
    order: LiveOrder,
    event_type: str,
    source_kind: str,
    new_status: str | None = None,
    details: dict[str, Any] | None = None,
    venue_status: str | None = None,
    payload: dict[str, Any] | None = None,
) -> LiveOrderEvent:
    fingerprint = _stable_hash(
        {
            "live_order_id": str(order.id),
            "source_kind": source_kind,
            "event_type": event_type,
            "venue_status": venue_status,
            "payload": payload or {},
            "details": details or {},
        }
    )
    existing = (
        await session.execute(select(LiveOrderEvent).where(LiveOrderEvent.fingerprint == fingerprint))
    ).scalar_one_or_none()
    if existing is not None:
        return existing
    observed_at = utcnow()
    if new_status is not None:
        order.status = new_status
    order.last_event_at = observed_at
    if new_status in LIVE_ORDER_TERMINAL_STATUSES:
        order.completed_at = observed_at
    row = LiveOrderEvent(
        live_order_id=order.id,
        source_kind=source_kind,
        event_type=event_type,
        venue_status=venue_status,
        observed_at_local=observed_at,
        payload_json=_json_safe(payload) if payload is not None else None,
        details_json=_json_safe(details or {}),
        fingerprint=fingerprint,
    )
    session.add(row)
    await session.flush()
    return row


async def record_control_plane_incident(
    session: AsyncSession,
    *,
    severity: str,
    incident_type: str,
    details: dict[str, Any] | None = None,
    live_order: LiveOrder | None = None,
    pilot_run: PolymarketPilotRun | None = None,
    strategy_family: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
) -> PolymarketControlPlaneIncident:
    resolved_strategy_family = await resolve_pilot_strategy_family(
        session,
        live_order=live_order,
        strategy_family=strategy_family,
        pilot_run=pilot_run,
    )
    row = PolymarketControlPlaneIncident(
        pilot_run_id=pilot_run.id if pilot_run is not None else None,
        strategy_version_id=await resolve_pilot_strategy_version_id(
            session,
            live_order=live_order,
            strategy_family=resolved_strategy_family,
            pilot_run=pilot_run,
        ),
        severity=severity,
        incident_type=incident_type,
        live_order_id=live_order.id if live_order is not None else None,
        condition_id=condition_id or (live_order.condition_id if live_order is not None else None),
        asset_id=asset_id or (live_order.asset_id if live_order is not None else None),
        details_json=_json_safe(details or {}),
        observed_at_local=utcnow(),
    )
    session.add(row)
    await session.flush()
    await _record_phase13a_incident_evaluation(
        session,
        row=row,
        strategy_family=resolved_strategy_family,
    )
    polymarket_control_plane_incidents_total.labels(incident_type=incident_type, severity=severity).inc()
    return row


async def arm_pilot(
    session: AsyncSession,
    *,
    pilot_config_id: int,
    operator_identity: str | None = None,
) -> dict[str, Any]:
    if not settings.polymarket_pilot_enabled:
        raise ValueError("Pilot mode is disabled by config")
    config = await session.get(PolymarketPilotConfig, pilot_config_id)
    if config is None:
        raise LookupError(f"Pilot config not found: {pilot_config_id}")
    if _normalize_strategy_family(config.strategy_family) != SUPPORTED_PHASE12_FAMILY:
        raise ValueError(f"Phase 12 pilot currently supports {SUPPORTED_PHASE12_FAMILY} only")
    await _close_other_active_runs(
        session,
        except_config_id=config.id,
        ended_status="disarmed",
        reason="manual",
        operator_identity=operator_identity,
    )
    config.active = True
    config.armed = True
    existing = await get_open_pilot_run(session, pilot_config_id=config.id)
    if existing is None:
        existing = PolymarketPilotRun(
            pilot_config_id=config.id,
            status="armed",
            reason="manual",
            details_json={"armed_by": operator_identity},
        )
        session.add(existing)
    else:
        existing.status = "armed"
        existing.reason = "manual"
        existing.details_json = _details_with(existing.details_json, armed_by=operator_identity, resumed=False)
    await session.flush()
    polymarket_pilot_runs_total.labels(status=existing.status, reason=existing.reason).inc()
    return {
        "pilot_config": serialize_pilot_config(config),
        "pilot_run": serialize_pilot_run(existing),
    }


async def pause_active_pilot(
    session: AsyncSession,
    *,
    reason: str,
    operator_identity: str | None = None,
    details: dict[str, Any] | None = None,
    incident_type: str | None = None,
    live_order: LiveOrder | None = None,
) -> dict[str, Any] | None:
    config = await get_active_pilot_config(session)
    run = await get_open_pilot_run(session, pilot_config_id=config.id) if config is not None else None
    if config is None or run is None:
        return None
    run.status = "paused"
    run.reason = reason
    run.details_json = _details_with(run.details_json, paused_by=operator_identity, pause_reason=reason, **(details or {}))
    if incident_type:
        await record_control_plane_incident(
            session,
            severity="warning",
            incident_type=incident_type,
            details=details,
            live_order=live_order,
            pilot_run=run,
            strategy_family=config.strategy_family,
        )
    if incident_type == "restart_425":
        polymarket_restart_pauses_total.inc()
    polymarket_pilot_runs_total.labels(status=run.status, reason=run.reason).inc()
    await session.flush()
    return {
        "pilot_config": serialize_pilot_config(config),
        "pilot_run": serialize_pilot_run(run),
    }


async def resume_active_pilot(
    session: AsyncSession,
    *,
    operator_identity: str | None = None,
) -> dict[str, Any]:
    config = await get_active_pilot_config(session)
    if config is None:
        raise LookupError("No active pilot config")
    run = await get_open_pilot_run(session, pilot_config_id=config.id)
    if run is None:
        raise LookupError("No active pilot run")
    config.armed = True
    run.status = "armed"
    run.reason = "manual"
    run.details_json = _details_with(run.details_json, resumed_by=operator_identity)
    polymarket_pilot_runs_total.labels(status=run.status, reason=run.reason).inc()
    await session.flush()
    return {
        "pilot_config": serialize_pilot_config(config),
        "pilot_run": serialize_pilot_run(run),
    }


async def disarm_active_pilot(
    session: AsyncSession,
    *,
    operator_identity: str | None = None,
    reason: str = "manual",
) -> dict[str, Any] | None:
    config = await get_active_pilot_config(session)
    if config is None:
        return None
    run = await get_open_pilot_run(session, pilot_config_id=config.id)
    config.active = False
    config.armed = False
    if run is not None:
        run.status = "disarmed" if reason == "manual" else "aborted"
        run.reason = reason
        run.ended_at = utcnow()
        run.details_json = _details_with(run.details_json, disarmed_by=operator_identity, disarm_reason=reason)
        polymarket_pilot_runs_total.labels(status=run.status, reason=run.reason).inc()
        if run.status == "aborted":
            polymarket_pilot_failures_total.labels(reason=reason).inc()
    await session.flush()
    return {
        "pilot_config": serialize_pilot_config(config),
        "pilot_run": serialize_pilot_run(run) if run is not None else None,
    }


async def set_heartbeat_status(
    session: AsyncSession,
    *,
    healthy: bool | None,
    error: str | None = None,
) -> PolymarketLiveState:
    state = await ensure_live_state_row(session)
    state.heartbeat_healthy = healthy
    state.heartbeat_last_checked_at = utcnow()
    if healthy:
        state.heartbeat_last_success_at = state.heartbeat_last_checked_at
        state.heartbeat_last_error = None
        polymarket_heartbeat_healthy.set(1)
    elif healthy is False:
        state.heartbeat_last_error = error
        polymarket_heartbeat_healthy.set(0)
    else:
        polymarket_heartbeat_healthy.set(0)
    await session.flush()
    return state


async def active_live_order_count(
    session: AsyncSession,
    *,
    pilot_config: PolymarketPilotConfig | None,
) -> int:
    query = select(func.count(LiveOrder.id)).where(
        LiveOrder.dry_run.is_(False),
        LiveOrder.status.not_in(LIVE_ORDER_TERMINAL_STATUSES),
    )
    if pilot_config is not None:
        query = query.where(LiveOrder.pilot_config_id == pilot_config.id)
    return int((await session.execute(query)).scalar_one() or 0)


async def today_live_notional(
    session: AsyncSession,
    *,
    pilot_config: PolymarketPilotConfig | None,
) -> Decimal:
    today_start = datetime.combine(date.today(), datetime.min.time(), tzinfo=timezone.utc)
    rows = (
        await session.execute(
            select(LiveOrder).where(
                LiveOrder.dry_run.is_(False),
                LiveOrder.created_at >= today_start,
            )
        )
    ).scalars().all()
    total = ZERO
    for row in rows:
        if pilot_config is not None and row.pilot_config_id != pilot_config.id:
            continue
        total += _live_order_notional(row)
    return total.quantize(PRICE_Q)


async def _scope_allows_order(
    session: AsyncSession,
    *,
    order: LiveOrder,
    config: PolymarketPilotConfig | None,
) -> bool:
    market_allowlist = set(str(value).lower() for value in (config.market_allowlist_json or [])) if config and config.market_allowlist_json else set()
    category_allowlist = set(str(value).lower() for value in (config.category_allowlist_json or [])) if config and config.category_allowlist_json else set()
    if not market_allowlist and not category_allowlist:
        return True
    market_dim = await session.get(PolymarketMarketDim, order.market_dim_id) if order.market_dim_id is not None else None
    event_dim = await session.get(PolymarketEventDim, market_dim.event_dim_id) if market_dim is not None and market_dim.event_dim_id is not None else None
    if market_allowlist:
        observed = {value.lower() for value in (order.condition_id, market_dim.market_slug if market_dim is not None else None) if value}
        if observed.isdisjoint(market_allowlist):
            return False
    if category_allowlist:
        observed_category = str(event_dim.category or "").strip().lower() if event_dim is not None and event_dim.category else None
        if observed_category not in category_allowlist:
            return False
    return True


async def evaluate_live_submission(
    session: AsyncSession,
    *,
    order: LiveOrder,
    decision: ExecutionDecision | None,
) -> str | None:
    if order.dry_run:
        if order.manual_approval_required and order.approval_state != "approved":
            return "manual_approval_required"
        if order.approval_state == "expired":
            return "approval_expired"
        return None
    if not settings.polymarket_pilot_enabled:
        return "pilot_disabled"
    state = await fetch_live_state_row(session)
    if effective_kill_switch_enabled(state):
        return "kill_switch_enabled"
    if not settings.polymarket_live_trading_enabled:
        return "live_trading_disabled"
    config = await get_active_pilot_config(session)
    if config is None:
        return "pilot_not_active"
    if not config.armed:
        return "pilot_not_armed"
    if config.strategy_family != order.strategy_family:
        return "strategy_family_not_armed"
    if config.strategy_family != SUPPORTED_PHASE12_FAMILY:
        return "strategy_family_not_supported"
    if not config.live_enabled:
        return "pilot_live_disabled"
    run = await get_open_pilot_run(session, pilot_config_id=config.id)
    if run is None:
        return "pilot_run_missing"
    if run.status == "paused":
        return "pilot_paused"
    if order.strategy_version_id is not None:
        autonomy_state = await build_active_autonomy_state(
            session,
            strategy_family=order.strategy_family or config.strategy_family,
            strategy_version_id=int(order.strategy_version_id),
            supported_strategy_family=SUPPORTED_PHASE12_FAMILY,
            pilot_enabled=settings.polymarket_pilot_enabled,
            live_trading_enabled=settings.polymarket_live_trading_enabled,
            live_dry_run=settings.polymarket_live_dry_run,
            kill_switch_enabled=effective_kill_switch_enabled(state),
            manual_approval_required=order.manual_approval_required,
            live_submission_permitted=True,
            active_pilot=serialize_pilot_config(config),
            active_run=serialize_pilot_run(run),
        )
        if autonomy_state.get("demotion_active"):
            return "strategy_demoted"
    if order.pilot_config_id is not None and order.pilot_config_id != config.id:
        return "pilot_scope_mismatch"
    if order.manual_approval_required and order.approval_state != "approved":
        return "manual_approval_required"
    if order.approval_state == "expired":
        return "approval_expired"
    if order.approval_expires_at is not None and order.approval_state != "approved" and order.approval_expires_at < utcnow():
        return "approval_expired"
    if not await _scope_allows_order(session, order=order, config=config):
        return "pilot_scope_blocked"
    if decision is None:
        return "missing_execution_decision"
    max_decision_age = _pilot_limit_int(config, "max_decision_age_seconds", settings.polymarket_live_decision_max_age_seconds)
    if max_decision_age is not None and decision.decision_at < utcnow() - timedelta(seconds=max_decision_age):
        return "stale_execution_decision"
    if settings.polymarket_execution_policy_require_live_book:
        if decision.missing_orderbook_context or decision.stale_orderbook_context:
            return "untrusted_live_book_context"
        recon_state = (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == order.asset_id)
            )
        ).scalar_one_or_none()
        if recon_state is None or recon_state.status != "live":
            return "untrusted_recon_state"
    max_open_orders = _pilot_limit_int(config, "max_open_orders", settings.polymarket_pilot_max_open_orders)
    if max_open_orders is not None and await active_live_order_count(session, pilot_config=config) >= max_open_orders:
        return "pilot_max_open_orders_exceeded"
    max_notional_per_order = _pilot_limit_decimal(config, "max_notional_per_order_usd", settings.polymarket_pilot_max_daily_notional_usd)
    if max_notional_per_order is not None and _live_order_notional(order) > max_notional_per_order:
        return "pilot_max_notional_per_order_exceeded"
    max_daily_notional = _pilot_limit_decimal(config, "max_notional_per_day_usd", settings.polymarket_pilot_max_daily_notional_usd)
    if max_daily_notional is not None and (await today_live_notional(session, pilot_config=config) + _live_order_notional(order)) > max_daily_notional:
        return "pilot_daily_notional_exceeded"
    open_live_orders = await active_live_order_count(session, pilot_config=config)
    if open_live_orders > 0 and settings.polymarket_heartbeat_enabled and state is not None and state.heartbeat_healthy is False:
        return "heartbeat_degraded"
    return None


async def record_submission_block(
    session: AsyncSession,
    *,
    order: LiveOrder,
    reason: str,
    operator_identity: str | None = None,
) -> None:
    order.blocked_reason_code = reason
    if order.validation_error is None:
        order.validation_error = reason
    pilot_run = await get_open_pilot_run(session, pilot_config_id=order.pilot_config_id) if order.pilot_config_id is not None else None
    await append_live_order_event(
        session,
        order=order,
        source_kind="control_plane",
        event_type="submission_blocked",
        new_status="submit_blocked",
        details={"reason": reason, "operator": operator_identity},
    )
    await record_control_plane_incident(
        session,
        severity="warning",
        incident_type="submission_blocked",
        details={"reason": reason, "operator": operator_identity},
        live_order=order,
        pilot_run=pilot_run,
    )
    guardrail = _guardrail_from_submission_reason(reason)
    if guardrail is not None:
        guardrail_type, severity, action_taken = guardrail
        await _pilot_evidence.record_guardrail_event(
            session,
            strategy_family=order.strategy_family or SUPPORTED_PHASE12_FAMILY,
            guardrail_type=guardrail_type,
            severity=severity,
            action_taken=action_taken,
            live_order=order,
            pilot_run=pilot_run,
            details={"reason": reason, "operator": operator_identity},
        )
    polymarket_live_submissions_blocked_by_pilot_total.labels(reason=reason).inc()


async def register_restart_pause(
    session: AsyncSession,
    *,
    error: str,
    live_order: LiveOrder | None = None,
) -> None:
    await pause_active_pilot(
        session,
        reason="restart_window",
        details={"error": error},
        incident_type="restart_425",
        live_order=live_order,
    )
    pilot_run = await get_open_pilot_run(session, pilot_config_id=live_order.pilot_config_id) if live_order is not None and live_order.pilot_config_id is not None else None
    await _pilot_evidence.record_guardrail_event(
        session,
        strategy_family=(live_order.strategy_family if live_order is not None else SUPPORTED_PHASE12_FAMILY),
        guardrail_type="restart_pause",
        severity="error",
        action_taken="pause_pilot",
        live_order=live_order,
        pilot_run=pilot_run,
        details={"error": error},
    )


def is_restart_window_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code == 425:
        return True
    message = str(exc).lower()
    return " 425" in message or "status 425" in message or "restart" in message


def _structure_context_classification(row: MarketStructureOpportunity) -> str:
    if row.actionable:
        return "actionable_candidate"
    if row.executable or row.executable_all_legs:
        return "executable_candidate"
    return "informational"


def _structure_context_reason(row: MarketStructureOpportunity) -> str | None:
    if row.invalid_reason:
        return row.invalid_reason
    if isinstance(row.details_json, dict):
        reason_codes = row.details_json.get("reason_codes")
        if isinstance(reason_codes, list) and reason_codes:
            return str(reason_codes[0])
        reason_code = row.details_json.get("reason_code")
        if reason_code:
            return str(reason_code)
    return None


async def expire_stale_approvals(session: AsyncSession, *, now: datetime | None = None) -> int:
    observed_at = _ensure_utc(now) or utcnow()
    rows = (
        await session.execute(
            select(LiveOrder).where(
                LiveOrder.approval_state == "queued",
                LiveOrder.approval_expires_at.is_not(None),
                LiveOrder.approval_expires_at <= observed_at,
                LiveOrder.status.not_in(LIVE_ORDER_TERMINAL_STATUSES),
            )
        )
    ).scalars().all()
    for row in rows:
        row.approval_state = "expired"
        row.blocked_reason_code = "approval_expired"
        row.validation_error = "manual approval expired"
        await append_live_order_event(
            session,
            order=row,
            source_kind="control_plane",
            event_type="approval_expired",
            new_status="submit_blocked",
            details={"expired_at": observed_at},
        )
        await record_approval_event(
            session,
            live_order=row,
            pilot_run_id=row.pilot_run_id,
            action="expired",
            reason_code="approval_expired",
            details={"expired_at": observed_at},
        )
        await record_control_plane_incident(
            session,
            severity="warning",
            incident_type="approval_timeout",
            details={"expired_at": observed_at},
            live_order=row,
        )
        pilot_run = await get_open_pilot_run(session, pilot_config_id=row.pilot_config_id) if row.pilot_config_id is not None else None
        await _pilot_evidence.record_guardrail_event(
            session,
            strategy_family=row.strategy_family or SUPPORTED_PHASE12_FAMILY,
            guardrail_type="approval_ttl",
            severity="warning",
            action_taken="block",
            live_order=row,
            pilot_run=pilot_run,
            details={"expired_at": observed_at},
            observed_at=observed_at,
        )
    return len(rows)


async def list_approval_queue(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    approval_state: str | None = None,
    status: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = select(LiveOrder).where(LiveOrder.manual_approval_required.is_(True)).order_by(LiveOrder.created_at.desc())
    if strategy_family:
        query = query.where(LiveOrder.strategy_family == _normalize_strategy_family(strategy_family))
    if approval_state:
        query = query.where(LiveOrder.approval_state == approval_state)
    if status:
        query = query.where(LiveOrder.status == status)
    if condition_id:
        query = query.where(LiveOrder.condition_id == condition_id)
    if asset_id:
        query = query.where(LiveOrder.asset_id == asset_id)
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return [serialize_live_order(row) for row in rows]


async def list_control_plane_incidents(
    session: AsyncSession,
    *,
    incident_type: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = select(PolymarketControlPlaneIncident).order_by(
        PolymarketControlPlaneIncident.observed_at_local.desc(),
        PolymarketControlPlaneIncident.id.desc(),
    )
    if incident_type:
        query = query.where(PolymarketControlPlaneIncident.incident_type == incident_type)
    if condition_id:
        query = query.where(PolymarketControlPlaneIncident.condition_id == condition_id)
    if asset_id:
        query = query.where(PolymarketControlPlaneIncident.asset_id == asset_id)
    if start is not None:
        query = query.where(PolymarketControlPlaneIncident.observed_at_local >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketControlPlaneIncident.observed_at_local <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return await _serialize_control_plane_incidents_with_lifecycle(session, rows)

async def list_live_shadow_evaluations(
    session: AsyncSession,
    *,
    variant_name: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = (
        select(PolymarketLiveShadowEvaluation)
        .join(LiveOrder, LiveOrder.id == PolymarketLiveShadowEvaluation.live_order_id, isouter=True)
        .order_by(PolymarketLiveShadowEvaluation.updated_at.desc(), PolymarketLiveShadowEvaluation.id.desc())
    )
    if variant_name:
        query = query.where(PolymarketLiveShadowEvaluation.variant_name == variant_name)
    if condition_id:
        query = query.where(LiveOrder.condition_id == condition_id)
    if asset_id:
        query = query.where(LiveOrder.asset_id == asset_id)
    if start is not None:
        query = query.where(PolymarketLiveShadowEvaluation.updated_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketLiveShadowEvaluation.updated_at <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return [serialize_shadow_evaluation(row) for row in rows]


async def compute_live_shadow_summary(session: AsyncSession) -> dict[str, Any]:
    since = utcnow() - timedelta(hours=24)
    rows = (
        await session.execute(
            select(PolymarketLiveShadowEvaluation).where(PolymarketLiveShadowEvaluation.updated_at >= since)
        )
    ).scalars().all()
    gap_values = [abs(_to_decimal(row.gap_bps) or ZERO) for row in rows if row.gap_bps is not None and not row.coverage_limited]
    breach_threshold = Decimal(str(settings.polymarket_pilot_shadow_gap_breach_bps))
    breach_count = sum(1 for gap in gap_values if gap >= breach_threshold)
    avg_gap = (sum(gap_values, ZERO) / Decimal(len(gap_values))).quantize(BPS_Q) if gap_values else None
    worst_gap = max(gap_values) if gap_values else None
    return {
        "recent_count_24h": len(rows),
        "average_gap_bps_24h": _serialize_decimal(avg_gap),
        "worst_gap_bps_24h": _serialize_decimal(worst_gap),
        "breach_count_24h": breach_count,
    }


async def upsert_live_shadow_evaluation(
    session: AsyncSession,
    *,
    live_order: LiveOrder,
) -> dict[str, Any] | None:
    variant_name = live_order.strategy_family or SUPPORTED_PHASE12_FAMILY
    existing = (
        await session.execute(
            select(PolymarketLiveShadowEvaluation)
            .where(
                PolymarketLiveShadowEvaluation.live_order_id == live_order.id,
                PolymarketLiveShadowEvaluation.variant_name == variant_name,
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    replay_lookup = (
        await session.execute(
            select(PolymarketReplayOrder, PolymarketReplayScenario, PolymarketReplayRun)
            .join(PolymarketReplayScenario, PolymarketReplayScenario.id == PolymarketReplayOrder.scenario_id)
            .join(PolymarketReplayRun, PolymarketReplayRun.id == PolymarketReplayScenario.run_id)
            .where(
                PolymarketReplayOrder.source_execution_decision_id == live_order.execution_decision_id,
                PolymarketReplayOrder.variant_name == variant_name,
                PolymarketReplayRun.status.in_(tuple(REPLAY_SUCCESS_STATUSES)),
            )
            .order_by(PolymarketReplayOrder.decision_ts.desc(), PolymarketReplayOrder.id.desc())
            .limit(1)
        )
    ).first()
    replay_order = replay_lookup[0] if replay_lookup is not None else None
    scenario = replay_lookup[1] if replay_lookup is not None else None
    replay_run = replay_lookup[2] if replay_lookup is not None else None
    replay_fills: list[PolymarketReplayFill] = []
    metric_row: PolymarketReplayMetric | None = None
    if replay_order is not None:
        replay_fills = (
            await session.execute(
                select(PolymarketReplayFill)
                .where(PolymarketReplayFill.replay_order_id == replay_order.id)
                .order_by(PolymarketReplayFill.fill_ts.asc(), PolymarketReplayFill.fill_index.asc())
            )
        ).scalars().all()
        metric_row = (
            await session.execute(
                select(PolymarketReplayMetric).where(
                    PolymarketReplayMetric.scenario_id == replay_order.scenario_id,
                    PolymarketReplayMetric.variant_name == variant_name,
                    PolymarketReplayMetric.metric_scope == "scenario",
                )
            )
        ).scalar_one_or_none()
    expected_fill_size = sum((_to_decimal(fill.size) or ZERO for fill in replay_fills), ZERO) if replay_fills else _to_decimal(replay_order.submitted_size if replay_order is not None else None) or _to_decimal(replay_order.requested_size if replay_order is not None else None)
    weighted_notional = sum(((_to_decimal(fill.size) or ZERO) * (_to_decimal(fill.price) or ZERO) for fill in replay_fills), ZERO)
    expected_fill_price = (weighted_notional / expected_fill_size).quantize(PRICE_Q) if replay_fills and expected_fill_size and expected_fill_size > ZERO else _to_decimal(replay_order.limit_price if replay_order is not None else None)
    actual_fill_price = _to_decimal(live_order.avg_fill_price)
    actual_fill_size = _to_decimal(live_order.filled_size)
    decision = await session.get(ExecutionDecision, live_order.execution_decision_id) if live_order.execution_decision_id is not None else None
    expected_net_ev_bps = _to_decimal(decision.chosen_est_net_ev_bps if decision is not None else None)
    gap_bps = _price_gap_bps(expected=expected_fill_price, actual=actual_fill_price, side=live_order.side)
    realized_net_bps = (expected_net_ev_bps - gap_bps).quantize(BPS_Q) if expected_net_ev_bps is not None and gap_bps is not None else None
    coverage_limited = (
        replay_order is None
        or scenario is None
        or scenario.status == "coverage_limited"
        or bool(metric_row is not None and isinstance(metric_row.details_json, dict) and metric_row.details_json.get("coverage_limited"))
        or expected_fill_price is None
        or actual_fill_price is None
        or actual_fill_size in (None, ZERO)
    )
    row = existing or PolymarketLiveShadowEvaluation(
        live_order_id=live_order.id,
        execution_decision_id=live_order.execution_decision_id,
        variant_name=variant_name,
    )
    row.replay_run_id = replay_run.id if replay_run is not None else None
    row.expected_fill_price = expected_fill_price
    row.actual_fill_price = actual_fill_price
    row.expected_fill_size = expected_fill_size
    row.actual_fill_size = actual_fill_size
    row.expected_net_ev_bps = expected_net_ev_bps
    row.realized_net_bps = realized_net_bps
    row.gap_bps = gap_bps
    row.reason_code = "coverage_limited" if coverage_limited else "replay_matched"
    row.coverage_limited = coverage_limited
    row.details_json = {
        "replay_order_id": replay_order.id if replay_order is not None else None,
        "scenario_id": replay_order.scenario_id if replay_order is not None else None,
        "scenario_status": scenario.status if scenario is not None else None,
        "replay_fill_count": len(replay_fills),
    }
    if existing is None:
        session.add(row)
    await session.flush()
    polymarket_live_shadow_evaluations_total.labels(
        variant_name=variant_name,
        coverage_limited=str(bool(coverage_limited)).lower(),
    ).inc()
    if live_order.dry_run is False and (actual_fill_size or ZERO) > ZERO:
        polymarket_live_last_successful_fill_timestamp.set(utcnow().timestamp())
    if gap_bps is not None and not coverage_limited and gap_bps >= Decimal(str(settings.polymarket_pilot_shadow_gap_breach_bps)):
        polymarket_shadow_gap_breaches_total.labels(variant_name=variant_name).inc()
        await record_control_plane_incident(
            session,
            severity="warning",
            incident_type="shadow_gap_breach",
            details={"gap_bps": gap_bps, "threshold_bps": settings.polymarket_pilot_shadow_gap_breach_bps},
            live_order=live_order,
        )
        pilot_run = await get_open_pilot_run(session, pilot_config_id=live_order.pilot_config_id) if live_order.pilot_config_id is not None else None
        await _pilot_evidence.record_guardrail_event(
            session,
            strategy_family=variant_name,
            guardrail_type="shadow_gap_breach",
            severity="warning",
            action_taken="pause_pilot" if settings.polymarket_pilot_pause_on_shadow_gap_breach else "warn",
            live_order=live_order,
            pilot_run=pilot_run,
            trigger_value=gap_bps,
            threshold_value=settings.polymarket_pilot_shadow_gap_breach_bps,
            details={"gap_bps": gap_bps, "threshold_bps": settings.polymarket_pilot_shadow_gap_breach_bps},
        )
        if settings.polymarket_pilot_pause_on_shadow_gap_breach:
            await pause_active_pilot(
                session,
                reason="incident",
                details={"gap_bps": str(gap_bps), "variant_name": variant_name},
                incident_type="shadow_gap_breach",
                live_order=live_order,
            )
    return serialize_shadow_evaluation(row)


async def evaluate_recent_live_shadow(
    session: AsyncSession,
    *,
    limit: int = 20,
) -> int:
    rows = (
        await session.execute(
            select(LiveOrder)
            .where(
                LiveOrder.dry_run.is_(False),
                LiveOrder.filled_size > ZERO,
                LiveOrder.execution_decision_id.is_not(None),
            )
            .order_by(LiveOrder.updated_at.desc(), LiveOrder.created_at.desc())
            .limit(limit)
        )
    ).scalars().all()
    count = 0
    for row in rows:
        await upsert_live_shadow_evaluation(session, live_order=row)
        count += 1
    return count


async def fetch_pilot_status(session: AsyncSession) -> dict[str, Any]:
    state = await fetch_live_state_row(session)
    config = await get_active_pilot_config(session)
    run = await get_open_pilot_run(session, pilot_config_id=config.id) if config is not None else None
    incidents_since = utcnow() - timedelta(hours=24)
    family_for_state = (
        config.strategy_family
        if config is not None and config.strategy_family
        else settings.polymarket_pilot_default_strategy_family
    )
    strategy_version = (
        await get_current_strategy_version(session, family_for_state)
        if family_for_state
        else None
    )
    latest_evaluation = None
    if strategy_version is not None and strategy_version.id is not None:
        latest_evaluation = (
            await get_latest_promotion_evaluation_by_version(
                session,
                version_ids=[int(strategy_version.id)],
                include_supporting=True,
            )
        ).get(int(strategy_version.id))
    active_family_budget = None
    if config is not None and config.strategy_family:
        active_family_budget = serialize_risk_budget_status(
            await build_strategy_budget_status(
                session,
                strategy_family=config.strategy_family,
                strategy_version_id=int(strategy_version.id) if strategy_version is not None and strategy_version.id is not None else None,
            )
        )
    live_submission_permitted = (
        settings.polymarket_live_trading_enabled
        and not settings.polymarket_live_dry_run
        and not effective_kill_switch_enabled(state)
        and config is not None
        and config.armed
        and config.live_enabled
        and run is not None
        and run.status != "paused"
    )
    active_pilot_payload = serialize_pilot_config(config) if config is not None else None
    active_run_payload = serialize_pilot_run(run) if run is not None else None
    active_autonomy_state = await build_active_autonomy_state(
        session,
        strategy_family=family_for_state,
        family_source=(
            "active_pilot_config"
            if config is not None and config.strategy_family
            else "settings_default"
            if family_for_state
            else "unresolved"
        ),
        strategy_version=serialize_strategy_version_snapshot(strategy_version),
        strategy_version_id=int(strategy_version.id) if strategy_version is not None and strategy_version.id is not None else None,
        latest_promotion_evaluation=latest_evaluation,
        risk_budget_status=active_family_budget,
        supported_strategy_family=SUPPORTED_PHASE12_FAMILY,
        pilot_enabled=settings.polymarket_pilot_enabled,
        live_trading_enabled=settings.polymarket_live_trading_enabled,
        live_dry_run=settings.polymarket_live_dry_run,
        kill_switch_enabled=effective_kill_switch_enabled(state),
        manual_approval_required=_approval_required(config),
        live_submission_permitted=live_submission_permitted,
        active_pilot=active_pilot_payload,
        active_run=active_run_payload,
    )
    approval_queue_count = int(
        (
            await session.execute(
                select(func.count(LiveOrder.id)).where(LiveOrder.approval_state == "queued")
            )
        ).scalar_one()
        or 0
    )
    incidents_24h = int(
        (
            await session.execute(
                select(func.count(PolymarketControlPlaneIncident.id)).where(
                    PolymarketControlPlaneIncident.observed_at_local >= incidents_since
                )
            )
        ).scalar_one()
        or 0
    )
    open_live_orders = await active_live_order_count(session, pilot_config=config)
    recent_incidents = await list_control_plane_incidents(
        session,
        start=incidents_since,
        limit=5,
    )
    return {
        "pilot_enabled": settings.polymarket_pilot_enabled,
        "supported_strategy_family": SUPPORTED_PHASE12_FAMILY,
        "default_strategy_family": settings.polymarket_pilot_default_strategy_family,
        "active_pilot": active_pilot_payload,
        "active_run": active_run_payload,
        "active_strategy_version": serialize_strategy_version_snapshot(strategy_version),
        "latest_promotion_evaluation": latest_evaluation,
        "active_family_budget": active_family_budget,
        "active_autonomy_state": active_autonomy_state,
        "manual_approval_required": _approval_required(config),
        "approval_queue_count": approval_queue_count,
        "heartbeat_status": _heartbeat_status(state, needed=bool(config is not None and config.armed and open_live_orders > 0)),
        "heartbeat_last_checked_at": state.heartbeat_last_checked_at if state is not None else None,
        "heartbeat_last_success_at": state.heartbeat_last_success_at if state is not None else None,
        "heartbeat_last_error": state.heartbeat_last_error if state is not None else None,
        "recent_incident_count_24h": incidents_24h,
        "recent_incidents": recent_incidents,
        "open_live_order_count": open_live_orders,
        "kill_switch_enabled": effective_kill_switch_enabled(state),
    }


async def fetch_execution_console_summary(session: AsyncSession) -> dict[str, Any]:
    pilot_status = await fetch_pilot_status(session)
    active_config = await get_active_pilot_config(session)
    active_family = active_config.strategy_family if active_config is not None else SUPPORTED_PHASE12_FAMILY
    active_family_budget = serialize_risk_budget_status(
        await build_strategy_budget_status(
            session,
            strategy_family=active_family,
        )
    )
    evidence_summary = await _pilot_evidence.fetch_pilot_evidence_summary(
        session,
        strategy_family=active_family,
    )
    recent_orders = (
        await session.execute(
            select(LiveOrder).order_by(LiveOrder.created_at.desc(), LiveOrder.updated_at.desc()).limit(25)
        )
    ).scalars().all()
    recent_fills = (
        await session.execute(
            select(LiveFill).order_by(LiveFill.observed_at_local.desc(), LiveFill.id.desc()).limit(25)
        )
    ).scalars().all()
    recent_events = (
        await session.execute(
            select(LiveOrderEvent)
            .order_by(LiveOrderEvent.observed_at_local.desc(), LiveOrderEvent.id.desc())
            .limit(25)
        )
    ).scalars().all()
    blocked_events = (
        await session.execute(
            select(LiveOrderEvent)
            .where(LiveOrderEvent.event_type == "submission_blocked")
            .order_by(LiveOrderEvent.observed_at_local.desc(), LiveOrderEvent.id.desc())
            .limit(25)
        )
    ).scalars().all()
    related_order_ids = {
        row.live_order_id
        for row in [*recent_events, *blocked_events]
        if row.live_order_id is not None
    }
    related_order_ids.update(row.live_order_id for row in recent_fills if row.live_order_id is not None)
    related_orders = (
        await session.execute(
            select(LiveOrder).where(LiveOrder.id.in_(tuple(related_order_ids)))
        )
    ).scalars().all() if related_order_ids else []
    order_map = {row.id: row for row in related_orders}
    recent_event_pairs = [(row, order_map.get(row.live_order_id)) for row in recent_events]
    blocked_event_pairs = [(row, order_map.get(row.live_order_id)) for row in blocked_events]
    recent_fill_pairs = [(row, order_map.get(row.live_order_id) if row.live_order_id is not None else None) for row in recent_fills]
    return {
        "pilot": pilot_status,
        "active_pilot_family": active_config.strategy_family if active_config is not None else None,
        "active_family_budget": active_family_budget,
        "active_autonomy_state": pilot_status.get("active_autonomy_state"),
        "approvals": await list_approval_queue(session, approval_state="queued", limit=25),
        "incidents": await list_control_plane_incidents(session, limit=25),
        "guardrail_events": await list_pilot_guardrail_events(
            session,
            strategy_family=active_family,
            limit=25,
        ),
        "scorecards": await list_pilot_scorecards(
            session,
            strategy_family=active_family,
            limit=10,
        ),
        "readiness_reports": await list_pilot_readiness_reports(
            session,
            strategy_family=active_family,
            limit=10,
        ),
        "recent_orders": await serialize_live_orders_with_lifecycle(session, recent_orders),
        "recent_fills": await serialize_live_fills_with_lifecycle(session, recent_fill_pairs),
        "recent_order_events": await serialize_live_order_events_with_lifecycle(session, recent_event_pairs),
        "recent_blocked_submissions": await serialize_live_order_events_with_lifecycle(session, blocked_event_pairs),
        "live_shadow_summary": await compute_live_shadow_summary(session),
        "evidence_summary": evidence_summary,
    }


async def fetch_market_tape_view(
    session: AsyncSession,
    *,
    condition_id: str | None = None,
    asset_id: str | None = None,
    limit: int = 25,
) -> dict[str, Any]:
    bbo_query: Select[Any] = select(PolymarketBboEvent).order_by(PolymarketBboEvent.event_ts_exchange.desc(), PolymarketBboEvent.id.desc())
    trade_query: Select[Any] = select(PolymarketTradeTape).order_by(PolymarketTradeTape.event_ts_exchange.desc(), PolymarketTradeTape.id.desc())
    order_query: Select[Any] = select(LiveOrder).order_by(LiveOrder.created_at.desc(), LiveOrder.updated_at.desc())
    event_query: Select[Any] = select(LiveOrderEvent).join(LiveOrder, LiveOrder.id == LiveOrderEvent.live_order_id).order_by(
        LiveOrderEvent.observed_at_local.desc(),
        LiveOrderEvent.id.desc(),
    )
    recon_query: Select[Any] = select(PolymarketBookReconState).order_by(PolymarketBookReconState.last_reconciled_at.desc())
    structure_query: Select[Any] = select(MarketStructureOpportunity).order_by(
        MarketStructureOpportunity.created_at.desc(),
        MarketStructureOpportunity.id.desc(),
    )
    quote_query: Select[Any] = select(PolymarketQuoteRecommendation).order_by(
        PolymarketQuoteRecommendation.created_at.desc(),
    )
    if condition_id:
        bbo_query = bbo_query.where(PolymarketBboEvent.condition_id == condition_id)
        trade_query = trade_query.where(PolymarketTradeTape.condition_id == condition_id)
        order_query = order_query.where(LiveOrder.condition_id == condition_id)
        event_query = event_query.where(LiveOrder.condition_id == condition_id)
        recon_query = recon_query.where(PolymarketBookReconState.condition_id == condition_id)
        structure_query = structure_query.where(MarketStructureOpportunity.anchor_condition_id == condition_id)
        quote_query = quote_query.where(PolymarketQuoteRecommendation.condition_id == condition_id)
    if asset_id:
        bbo_query = bbo_query.where(PolymarketBboEvent.asset_id == asset_id)
        trade_query = trade_query.where(PolymarketTradeTape.asset_id == asset_id)
        order_query = order_query.where(LiveOrder.asset_id == asset_id)
        event_query = event_query.where(LiveOrder.asset_id == asset_id)
        recon_query = recon_query.where(PolymarketBookReconState.asset_id == asset_id)
        structure_query = structure_query.where(MarketStructureOpportunity.anchor_asset_id == asset_id)
        quote_query = quote_query.where(PolymarketQuoteRecommendation.asset_id == asset_id)
    bbo_rows = (await session.execute(bbo_query.limit(limit))).scalars().all()
    trade_rows = (await session.execute(trade_query.limit(limit))).scalars().all()
    order_rows = (await session.execute(order_query.limit(limit))).scalars().all()
    event_rows = (await session.execute(event_query.limit(limit))).scalars().all()
    event_order_map = {row.id: row for row in order_rows}
    missing_event_order_ids = {
        row.live_order_id
        for row in event_rows
        if row.live_order_id is not None and row.live_order_id not in event_order_map
    }
    if missing_event_order_ids:
        extra_event_orders = (
            await session.execute(
                select(LiveOrder).where(LiveOrder.id.in_(tuple(missing_event_order_ids)))
            )
        ).scalars().all()
        event_order_map.update({row.id: row for row in extra_event_orders})
    recon_row = (await session.execute(recon_query.limit(1))).scalar_one_or_none()
    structure_rows = (await session.execute(structure_query.limit(5))).scalars().all()
    quote_rows = (await session.execute(quote_query.limit(5))).scalars().all()
    return {
        "selected_condition_id": condition_id or (recon_row.condition_id if recon_row is not None else None),
        "selected_asset_id": asset_id or (recon_row.asset_id if recon_row is not None else None),
        "recon_state": {
            "condition_id": recon_row.condition_id,
            "asset_id": recon_row.asset_id,
            "status": recon_row.status,
            "best_bid": _serialize_decimal(_to_decimal(recon_row.best_bid)),
            "best_ask": _serialize_decimal(_to_decimal(recon_row.best_ask)),
            "spread": _serialize_decimal(_to_decimal(recon_row.spread)),
            "last_reconciled_at": recon_row.last_reconciled_at,
        } if recon_row is not None else None,
        "bbo": [
            {
                "id": row.id,
                "condition_id": row.condition_id,
                "asset_id": row.asset_id,
                "event_ts_exchange": row.event_ts_exchange,
                "best_bid": _serialize_decimal(_to_decimal(row.best_bid)),
                "best_ask": _serialize_decimal(_to_decimal(row.best_ask)),
                "spread": _serialize_decimal(_to_decimal(row.spread)),
            }
            for row in bbo_rows
        ],
        "trades": [
            {
                "id": row.id,
                "condition_id": row.condition_id,
                "asset_id": row.asset_id,
                "event_ts_exchange": row.event_ts_exchange,
                "price": _serialize_decimal(_to_decimal(row.price)),
                "size": _serialize_decimal(_to_decimal(row.size)),
                "side": row.side,
                "outcome_name": row.outcome_name,
            }
            for row in trade_rows
        ],
        "live_orders": await serialize_live_orders_with_lifecycle(session, order_rows),
        "live_order_events": await serialize_live_order_events_with_lifecycle(
            session,
            [
                (row, event_order_map.get(row.live_order_id))
                for row in event_rows
            ],
        ),
        "structure_context": [
            {
                "id": row.id,
                "condition_id": row.anchor_condition_id,
                "opportunity_type": row.opportunity_type,
                "classification": _structure_context_classification(row),
                "reason_code": _structure_context_reason(row),
                "created_at": row.created_at,
            }
            for row in structure_rows
        ],
        "quote_context": [
            {
                "id": str(row.id),
                "condition_id": row.condition_id,
                "asset_id": row.asset_id,
                "status": row.status,
                "recommendation_action": row.recommendation_action,
                "created_at": row.created_at,
            }
            for row in quote_rows
        ],
    }
