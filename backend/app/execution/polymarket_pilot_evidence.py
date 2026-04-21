from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.ingestion.polymarket_common import utcnow
from app.metrics import (
    polymarket_pilot_approval_expirations_total,
    polymarket_pilot_daily_realized_pnl_usd,
    polymarket_pilot_guardrail_triggers_total,
    polymarket_pilot_latest_readiness_report_timestamp,
    polymarket_pilot_readiness_reports_total,
    polymarket_pilot_scorecards_total,
    polymarket_pilot_shadow_gap_breach_events_total,
)
from app.models.polymarket_live_execution import (
    CapitalReservation,
    LiveFill,
    LiveOrder,
    PositionLot,
    PositionLotEvent,
)
from app.models.polymarket_pilot import (
    PolymarketControlPlaneIncident,
    PolymarketLiveShadowEvaluation,
    PolymarketPilotApprovalEvent,
    PolymarketPilotConfig,
    PolymarketPilotGuardrailEvent,
    PolymarketPilotReadinessReport,
    PolymarketPilotRun,
    PolymarketPilotScorecard,
)
from app.strategies.promotion import (
    PROMOTION_EVALUATION_KIND_PILOT_READINESS,
    hash_json_payload,
    map_readiness_status_to_promotion_verdict,
    upsert_promotion_evaluation,
)
from app.strategies.registry import (
    PROMOTION_GATE_POLICY_V1,
    get_current_strategy_version,
    get_latest_promotion_evaluation_by_version,
    get_strategy_version_snapshot_map,
    sync_strategy_registry,
)

SUPPORTED_PHASE12_FAMILY = "exec_policy"
ACTIVE_FILL_STATUSES = {"matched", "mined", "confirmed"}
OPEN_LOT_STATUSES = {"open", "partially_closed"}
SERIOUS_GUARDRAIL_ACTIONS = {"pause_pilot", "disarm_pilot", "kill_switch"}
ZERO = Decimal("0")
PRICE_Q = Decimal("0.00000001")


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


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


async def resolve_pilot_strategy_version_id(
    session: AsyncSession,
    *,
    live_order: LiveOrder | None = None,
    strategy_family: str | None = None,
    pilot_run: PolymarketPilotRun | None = None,
) -> int | None:
    if live_order is not None and live_order.strategy_version_id is not None:
        return int(live_order.strategy_version_id)

    family: str | None = None
    if live_order is not None and live_order.strategy_family:
        family = _normalized_strategy_family(live_order.strategy_family)
    elif strategy_family:
        family = _normalized_strategy_family(strategy_family)
    elif pilot_run is not None and pilot_run.pilot_config_id is not None:
        config = await session.get(PolymarketPilotConfig, pilot_run.pilot_config_id)
        if config is not None and config.strategy_family:
            family = _normalized_strategy_family(config.strategy_family)

    if not family:
        return None

    strategy_version = await get_current_strategy_version(session, family)
    if strategy_version is None or strategy_version.id is None:
        return None
    return int(strategy_version.id)


async def _record_phase13a_readiness_evaluation(
    session: AsyncSession,
    *,
    family: str,
    strategy_version,
    scorecard_row: PolymarketPilotScorecard | None,
    readiness_row: PolymarketPilotReadinessReport,
    readiness_status: str,
    blockers: list[str],
    candidate_scorecards: list[PolymarketPilotScorecard],
    candidate_avg_gap: Decimal | None,
    candidate_gap_threshold: Decimal,
    incidents: list[PolymarketControlPlaneIncident],
    backlog_orders: list[LiveOrder],
    live_orders: list[LiveOrder],
) -> None:
    if strategy_version is None:
        return

    registry_state = await sync_strategy_registry(session)
    family_row = registry_state["family_rows"].get(family)
    gate_policy = registry_state["gate_policy_rows"].get(PROMOTION_GATE_POLICY_V1)
    if family_row is None:
        return

    evaluation_status, recommended_tier = map_readiness_status_to_promotion_verdict(readiness_status)
    execution_policy_versions = sorted(
        {
            str(row.policy_version).strip()
            for row in live_orders
            if row.policy_version not in (None, "")
        }
    )
    market_universe = sorted(
        {
            str(value)
            for row in live_orders
            for value in (row.condition_id, row.asset_id)
            if value
        }
    )
    config_payload = {
        "strategy_version_key": strategy_version.version_key,
        "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
        "candidate_min_days": settings.polymarket_pilot_semi_auto_candidate_min_days,
        "candidate_max_avg_shadow_gap_bps": settings.polymarket_pilot_semi_auto_max_avg_shadow_gap_bps,
        "approval_ttl_seconds": settings.polymarket_pilot_approval_ttl_seconds,
    }
    summary = {
        "readiness_status": readiness_status,
        "recommended_tier": recommended_tier,
        "scorecard_status": scorecard_row.status if scorecard_row is not None else None,
        "scorecard_net_pnl": _serialize_decimal(_to_decimal(scorecard_row.net_pnl) if scorecard_row is not None else None),
        "scorecard_coverage_limited_count": scorecard_row.coverage_limited_count if scorecard_row is not None else None,
        "readiness_blockers": blockers,
        "approval_backlog_count": len(backlog_orders),
        "incident_count": len(incidents),
        "candidate_scorecard_days": len(candidate_scorecards),
        "candidate_avg_shadow_gap_bps": _serialize_decimal(candidate_avg_gap),
        "candidate_gap_threshold_bps": _serialize_decimal(candidate_gap_threshold),
        "live_order_count": len(live_orders),
    }
    provenance = {
        "source": "polymarket_pilot_readiness_report",
        "strategy_family": family,
        "strategy_version_key": strategy_version.version_key,
        "strategy_version_status": strategy_version.version_status,
        "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
        "promotion_gate_policy_label": gate_policy.label if gate_policy is not None else None,
        "scorecard_id": scorecard_row.id if scorecard_row is not None else None,
        "readiness_report_id": readiness_row.id,
        "execution_policy_version": (
            execution_policy_versions[0]
            if len(execution_policy_versions) == 1
            else "mixed"
            if execution_policy_versions
            else None
        ),
        "risk_policy_version": None,
        "fee_schedule_version": "live_fill_fee_history_unversioned",
        "reward_schedule_version": "not_tracked_in_phase13a",
        "market_universe_hash": hash_json_payload(market_universe),
        "config_hash": hash_json_payload(config_payload),
    }
    await upsert_promotion_evaluation(
        session,
        family_id=family_row.id,
        strategy_version_id=strategy_version.id,
        gate_policy_id=gate_policy.id if gate_policy is not None else None,
        evaluation_kind=PROMOTION_EVALUATION_KIND_PILOT_READINESS,
        evaluation_status=evaluation_status,
        autonomy_tier=recommended_tier,
        evaluation_window_start=readiness_row.window_start,
        evaluation_window_end=readiness_row.window_end,
        provenance_json=provenance,
        summary_json=summary,
    )


def _normalized_strategy_family(value: str | None) -> str:
    normalized = str(value or settings.polymarket_pilot_default_strategy_family or SUPPORTED_PHASE12_FAMILY).strip().lower()
    if normalized != SUPPORTED_PHASE12_FAMILY:
        raise ValueError(f"Phase 12B pilot evidence supports {SUPPORTED_PHASE12_FAMILY} only")
    return normalized


def _lot_closed_size(lot: PositionLot) -> Decimal:
    return max((_to_decimal(lot.open_size) or ZERO) - (_to_decimal(lot.remaining_size) or ZERO), ZERO)


def _weighted_average(
    *,
    prior_average: Decimal | None,
    prior_size: Decimal,
    next_price: Decimal,
    next_size: Decimal,
) -> Decimal | None:
    if next_size <= ZERO:
        return prior_average
    total_size = prior_size + next_size
    if total_size <= ZERO:
        return None
    weighted_prior = (prior_average or ZERO) * prior_size
    return ((weighted_prior + (next_price * next_size)) / total_size).quantize(PRICE_Q)


def _allocate_pro_rata(total: Decimal | None, sizes: list[Decimal]) -> list[Decimal]:
    amount = _to_decimal(total) or ZERO
    if amount == ZERO or not sizes:
        return [ZERO for _ in sizes]
    total_size = sum((size for size in sizes if size > ZERO), ZERO)
    if total_size <= ZERO:
        return [ZERO for _ in sizes]
    allocated: list[Decimal] = []
    running = ZERO
    last_index = len(sizes) - 1
    for index, size in enumerate(sizes):
        normalized_size = size if size > ZERO else ZERO
        if index == last_index:
            share = amount - running
        else:
            share = (amount * normalized_size / total_size).quantize(PRICE_Q)
            running += share
        allocated.append(share.quantize(PRICE_Q))
    return allocated


def _day_bounds(anchor: datetime | None = None) -> tuple[datetime, datetime]:
    observed = _ensure_utc(anchor) or utcnow()
    start = datetime.combine(observed.date(), time.min, tzinfo=timezone.utc)
    return start, observed


def _previous_day_bounds(anchor: datetime | None = None) -> tuple[datetime, datetime]:
    observed = _ensure_utc(anchor) or utcnow()
    end = datetime.combine(observed.date(), time.min, tzinfo=timezone.utc)
    start = end - timedelta(days=1)
    return start, end


def _previous_week_bounds(anchor: datetime | None = None) -> tuple[datetime, datetime]:
    observed = _ensure_utc(anchor) or utcnow()
    week_start = datetime.combine(observed.date(), time.min, tzinfo=timezone.utc) - timedelta(days=observed.weekday())
    previous_week_end = week_start
    previous_week_start = previous_week_end - timedelta(days=7)
    return previous_week_start, previous_week_end


def serialize_position_lot(row: PositionLot) -> dict[str, Any]:
    realized = _to_decimal(row.realized_pnl) or ZERO
    fees = _to_decimal(row.fee_paid) or ZERO
    return {
        "id": row.id,
        "condition_id": row.condition_id,
        "asset_id": row.asset_id,
        "strategy_family": row.strategy_family,
        "pilot_run_id": str(row.pilot_run_id) if row.pilot_run_id is not None else None,
        "source_live_order_id": str(row.source_live_order_id) if row.source_live_order_id is not None else None,
        "source_fill_id": row.source_fill_id,
        "side": row.side,
        "opened_at": row.opened_at,
        "closed_at": row.closed_at,
        "open_size": _serialize_decimal(_to_decimal(row.open_size)),
        "closed_size": _serialize_decimal(_lot_closed_size(row)),
        "remaining_size": _serialize_decimal(_to_decimal(row.remaining_size)),
        "avg_open_price": _serialize_decimal(_to_decimal(row.avg_open_price)),
        "avg_close_price": _serialize_decimal(_to_decimal(row.avg_close_price)),
        "realized_pnl": _serialize_decimal(realized),
        "fee_paid": _serialize_decimal(fees),
        "net_realized_pnl": _serialize_decimal(realized - fees),
        "status": row.status,
        "details_json": _json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_position_lot_event(row: PositionLotEvent) -> dict[str, Any]:
    return {
        "id": row.id,
        "lot_id": row.lot_id,
        "live_fill_id": row.live_fill_id,
        "event_type": row.event_type,
        "size_delta": _serialize_decimal(_to_decimal(row.size_delta)),
        "price": _serialize_decimal(_to_decimal(row.price)),
        "fee_delta": _serialize_decimal(_to_decimal(row.fee_delta)),
        "realized_pnl_delta": _serialize_decimal(_to_decimal(row.realized_pnl_delta)),
        "observed_at_local": row.observed_at_local,
        "details_json": _json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_pilot_scorecard(
    row: PolymarketPilotScorecard,
    *,
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": row.id,
        "strategy_family": row.strategy_family,
        "strategy_version_id": row.strategy_version_id,
        "strategy_version": strategy_version,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "window_start": row.window_start,
        "window_end": row.window_end,
        "status": row.status,
        "live_orders_count": row.live_orders_count,
        "fills_count": row.fills_count,
        "approval_count": row.approval_count,
        "approval_expired_count": row.approval_expired_count,
        "rejection_count": row.rejection_count,
        "incident_count": row.incident_count,
        "gross_pnl": _serialize_decimal(_to_decimal(row.gross_pnl)),
        "net_pnl": _serialize_decimal(_to_decimal(row.net_pnl)),
        "fees_paid": _serialize_decimal(_to_decimal(row.fees_paid)),
        "avg_shadow_gap_bps": _serialize_decimal(_to_decimal(row.avg_shadow_gap_bps)),
        "worst_shadow_gap_bps": _serialize_decimal(_to_decimal(row.worst_shadow_gap_bps)),
        "coverage_limited_count": row.coverage_limited_count,
        "details_json": _json_safe(row.details_json),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_pilot_guardrail_event(
    row: PolymarketPilotGuardrailEvent,
    *,
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": row.id,
        "strategy_family": row.strategy_family,
        "strategy_version_id": row.strategy_version_id,
        "strategy_version": strategy_version,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "guardrail_type": row.guardrail_type,
        "severity": row.severity,
        "live_order_id": str(row.live_order_id) if row.live_order_id is not None else None,
        "pilot_run_id": str(row.pilot_run_id) if row.pilot_run_id is not None else None,
        "trigger_value": _serialize_decimal(_to_decimal(row.trigger_value)),
        "threshold_value": _serialize_decimal(_to_decimal(row.threshold_value)),
        "action_taken": row.action_taken,
        "details_json": _json_safe(row.details_json),
        "observed_at_local": row.observed_at_local,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def serialize_pilot_readiness_report(
    row: PolymarketPilotReadinessReport,
    *,
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": row.id,
        "strategy_family": row.strategy_family,
        "strategy_version_id": row.strategy_version_id,
        "strategy_version": strategy_version,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "window_start": row.window_start,
        "window_end": row.window_end,
        "status": row.status,
        "scorecard_id": row.scorecard_id,
        "open_incidents": row.open_incidents,
        "approval_backlog_count": row.approval_backlog_count,
        "coverage_limited_count": row.coverage_limited_count,
        "shadow_gap_breach_count": row.shadow_gap_breach_count,
        "details_json": _json_safe(row.details_json),
        "generated_at": row.generated_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


async def serialize_pilot_scorecards_with_lifecycle(
    session: AsyncSession,
    rows: list[PolymarketPilotScorecard],
) -> list[dict[str, Any]]:
    version_ids = {int(row.strategy_version_id) for row in rows if row.strategy_version_id is not None}
    version_map, evaluation_map = await _strategy_lifecycle_maps(session, version_ids=version_ids)
    return [
        serialize_pilot_scorecard(
            row,
            strategy_version=version_map.get(int(row.strategy_version_id)) if row.strategy_version_id is not None else None,
            latest_promotion_evaluation=evaluation_map.get(int(row.strategy_version_id)) if row.strategy_version_id is not None else None,
        )
        for row in rows
    ]


async def serialize_pilot_readiness_reports_with_lifecycle(
    session: AsyncSession,
    rows: list[PolymarketPilotReadinessReport],
) -> list[dict[str, Any]]:
    version_ids = {int(row.strategy_version_id) for row in rows if row.strategy_version_id is not None}
    version_map, evaluation_map = await _strategy_lifecycle_maps(session, version_ids=version_ids)
    return [
        serialize_pilot_readiness_report(
            row,
            strategy_version=version_map.get(int(row.strategy_version_id)) if row.strategy_version_id is not None else None,
            latest_promotion_evaluation=evaluation_map.get(int(row.strategy_version_id)) if row.strategy_version_id is not None else None,
        )
        for row in rows
    ]


async def serialize_pilot_guardrail_events_with_lifecycle(
    session: AsyncSession,
    rows: list[PolymarketPilotGuardrailEvent],
) -> list[dict[str, Any]]:
    version_ids = {int(row.strategy_version_id) for row in rows if row.strategy_version_id is not None}
    version_map, evaluation_map = await _strategy_lifecycle_maps(session, version_ids=version_ids)
    return [
        serialize_pilot_guardrail_event(
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


async def list_position_lots(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    status: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(PositionLot).order_by(PositionLot.opened_at.desc(), PositionLot.id.desc())
    if strategy_family:
        query = query.where(PositionLot.strategy_family == strategy_family)
    if condition_id:
        query = query.where(PositionLot.condition_id == condition_id)
    if asset_id:
        query = query.where(PositionLot.asset_id == asset_id)
    if status:
        query = query.where(PositionLot.status == status)
    if start is not None:
        query = query.where(PositionLot.opened_at >= _ensure_utc(start))
    if end is not None:
        query = query.where(PositionLot.opened_at <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return [serialize_position_lot(row) for row in rows]


async def list_position_lot_events(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    condition_id: str | None = None,
    asset_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    query = (
        select(PositionLotEvent)
        .join(PositionLot, PositionLot.id == PositionLotEvent.lot_id)
        .order_by(PositionLotEvent.observed_at_local.desc(), PositionLotEvent.id.desc())
    )
    if strategy_family:
        query = query.where(PositionLot.strategy_family == strategy_family)
    if condition_id:
        query = query.where(PositionLot.condition_id == condition_id)
    if asset_id:
        query = query.where(PositionLot.asset_id == asset_id)
    if start is not None:
        query = query.where(PositionLotEvent.observed_at_local >= _ensure_utc(start))
    if end is not None:
        query = query.where(PositionLotEvent.observed_at_local <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return [serialize_position_lot_event(row) for row in rows]


async def list_pilot_scorecards(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    status: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(PolymarketPilotScorecard).order_by(
        PolymarketPilotScorecard.window_end.desc(),
        PolymarketPilotScorecard.id.desc(),
    )
    if strategy_family:
        query = query.where(PolymarketPilotScorecard.strategy_family == strategy_family)
    if status:
        query = query.where(PolymarketPilotScorecard.status == status)
    if start is not None:
        query = query.where(PolymarketPilotScorecard.window_end >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketPilotScorecard.window_start <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return await serialize_pilot_scorecards_with_lifecycle(session, rows)


async def list_pilot_guardrail_events(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    guardrail_type: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(PolymarketPilotGuardrailEvent).order_by(
        PolymarketPilotGuardrailEvent.observed_at_local.desc(),
        PolymarketPilotGuardrailEvent.id.desc(),
    )
    if strategy_family:
        query = query.where(PolymarketPilotGuardrailEvent.strategy_family == strategy_family)
    if guardrail_type:
        query = query.where(PolymarketPilotGuardrailEvent.guardrail_type == guardrail_type)
    if start is not None:
        query = query.where(PolymarketPilotGuardrailEvent.observed_at_local >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketPilotGuardrailEvent.observed_at_local <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return await serialize_pilot_guardrail_events_with_lifecycle(session, rows)


async def list_pilot_readiness_reports(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    status: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = select(PolymarketPilotReadinessReport).order_by(
        PolymarketPilotReadinessReport.generated_at.desc(),
        PolymarketPilotReadinessReport.id.desc(),
    )
    if strategy_family:
        query = query.where(PolymarketPilotReadinessReport.strategy_family == strategy_family)
    if status:
        query = query.where(PolymarketPilotReadinessReport.status == status)
    if start is not None:
        query = query.where(PolymarketPilotReadinessReport.window_end >= _ensure_utc(start))
    if end is not None:
        query = query.where(PolymarketPilotReadinessReport.window_start <= _ensure_utc(end))
    rows = (await session.execute(query.limit(limit))).scalars().all()
    return await serialize_pilot_readiness_reports_with_lifecycle(session, rows)


class PolymarketPilotEvidenceService:
    async def _record_guardrail_if_new(
        self,
        session: AsyncSession,
        *,
        strategy_family: str,
        guardrail_type: str,
        severity: str,
        action_taken: str,
        live_order: LiveOrder | None = None,
        pilot_run: PolymarketPilotRun | None = None,
        trigger_value: Decimal | float | str | None = None,
        threshold_value: Decimal | float | str | None = None,
        details: dict[str, Any] | None = None,
        observed_at: datetime | None = None,
        dedupe_window_seconds: int = 300,
    ) -> tuple[PolymarketPilotGuardrailEvent, bool]:
        observed = _ensure_utc(observed_at) or utcnow()
        normalized_details = _json_safe(details or {})
        recent = (
            await session.execute(
                select(PolymarketPilotGuardrailEvent)
                .where(
                    PolymarketPilotGuardrailEvent.strategy_family == _normalized_strategy_family(strategy_family),
                    PolymarketPilotGuardrailEvent.guardrail_type == guardrail_type,
                    PolymarketPilotGuardrailEvent.action_taken == action_taken,
                    PolymarketPilotGuardrailEvent.live_order_id == (live_order.id if live_order is not None else None),
                    PolymarketPilotGuardrailEvent.pilot_run_id == (pilot_run.id if pilot_run is not None else None),
                    PolymarketPilotGuardrailEvent.observed_at_local >= observed - timedelta(seconds=dedupe_window_seconds),
                )
                .order_by(PolymarketPilotGuardrailEvent.observed_at_local.desc(), PolymarketPilotGuardrailEvent.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if (
            recent is not None
            and _to_decimal(recent.trigger_value) == _to_decimal(trigger_value)
            and _to_decimal(recent.threshold_value) == _to_decimal(threshold_value)
            and _json_safe(recent.details_json) == normalized_details
        ):
            return recent, False
        row = await self.record_guardrail_event(
            session,
            strategy_family=strategy_family,
            guardrail_type=guardrail_type,
            severity=severity,
            action_taken=action_taken,
            live_order=live_order,
            pilot_run=pilot_run,
            trigger_value=trigger_value,
            threshold_value=threshold_value,
            details=details,
            observed_at=observed,
        )
        return row, True

    async def record_guardrail_event(
        self,
        session: AsyncSession,
        *,
        strategy_family: str,
        guardrail_type: str,
        severity: str,
        action_taken: str,
        live_order: LiveOrder | None = None,
        pilot_run: PolymarketPilotRun | None = None,
        trigger_value: Decimal | float | str | None = None,
        threshold_value: Decimal | float | str | None = None,
        details: dict[str, Any] | None = None,
        observed_at: datetime | None = None,
    ) -> PolymarketPilotGuardrailEvent:
        row = PolymarketPilotGuardrailEvent(
            strategy_family=_normalized_strategy_family(strategy_family),
            strategy_version_id=await resolve_pilot_strategy_version_id(
                session,
                live_order=live_order,
                strategy_family=strategy_family,
                pilot_run=pilot_run,
            ),
            guardrail_type=guardrail_type,
            severity=severity,
            live_order_id=live_order.id if live_order is not None else None,
            pilot_run_id=pilot_run.id if pilot_run is not None else None,
            trigger_value=_to_decimal(trigger_value),
            threshold_value=_to_decimal(threshold_value),
            action_taken=action_taken,
            details_json=_json_safe(details or {}),
            observed_at_local=_ensure_utc(observed_at) or utcnow(),
        )
        session.add(row)
        await session.flush()
        polymarket_pilot_guardrail_triggers_total.labels(
            strategy_family=row.strategy_family,
            guardrail_type=row.guardrail_type,
            action_taken=row.action_taken,
        ).inc()
        if row.guardrail_type == "approval_ttl":
            polymarket_pilot_approval_expirations_total.inc()
        if row.guardrail_type == "shadow_gap_breach":
            polymarket_pilot_shadow_gap_breach_events_total.inc()
        return row

    async def sync_position_lots(
        self,
        session: AsyncSession,
        *,
        strategy_family: str = SUPPORTED_PHASE12_FAMILY,
    ) -> dict[str, int]:
        family = _normalized_strategy_family(strategy_family)
        fills = (
            await session.execute(
                select(LiveFill)
                .join(LiveOrder, LiveOrder.id == LiveFill.live_order_id)
                .where(
                    LiveOrder.strategy_family == family,
                    LiveOrder.dry_run.is_(False),
                    LiveFill.fill_status.in_(tuple(ACTIVE_FILL_STATUSES)),
                )
                .order_by(LiveFill.observed_at_local.asc(), LiveFill.id.asc())
            )
        ).scalars().all()
        synced_fills = 0
        opened_lots = 0
        closed_lots = 0
        fee_updates = 0
        skipped_fills = 0
        for fill in fills:
            events = (
                await session.execute(
                    select(PositionLotEvent)
                    .where(PositionLotEvent.live_fill_id == fill.id)
                    .order_by(PositionLotEvent.id.asc())
                )
            ).scalars().all()
            accounting_events = [event for event in events if event.event_type in {"open", "partial_close", "close"}]
            if not accounting_events:
                result = await self._apply_fill_to_lots(session, fill=fill)
                synced_fills += 1 if result["processed"] else 0
                opened_lots += result["opened_lots"]
                closed_lots += result["closed_lots"]
                skipped_fills += 0 if result["processed"] else 1
                continue
            fee_updates += await self._apply_fee_updates(session, fill=fill, fragment_events=accounting_events)
        return {
            "fills_processed": synced_fills,
            "lots_opened": opened_lots,
            "lots_closed": closed_lots,
            "fee_updates": fee_updates,
            "fills_skipped": skipped_fills,
        }

    async def _apply_fill_to_lots(
        self,
        session: AsyncSession,
        *,
        fill: LiveFill,
    ) -> dict[str, int | bool]:
        order = await session.get(LiveOrder, fill.live_order_id) if fill.live_order_id is not None else None
        if order is None or order.strategy_family != SUPPORTED_PHASE12_FAMILY or order.dry_run:
            return {"processed": False, "opened_lots": 0, "closed_lots": 0}

        remaining = _to_decimal(fill.size) or ZERO
        if remaining <= ZERO:
            return {"processed": False, "opened_lots": 0, "closed_lots": 0}

        observed_at = _ensure_utc(fill.event_ts_exchange) or _ensure_utc(fill.observed_at_local) or utcnow()
        fill_price = _to_decimal(fill.price) or ZERO
        fill_side = str(fill.side or "").upper()
        fill_fee = _to_decimal(fill.fee_paid) or ZERO

        open_lots = (
            await session.execute(
                select(PositionLot)
                .where(
                    PositionLot.strategy_family == order.strategy_family,
                    PositionLot.condition_id == order.condition_id,
                    PositionLot.asset_id == order.asset_id,
                    PositionLot.status.in_(tuple(OPEN_LOT_STATUSES)),
                    PositionLot.remaining_size > ZERO,
                )
                .order_by(PositionLot.opened_at.asc(), PositionLot.id.asc())
            )
        ).scalars().all()
        opposite_side = "SELL" if fill_side == "BUY" else "BUY"
        candidate_lots = [lot for lot in open_lots if str(lot.side or "").upper() == opposite_side]

        fragments: list[tuple[str, PositionLot | None, Decimal]] = []
        for lot in candidate_lots:
            if remaining <= ZERO:
                break
            lot_remaining = _to_decimal(lot.remaining_size) or ZERO
            if lot_remaining <= ZERO:
                continue
            closing_size = min(remaining, lot_remaining)
            if closing_size <= ZERO:
                continue
            fragments.append(("close", lot, closing_size))
            remaining -= closing_size
        if remaining > ZERO:
            fragments.append(("open", None, remaining))

        fee_allocations = _allocate_pro_rata(fill_fee, [fragment_size for _, _, fragment_size in fragments])
        opened_lots = 0
        closed_lots = 0
        for index, (fragment_kind, lot, fragment_size) in enumerate(fragments):
            fragment_fee = fee_allocations[index]
            if fragment_kind == "close" and lot is not None:
                lot_closed_before = _lot_closed_size(lot)
                prior_realized = _to_decimal(lot.realized_pnl) or ZERO
                prior_fees = _to_decimal(lot.fee_paid) or ZERO
                prior_remaining = _to_decimal(lot.remaining_size) or ZERO
                realized_delta = (
                    (fill_price - (_to_decimal(lot.avg_open_price) or ZERO)) * fragment_size
                    if str(lot.side or "").upper() == "BUY"
                    else ((_to_decimal(lot.avg_open_price) or ZERO) - fill_price) * fragment_size
                ).quantize(PRICE_Q)
                lot.remaining_size = max(prior_remaining - fragment_size, ZERO).quantize(PRICE_Q)
                lot.realized_pnl = (prior_realized + realized_delta).quantize(PRICE_Q)
                lot.fee_paid = (prior_fees + fragment_fee).quantize(PRICE_Q)
                lot.avg_close_price = _weighted_average(
                    prior_average=_to_decimal(lot.avg_close_price),
                    prior_size=lot_closed_before,
                    next_price=fill_price,
                    next_size=fragment_size,
                )
                if lot.remaining_size <= ZERO:
                    lot.remaining_size = ZERO
                    lot.status = "closed"
                    lot.closed_at = observed_at
                    closed_lots += 1
                    event_type = "close"
                else:
                    lot.status = "partially_closed"
                    event_type = "partial_close"
                lot.details_json = _json_safe(
                    {
                        **(lot.details_json if isinstance(lot.details_json, dict) else {}),
                        "last_close_fill_id": fill.id,
                        "last_close_trade_id": fill.trade_id,
                        "closed_size": str(_lot_closed_size(lot)),
                    }
                )
                session.add(
                    PositionLotEvent(
                        lot_id=lot.id,
                        live_fill_id=fill.id,
                        event_type=event_type,
                        size_delta=(-fragment_size).quantize(PRICE_Q),
                        price=fill_price,
                        fee_delta=fragment_fee,
                        realized_pnl_delta=realized_delta,
                        observed_at_local=observed_at,
                        details_json={
                            "condition_id": fill.condition_id,
                            "asset_id": fill.asset_id,
                            "trade_id": fill.trade_id,
                            "transaction_hash": fill.transaction_hash,
                            "fill_accounting_signature": f"{fill.side}|{fill.price}|{fill.size}",
                        },
                    )
                )
                continue

            new_lot = PositionLot(
                condition_id=order.condition_id,
                asset_id=order.asset_id,
                strategy_family=order.strategy_family or SUPPORTED_PHASE12_FAMILY,
                pilot_run_id=order.pilot_run_id,
                source_live_order_id=order.id,
                source_fill_id=fill.id,
                side=fill_side,
                opened_at=observed_at,
                open_size=fragment_size.quantize(PRICE_Q),
                remaining_size=fragment_size.quantize(PRICE_Q),
                avg_open_price=fill_price,
                realized_pnl=ZERO,
                fee_paid=fragment_fee,
                status="open",
                details_json={
                    "trade_id": fill.trade_id,
                    "transaction_hash": fill.transaction_hash,
                    "fill_status": fill.fill_status,
                },
            )
            session.add(new_lot)
            await session.flush()
            session.add(
                PositionLotEvent(
                    lot_id=new_lot.id,
                    live_fill_id=fill.id,
                    event_type="open",
                    size_delta=fragment_size.quantize(PRICE_Q),
                    price=fill_price,
                    fee_delta=fragment_fee,
                    realized_pnl_delta=ZERO,
                    observed_at_local=observed_at,
                    details_json={
                        "condition_id": fill.condition_id,
                        "asset_id": fill.asset_id,
                        "trade_id": fill.trade_id,
                        "transaction_hash": fill.transaction_hash,
                        "fill_accounting_signature": f"{fill.side}|{fill.price}|{fill.size}",
                    },
                )
            )
            opened_lots += 1
        await session.flush()
        return {"processed": True, "opened_lots": opened_lots, "closed_lots": closed_lots}

    async def _apply_fee_updates(
        self,
        session: AsyncSession,
        *,
        fill: LiveFill,
        fragment_events: list[PositionLotEvent],
    ) -> int:
        current_fee = _to_decimal(fill.fee_paid) or ZERO
        allocated_fee = sum((_to_decimal(event.fee_delta) or ZERO for event in fragment_events), ZERO)
        fee_delta = (current_fee - allocated_fee).quantize(PRICE_Q)
        if fee_delta == ZERO:
            return 0
        allocations = _allocate_pro_rata(
            fee_delta,
            [abs(_to_decimal(event.size_delta) or ZERO) for event in fragment_events],
        )
        updates = 0
        observed_at = _ensure_utc(fill.event_ts_exchange) or _ensure_utc(fill.observed_at_local) or utcnow()
        for event, allocation in zip(fragment_events, allocations):
            if allocation == ZERO:
                continue
            lot = await session.get(PositionLot, event.lot_id)
            if lot is None:
                continue
            lot.fee_paid = ((_to_decimal(lot.fee_paid) or ZERO) + allocation).quantize(PRICE_Q)
            session.add(
                PositionLotEvent(
                    lot_id=lot.id,
                    live_fill_id=fill.id,
                    event_type="fee_update",
                    price=_to_decimal(fill.price),
                    fee_delta=allocation,
                    realized_pnl_delta=ZERO,
                    observed_at_local=observed_at,
                    details_json={
                        "trade_id": fill.trade_id,
                        "transaction_hash": fill.transaction_hash,
                        "reason": "fill_fee_reconciled",
                    },
                )
            )
            updates += 1
        if updates:
            await session.flush()
        return updates

    async def generate_scorecard(
        self,
        session: AsyncSession,
        *,
        strategy_family: str = SUPPORTED_PHASE12_FAMILY,
        window_start: datetime,
        window_end: datetime,
        label: str | None = None,
    ) -> dict[str, Any]:
        family = _normalized_strategy_family(strategy_family)
        start = _ensure_utc(window_start) or utcnow()
        end = _ensure_utc(window_end) or utcnow()
        if end <= start:
            raise ValueError("window_end must be after window_start")

        existing = (
            await session.execute(
                select(PolymarketPilotScorecard).where(
                    PolymarketPilotScorecard.strategy_family == family,
                    PolymarketPilotScorecard.window_start == start,
                    PolymarketPilotScorecard.window_end == end,
                )
            )
        ).scalar_one_or_none()

        live_orders = (
            await session.execute(
                select(LiveOrder).where(
                    LiveOrder.strategy_family == family,
                    LiveOrder.dry_run.is_(False),
                    LiveOrder.created_at >= start,
                    LiveOrder.created_at < end,
                )
            )
        ).scalars().all()
        fills = (
            await session.execute(
                select(LiveFill)
                .join(LiveOrder, LiveOrder.id == LiveFill.live_order_id)
                .where(
                    LiveOrder.strategy_family == family,
                    LiveOrder.dry_run.is_(False),
                    LiveFill.fill_status.in_(tuple(ACTIVE_FILL_STATUSES)),
                    LiveFill.observed_at_local >= start,
                    LiveFill.observed_at_local < end,
                )
            )
        ).scalars().all()
        approvals = (
            await session.execute(
                select(PolymarketPilotApprovalEvent).where(
                    PolymarketPilotApprovalEvent.observed_at_local >= start,
                    PolymarketPilotApprovalEvent.observed_at_local < end,
                )
            )
        ).scalars().all()
        incidents = (
            await session.execute(
                select(PolymarketControlPlaneIncident).where(
                    PolymarketControlPlaneIncident.observed_at_local >= start,
                    PolymarketControlPlaneIncident.observed_at_local < end,
                )
            )
        ).scalars().all()
        guardrails = (
            await session.execute(
                select(PolymarketPilotGuardrailEvent).where(
                    PolymarketPilotGuardrailEvent.strategy_family == family,
                    PolymarketPilotGuardrailEvent.observed_at_local >= start,
                    PolymarketPilotGuardrailEvent.observed_at_local < end,
                )
            )
        ).scalars().all()
        shadow_rows = (
            await session.execute(
                select(PolymarketLiveShadowEvaluation)
                .join(LiveOrder, LiveOrder.id == PolymarketLiveShadowEvaluation.live_order_id, isouter=True)
                .where(
                    LiveOrder.strategy_family == family,
                    PolymarketLiveShadowEvaluation.updated_at >= start,
                    PolymarketLiveShadowEvaluation.updated_at < end,
                )
            )
        ).scalars().all()
        pnl_events = (
            await session.execute(
                select(PositionLotEvent)
                .join(PositionLot, PositionLot.id == PositionLotEvent.lot_id)
                .where(
                    PositionLot.strategy_family == family,
                    PositionLotEvent.observed_at_local >= start,
                    PositionLotEvent.observed_at_local < end,
                )
            )
        ).scalars().all()

        gross_pnl = sum((_to_decimal(event.realized_pnl_delta) or ZERO for event in pnl_events), ZERO).quantize(PRICE_Q)
        fees_paid = sum((_to_decimal(event.fee_delta) or ZERO for event in pnl_events), ZERO).quantize(PRICE_Q)
        net_pnl = (gross_pnl - fees_paid).quantize(PRICE_Q)
        gap_values = [abs(_to_decimal(row.gap_bps) or ZERO) for row in shadow_rows if row.gap_bps is not None and not row.coverage_limited]
        avg_gap = (sum(gap_values, ZERO) / Decimal(len(gap_values))).quantize(PRICE_Q) if gap_values else None
        worst_gap = max(gap_values) if gap_values else None

        approval_latencies: list[float] = []
        approval_count = 0
        rejection_count = 0
        expired_count = 0
        for event in approvals:
            if event.action == "approved":
                approval_count += 1
            elif event.action == "rejected":
                rejection_count += 1
            elif event.action == "expired":
                expired_count += 1
            if event.live_order_id is None:
                continue
            order = await session.get(LiveOrder, event.live_order_id)
            if order is None or order.strategy_family != family or order.approval_requested_at is None:
                continue
            approval_latencies.append(max((event.observed_at_local - order.approval_requested_at).total_seconds(), 0.0))

        severe_guardrails = [row for row in guardrails if row.action_taken in SERIOUS_GUARDRAIL_ACTIONS]
        shadow_breaches = [row for row in guardrails if row.guardrail_type == "shadow_gap_breach"]
        coverage_limited_count = sum(1 for row in shadow_rows if row.coverage_limited)
        if severe_guardrails or expired_count > 0:
            status = "blocked"
        elif incidents or shadow_breaches or coverage_limited_count > 0:
            status = "degraded"
        elif rejection_count > 0:
            status = "watch"
        else:
            status = "ok"

        details = {
            "window_label": label,
            "approval_latency_avg_seconds": (sum(approval_latencies) / len(approval_latencies)) if approval_latencies else None,
            "approval_latency_max_seconds": max(approval_latencies) if approval_latencies else None,
            "guardrail_counts": {
                guardrail_type: sum(1 for row in guardrails if row.guardrail_type == guardrail_type)
                for guardrail_type in sorted({row.guardrail_type for row in guardrails})
            },
            "incident_types": {
                incident_type: sum(1 for row in incidents if row.incident_type == incident_type)
                for incident_type in sorted({row.incident_type for row in incidents})
            },
        }
        strategy_version = await get_current_strategy_version(session, family)

        row = existing or PolymarketPilotScorecard(
            strategy_family=family,
            window_start=start,
            window_end=end,
        )
        row.strategy_version_id = strategy_version.id if strategy_version is not None else None
        row.status = status
        row.live_orders_count = len(live_orders)
        row.fills_count = len(fills)
        row.approval_count = approval_count
        row.approval_expired_count = expired_count
        row.rejection_count = rejection_count
        row.incident_count = len(incidents)
        row.gross_pnl = gross_pnl
        row.net_pnl = net_pnl
        row.fees_paid = fees_paid
        row.avg_shadow_gap_bps = avg_gap
        row.worst_shadow_gap_bps = worst_gap
        row.coverage_limited_count = coverage_limited_count
        row.details_json = _json_safe(details)
        if existing is None:
            session.add(row)
        await session.flush()
        if existing is None:
            polymarket_pilot_scorecards_total.labels(strategy_family=family, status=row.status).inc()
        return (await serialize_pilot_scorecards_with_lifecycle(session, [row]))[0]

    async def generate_readiness_report(
        self,
        session: AsyncSession,
        *,
        strategy_family: str = SUPPORTED_PHASE12_FAMILY,
        window_start: datetime,
        window_end: datetime,
    ) -> dict[str, Any]:
        family = _normalized_strategy_family(strategy_family)
        start = _ensure_utc(window_start) or utcnow()
        end = _ensure_utc(window_end) or utcnow()
        if end <= start:
            raise ValueError("window_end must be after window_start")

        scorecard = await self.generate_scorecard(
            session,
            strategy_family=family,
            window_start=start,
            window_end=end,
            label="readiness_window",
        )
        scorecard_row = await session.get(PolymarketPilotScorecard, scorecard["id"])
        incidents = (
            await session.execute(
                select(PolymarketControlPlaneIncident).where(
                    PolymarketControlPlaneIncident.observed_at_local >= start,
                    PolymarketControlPlaneIncident.observed_at_local < end,
                )
            )
        ).scalars().all()
        backlog_orders = (
            await session.execute(
                select(LiveOrder).where(
                    LiveOrder.strategy_family == family,
                    LiveOrder.approval_state == "queued",
                )
            )
        ).scalars().all()
        live_orders = (
            await session.execute(
                select(LiveOrder).where(
                    LiveOrder.strategy_family == family,
                    LiveOrder.created_at >= start,
                    LiveOrder.created_at < end,
                )
            )
        ).scalars().all()
        guardrails = (
            await session.execute(
                select(PolymarketPilotGuardrailEvent).where(
                    PolymarketPilotGuardrailEvent.strategy_family == family,
                    PolymarketPilotGuardrailEvent.observed_at_local >= start,
                    PolymarketPilotGuardrailEvent.observed_at_local < end,
                )
            )
        ).scalars().all()
        shadow_gap_breach_count = sum(1 for row in guardrails if row.guardrail_type == "shadow_gap_breach")
        severe_guardrail_count = sum(1 for row in guardrails if row.action_taken in SERIOUS_GUARDRAIL_ACTIONS)
        recent_daily_scorecards = (
            await session.execute(
                select(PolymarketPilotScorecard)
                .where(
                    PolymarketPilotScorecard.strategy_family == family,
                    PolymarketPilotScorecard.window_end <= end,
                    PolymarketPilotScorecard.window_end >= end - timedelta(days=max(settings.polymarket_pilot_semi_auto_candidate_min_days, 1) + 2),
                )
                .order_by(PolymarketPilotScorecard.window_end.desc())
            )
        ).scalars().all()
        candidate_days = max(settings.polymarket_pilot_semi_auto_candidate_min_days, 1)
        candidate_cutoff = end - timedelta(days=candidate_days)
        candidate_scorecards = [
            row for row in recent_daily_scorecards
            if (
                (_ensure_utc(row.window_start) or row.window_start) >= candidate_cutoff
                and ((_ensure_utc(row.window_end) or row.window_end) - (_ensure_utc(row.window_start) or row.window_start)) <= timedelta(days=1, minutes=1)
            )
        ]
        candidate_gaps = [_to_decimal(row.avg_shadow_gap_bps) for row in candidate_scorecards if row.avg_shadow_gap_bps is not None]
        candidate_avg_gap = (
            (sum((gap or ZERO for gap in candidate_gaps), ZERO) / Decimal(len(candidate_gaps))).quantize(PRICE_Q)
            if candidate_gaps
            else None
        )
        candidate_gap_threshold = Decimal(str(settings.polymarket_pilot_semi_auto_max_avg_shadow_gap_bps))
        blockers = []
        if incidents:
            blockers.append("recent_incidents")
        if backlog_orders:
            blockers.append("approval_backlog")
        if scorecard_row is not None and scorecard_row.coverage_limited_count > 0:
            blockers.append("coverage_limited")
        if shadow_gap_breach_count > 0:
            blockers.append("shadow_gap_breach")
        if severe_guardrail_count > 0:
            blockers.append("serious_guardrail")

        if blockers:
            status = "not_ready"
        elif (
            len(candidate_scorecards) >= candidate_days
            and candidate_avg_gap is not None
            and candidate_avg_gap <= candidate_gap_threshold
            and all(row.status == "ok" for row in candidate_scorecards)
            and not backlog_orders
            and shadow_gap_breach_count == 0
            and scorecard_row is not None
            and scorecard_row.approval_expired_count == 0
            and scorecard_row.coverage_limited_count == 0
        ):
            status = "candidate_for_semi_auto"
        else:
            status = "manual_only"

        existing = (
            await session.execute(
                select(PolymarketPilotReadinessReport).where(
                    PolymarketPilotReadinessReport.strategy_family == family,
                    PolymarketPilotReadinessReport.window_start == start,
                    PolymarketPilotReadinessReport.window_end == end,
                )
            )
        ).scalar_one_or_none()
        generated_at = utcnow()
        strategy_version = await get_current_strategy_version(session, family)
        row = existing or PolymarketPilotReadinessReport(
            strategy_family=family,
            window_start=start,
            window_end=end,
        )
        row.strategy_version_id = strategy_version.id if strategy_version is not None else None
        row.status = status
        row.scorecard_id = scorecard_row.id if scorecard_row is not None else None
        row.open_incidents = len(incidents)
        row.approval_backlog_count = len(backlog_orders)
        row.coverage_limited_count = scorecard_row.coverage_limited_count if scorecard_row is not None else 0
        row.shadow_gap_breach_count = shadow_gap_breach_count
        row.generated_at = generated_at
        row.details_json = _json_safe(
            {
                "blockers": blockers,
                "candidate_scorecard_days": len(candidate_scorecards),
                "candidate_avg_shadow_gap_bps": candidate_avg_gap,
                "candidate_gap_threshold_bps": candidate_gap_threshold,
                "manual_only_default": True,
            }
        )
        if existing is None:
            session.add(row)
        await session.flush()
        await _record_phase13a_readiness_evaluation(
            session,
            family=family,
            strategy_version=strategy_version,
            scorecard_row=scorecard_row,
            readiness_row=row,
            readiness_status=status,
            blockers=blockers,
            candidate_scorecards=candidate_scorecards,
            candidate_avg_gap=candidate_avg_gap,
            candidate_gap_threshold=candidate_gap_threshold,
            incidents=incidents,
            backlog_orders=backlog_orders,
            live_orders=live_orders,
        )
        if existing is None:
            polymarket_pilot_readiness_reports_total.labels(strategy_family=family, status=row.status).inc()
        polymarket_pilot_latest_readiness_report_timestamp.set(generated_at.timestamp())
        return (await serialize_pilot_readiness_reports_with_lifecycle(session, [row]))[0]

    async def enforce_periodic_guardrails(
        self,
        session: AsyncSession,
        *,
        strategy_family: str = SUPPORTED_PHASE12_FAMILY,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        family = _normalized_strategy_family(strategy_family)
        observed = _ensure_utc(now) or utcnow()
        triggered: list[PolymarketPilotGuardrailEvent] = []
        day_start, day_end = _day_bounds(observed)
        daily_summary = await self.compute_realized_pnl_summary(
            session,
            strategy_family=family,
            start=day_start,
            end=day_end,
        )
        if daily_summary["net_realized_pnl"] is not None:
            polymarket_pilot_daily_realized_pnl_usd.set(daily_summary["net_realized_pnl"])
        open_run = (
            await session.execute(
                select(PolymarketPilotRun)
                .join(PolymarketPilotConfig, PolymarketPilotConfig.id == PolymarketPilotRun.pilot_config_id)
                .where(
                    PolymarketPilotConfig.strategy_family == family,
                    PolymarketPilotConfig.active.is_(True),
                    PolymarketPilotRun.ended_at.is_(None),
                )
                .order_by(PolymarketPilotRun.started_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

        max_daily_loss = Decimal(str(settings.polymarket_pilot_max_daily_loss_usd))
        net_realized = _to_decimal(daily_summary["net_realized_pnl"]) or ZERO
        if max_daily_loss > ZERO and net_realized <= -max_daily_loss:
            row, created = await self._record_guardrail_if_new(
                session,
                strategy_family=family,
                guardrail_type="max_daily_loss",
                severity="error",
                action_taken="pause_pilot",
                pilot_run=open_run,
                trigger_value=abs(net_realized),
                threshold_value=max_daily_loss,
                details={
                    "window_start": day_start,
                    "window_end": day_end,
                    "net_realized_pnl": net_realized,
                },
                observed_at=observed,
            )
            if created:
                triggered.append(row)

        outstanding_limit = Decimal(str(settings.polymarket_max_outstanding_notional_usd))
        if outstanding_limit > ZERO:
            reservations = (
                await session.execute(
                    select(CapitalReservation).order_by(
                        CapitalReservation.observed_at_local.desc(),
                        CapitalReservation.id.desc(),
                    )
                )
            ).scalars().all()
            latest_by_order: dict[str, CapitalReservation] = {}
            for row in reservations:
                key = str(row.live_order_id) if row.live_order_id is not None else f"orphan:{row.id}"
                if key not in latest_by_order:
                    latest_by_order[key] = row
            outstanding_total = sum(
                (
                    _to_decimal(row.open_amount) or ZERO
                    for row in latest_by_order.values()
                    if row.status not in {"released", "failed"} and (_to_decimal(row.open_amount) or ZERO) > ZERO
                ),
                ZERO,
            ).quantize(PRICE_Q)
            if outstanding_total > outstanding_limit:
                row, created = await self._record_guardrail_if_new(
                    session,
                    strategy_family=family,
                    guardrail_type="max_outstanding_notional",
                    severity="error",
                    action_taken="pause_pilot",
                    pilot_run=open_run,
                    trigger_value=outstanding_total,
                    threshold_value=outstanding_limit,
                    details={"current_outstanding": outstanding_total},
                    observed_at=observed,
                )
                if created:
                    triggered.append(row)
        return await serialize_pilot_guardrail_events_with_lifecycle(session, triggered)

    async def maybe_generate_scheduled_artifacts(
        self,
        session: AsyncSession,
        *,
        strategy_family: str = SUPPORTED_PHASE12_FAMILY,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        family = _normalized_strategy_family(strategy_family)
        observed = _ensure_utc(now) or utcnow()
        generated: dict[str, Any] = {"scorecards": [], "readiness_reports": []}
        if settings.polymarket_pilot_scorecard_enabled:
            previous_day_start, previous_day_end = _previous_day_bounds(observed)
            previous_week_start, previous_week_end = _previous_week_bounds(observed)
            generated["scorecards"].append(
                await self.generate_scorecard(
                    session,
                    strategy_family=family,
                    window_start=previous_day_start,
                    window_end=previous_day_end,
                    label="daily",
                )
            )
            generated["scorecards"].append(
                await self.generate_scorecard(
                    session,
                    strategy_family=family,
                    window_start=previous_week_start,
                    window_end=previous_week_end,
                    label="weekly",
                )
            )
        if settings.polymarket_pilot_readiness_report_enabled:
            readiness_end = datetime.combine(observed.date(), time.min, tzinfo=timezone.utc)
            readiness_start = readiness_end - timedelta(days=max(settings.polymarket_pilot_semi_auto_candidate_min_days, 1))
            generated["readiness_reports"].append(
                await self.generate_readiness_report(
                    session,
                    strategy_family=family,
                    window_start=readiness_start,
                    window_end=readiness_end,
                )
            )
        return generated

    async def compute_realized_pnl_summary(
        self,
        session: AsyncSession,
        *,
        strategy_family: str = SUPPORTED_PHASE12_FAMILY,
        start: datetime,
        end: datetime,
    ) -> dict[str, float | None]:
        family = _normalized_strategy_family(strategy_family)
        rows = (
            await session.execute(
                select(PositionLotEvent)
                .join(PositionLot, PositionLot.id == PositionLotEvent.lot_id)
                .where(
                    PositionLot.strategy_family == family,
                    PositionLotEvent.observed_at_local >= _ensure_utc(start),
                    PositionLotEvent.observed_at_local < _ensure_utc(end),
                )
            )
        ).scalars().all()
        gross = sum((_to_decimal(row.realized_pnl_delta) or ZERO for row in rows), ZERO).quantize(PRICE_Q)
        fees = sum((_to_decimal(row.fee_delta) or ZERO for row in rows), ZERO).quantize(PRICE_Q)
        net = (gross - fees).quantize(PRICE_Q)
        return {
            "gross_realized_pnl": _serialize_decimal(gross),
            "fees_paid": _serialize_decimal(fees),
            "net_realized_pnl": _serialize_decimal(net),
        }

    async def fetch_pilot_evidence_summary(
        self,
        session: AsyncSession,
        *,
        strategy_family: str = SUPPORTED_PHASE12_FAMILY,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        family = _normalized_strategy_family(strategy_family)
        observed = _ensure_utc(now) or utcnow()
        day_start, day_end = _day_bounds(observed)
        pnl = await self.compute_realized_pnl_summary(
            session,
            strategy_family=family,
            start=day_start,
            end=day_end,
        )
        if pnl["net_realized_pnl"] is not None:
            polymarket_pilot_daily_realized_pnl_usd.set(pnl["net_realized_pnl"])
        shadow_rows = (
            await session.execute(
                select(PolymarketLiveShadowEvaluation)
                .join(LiveOrder, LiveOrder.id == PolymarketLiveShadowEvaluation.live_order_id, isouter=True)
                .where(
                    LiveOrder.strategy_family == family,
                    PolymarketLiveShadowEvaluation.updated_at >= observed - timedelta(hours=24),
                )
            )
        ).scalars().all()
        shadow_gaps = [abs(_to_decimal(row.gap_bps) or ZERO) for row in shadow_rows if row.gap_bps is not None and not row.coverage_limited]
        recent_guardrails = (
            await session.execute(
                select(PolymarketPilotGuardrailEvent)
                .where(
                    PolymarketPilotGuardrailEvent.strategy_family == family,
                    PolymarketPilotGuardrailEvent.observed_at_local >= observed - timedelta(hours=24),
                )
                .order_by(PolymarketPilotGuardrailEvent.observed_at_local.desc(), PolymarketPilotGuardrailEvent.id.desc())
                .limit(5)
            )
        ).scalars().all()
        latest_readiness = (
            await session.execute(
                select(PolymarketPilotReadinessReport)
                .where(PolymarketPilotReadinessReport.strategy_family == family)
                .order_by(PolymarketPilotReadinessReport.generated_at.desc(), PolymarketPilotReadinessReport.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        latest_scorecard = (
            await session.execute(
                select(PolymarketPilotScorecard)
                .where(PolymarketPilotScorecard.strategy_family == family)
                .order_by(PolymarketPilotScorecard.window_end.desc(), PolymarketPilotScorecard.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        backlog_orders = (
            await session.execute(
                select(LiveOrder).where(
                    LiveOrder.strategy_family == family,
                    LiveOrder.approval_state == "queued",
                )
            )
        ).scalars().all()
        approval_expirations = (
            await session.execute(
                select(PolymarketPilotApprovalEvent).where(
                    PolymarketPilotApprovalEvent.action == "expired",
                    PolymarketPilotApprovalEvent.observed_at_local >= observed - timedelta(hours=24),
                )
            )
        ).scalars().all()
        strategy_version = await get_current_strategy_version(session, family)
        latest_promotion_evaluation = None
        if strategy_version is not None and strategy_version.id is not None:
            latest_promotion_evaluation = (
                await get_latest_promotion_evaluation_by_version(session, version_ids=[int(strategy_version.id)])
            ).get(int(strategy_version.id))
        latest_scorecard_payload = (
            await serialize_pilot_scorecards_with_lifecycle(session, [latest_scorecard])
            if latest_scorecard is not None
            else []
        )
        latest_readiness_payload = (
            await serialize_pilot_readiness_reports_with_lifecycle(session, [latest_readiness])
            if latest_readiness is not None
            else []
        )
        return {
            "strategy_family": family,
            "strategy_version": (
                (await get_strategy_version_snapshot_map(session, version_ids=[int(strategy_version.id)]))[int(strategy_version.id)]
                if strategy_version is not None and strategy_version.id is not None
                else None
            ),
            "latest_promotion_evaluation": latest_promotion_evaluation,
            "window_start": day_start,
            "window_end": day_end,
            "daily_realized_pnl": pnl,
            "approval_backlog_count": len(backlog_orders),
            "approval_expired_count_24h": len(approval_expirations),
            "live_shadow_summary": {
                "average_gap_bps_24h": _serialize_decimal((sum(shadow_gaps, ZERO) / Decimal(len(shadow_gaps))).quantize(PRICE_Q) if shadow_gaps else None),
                "worst_gap_bps_24h": _serialize_decimal(max(shadow_gaps) if shadow_gaps else None),
                "breach_count_24h": sum(1 for row in recent_guardrails if row.guardrail_type == "shadow_gap_breach"),
                "coverage_limited_count_24h": sum(1 for row in shadow_rows if row.coverage_limited),
            },
            "recent_guardrail_triggers": await serialize_pilot_guardrail_events_with_lifecycle(session, recent_guardrails),
            "latest_scorecard": latest_scorecard_payload[0] if latest_scorecard_payload else None,
            "latest_readiness_report": latest_readiness_payload[0] if latest_readiness_payload else None,
        }
