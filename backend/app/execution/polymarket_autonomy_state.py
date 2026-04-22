from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.strategy_registry import (
    AUTONOMY_TIER_ASSISTED_LIVE,
    AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT,
    AUTONOMY_TIER_BOUNDED_UNATTENDED,
    AUTONOMY_TIER_SHADOW_ONLY,
    DemotionEvent,
    PromotionEvaluation,
    PromotionGatePolicy,
    StrategyFamilyRegistry,
    StrategyVersion,
)
from app.risk.budgets import build_strategy_budget_status, serialize_risk_budget_status
from app.strategy_families import get_strategy_family_review

AUTONOMY_TIER_ORDER = {
    AUTONOMY_TIER_SHADOW_ONLY: 0,
    AUTONOMY_TIER_ASSISTED_LIVE: 1,
    AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT: 2,
    AUTONOMY_TIER_BOUNDED_UNATTENDED: 3,
}
ALL_PROMOTION_EVALUATION_KINDS = frozenset({
    "pilot_readiness_gate",
    "replay_gate",
    "scorecard_gate",
    "incident_gate",
    "guardrail_gate",
    "capital_budget_gate",
})


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    normalized = _ensure_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            return _ensure_utc(datetime.fromisoformat(text))
        except ValueError:
            return None
    return None


def _serialize_strategy_version(row: StrategyVersion | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "family_id": row.family_id,
        "version_key": row.version_key,
        "version_label": row.version_label,
        "strategy_name": row.strategy_name,
        "version_status": row.version_status,
        "autonomy_tier": row.autonomy_tier,
        "is_current": row.is_current,
        "is_frozen": row.is_frozen,
        "created_at": _isoformat(row.created_at),
        "updated_at": _isoformat(row.updated_at),
    }


def _serialize_gate_policy(row: PromotionGatePolicy | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "policy_key": row.policy_key,
        "policy_label": row.label,
        "policy_status": row.status,
    }


def _serialize_promotion_evaluation(row: PromotionEvaluation | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "family_id": row.family_id,
        "strategy_version_id": row.strategy_version_id,
        "gate_policy_id": row.gate_policy_id,
        "evaluation_kind": row.evaluation_kind,
        "evaluation_status": row.evaluation_status,
        "autonomy_tier": row.autonomy_tier,
        "evaluation_window_start": _isoformat(row.evaluation_window_start),
        "evaluation_window_end": _isoformat(row.evaluation_window_end),
        "provenance_json": row.provenance_json or {},
        "summary_json": row.summary_json or {},
        "created_at": _isoformat(row.created_at),
        "updated_at": _isoformat(row.updated_at),
    }


def _serialize_demotion_event(row: DemotionEvent | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "family_id": row.family_id,
        "strategy_version_id": row.strategy_version_id,
        "prior_autonomy_tier": row.prior_autonomy_tier,
        "fallback_autonomy_tier": row.fallback_autonomy_tier,
        "reason_code": row.reason_code,
        "cooling_off_ends_at": _isoformat(row.cooling_off_ends_at),
        "details_json": row.details_json or {},
        "observed_at_local": _isoformat(row.observed_at_local),
        "created_at": _isoformat(row.created_at),
        "updated_at": _isoformat(row.updated_at),
    }


def _normalize_tier(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in AUTONOMY_TIER_ORDER else AUTONOMY_TIER_SHADOW_ONLY


def min_autonomy_tier(*tiers: Any) -> str:
    normalized = [_normalize_tier(value) for value in tiers if value is not None]
    if not normalized:
        return AUTONOMY_TIER_SHADOW_ONLY
    return min(normalized, key=lambda value: AUTONOMY_TIER_ORDER.get(value, 0))


def _first_truthy(values: list[Any]) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _gate_blockers(evaluation: dict[str, Any] | None) -> list[str]:
    if not isinstance(evaluation, dict):
        return []
    summary = evaluation.get("summary_json")
    if not isinstance(summary, dict):
        summary = {}
    kind = str(evaluation.get("evaluation_kind") or "").strip().lower()
    if kind == "pilot_readiness_gate":
        blockers = summary.get("readiness_blockers")
        if isinstance(blockers, list):
            return [str(value).strip() for value in blockers if str(value).strip()]
        status = summary.get("readiness_status")
        return [str(status).strip()] if status else []
    if kind == "capital_budget_gate":
        blockers = summary.get("reason_codes")
        if isinstance(blockers, list):
            return [str(value).strip() for value in blockers if str(value).strip()]
        status = summary.get("capacity_status")
        return [str(status).strip()] if status else []
    if kind == "incident_gate":
        latest = summary.get("latest_incident_type")
        return [str(latest).strip()] if latest else []
    if kind == "guardrail_gate":
        latest = summary.get("latest_guardrail_type")
        return [str(latest).strip()] if latest else []
    if kind == "replay_gate":
        status = summary.get("replay_status")
        return [str(status).strip()] if status else []
    return []


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _resolve_submission_mode(
    *,
    effective_tier: str,
    pilot_enabled: bool | None,
    live_trading_enabled: bool | None,
    live_dry_run: bool | None,
    kill_switch_enabled: bool | None,
    active_pilot: dict[str, Any] | None,
    active_run: dict[str, Any] | None,
    manual_approval_required: bool | None,
    live_submission_permitted: bool | None,
) -> str:
    if effective_tier == AUTONOMY_TIER_SHADOW_ONLY:
        return AUTONOMY_TIER_SHADOW_ONLY
    if pilot_enabled is False:
        return "pilot_disabled"
    if live_trading_enabled is False:
        return "live_disabled"
    if live_dry_run:
        return "dry_run"
    if kill_switch_enabled:
        return "kill_switch"
    if active_run is not None and str(active_run.get("status") or "").strip().lower() == "paused":
        return "pilot_paused"
    if active_pilot is not None and not bool(active_pilot.get("armed")):
        return "pilot_disarmed"
    if active_pilot is not None and not bool(active_pilot.get("live_enabled", True)):
        return "pilot_live_disabled"
    if manual_approval_required:
        return "manual_approval"
    if live_submission_permitted is True:
        return effective_tier
    if active_pilot is not None:
        return "live_blocked"
    return "inactive"


def _version_timestamp(version: dict[str, Any] | None) -> datetime | None:
    if not isinstance(version, dict):
        return None
    return _coerce_datetime(version.get("updated_at")) or _coerce_datetime(version.get("created_at"))


def _evaluation_timestamp(evaluation: dict[str, Any] | None) -> datetime | None:
    if not isinstance(evaluation, dict):
        return None
    return (
        _coerce_datetime(evaluation.get("evaluation_window_end"))
        or _coerce_datetime(evaluation.get("updated_at"))
        or _coerce_datetime(evaluation.get("created_at"))
    )


def _demotion_timestamp(event: dict[str, Any] | None) -> datetime | None:
    if not isinstance(event, dict):
        return None
    return _coerce_datetime(event.get("observed_at_local")) or _coerce_datetime(event.get("created_at"))


def summarize_autonomy_state(
    *,
    strategy_family: str | None,
    family_source: str = "unresolved",
    strategy_version: dict[str, Any] | None = None,
    strategy_version_source: str = "unresolved",
    latest_promotion_evaluation: dict[str, Any] | None = None,
    latest_demotion_event: dict[str, Any] | None = None,
    gate_policy: dict[str, Any] | None = None,
    risk_budget_status: dict[str, Any] | None = None,
    posture: str | None = None,
    supported_strategy_family: str | None = None,
    pilot_enabled: bool | None = None,
    live_trading_enabled: bool | None = None,
    live_dry_run: bool | None = None,
    kill_switch_enabled: bool | None = None,
    manual_approval_required: bool | None = None,
    live_submission_permitted: bool | None = None,
    active_pilot: dict[str, Any] | None = None,
    active_run: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    observed_now = _ensure_utc(now) or datetime.now(timezone.utc)
    configured_tier = _normalize_tier(strategy_version.get("autonomy_tier") if isinstance(strategy_version, dict) else None)
    raw_recommended_tier = (
        latest_promotion_evaluation.get("autonomy_tier")
        if isinstance(latest_promotion_evaluation, dict)
        else None
    )
    recommended_tier = _normalize_tier(raw_recommended_tier) if raw_recommended_tier is not None else configured_tier
    effective_tier = min_autonomy_tier(configured_tier, recommended_tier)
    if manual_approval_required is None and isinstance(active_pilot, dict):
        manual_approval_required = bool(active_pilot.get("manual_approval_required"))

    demotion_reason = None
    demotion_active = False
    cooling_off_active = False
    cooling_off_ends_at = _coerce_datetime(
        latest_demotion_event.get("cooling_off_ends_at")
        if isinstance(latest_demotion_event, dict)
        else None
    )
    if cooling_off_ends_at is not None and cooling_off_ends_at >= observed_now:
        cooling_off_active = True
    demotion_timestamp = _demotion_timestamp(latest_demotion_event)
    evaluation_timestamp = _evaluation_timestamp(latest_promotion_evaluation)
    if isinstance(latest_demotion_event, dict):
        demotion_reason = str(latest_demotion_event.get("reason_code") or "").strip() or None
        demotion_fallback = _normalize_tier(latest_demotion_event.get("fallback_autonomy_tier"))
        demotion_active = cooling_off_active or (
            demotion_timestamp is not None
            and (evaluation_timestamp is None or demotion_timestamp >= evaluation_timestamp)
        )
        if demotion_active:
            effective_tier = min_autonomy_tier(effective_tier, demotion_fallback)

    posture_reason = None
    normalized_posture = str(posture or "").strip().lower() or None
    if normalized_posture == "benchmark_only":
        posture_reason = "benchmark_only"
        effective_tier = AUTONOMY_TIER_SHADOW_ONLY
    elif normalized_posture == "disabled":
        posture_reason = "family_disabled"
        effective_tier = AUTONOMY_TIER_SHADOW_ONLY

    gate_blockers = _gate_blockers(latest_promotion_evaluation)
    budget_reasons = []
    if isinstance(risk_budget_status, dict):
        reason_codes = risk_budget_status.get("reason_codes")
        if isinstance(reason_codes, list):
            budget_reasons = [str(value).strip() for value in reason_codes if str(value).strip()]

    live_blockers: list[str] = []
    if supported_strategy_family and strategy_family and strategy_family != supported_strategy_family:
        live_blockers.append("unsupported_rollout_family")
    if pilot_enabled is False:
        live_blockers.append("pilot_disabled")
    if live_trading_enabled is False:
        live_blockers.append("live_trading_disabled")
    if live_dry_run:
        live_blockers.append("dry_run")
    if kill_switch_enabled:
        live_blockers.append("kill_switch_enabled")
    if active_run is not None and str(active_run.get("status") or "").strip().lower() == "paused":
        live_blockers.append("pilot_paused")
    if active_pilot is not None and not bool(active_pilot.get("armed")):
        live_blockers.append("pilot_not_armed")
    if active_pilot is not None and not bool(active_pilot.get("live_enabled", True)):
        live_blockers.append("pilot_live_disabled")
    if manual_approval_required:
        live_blockers.append("manual_approval_required")

    blocked_reasons = _dedupe([
        posture_reason,
        demotion_reason if demotion_active else None,
        *budget_reasons,
        *gate_blockers,
        *live_blockers,
    ])
    state_reason = _first_truthy(blocked_reasons) or (
        "shadow_only_configured"
        if effective_tier == AUTONOMY_TIER_SHADOW_ONLY and configured_tier == AUTONOMY_TIER_SHADOW_ONLY
        else None
    )
    latest_change_at = _first_truthy([
        _isoformat(demotion_timestamp if demotion_active else None),
        _isoformat(evaluation_timestamp),
        _isoformat(_version_timestamp(strategy_version)),
    ])
    gate = None
    if latest_promotion_evaluation is not None:
        gate = {
            "policy_id": latest_promotion_evaluation.get("gate_policy_id"),
            "policy_key": gate_policy.get("policy_key") if isinstance(gate_policy, dict) else None,
            "policy_label": gate_policy.get("policy_label") if isinstance(gate_policy, dict) else None,
            "policy_status": gate_policy.get("policy_status") if isinstance(gate_policy, dict) else None,
            "evaluation_kind": latest_promotion_evaluation.get("evaluation_kind"),
            "evaluation_status": latest_promotion_evaluation.get("evaluation_status"),
            "autonomy_tier": latest_promotion_evaluation.get("autonomy_tier"),
            "evaluation_window_start": latest_promotion_evaluation.get("evaluation_window_start"),
            "evaluation_window_end": latest_promotion_evaluation.get("evaluation_window_end"),
            "created_at": latest_promotion_evaluation.get("created_at"),
        }
    submission_mode = _resolve_submission_mode(
        effective_tier=effective_tier,
        pilot_enabled=pilot_enabled,
        live_trading_enabled=live_trading_enabled,
        live_dry_run=live_dry_run,
        kill_switch_enabled=kill_switch_enabled,
        active_pilot=active_pilot,
        active_run=active_run,
        manual_approval_required=manual_approval_required,
        live_submission_permitted=live_submission_permitted,
    )
    if demotion_active and submission_mode not in {
        "pilot_disabled",
        "live_disabled",
        "dry_run",
        "kill_switch",
        "pilot_paused",
        "pilot_disarmed",
        "pilot_live_disabled",
        "manual_approval",
    }:
        submission_mode = "demoted_fallback"
    return {
        "strategy_family": strategy_family,
        "family_source": family_source,
        "posture": normalized_posture,
        "strategy_version": strategy_version,
        "strategy_version_source": strategy_version_source,
        "configured_autonomy_tier": configured_tier,
        "recommended_autonomy_tier": recommended_tier,
        "effective_autonomy_tier": effective_tier,
        "effective_tier_source": (
            "demotion_event"
            if demotion_active
            else "promotion_evaluation"
            if latest_promotion_evaluation is not None
            else "strategy_version"
            if strategy_version is not None
            else "unresolved"
        ),
        "gate": gate,
        "latest_promotion_evaluation": latest_promotion_evaluation,
        "latest_demotion_event": latest_demotion_event,
        "demotion_active": demotion_active,
        "cooling_off_active": cooling_off_active,
        "cooling_off_ends_at": _isoformat(cooling_off_ends_at),
        "blocked_reasons": blocked_reasons,
        "state_reason": state_reason,
        "submission_mode": submission_mode,
        "operator_required": bool(manual_approval_required),
        "live_submission_permitted": live_submission_permitted,
        "pilot_run_status": active_run.get("status") if isinstance(active_run, dict) else None,
        "gate_status": latest_promotion_evaluation.get("evaluation_status") if isinstance(latest_promotion_evaluation, dict) else None,
        "gate_kind": latest_promotion_evaluation.get("evaluation_kind") if isinstance(latest_promotion_evaluation, dict) else None,
        "latest_change_at": latest_change_at,
        "as_of": observed_now.isoformat(),
    }


async def get_latest_demotion_event_by_version(
    session: AsyncSession,
    *,
    version_ids: list[int] | set[int] | tuple[int, ...],
) -> dict[int, dict[str, Any]]:
    normalized_ids = sorted({int(value) for value in version_ids if value is not None})
    if not normalized_ids:
        return {}
    rows = (
        await session.execute(
            select(DemotionEvent)
            .where(DemotionEvent.strategy_version_id.in_(normalized_ids))
            .order_by(DemotionEvent.observed_at_local.desc(), DemotionEvent.id.desc())
        )
    ).scalars().all()
    latest_by_version: dict[int, dict[str, Any]] = {}
    for row in rows:
        latest_by_version.setdefault(int(row.strategy_version_id), _serialize_demotion_event(row))
    return latest_by_version


async def build_active_autonomy_state(
    session: AsyncSession,
    *,
    strategy_family: str | None = None,
    strategy_version_id: int | None = None,
    family_source: str = "unresolved",
    strategy_version: dict[str, Any] | None = None,
    latest_promotion_evaluation: dict[str, Any] | None = None,
    latest_demotion_event: dict[str, Any] | None = None,
    risk_budget_status: dict[str, Any] | None = None,
    posture: str | None = None,
    supported_strategy_family: str | None = None,
    pilot_enabled: bool | None = None,
    live_trading_enabled: bool | None = None,
    live_dry_run: bool | None = None,
    kill_switch_enabled: bool | None = None,
    manual_approval_required: bool | None = None,
    live_submission_permitted: bool | None = None,
    active_pilot: dict[str, Any] | None = None,
    active_run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_family = str(strategy_family).strip().lower() if strategy_family else None
    strategy_version_source = "explicit_version" if strategy_version is not None or strategy_version_id is not None else "unresolved"
    family_row: StrategyFamilyRegistry | None = None

    if strategy_version is None:
        version_row = None
        if strategy_version_id is not None:
            version_row = await session.get(StrategyVersion, int(strategy_version_id))
        elif resolved_family:
            version_row = (
                await session.execute(
                    select(StrategyVersion, StrategyFamilyRegistry)
                    .join(StrategyFamilyRegistry, StrategyFamilyRegistry.id == StrategyVersion.family_id)
                    .where(
                        StrategyFamilyRegistry.family == resolved_family,
                        StrategyVersion.is_current.is_(True),
                    )
                    .order_by(StrategyVersion.updated_at.desc(), StrategyVersion.id.desc())
                    .limit(1)
                )
            ).one_or_none()
            if version_row is not None:
                version_row, family_row = version_row
        if version_row is not None:
            strategy_version = _serialize_strategy_version(version_row)
            strategy_version_id = int(version_row.id) if version_row.id is not None else strategy_version_id
            strategy_version_source = "current_registry_version"
            if resolved_family is None and family_row is None:
                family_row = await session.get(StrategyFamilyRegistry, int(version_row.family_id))
            if resolved_family is None and family_row is not None:
                resolved_family = family_row.family
                family_source = "current_registry_version"

    if family_row is None and resolved_family is not None:
        family_row = (
            await session.execute(
                select(StrategyFamilyRegistry).where(StrategyFamilyRegistry.family == resolved_family).limit(1)
            )
        ).scalar_one_or_none()
    if posture is None and resolved_family is not None:
        review = get_strategy_family_review(resolved_family)
        posture = review.get("posture") if review is not None else None

    if latest_promotion_evaluation is None and strategy_version_id is not None:
        evaluation_row = (
            await session.execute(
                select(PromotionEvaluation)
                .where(
                    PromotionEvaluation.strategy_version_id == int(strategy_version_id),
                    PromotionEvaluation.evaluation_kind.in_(tuple(sorted(ALL_PROMOTION_EVALUATION_KINDS))),
                )
                .order_by(PromotionEvaluation.created_at.desc(), PromotionEvaluation.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        latest_promotion_evaluation = _serialize_promotion_evaluation(evaluation_row)

    if latest_demotion_event is None and strategy_version_id is not None:
        latest_demotion_event = (
            await get_latest_demotion_event_by_version(session, version_ids=[int(strategy_version_id)])
        ).get(int(strategy_version_id))

    if risk_budget_status is None and resolved_family:
        risk_budget_status = serialize_risk_budget_status(
            await build_strategy_budget_status(
                session,
                strategy_family=resolved_family,
                strategy_version_id=int(strategy_version_id) if strategy_version_id is not None else None,
            )
        )

    gate_policy = None
    if (
        latest_promotion_evaluation is not None
        and latest_promotion_evaluation.get("gate_policy_id") is not None
    ):
        gate_policy_row = await session.get(PromotionGatePolicy, int(latest_promotion_evaluation["gate_policy_id"]))
        gate_policy = _serialize_gate_policy(gate_policy_row)

    return summarize_autonomy_state(
        strategy_family=resolved_family,
        family_source=family_source,
        strategy_version=strategy_version,
        strategy_version_source=strategy_version_source,
        latest_promotion_evaluation=latest_promotion_evaluation,
        latest_demotion_event=latest_demotion_event,
        gate_policy=gate_policy,
        risk_budget_status=risk_budget_status,
        posture=posture or (review.get("posture") if resolved_family and (review := get_strategy_family_review(resolved_family)) else None),
        supported_strategy_family=supported_strategy_family,
        pilot_enabled=pilot_enabled,
        live_trading_enabled=live_trading_enabled,
        live_dry_run=live_dry_run,
        kill_switch_enabled=kill_switch_enabled,
        manual_approval_required=manual_approval_required,
        live_submission_permitted=live_submission_permitted,
        active_pilot=active_pilot,
        active_run=active_run,
    )


__all__ = [
    "build_active_autonomy_state",
    "get_latest_demotion_event_by_version",
    "min_autonomy_tier",
    "summarize_autonomy_state",
]
