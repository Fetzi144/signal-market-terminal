from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.strategy_registry import (
    AUTONOMY_TIER_ASSISTED_LIVE,
    AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT,
    AUTONOMY_TIER_SHADOW_ONLY,
    DemotionEvent,
    PromotionEvaluation,
    PromotionGatePolicy,
    StrategyFamilyRegistry,
    StrategyVersion,
)
from app.risk.budgets import build_strategy_budget_status, serialize_risk_budget_status

PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY = "promotion_eligibility_gate"
PROMOTION_EVALUATION_KIND_PILOT_READINESS = "pilot_readiness_gate"
PROMOTION_EVALUATION_KIND_REPLAY = "replay_gate"
PROMOTION_EVALUATION_KIND_SCORECARD = "scorecard_gate"
PROMOTION_EVALUATION_KIND_INCIDENT = "incident_gate"
PROMOTION_EVALUATION_KIND_GUARDRAIL = "guardrail_gate"
PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET = "capital_budget_gate"
PROMOTION_EVALUATION_STATUS_BLOCKED = "blocked"
PROMOTION_EVALUATION_STATUS_OBSERVE = "observe"
PROMOTION_EVALUATION_STATUS_CANDIDATE = "candidate"
PRIMARY_PROMOTION_EVALUATION_KINDS = frozenset({
    PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY,
    PROMOTION_EVALUATION_KIND_PILOT_READINESS,
    PROMOTION_EVALUATION_KIND_REPLAY,
})
SUPPORTING_PROMOTION_EVALUATION_KINDS = frozenset({
    PROMOTION_EVALUATION_KIND_SCORECARD,
    PROMOTION_EVALUATION_KIND_INCIDENT,
    PROMOTION_EVALUATION_KIND_GUARDRAIL,
    PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET,
})
ELIGIBILITY_SOURCE_EVALUATION_KINDS = frozenset({
    PROMOTION_EVALUATION_KIND_REPLAY,
    PROMOTION_EVALUATION_KIND_PILOT_READINESS,
    PROMOTION_EVALUATION_KIND_SCORECARD,
    PROMOTION_EVALUATION_KIND_INCIDENT,
    PROMOTION_EVALUATION_KIND_GUARDRAIL,
    PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET,
})
ALL_PROMOTION_EVALUATION_KINDS = PRIMARY_PROMOTION_EVALUATION_KINDS | SUPPORTING_PROMOTION_EVALUATION_KINDS
PROMOTION_EVIDENCE_ROLLING_WINDOW = timedelta(hours=24)
PROMOTION_DEMOTION_COOLING_OFF = timedelta(hours=24)
AUTONOMY_TIER_ORDER = {
    AUTONOMY_TIER_SHADOW_ONLY: 0,
    AUTONOMY_TIER_ASSISTED_LIVE: 1,
    AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT: 2,
}


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        normalized = _ensure_utc(value)
        return normalized.isoformat() if normalized is not None else None
    return value


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def hash_json_payload(payload: Any) -> str | None:
    if payload in (None, {}, [], ""):
        return None
    encoded = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def normalize_promotion_evaluation_kinds(
    kinds: Collection[str] | None = None,
    *,
    include_supporting: bool = False,
) -> tuple[str, ...]:
    if kinds is not None:
        return tuple(sorted({str(kind).strip() for kind in kinds if str(kind).strip()}))
    selected = ALL_PROMOTION_EVALUATION_KINDS if include_supporting else PRIMARY_PROMOTION_EVALUATION_KINDS
    return tuple(sorted(selected))


def rolling_promotion_window_bounds(
    observed_at: datetime | None,
    *,
    window: timedelta = PROMOTION_EVIDENCE_ROLLING_WINDOW,
) -> tuple[datetime | None, datetime | None]:
    end = _ensure_utc(observed_at)
    if end is None:
        return None, None
    return end - window, end


def map_readiness_status_to_promotion_verdict(readiness_status: str | None) -> tuple[str, str]:
    normalized = str(readiness_status or "").strip().lower()
    if normalized == "candidate_for_semi_auto":
        return PROMOTION_EVALUATION_STATUS_CANDIDATE, AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT
    if normalized == "manual_only":
        return PROMOTION_EVALUATION_STATUS_OBSERVE, AUTONOMY_TIER_ASSISTED_LIVE
    return PROMOTION_EVALUATION_STATUS_BLOCKED, AUTONOMY_TIER_SHADOW_ONLY


def map_replay_summary_to_promotion_verdict(
    *,
    run_status: str | None,
    coverage_limited_scenarios: int = 0,
    variant_count: int = 0,
) -> tuple[str, str]:
    normalized_status = str(run_status or "").strip().lower()
    if normalized_status == "failed":
        return PROMOTION_EVALUATION_STATUS_BLOCKED, AUTONOMY_TIER_SHADOW_ONLY
    if coverage_limited_scenarios > 0 or variant_count <= 0:
        return PROMOTION_EVALUATION_STATUS_BLOCKED, AUTONOMY_TIER_SHADOW_ONLY
    return PROMOTION_EVALUATION_STATUS_OBSERVE, AUTONOMY_TIER_SHADOW_ONLY


def map_scorecard_status_to_promotion_verdict(scorecard_status: str | None) -> tuple[str, str]:
    normalized = str(scorecard_status or "").strip().lower()
    if normalized == "blocked":
        return PROMOTION_EVALUATION_STATUS_BLOCKED, AUTONOMY_TIER_SHADOW_ONLY
    return PROMOTION_EVALUATION_STATUS_OBSERVE, AUTONOMY_TIER_ASSISTED_LIVE


def map_incident_summary_to_promotion_verdict(
    *,
    incident_count: int = 0,
) -> tuple[str, str]:
    if incident_count > 0:
        return PROMOTION_EVALUATION_STATUS_BLOCKED, AUTONOMY_TIER_SHADOW_ONLY
    return PROMOTION_EVALUATION_STATUS_OBSERVE, AUTONOMY_TIER_ASSISTED_LIVE


def map_guardrail_summary_to_promotion_verdict(
    *,
    guardrail_count: int = 0,
    serious_guardrail_count: int = 0,
    shadow_gap_breach_count: int = 0,
    latest_severity: str | None = None,
) -> tuple[str, str]:
    normalized_severity = str(latest_severity or "").strip().lower()
    if serious_guardrail_count > 0 or shadow_gap_breach_count > 0 or normalized_severity in {"error", "critical"}:
        return PROMOTION_EVALUATION_STATUS_BLOCKED, AUTONOMY_TIER_SHADOW_ONLY
    if guardrail_count > 0:
        return PROMOTION_EVALUATION_STATUS_OBSERVE, AUTONOMY_TIER_ASSISTED_LIVE
    return PROMOTION_EVALUATION_STATUS_OBSERVE, AUTONOMY_TIER_ASSISTED_LIVE


def map_capital_budget_summary_to_promotion_verdict(
    *,
    breach: bool = False,
) -> tuple[str, str]:
    if breach:
        return PROMOTION_EVALUATION_STATUS_BLOCKED, AUTONOMY_TIER_SHADOW_ONLY
    return PROMOTION_EVALUATION_STATUS_OBSERVE, AUTONOMY_TIER_ASSISTED_LIVE


def _normalize_tier(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in AUTONOMY_TIER_ORDER else AUTONOMY_TIER_SHADOW_ONLY


def min_promotion_autonomy_tier(*tiers: Any) -> str:
    normalized = [_normalize_tier(value) for value in tiers if value is not None]
    if not normalized:
        return AUTONOMY_TIER_SHADOW_ONLY
    return min(normalized, key=lambda value: AUTONOMY_TIER_ORDER.get(value, 0))


def append_promotion_evaluation(
    session: AsyncSession,
    *,
    family_id: int,
    strategy_version_id: int,
    gate_policy_id: int | None,
    evaluation_kind: str,
    evaluation_status: str,
    autonomy_tier: str,
    evaluation_window_start: datetime | None,
    evaluation_window_end: datetime | None,
    provenance_json: dict[str, Any] | None,
    summary_json: dict[str, Any] | None,
) -> PromotionEvaluation:
    row = PromotionEvaluation(
        family_id=family_id,
        strategy_version_id=strategy_version_id,
        gate_policy_id=gate_policy_id,
        evaluation_kind=evaluation_kind,
        evaluation_status=evaluation_status,
        autonomy_tier=autonomy_tier,
        evaluation_window_start=evaluation_window_start,
        evaluation_window_end=evaluation_window_end,
        provenance_json=provenance_json or {},
        summary_json=summary_json or {},
    )
    session.add(row)
    return row


async def upsert_promotion_evaluation(
    session: AsyncSession,
    *,
    family_id: int,
    strategy_version_id: int,
    gate_policy_id: int | None,
    evaluation_kind: str,
    evaluation_status: str,
    autonomy_tier: str,
    evaluation_window_start: datetime | None,
    evaluation_window_end: datetime | None,
    provenance_json: dict[str, Any] | None,
    summary_json: dict[str, Any] | None,
) -> PromotionEvaluation:
    existing = (
        await session.execute(
            select(PromotionEvaluation)
            .where(
                PromotionEvaluation.family_id == family_id,
                PromotionEvaluation.strategy_version_id == strategy_version_id,
                PromotionEvaluation.gate_policy_id == gate_policy_id,
                PromotionEvaluation.evaluation_kind == evaluation_kind,
                PromotionEvaluation.evaluation_window_start == evaluation_window_start,
                PromotionEvaluation.evaluation_window_end == evaluation_window_end,
            )
            .order_by(PromotionEvaluation.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    row = existing or PromotionEvaluation(
        family_id=family_id,
        strategy_version_id=strategy_version_id,
        gate_policy_id=gate_policy_id,
        evaluation_kind=evaluation_kind,
        evaluation_window_start=evaluation_window_start,
        evaluation_window_end=evaluation_window_end,
    )
    row.evaluation_status = evaluation_status
    row.autonomy_tier = autonomy_tier
    row.provenance_json = provenance_json or {}
    row.summary_json = summary_json or {}
    if existing is None:
        session.add(row)
    await session.flush()
    return row


def _latest_row_timestamp(row: PromotionEvaluation | None) -> datetime | None:
    if row is None:
        return None
    return _ensure_utc(row.evaluation_window_end) or _ensure_utc(row.updated_at) or _ensure_utc(row.created_at)


def _earliest_row_timestamp(row: PromotionEvaluation | None) -> datetime | None:
    if row is None:
        return None
    return _ensure_utc(row.evaluation_window_start) or _ensure_utc(row.created_at) or _ensure_utc(row.updated_at)


def _first_timestamp(*values: datetime | None) -> datetime | None:
    candidates = [_ensure_utc(value) for value in values if value is not None]
    return min(candidates) if candidates else None


def _last_timestamp(*values: datetime | None) -> datetime | None:
    candidates = [_ensure_utc(value) for value in values if value is not None]
    return max(candidates) if candidates else None


def _blocker(
    code: str,
    *,
    source: str,
    severity: str,
    hard: bool,
    detail: Any = None,
) -> dict[str, Any]:
    return {
        "code": str(code).strip(),
        "source": str(source).strip(),
        "severity": str(severity).strip(),
        "hard": bool(hard),
        "detail": _json_safe(detail),
    }


def _dedupe_blockers(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, bool]] = set()
    for value in values:
        code = str(value.get("code") or "").strip()
        source = str(value.get("source") or "").strip()
        hard = bool(value.get("hard"))
        key = (code, source, hard)
        if not code or key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _summary_dict(row: PromotionEvaluation | None) -> dict[str, Any]:
    return row.summary_json if row is not None and isinstance(row.summary_json, dict) else {}


def _compact_input_summary(row: PromotionEvaluation | None) -> dict[str, Any]:
    if row is None:
        return {"present": False}
    summary = _summary_dict(row)
    observed_at = _last_timestamp(row.evaluation_window_end, row.updated_at, row.created_at)
    payload: dict[str, Any] = {
        "present": True,
        "evaluation_id": row.id,
        "evaluation_status": row.evaluation_status,
        "autonomy_tier": row.autonomy_tier,
        "observed_at": observed_at.isoformat() if observed_at is not None else None,
    }
    if row.evaluation_kind == PROMOTION_EVALUATION_KIND_REPLAY:
        payload.update({
            "status": summary.get("replay_status"),
            "coverage_limited_scenarios": summary.get("coverage_limited_scenarios"),
            "variant_count": summary.get("variant_count"),
        })
    elif row.evaluation_kind == PROMOTION_EVALUATION_KIND_PILOT_READINESS:
        payload.update({
            "status": summary.get("readiness_status"),
            "blockers": summary.get("readiness_blockers") or [],
            "approval_backlog_count": summary.get("approval_backlog_count"),
            "incident_count": summary.get("incident_count"),
        })
    elif row.evaluation_kind == PROMOTION_EVALUATION_KIND_SCORECARD:
        payload.update({
            "status": summary.get("scorecard_status"),
            "net_pnl": summary.get("net_pnl"),
            "coverage_limited_count": summary.get("coverage_limited_count"),
            "incident_count": summary.get("incident_count"),
        })
    elif row.evaluation_kind == PROMOTION_EVALUATION_KIND_INCIDENT:
        payload.update({
            "incident_count_24h": summary.get("incident_count_24h"),
            "latest_incident_type": summary.get("latest_incident_type"),
            "latest_severity": summary.get("latest_severity"),
        })
    elif row.evaluation_kind == PROMOTION_EVALUATION_KIND_GUARDRAIL:
        payload.update({
            "guardrail_count_24h": summary.get("guardrail_count_24h"),
            "serious_guardrail_count_24h": summary.get("serious_guardrail_count_24h"),
            "shadow_gap_breach_count_24h": summary.get("shadow_gap_breach_count_24h"),
            "latest_guardrail_type": summary.get("latest_guardrail_type"),
            "latest_severity": summary.get("latest_severity"),
        })
    elif row.evaluation_kind == PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET:
        payload.update({
            "capacity_status": summary.get("capacity_status"),
            "regime_label": summary.get("regime_label"),
            "risk_of_ruin_label": summary.get("risk_of_ruin_label"),
            "reason_codes": summary.get("reason_codes") or [],
            "breach": bool(summary.get("breach")),
        })
    return payload


def _component_hash(row: PromotionEvaluation | None) -> str | None:
    if row is None:
        return None
    return hash_json_payload(
        {
            "id": row.id,
            "evaluation_kind": row.evaluation_kind,
            "evaluation_status": row.evaluation_status,
            "autonomy_tier": row.autonomy_tier,
            "updated_at": _ensure_utc(row.updated_at),
            "summary": row.summary_json or {},
        }
    )


def _latest_row_before(
    rows: list[PromotionEvaluation],
    *,
    evaluation_kind: str,
    cutoff_at: datetime | None,
) -> PromotionEvaluation | None:
    best_row: PromotionEvaluation | None = None
    best_key: tuple[datetime, datetime, int] | None = None
    minimum_timestamp = datetime.min.replace(tzinfo=timezone.utc)
    for row in rows:
        if row.evaluation_kind != evaluation_kind:
            continue
        latest_at = _latest_row_timestamp(row)
        if cutoff_at is not None and latest_at is not None and latest_at > cutoff_at:
            continue
        row_key = (
            latest_at or minimum_timestamp,
            _ensure_utc(row.created_at) or minimum_timestamp,
            int(row.id or 0),
        )
        if best_key is None or row_key > best_key:
            best_row = row
            best_key = row_key
    return best_row


def _budget_status_from_row(row: PromotionEvaluation | None) -> dict[str, Any] | None:
    summary = _summary_dict(row)
    if not summary:
        return None
    if isinstance(summary.get("status"), dict):
        return serialize_risk_budget_status(summary.get("status"))
    return serialize_risk_budget_status(summary)


def _stable_budget_status(status: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(status, dict):
        return {}
    return {
        key: value
        for key, value in status.items()
        if key != "computed_at"
    }


def _evaluation_status_for_tier(tier: str) -> str:
    normalized = _normalize_tier(tier)
    if normalized == AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT:
        return PROMOTION_EVALUATION_STATUS_CANDIDATE
    if normalized == AUTONOMY_TIER_ASSISTED_LIVE:
        return PROMOTION_EVALUATION_STATUS_OBSERVE
    return PROMOTION_EVALUATION_STATUS_BLOCKED


def _demotion_reason_from_evaluation(row: PromotionEvaluation) -> str:
    summary = row.summary_json if isinstance(row.summary_json, dict) else {}
    decision = summary.get("decision") if isinstance(summary.get("decision"), dict) else {}
    state_reason = str(decision.get("state_reason") or "").strip()
    if state_reason:
        return state_reason
    blockers = summary.get("blockers")
    if isinstance(blockers, list):
        for blocker in blockers:
            if isinstance(blocker, dict):
                code = str(blocker.get("code") or "").strip()
                if code:
                    return code
            elif str(blocker).strip():
                return str(blocker).strip()
    blocker_codes = summary.get("blocker_codes")
    if isinstance(blocker_codes, list):
        for code in blocker_codes:
            if str(code).strip():
                return str(code).strip()
    return "promotion_gate_blocked"


async def record_demotion_event_from_promotion_evaluation(
    session: AsyncSession,
    *,
    evaluation: PromotionEvaluation | None,
    trigger_kind: str | None = None,
    trigger_ref: str | None = None,
    observed_at: datetime | None = None,
    cooling_off: timedelta = PROMOTION_DEMOTION_COOLING_OFF,
) -> DemotionEvent | None:
    if evaluation is None:
        return None
    if evaluation.evaluation_kind != PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY:
        return None
    if evaluation.evaluation_status != PROMOTION_EVALUATION_STATUS_BLOCKED:
        return None
    if _normalize_tier(evaluation.autonomy_tier) != AUTONOMY_TIER_SHADOW_ONLY:
        return None

    version = await session.get(StrategyVersion, int(evaluation.strategy_version_id))
    if version is not None and _normalize_tier(version.autonomy_tier) == AUTONOMY_TIER_SHADOW_ONLY:
        return None

    effective_observed = _ensure_utc(observed_at) or _ensure_utc(evaluation.evaluation_window_end) or datetime.now(timezone.utc)
    reason_code = _demotion_reason_from_evaluation(evaluation)
    recent = (
        await session.execute(
            select(DemotionEvent)
            .where(
                DemotionEvent.family_id == int(evaluation.family_id),
                DemotionEvent.strategy_version_id == int(evaluation.strategy_version_id),
                DemotionEvent.reason_code == reason_code,
                DemotionEvent.cooling_off_ends_at >= effective_observed,
            )
            .order_by(DemotionEvent.observed_at_local.desc(), DemotionEvent.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if recent is not None:
        return recent

    summary = evaluation.summary_json if isinstance(evaluation.summary_json, dict) else {}
    row = DemotionEvent(
        family_id=int(evaluation.family_id),
        strategy_version_id=int(evaluation.strategy_version_id),
        prior_autonomy_tier=version.autonomy_tier if version is not None else None,
        fallback_autonomy_tier=AUTONOMY_TIER_SHADOW_ONLY,
        reason_code=reason_code,
        cooling_off_ends_at=effective_observed + cooling_off,
        details_json={
            "source": "promotion_eligibility_engine",
            "evaluation_id": evaluation.id,
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "evaluation_status": evaluation.evaluation_status,
            "evaluation_autonomy_tier": evaluation.autonomy_tier,
            "decision": summary.get("decision") if isinstance(summary.get("decision"), dict) else {},
            "blocker_codes": summary.get("blocker_codes") if isinstance(summary.get("blocker_codes"), list) else [],
        },
        observed_at_local=effective_observed,
    )
    session.add(row)
    await session.flush()
    return row


async def record_promotion_eligibility_evaluation(
    session: AsyncSession,
    *,
    strategy_version_id: int,
    trigger_kind: str | None = None,
    trigger_ref: str | None = None,
    observed_at: datetime | None = None,
) -> PromotionEvaluation | None:
    version_row = (
        await session.execute(
            select(StrategyVersion, StrategyFamilyRegistry)
            .join(StrategyFamilyRegistry, StrategyFamilyRegistry.id == StrategyVersion.family_id)
            .where(StrategyVersion.id == int(strategy_version_id))
            .limit(1)
        )
    ).one_or_none()
    if version_row is None:
        return None
    version, family_row = version_row
    gate_policy = (
        await session.execute(
            select(PromotionGatePolicy)
            .where(PromotionGatePolicy.policy_key == "promotion_gate_policy_v1")
            .order_by(PromotionGatePolicy.updated_at.desc(), PromotionGatePolicy.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()

    source_rows = (
        await session.execute(
            select(PromotionEvaluation)
            .where(
                PromotionEvaluation.strategy_version_id == int(strategy_version_id),
                PromotionEvaluation.evaluation_kind.in_(tuple(sorted(ELIGIBILITY_SOURCE_EVALUATION_KINDS))),
            )
            .order_by(PromotionEvaluation.created_at.desc(), PromotionEvaluation.id.desc())
        )
    ).scalars().all()
    evaluation_reference_at = _ensure_utc(observed_at)
    replay_row = _latest_row_before(
        source_rows,
        evaluation_kind=PROMOTION_EVALUATION_KIND_REPLAY,
        cutoff_at=evaluation_reference_at,
    )
    readiness_row = _latest_row_before(
        source_rows,
        evaluation_kind=PROMOTION_EVALUATION_KIND_PILOT_READINESS,
        cutoff_at=evaluation_reference_at,
    )
    scorecard_row = _latest_row_before(
        source_rows,
        evaluation_kind=PROMOTION_EVALUATION_KIND_SCORECARD,
        cutoff_at=evaluation_reference_at,
    )
    incident_row = _latest_row_before(
        source_rows,
        evaluation_kind=PROMOTION_EVALUATION_KIND_INCIDENT,
        cutoff_at=evaluation_reference_at,
    )
    guardrail_row = _latest_row_before(
        source_rows,
        evaluation_kind=PROMOTION_EVALUATION_KIND_GUARDRAIL,
        cutoff_at=evaluation_reference_at,
    )
    budget_gate_row = _latest_row_before(
        source_rows,
        evaluation_kind=PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET,
        cutoff_at=evaluation_reference_at,
    )
    latest_by_kind = {
        row.evaluation_kind: row
        for row in (
            replay_row,
            readiness_row,
            scorecard_row,
            incident_row,
            guardrail_row,
            budget_gate_row,
        )
        if row is not None
    }

    if not latest_by_kind and trigger_kind is None:
        return None

    budget_status = _budget_status_from_row(budget_gate_row)
    if budget_status is None:
        budget_status = serialize_risk_budget_status(
            await build_strategy_budget_status(
                session,
                strategy_family=family_row.family,
                strategy_version_id=int(strategy_version_id),
                now=evaluation_reference_at,
            )
        ) or {}
    stable_budget_status = _stable_budget_status(budget_status)

    blockers: list[dict[str, Any]] = []
    missing_inputs: list[str] = []
    component_ceilings: dict[str, str] = {}
    max_supported_tier = AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT

    replay_summary = _summary_dict(replay_row)
    if replay_row is None:
        component_ceilings["replay"] = AUTONOMY_TIER_SHADOW_ONLY
        missing_inputs.append("replay")
        blockers.append(_blocker("replay_missing", source="replay", severity="error", hard=True))
    elif str(replay_summary.get("replay_status") or "").strip().lower() == "failed":
        component_ceilings["replay"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(_blocker("replay_failed", source="replay", severity="error", hard=True, detail=replay_summary.get("replay_status")))
    elif int(replay_summary.get("coverage_limited_scenarios") or 0) > 0:
        component_ceilings["replay"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(
            _blocker(
                "replay_coverage_limited",
                source="replay",
                severity="error",
                hard=True,
                detail=replay_summary.get("coverage_limited_scenarios"),
            )
        )
    elif int(replay_summary.get("variant_count") or 0) <= 0:
        component_ceilings["replay"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(_blocker("replay_missing", source="replay", severity="error", hard=True, detail="no_variants"))
    else:
        component_ceilings["replay"] = max_supported_tier

    readiness_summary = _summary_dict(readiness_row)
    readiness_status = str(readiness_summary.get("readiness_status") or "").strip().lower()
    readiness_blockers = [
        str(value).strip()
        for value in (readiness_summary.get("readiness_blockers") or [])
        if str(value).strip()
    ]
    if readiness_row is None:
        component_ceilings["readiness"] = AUTONOMY_TIER_SHADOW_ONLY
        missing_inputs.append("readiness")
        blockers.append(_blocker("readiness_missing", source="readiness", severity="error", hard=True))
    elif readiness_status == "candidate_for_semi_auto":
        component_ceilings["readiness"] = AUTONOMY_TIER_BOUNDED_AUTO_SUBMIT
    elif readiness_status == "manual_only":
        component_ceilings["readiness"] = AUTONOMY_TIER_ASSISTED_LIVE
    else:
        component_ceilings["readiness"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(
            _blocker(
                "readiness_not_ready",
                source="readiness",
                severity="error",
                hard=True,
                detail=readiness_status or "unknown",
            )
        )
    for blocker_code in readiness_blockers:
        blockers.append(
            _blocker(
                blocker_code,
                source="readiness",
                severity="error" if component_ceilings["readiness"] == AUTONOMY_TIER_SHADOW_ONLY else "warn",
                hard=component_ceilings["readiness"] == AUTONOMY_TIER_SHADOW_ONLY,
            )
        )

    scorecard_summary = _summary_dict(scorecard_row)
    scorecard_status = str(scorecard_summary.get("scorecard_status") or "").strip().lower()
    if scorecard_row is None:
        component_ceilings["scorecard"] = AUTONOMY_TIER_SHADOW_ONLY
        missing_inputs.append("scorecard")
        blockers.append(_blocker("scorecard_missing", source="scorecard", severity="error", hard=True))
    elif scorecard_status == "blocked":
        component_ceilings["scorecard"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(_blocker("scorecard_blocked", source="scorecard", severity="error", hard=True))
    elif scorecard_status in {"watch", "degraded"}:
        component_ceilings["scorecard"] = AUTONOMY_TIER_ASSISTED_LIVE
        blockers.append(_blocker(f"scorecard_{scorecard_status}", source="scorecard", severity="warn", hard=False))
    else:
        component_ceilings["scorecard"] = max_supported_tier

    incident_summary = _summary_dict(incident_row)
    if incident_row is not None and int(incident_summary.get("incident_count_24h") or 0) > 0:
        component_ceilings["incident"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(
            _blocker(
                "recent_incidents",
                source="incident",
                severity="error",
                hard=True,
                detail=incident_summary.get("latest_incident_type"),
            )
        )
    else:
        component_ceilings["incident"] = max_supported_tier

    guardrail_summary = _summary_dict(guardrail_row)
    serious_guardrails = int(guardrail_summary.get("serious_guardrail_count_24h") or 0)
    shadow_gap_breaches = int(guardrail_summary.get("shadow_gap_breach_count_24h") or 0)
    guardrail_count = int(guardrail_summary.get("guardrail_count_24h") or 0)
    latest_guardrail_severity = str(guardrail_summary.get("latest_severity") or "").strip().lower()
    if serious_guardrails > 0:
        component_ceilings["guardrail"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(
            _blocker(
                "serious_guardrail",
                source="guardrail",
                severity="error",
                hard=True,
                detail=guardrail_summary.get("latest_guardrail_type"),
            )
        )
    elif shadow_gap_breaches > 0:
        component_ceilings["guardrail"] = AUTONOMY_TIER_SHADOW_ONLY
        blockers.append(
            _blocker(
                "shadow_gap_breach",
                source="guardrail",
                severity="error",
                hard=True,
                detail=guardrail_summary.get("latest_guardrail_type"),
            )
        )
    elif latest_guardrail_severity in {"warning", "warn"} or guardrail_count > 0:
        component_ceilings["guardrail"] = AUTONOMY_TIER_ASSISTED_LIVE
        blockers.append(
            _blocker(
                "warning_guardrail",
                source="guardrail",
                severity="warn",
                hard=False,
                detail=guardrail_summary.get("latest_guardrail_type"),
            )
        )
    else:
        component_ceilings["guardrail"] = max_supported_tier

    budget_reason_codes = [
        str(value).strip()
        for value in (budget_status.get("reason_codes") or [])
        if str(value).strip()
    ]
    capacity_status = str(budget_status.get("capacity_status") or "").strip().lower()
    risk_of_ruin_label = str(budget_status.get("risk_of_ruin_label") or "").strip().lower()
    regime_label = str(budget_status.get("regime_label") or "").strip().lower()
    if budget_reason_codes or capacity_status == "breached" or risk_of_ruin_label == "critical" or regime_label == "halted":
        component_ceilings["budget"] = AUTONOMY_TIER_SHADOW_ONLY
        for code in (budget_reason_codes or ["budget_breached"]):
            blockers.append(_blocker(code, source="budget", severity="error", hard=True))
    elif capacity_status == "constrained":
        component_ceilings["budget"] = AUTONOMY_TIER_ASSISTED_LIVE
        blockers.append(_blocker("budget_constrained", source="budget", severity="warn", hard=False, detail=capacity_status))
    else:
        component_ceilings["budget"] = max_supported_tier

    component_ceilings = {
        key: _normalize_tier(value)
        for key, value in component_ceilings.items()
    }
    final_tier = min_promotion_autonomy_tier(*component_ceilings.values())
    evaluation_status = _evaluation_status_for_tier(final_tier)
    blockers = _dedupe_blockers(blockers)
    blocker_codes = [row["code"] for row in blockers]
    decision = {
        "eligible": final_tier != AUTONOMY_TIER_SHADOW_ONLY,
        "recommended_tier": final_tier,
        "evaluation_status": evaluation_status,
        "state_reason": blocker_codes[0] if blocker_codes else "eligible",
        "missing_inputs": missing_inputs,
    }
    summary_json = {
        "inputs": {
            "replay": _compact_input_summary(replay_row),
            "readiness": _compact_input_summary(readiness_row),
            "scorecard": _compact_input_summary(scorecard_row),
            "incident": _compact_input_summary(incident_row),
            "guardrail": _compact_input_summary(guardrail_row),
            "budget": {
                    "present": True,
                "status": budget_status,
                "latest_evaluation": _compact_input_summary(budget_gate_row),
            },
        },
        "blockers": blockers,
        "blocker_codes": blocker_codes,
        "component_ceilings": component_ceilings,
        "decision": decision,
    }
    source_ids = {
        "replay": replay_row.id if replay_row is not None else None,
        "readiness": readiness_row.id if readiness_row is not None else None,
        "scorecard": scorecard_row.id if scorecard_row is not None else None,
        "incident": incident_row.id if incident_row is not None else None,
        "guardrail": guardrail_row.id if guardrail_row is not None else None,
        "budget": budget_gate_row.id if budget_gate_row is not None else None,
    }
    component_hashes = {
        key: value
        for key, value in {
            "replay": _component_hash(replay_row),
            "readiness": _component_hash(readiness_row),
            "scorecard": _component_hash(scorecard_row),
            "incident": _component_hash(incident_row),
            "guardrail": _component_hash(guardrail_row),
            "budget": _component_hash(budget_gate_row),
            "budget_status": hash_json_payload(stable_budget_status),
        }.items()
        if value is not None
    }
    decision_fingerprint = hash_json_payload(
        {
            "strategy_version_id": int(strategy_version_id),
            "source_ids": source_ids,
            "component_hashes": component_hashes,
            "decision": decision,
            "component_ceilings": component_ceilings,
            "blocker_codes": blocker_codes,
        }
    )
    provenance_json = {
        "source": "promotion_eligibility_engine",
        "evaluator_version": "v1",
        "trigger_kind": trigger_kind,
        "trigger_ref": trigger_ref,
        "strategy_family": family_row.family,
        "strategy_version_key": version.version_key,
        "strategy_version_status": version.version_status,
        "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
        "promotion_gate_policy_label": gate_policy.label if gate_policy is not None else None,
        "component_evaluation_ids": source_ids,
        "component_hashes": component_hashes,
        "config_hash": hash_json_payload(
            {
                "strategy_version_key": version.version_key,
                "promotion_gate_policy_key": gate_policy.policy_key if gate_policy is not None else None,
                "component_ceilings": component_ceilings,
            }
        ),
        "decision_fingerprint": decision_fingerprint,
    }

    latest_existing = (
        await session.execute(
            select(PromotionEvaluation)
            .where(
                PromotionEvaluation.strategy_version_id == int(strategy_version_id),
                PromotionEvaluation.evaluation_kind == PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY,
            )
            .order_by(PromotionEvaluation.created_at.desc(), PromotionEvaluation.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if (
        latest_existing is not None
        and isinstance(latest_existing.provenance_json, dict)
        and latest_existing.provenance_json.get("decision_fingerprint") == decision_fingerprint
    ):
        return latest_existing

    evaluation_window_start = readiness_row.evaluation_window_start if readiness_row is not None else _first_timestamp(
        _earliest_row_timestamp(replay_row),
        _earliest_row_timestamp(readiness_row),
        _earliest_row_timestamp(scorecard_row),
        _earliest_row_timestamp(incident_row),
        _earliest_row_timestamp(guardrail_row),
        _earliest_row_timestamp(budget_gate_row),
        observed_at,
    )
    evaluation_window_end = _last_timestamp(
        _latest_row_timestamp(replay_row),
        _latest_row_timestamp(readiness_row),
        _latest_row_timestamp(scorecard_row),
        _latest_row_timestamp(incident_row),
        _latest_row_timestamp(guardrail_row),
        _latest_row_timestamp(budget_gate_row),
        evaluation_reference_at or datetime.now(timezone.utc),
    )
    row = append_promotion_evaluation(
        session,
        family_id=family_row.id,
        strategy_version_id=int(strategy_version_id),
        gate_policy_id=gate_policy.id if gate_policy is not None else None,
        evaluation_kind=PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY,
        evaluation_status=evaluation_status,
        autonomy_tier=final_tier,
        evaluation_window_start=evaluation_window_start,
        evaluation_window_end=evaluation_window_end,
        provenance_json=provenance_json,
        summary_json=summary_json,
    )
    await session.flush()
    return row


def serialize_promotion_gate_policy(row: PromotionGatePolicy) -> dict:
    return {
        "id": row.id,
        "policy_key": row.policy_key,
        "label": row.label,
        "status": row.status,
        "policy_json": row.policy_json or {},
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
        "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


def serialize_promotion_evaluation(row: PromotionEvaluation | None) -> dict | None:
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
        "evaluation_window_start": _ensure_utc(row.evaluation_window_start).isoformat() if row.evaluation_window_start else None,
        "evaluation_window_end": _ensure_utc(row.evaluation_window_end).isoformat() if row.evaluation_window_end else None,
        "provenance_json": row.provenance_json or {},
        "summary_json": row.summary_json or {},
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
        "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


def serialize_demotion_event(row: DemotionEvent | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": row.id,
        "family_id": row.family_id,
        "strategy_version_id": row.strategy_version_id,
        "prior_autonomy_tier": row.prior_autonomy_tier,
        "fallback_autonomy_tier": row.fallback_autonomy_tier,
        "reason_code": row.reason_code,
        "cooling_off_ends_at": _ensure_utc(row.cooling_off_ends_at).isoformat() if row.cooling_off_ends_at else None,
        "details_json": row.details_json or {},
        "observed_at_local": _ensure_utc(row.observed_at_local).isoformat() if row.observed_at_local else None,
        "created_at": _ensure_utc(row.created_at).isoformat() if row.created_at else None,
        "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
    }


__all__ = [
    "PROMOTION_EVALUATION_KIND_CAPITAL_BUDGET",
    "PROMOTION_EVALUATION_KIND_GUARDRAIL",
    "PROMOTION_EVALUATION_KIND_INCIDENT",
    "PROMOTION_EVALUATION_KIND_PILOT_READINESS",
    "PROMOTION_EVALUATION_KIND_PROMOTION_ELIGIBILITY",
    "PROMOTION_EVALUATION_KIND_REPLAY",
    "PROMOTION_EVALUATION_KIND_SCORECARD",
    "PROMOTION_DEMOTION_COOLING_OFF",
    "PROMOTION_EVALUATION_STATUS_BLOCKED",
    "PROMOTION_EVALUATION_STATUS_CANDIDATE",
    "PROMOTION_EVALUATION_STATUS_OBSERVE",
    "PRIMARY_PROMOTION_EVALUATION_KINDS",
    "SUPPORTING_PROMOTION_EVALUATION_KINDS",
    "ALL_PROMOTION_EVALUATION_KINDS",
    "ELIGIBILITY_SOURCE_EVALUATION_KINDS",
    "append_promotion_evaluation",
    "hash_json_payload",
    "map_capital_budget_summary_to_promotion_verdict",
    "map_guardrail_summary_to_promotion_verdict",
    "map_incident_summary_to_promotion_verdict",
    "map_readiness_status_to_promotion_verdict",
    "map_replay_summary_to_promotion_verdict",
    "map_scorecard_status_to_promotion_verdict",
    "min_promotion_autonomy_tier",
    "normalize_promotion_evaluation_kinds",
    "record_demotion_event_from_promotion_evaluation",
    "record_promotion_eligibility_evaluation",
    "rolling_promotion_window_bounds",
    "serialize_demotion_event",
    "serialize_promotion_evaluation",
    "serialize_promotion_gate_policy",
    "upsert_promotion_evaluation",
]
