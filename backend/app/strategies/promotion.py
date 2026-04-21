from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.strategy_registry import DemotionEvent, PromotionEvaluation, PromotionGatePolicy

PROMOTION_EVALUATION_KIND_PILOT_READINESS = "pilot_readiness_gate"
PROMOTION_EVALUATION_KIND_REPLAY = "replay_gate"
PROMOTION_EVALUATION_STATUS_BLOCKED = "blocked"
PROMOTION_EVALUATION_STATUS_OBSERVE = "observe"
PROMOTION_EVALUATION_STATUS_CANDIDATE = "candidate"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
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


def map_readiness_status_to_promotion_verdict(readiness_status: str | None) -> tuple[str, str]:
    normalized = str(readiness_status or "").strip().lower()
    if normalized == "candidate_for_semi_auto":
        return PROMOTION_EVALUATION_STATUS_CANDIDATE, "bounded_auto_submit"
    if normalized == "manual_only":
        return PROMOTION_EVALUATION_STATUS_OBSERVE, "assisted_live"
    return PROMOTION_EVALUATION_STATUS_BLOCKED, "shadow_only"


def map_replay_summary_to_promotion_verdict(
    *,
    run_status: str | None,
    coverage_limited_scenarios: int = 0,
    variant_count: int = 0,
) -> tuple[str, str]:
    normalized_status = str(run_status or "").strip().lower()
    if normalized_status == "failed":
        return PROMOTION_EVALUATION_STATUS_BLOCKED, "shadow_only"
    if coverage_limited_scenarios > 0 or variant_count <= 0:
        return PROMOTION_EVALUATION_STATUS_BLOCKED, "shadow_only"
    # Replay is a promotion input in Phase 13A, not sufficient proof for a wider tier.
    return PROMOTION_EVALUATION_STATUS_OBSERVE, "shadow_only"


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
