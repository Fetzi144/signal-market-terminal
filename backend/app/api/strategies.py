from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.strategies.registry import (
    get_strategy_registry_payload,
    get_strategy_version_detail_payload,
)

router = APIRouter(prefix="/api/v1/strategies", tags=["strategies"])


class StrategyRegistrySummaryOut(BaseModel):
    phase: str
    family_count: int
    version_count: int
    gate_policy_count: int
    benchmark_family: str


class PromotionGatePolicyOut(BaseModel):
    id: int
    policy_key: str
    label: str
    status: str
    policy_json: dict[str, Any] | list[Any] | str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class StrategyLifecycleEventOut(BaseModel):
    id: int
    family_id: int
    strategy_version_id: int
    gate_policy_id: int | None = None
    evaluation_kind: str | None = None
    evaluation_status: str | None = None
    autonomy_tier: str | None = None
    evaluation_window_start: str | None = None
    evaluation_window_end: str | None = None
    provenance_json: dict[str, Any] | list[Any] | str | None = None
    summary_json: dict[str, Any] | list[Any] | str | None = None
    prior_autonomy_tier: str | None = None
    fallback_autonomy_tier: str | None = None
    reason_code: str | None = None
    cooling_off_ends_at: str | None = None
    details_json: dict[str, Any] | list[Any] | str | None = None
    observed_at_local: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class StrategyReplayAlignmentOut(BaseModel):
    id: str
    run_key: str
    run_type: str
    reason: str
    status: str
    scenario_count: int
    strategy_version_id: int | None = None
    strategy_version_key: str | None = None
    strategy_version_label: str | None = None
    time_window_start: str | None = None
    time_window_end: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    promotion_evaluation: StrategyLifecycleEventOut | None = None


class StrategyLiveShadowAlignmentOut(BaseModel):
    latest_updated_at: str | None = None
    latest_variant_name: str | None = None
    latest_reason_code: str | None = None
    latest_gap_bps: str | None = None
    latest_realized_net_bps: str | None = None
    latest_replay_run_id: str | None = None
    recent_count_24h: int
    coverage_limited_count_24h: int
    average_gap_bps_24h: str | None = None
    worst_gap_bps_24h: str | None = None
    breach_count_24h: int


class StrategyScorecardAlignmentOut(BaseModel):
    id: int
    status: str
    window_start: str | None = None
    window_end: str | None = None
    live_orders_count: int
    fills_count: int
    incident_count: int
    net_pnl: str | None = None
    avg_shadow_gap_bps: str | None = None
    coverage_limited_count: int
    created_at: str | None = None


class StrategyReadinessAlignmentOut(BaseModel):
    id: int
    status: str
    window_start: str | None = None
    window_end: str | None = None
    generated_at: str | None = None
    approval_backlog_count: int
    coverage_limited_count: int
    shadow_gap_breach_count: int
    open_incidents: int


class StrategyEvidenceAlignmentOut(BaseModel):
    surface_status: str
    surfaces_present: int
    surface_keys_present: list[str]
    latest_surface_at: str | None = None
    latest_promotion_evaluation: StrategyLifecycleEventOut | None = None
    latest_replay_run: StrategyReplayAlignmentOut | None = None
    live_shadow: StrategyLiveShadowAlignmentOut | None = None
    latest_scorecard: StrategyScorecardAlignmentOut | None = None
    latest_readiness_report: StrategyReadinessAlignmentOut | None = None


class StrategyRegistryVersionOut(BaseModel):
    id: int
    version_key: str
    version_label: str
    strategy_name: str | None = None
    version_status: str
    autonomy_tier: str
    is_current: bool
    is_frozen: bool
    config_json: dict[str, Any] | list[Any] | str | None = None
    provenance_json: dict[str, Any] | list[Any] | str | None = None
    latest_promotion_evaluation: StrategyLifecycleEventOut | None = None
    evidence_alignment: StrategyEvidenceAlignmentOut | None = None
    evidence_counts: dict[str, int]
    created_at: str | None = None
    updated_at: str | None = None


class StrategyFamilyRegistryOut(BaseModel):
    id: int
    family: str
    label: str
    posture: str
    configured: bool
    review_enabled: bool
    primary_surface: str
    description: str
    disabled_reason: str | None = None
    family_kind: str
    seeded_from: str
    current_version: StrategyRegistryVersionOut | None = None
    versions: list[StrategyRegistryVersionOut]
    latest_promotion_evaluation: StrategyLifecycleEventOut | None = None
    latest_demotion_event: StrategyLifecycleEventOut | None = None
    created_at: str | None = None
    updated_at: str | None = None


class StrategyFamilyReferenceOut(BaseModel):
    id: int
    family: str
    label: str
    posture: str
    primary_surface: str
    family_kind: str
    description: str
    disabled_reason: str | None = None
    seeded_from: str
    created_at: str | None = None
    updated_at: str | None = None


class StrategyLiveShadowDetailOut(BaseModel):
    id: int
    live_order_id: str | None = None
    client_order_id: str | None = None
    condition_id: str | None = None
    asset_id: str | None = None
    side: str | None = None
    live_order_status: str | None = None
    variant_name: str
    gap_bps: str | None = None
    realized_net_bps: str | None = None
    expected_net_ev_bps: str | None = None
    coverage_limited: bool
    reason_code: str | None = None
    replay_run_id: str | None = None
    details_json: dict[str, Any] | list[Any] | str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class StrategyVersionDetailOut(BaseModel):
    family: StrategyFamilyReferenceOut | None = None
    version: StrategyRegistryVersionOut
    latest_demotion_event: StrategyLifecycleEventOut | None = None
    replay_runs: list[StrategyReplayAlignmentOut]
    live_shadow_evaluations: list[StrategyLiveShadowDetailOut]
    scorecards: list[StrategyScorecardAlignmentOut]
    readiness_reports: list[StrategyReadinessAlignmentOut]
    promotion_evaluations: list[StrategyLifecycleEventOut]
    gate_history: list[StrategyLifecycleEventOut]
    demotion_events: list[StrategyLifecycleEventOut]
    generated_at: str


class StrategyRegistryOut(BaseModel):
    summary: StrategyRegistrySummaryOut
    families: list[StrategyFamilyRegistryOut]
    gate_policies: list[PromotionGatePolicyOut]
    generated_at: str


@router.get("", response_model=StrategyRegistryOut)
async def get_strategy_registry(db: AsyncSession = Depends(get_db)):
    payload = await get_strategy_registry_payload(db)
    await db.commit()
    return StrategyRegistryOut(**payload)


@router.get("/versions/{version_id}", response_model=StrategyVersionDetailOut)
async def get_strategy_version_detail(version_id: int, db: AsyncSession = Depends(get_db)):
    payload = await get_strategy_version_detail_payload(db, version_id=version_id)
    if payload is None:
        await db.rollback()
        raise HTTPException(status_code=404, detail="Strategy version not found")
    await db.commit()
    return StrategyVersionDetailOut(**payload)
