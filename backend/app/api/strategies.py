from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.strategies.registry import get_strategy_registry_payload

router = APIRouter(prefix="/api/v1/strategies", tags=["strategies"])


class StrategyRegistrySummaryOut(BaseModel):
    phase: str
    family_count: int
    version_count: int
    gate_policy_count: int
    benchmark_family: str


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
    evidence_counts: dict[str, int]
    created_at: str | None = None
    updated_at: str | None = None


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

