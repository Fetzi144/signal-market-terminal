from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.paper_trading.analysis import (
    PROFITABILITY_MIN_RESOLVED_TRADES,
    PROFITABILITY_OPERATING_WINDOW_DAYS,
    build_paper_lane_profitability_snapshot,
    get_profitability_snapshot,
)
from app.reports.kalshi_down_yes_fade import (
    build_kalshi_down_yes_fade_snapshot,
    kalshi_down_yes_fade_lane_payload,
)
from app.reports.kalshi_low_yes_fade import (
    build_kalshi_low_yes_fade_snapshot,
    kalshi_low_yes_fade_lane_payload,
)
from app.strategies.registry import (
    get_strategy_registry_payload,
    get_strategy_version_detail_payload,
)

router = APIRouter(prefix="/api/v1/strategies", tags=["strategies"])


def _float_or_none(value: Any) -> float | None:
    return None if value is None else float(value)


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _kalshi_profitability_snapshot(snapshot: dict[str, Any], lane_payload: dict[str, Any]) -> dict[str, Any]:
    paper = snapshot.get("paper") or {}
    return {
        "family": lane_payload["family"],
        "strategy_version": lane_payload["strategy_version"],
        "strategy_name": snapshot.get("strategy_name"),
        "source_kind": lane_payload["source_kind"],
        "source_ref": lane_payload["source_ref"],
        "schema_version": snapshot.get("schema_version"),
        "window_start": _iso_or_none(snapshot.get("window_start")),
        "window_end": _iso_or_none(snapshot.get("window_end")),
        "operating_window_days": snapshot.get("window_days"),
        "minimum_resolved_trades": PROFITABILITY_MIN_RESOLVED_TRADES,
        "realized_pnl": _float_or_none(lane_payload.get("realized_pnl")),
        "mark_to_market_pnl": _float_or_none(lane_payload.get("mark_to_market_pnl")),
        "open_mark_to_market_pnl": _float_or_none(lane_payload.get("mark_to_market_pnl")),
        "open_exposure": _float_or_none(lane_payload.get("open_exposure")),
        "open_trades": int(paper.get("open_trades") or 0),
        "resolved_trades": int(lane_payload.get("resolved_trades") or 0),
        "avg_clv": _float_or_none(lane_payload.get("avg_clv")),
        "execution_adjusted_paper_pnl": _float_or_none(lane_payload.get("realized_pnl")),
        "replay_net_pnl": None,
        "replay_coverage_mode": lane_payload.get("coverage_mode"),
        "profitability_blockers": lane_payload.get("blockers") or [],
        "blockers": lane_payload.get("blockers") or [],
        "verdict": lane_payload.get("verdict"),
        "paper_only": True,
        "live_submission_permitted": False,
        "live_orders_enabled": False,
        "pilot_arming_enabled": False,
        "snapshot": lane_payload.get("details_json", {}).get("snapshot", {}),
    }


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
    risk_budget_policy: dict[str, Any] | list[Any] | str | None = None
    risk_budget_status: dict[str, Any] | list[Any] | str | None = None
    autonomy_state: dict[str, Any] | None = None
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
    autonomy_state: dict[str, Any] | None = None
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


@router.get("/profitability")
async def get_strategy_profitability(db: AsyncSession = Depends(get_db)):
    registry_payload = await get_strategy_registry_payload(db)
    observed_at = datetime.now(timezone.utc)
    default_snapshot = await get_profitability_snapshot(db)
    snapshots = []
    for family in registry_payload["families"]:
        family_key = family["family"]
        current_version = family.get("current_version") or {}
        strategy_version = current_version.get("version_key")
        if family_key == "default_strategy":
            snapshots.append(default_snapshot)
            continue
        if family_key == "kalshi_down_yes_fade":
            snapshot = await build_kalshi_down_yes_fade_snapshot(db, as_of=observed_at)
            snapshots.append(_kalshi_profitability_snapshot(snapshot, kalshi_down_yes_fade_lane_payload(snapshot)))
            continue
        if family_key == "kalshi_low_yes_fade":
            snapshot = await build_kalshi_low_yes_fade_snapshot(db, as_of=observed_at)
            snapshots.append(_kalshi_profitability_snapshot(snapshot, kalshi_low_yes_fade_lane_payload(snapshot)))
            continue
        snapshots.append(
            build_paper_lane_profitability_snapshot(
                family=family_key,
                strategy_version=strategy_version,
                observed_at=observed_at,
                disabled_reason=family.get("disabled_reason"),
            )
        )

    await db.commit()
    return {
        "generated_at": observed_at.isoformat(),
        "operating_window_days": PROFITABILITY_OPERATING_WINDOW_DAYS,
        "minimum_resolved_trades": PROFITABILITY_MIN_RESOLVED_TRADES,
        "paper_only": True,
        "live_submission_permitted": False,
        "snapshots": snapshots,
    }


@router.get("/versions/{version_id}", response_model=StrategyVersionDetailOut)
async def get_strategy_version_detail(version_id: int, db: AsyncSession = Depends(get_db)):
    payload = await get_strategy_version_detail_payload(db, version_id=version_id)
    if payload is None:
        await db.rollback()
        raise HTTPException(status_code=404, detail="Strategy version not found")
    await db.commit()
    return StrategyVersionDetailOut(**payload)
