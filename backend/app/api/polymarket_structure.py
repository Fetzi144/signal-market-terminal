from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import get_db, get_session_factory
from app.ingestion.polymarket_maker_economics import (
    evaluate_structure_maker_economics,
    generate_quote_recommendation,
    get_latest_maker_economics_snapshot,
    get_latest_quote_recommendation,
    list_maker_economics_snapshots,
    list_quote_recommendations,
)
from app.ingestion.structure_engine import (
    fetch_market_structure_status,
    list_market_structure_runs,
    lookup_cross_venue_market_links,
    lookup_market_structure_group_members,
    lookup_market_structure_groups,
    lookup_market_structure_opportunities,
    lookup_market_structure_opportunity_legs,
    trigger_manual_structure_group_build,
    trigger_manual_structure_opportunity_scan,
    trigger_manual_structure_paper_plan_create,
    trigger_manual_structure_paper_plan_route,
    trigger_manual_structure_validation,
    upsert_cross_venue_market_link,
)
from app.ingestion.structure_phase8b import (
    approve_market_structure_paper_plan,
    get_market_structure_opportunity_detail,
    get_market_structure_paper_plan_detail,
    list_market_structure_paper_plans,
    list_market_structure_validations,
    reject_market_structure_paper_plan,
    serialize_structure_paper_plan,
)

router = APIRouter(prefix="/api/v1/ingest/polymarket/structure", tags=["ingest"])


class StructureRunOut(BaseModel):
    id: uuid.UUID
    run_type: str
    reason: str
    started_at: datetime
    completed_at: datetime | None = None
    status: str
    scope_json: dict[str, Any] | list[Any] | str | None = None
    cursor_json: dict[str, Any] | list[Any] | str | None = None
    rows_inserted_json: dict[str, Any] | list[Any] | str | None = None
    error_count: int
    details_json: dict[str, Any] | list[Any] | str | None = None


class StructureStatusOut(BaseModel):
    enabled: bool
    on_startup: bool
    interval_seconds: int
    min_net_edge_bps: float
    require_executable_all_legs: bool
    include_cross_venue: bool
    allow_augmented_neg_risk: bool
    max_groups_per_run: int
    cross_venue_max_staleness_seconds: int
    max_leg_slippage_bps: float
    run_lock_enabled: bool
    retention_days: int
    validation_enabled: bool
    paper_routing_enabled: bool
    paper_require_manual_approval: bool
    max_notional_per_plan: float
    min_depth_per_leg: float
    plan_max_age_seconds: int
    link_review_required: bool
    last_successful_group_build_at: datetime | None = None
    last_successful_scan_at: datetime | None = None
    last_successful_validation_at: datetime | None = None
    last_successful_paper_plan_at: datetime | None = None
    last_successful_paper_route_at: datetime | None = None
    last_successful_retention_prune_at: datetime | None = None
    last_group_build_status: str | None = None
    last_group_build_started_at: datetime | None = None
    last_group_build_duration_seconds: float | None = None
    last_scan_status: str | None = None
    last_scan_started_at: datetime | None = None
    last_scan_duration_seconds: float | None = None
    last_validation_status: str | None = None
    last_validation_started_at: datetime | None = None
    last_validation_duration_seconds: float | None = None
    last_paper_plan_status: str | None = None
    last_paper_plan_started_at: datetime | None = None
    last_paper_plan_duration_seconds: float | None = None
    last_paper_route_status: str | None = None
    last_paper_route_started_at: datetime | None = None
    last_paper_route_duration_seconds: float | None = None
    last_retention_prune_status: str | None = None
    last_retention_prune_started_at: datetime | None = None
    last_retention_prune_duration_seconds: float | None = None
    recent_actionable_by_type: dict[str, int]
    recent_non_executable_count: int
    informational_augmented_group_count: int
    active_group_counts: dict[str, int]
    active_cross_venue_link_count: int
    informational_only_opportunity_count: int
    blocked_opportunity_count: int
    executable_candidate_count: int
    opportunity_counts_by_type: dict[str, int]
    validation_reason_counts: dict[str, int]
    stale_cross_venue_link_count: int
    skipped_group_count: int
    pending_approval_count: int
    recent_runs: list[StructureRunOut]


class StructureQueryOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


class StructureManualRunRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    group_type: str | None = None
    event_slug: str | None = None
    venue: str | None = None
    limit: int | None = Field(default=None, ge=1, le=1000)


class CrossVenueLinkUpsertRequest(BaseModel):
    link_key: str | None = None
    left_venue: str
    left_market_id: uuid.UUID | None = None
    left_outcome_id: uuid.UUID | None = None
    left_condition_id: str | None = None
    left_asset_id: str | None = None
    left_external_id: str | None = None
    left_symbol: str | None = None
    right_venue: str
    right_market_id: uuid.UUID | None = None
    right_outcome_id: uuid.UUID | None = None
    right_condition_id: str | None = None
    right_asset_id: str | None = None
    right_external_id: str | None = None
    right_symbol: str | None = None
    mapping_kind: str
    provenance_source: str | None = None
    owner: str | None = None
    reviewed_by: str | None = None
    review_status: str | None = None
    confidence: float | None = None
    notes: str | None = None
    last_reviewed_at: datetime | None = None
    expires_at: datetime | None = None
    active: bool = True
    details_json: dict[str, Any] | list[Any] | str | None = None


class StructureValidationRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    opportunity_id: int | None = None
    scan_run_id: uuid.UUID | None = None
    limit: int | None = Field(default=None, ge=1, le=1000)


class StructurePlanRequest(BaseModel):
    actor: str = Field(default="operator", min_length=1, max_length=128)
    validation_id: int | None = None
    auto_created: bool = False


class StructurePlanRejectRequest(BaseModel):
    actor: str = Field(default="operator", min_length=1, max_length=128)
    reason: str | None = Field(default=None, max_length=512)


class StructureMakerRequest(BaseModel):
    as_of: datetime | None = None


@router.get("/status", response_model=StructureStatusOut)
async def get_market_structure_status(db: AsyncSession = Depends(get_db)):
    return await fetch_market_structure_status(db)


@router.get("/runs", response_model=StructureQueryOut)
async def get_market_structure_runs(
    run_type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_market_structure_runs(db, run_type=run_type, limit=limit)
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/groups", response_model=StructureQueryOut)
async def get_market_structure_groups(
    group_type: str | None = Query(default=None),
    event_slug: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    venue: str | None = Query(default=None),
    actionable: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_market_structure_groups(
        db,
        group_type=group_type,
        event_slug=event_slug,
        condition_id=condition_id,
        asset_id=asset_id,
        venue=venue,
        actionable=actionable,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/group-members", response_model=StructureQueryOut)
async def get_market_structure_group_members(
    group_id: int | None = Query(default=None),
    group_type: str | None = Query(default=None),
    event_slug: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    venue: str | None = Query(default=None),
    actionable: bool | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_market_structure_group_members(
        db,
        group_id=group_id,
        group_type=group_type,
        event_slug=event_slug,
        condition_id=condition_id,
        asset_id=asset_id,
        venue=venue,
        actionable=actionable,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/opportunities", response_model=StructureQueryOut)
async def get_market_structure_opportunities(
    group_type: str | None = Query(default=None),
    opportunity_type: str | None = Query(default=None),
    event_slug: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    venue: str | None = Query(default=None),
    actionable: bool | None = Query(default=None),
    classification: str | None = Query(default=None),
    reason_code: str | None = Query(default=None),
    edge_bucket: str | None = Query(default=None),
    plan_status: str | None = Query(default=None),
    review_status: str | None = Query(default=None),
    confidence_min: float | None = Query(default=None, ge=0, le=1),
    executable_only: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_market_structure_opportunities(
        db,
        group_type=group_type,
        opportunity_type=opportunity_type,
        event_slug=event_slug,
        condition_id=condition_id,
        asset_id=asset_id,
        venue=venue,
        actionable=actionable,
        classification=classification,
        reason_code=reason_code,
        edge_bucket=edge_bucket,
        plan_status=plan_status,
        review_status=review_status,
        confidence_min=confidence_min,
        executable_only=executable_only,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/opportunities/{opportunity_id}")
async def get_market_structure_opportunity_detail_view(
    opportunity_id: int,
    db: AsyncSession = Depends(get_db),
):
    detail = await get_market_structure_opportunity_detail(db, opportunity_id=opportunity_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Structure opportunity not found")
    return detail


@router.get("/opportunities/{opportunity_id}/maker-economics/latest")
async def get_market_structure_opportunity_maker_economics_latest(
    opportunity_id: int,
    db: AsyncSession = Depends(get_db),
):
    detail = await get_latest_maker_economics_snapshot(db, opportunity_id=opportunity_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Maker economics snapshot not found")
    return detail


@router.post("/opportunities/{opportunity_id}/maker-economics")
async def run_market_structure_opportunity_maker_economics(
    opportunity_id: int,
    body: StructureMakerRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await evaluate_structure_maker_economics(db, opportunity_id=opportunity_id, as_of=body.as_of)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/opportunities/{opportunity_id}/quote-recommendations")
async def run_market_structure_opportunity_quote_recommendation(
    opportunity_id: int,
    body: StructureMakerRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await generate_quote_recommendation(db, opportunity_id=opportunity_id, as_of=body.as_of)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/legs", response_model=StructureQueryOut)
async def get_market_structure_opportunity_legs(
    opportunity_id: int | None = Query(default=None),
    opportunity_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    venue: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_market_structure_opportunity_legs(
        db,
        opportunity_id=opportunity_id,
        opportunity_type=opportunity_type,
        condition_id=condition_id,
        asset_id=asset_id,
        venue=venue,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/maker-economics/snapshots", response_model=StructureQueryOut)
async def get_market_structure_maker_economics_snapshots(
    opportunity_id: int | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_maker_economics_snapshots(
        db,
        opportunity_id=opportunity_id,
        condition_id=condition_id,
        asset_id=asset_id,
        status=status,
        start=start,
        end=end,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/quote-recommendations", response_model=StructureQueryOut)
async def get_market_structure_quote_recommendations(
    opportunity_id: int | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_quote_recommendations(
        db,
        opportunity_id=opportunity_id,
        condition_id=condition_id,
        asset_id=asset_id,
        status=status,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/opportunities/{opportunity_id}/quote-recommendations/latest")
async def get_market_structure_opportunity_quote_recommendation_latest(
    opportunity_id: int,
    db: AsyncSession = Depends(get_db),
):
    detail = await get_latest_quote_recommendation(db, opportunity_id=opportunity_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Quote recommendation not found")
    return detail


@router.post("/groups/build", response_model=StructureRunOut)
async def run_market_structure_group_build(
    body: StructureManualRunRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    result = await trigger_manual_structure_group_build(
        session_factory,
        reason=body.reason,
        group_type=body.group_type,
        event_slug=body.event_slug,
        limit=body.limit,
    )
    return StructureRunOut(**result)


@router.post("/opportunities/scan", response_model=StructureRunOut)
async def run_market_structure_opportunity_scan(
    body: StructureManualRunRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    result = await trigger_manual_structure_opportunity_scan(
        session_factory,
        reason=body.reason,
        group_type=body.group_type,
        event_slug=body.event_slug,
        venue=body.venue,
        limit=body.limit,
    )
    return StructureRunOut(**result)


@router.post("/opportunities/validate", response_model=StructureRunOut)
async def run_market_structure_validation(
    body: StructureValidationRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    result = await trigger_manual_structure_validation(
        session_factory,
        reason=body.reason,
        opportunity_id=body.opportunity_id,
        scan_run_id=body.scan_run_id,
        limit=body.limit,
    )
    return StructureRunOut(**result)


@router.get("/validations", response_model=StructureQueryOut)
async def get_market_structure_validations_view(
    opportunity_id: int | None = Query(default=None),
    classification: str | None = Query(default=None),
    evaluation_kind: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_market_structure_validations(
        db,
        opportunity_id=opportunity_id,
        classification=classification,
        evaluation_kind=evaluation_kind,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/cross-venue-links", response_model=StructureQueryOut)
async def get_cross_venue_links(
    venue: str | None = Query(default=None),
    actionable: bool | None = Query(default=None),
    review_status: str | None = Query(default=None),
    confidence_min: float | None = Query(default=None, ge=0, le=1),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_cross_venue_market_links(
        db,
        venue=venue,
        actionable=actionable,
        review_status=review_status,
        confidence_min=confidence_min,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.post("/cross-venue-links")
async def create_cross_venue_link(
    body: CrossVenueLinkUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    return await upsert_cross_venue_market_link(db, payload=body.model_dump())


@router.patch("/cross-venue-links/{link_id}")
async def update_cross_venue_link(
    link_id: int,
    body: CrossVenueLinkUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    return await upsert_cross_venue_market_link(db, link_id=link_id, payload=body.model_dump())


@router.get("/paper-plans", response_model=StructureQueryOut)
async def get_market_structure_paper_plans_view(
    opportunity_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_market_structure_paper_plans(
        db,
        opportunity_id=opportunity_id,
        status=status,
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


@router.get("/paper-plans/{plan_id}")
async def get_market_structure_paper_plan_detail_view(
    plan_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    detail = await get_market_structure_paper_plan_detail(db, plan_id=plan_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Structure paper plan not found")
    return detail


@router.post("/opportunities/{opportunity_id}/paper-plans")
async def create_market_structure_paper_plan_view(
    opportunity_id: int,
    body: StructurePlanRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        return await trigger_manual_structure_paper_plan_create(
            session_factory,
            opportunity_id=opportunity_id,
            validation_id=body.validation_id,
            actor=body.actor,
            auto_created=body.auto_created,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/paper-plans/{plan_id}/approve")
async def approve_market_structure_paper_plan_view(
    plan_id: uuid.UUID,
    body: StructurePlanRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        plan = await approve_market_structure_paper_plan(db, plan_id=plan_id, actor=body.actor)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    await db.refresh(plan)
    return serialize_structure_paper_plan(plan)


@router.post("/paper-plans/{plan_id}/reject")
async def reject_market_structure_paper_plan_view(
    plan_id: uuid.UUID,
    body: StructurePlanRejectRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        plan = await reject_market_structure_paper_plan(
            db,
            plan_id=plan_id,
            actor=body.actor,
            reason=body.reason,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await db.commit()
    await db.refresh(plan)
    return serialize_structure_paper_plan(plan)


@router.post("/paper-plans/{plan_id}/route")
async def route_market_structure_paper_plan_view(
    plan_id: uuid.UUID,
    body: StructurePlanRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        return await trigger_manual_structure_paper_plan_route(
            session_factory,
            plan_id=plan_id,
            actor=body.actor,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
