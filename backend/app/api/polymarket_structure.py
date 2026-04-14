from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import get_db, get_session_factory
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
    upsert_cross_venue_market_link,
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
    last_successful_group_build_at: datetime | None = None
    last_successful_scan_at: datetime | None = None
    last_group_build_status: str | None = None
    last_group_build_started_at: datetime | None = None
    last_scan_status: str | None = None
    last_scan_started_at: datetime | None = None
    recent_actionable_by_type: dict[str, int]
    recent_non_executable_count: int
    informational_augmented_group_count: int
    active_group_counts: dict[str, int]
    active_cross_venue_link_count: int
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
    active: bool = True
    details_json: dict[str, Any] | list[Any] | str | None = None


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
        limit=limit,
    )
    return StructureQueryOut(rows=rows, limit=limit)


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


@router.get("/cross-venue-links", response_model=StructureQueryOut)
async def get_cross_venue_links(
    venue: str | None = Query(default=None),
    actionable: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_cross_venue_market_links(db, venue=venue, actionable=actionable, limit=limit)
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
