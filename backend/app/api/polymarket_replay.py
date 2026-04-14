from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import get_db, get_session_factory
from app.ingestion.polymarket_replay_simulator import (
    fetch_polymarket_replay_policy_summary,
    fetch_polymarket_replay_status,
    get_polymarket_replay_scenario_detail,
    list_polymarket_replay_decision_traces,
    list_polymarket_replay_metrics,
    list_polymarket_replay_runs,
    list_polymarket_replay_scenarios,
    trigger_manual_polymarket_replay,
)

router = APIRouter(prefix="/api/v1/ingest/polymarket/replay", tags=["ingest"])


class ReplayQueryOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


class ReplayStatusOut(BaseModel):
    enabled: bool
    on_startup: bool
    interval_seconds: int
    default_window_minutes: int
    max_scenarios_per_run: int
    structure_enabled: bool
    maker_enabled: bool
    risk_adjustments_enabled: bool
    require_complete_book_coverage: bool
    passive_fill_timeout_seconds: int
    advisory_only: bool
    live_disabled_by_default: bool
    last_replay_run: dict[str, Any] | None = None
    last_successful_policy_comparison: dict[str, Any] | None = None
    recent_scenario_count_24h: int
    recent_coverage_limited_run_count_24h: int
    recent_failed_run_count_24h: int
    recent_variant_summary: dict[str, dict[str, Any]]
    recent_runs: list[dict[str, Any]]


class ReplayScenarioDetailOut(BaseModel):
    scenario: dict[str, Any]
    orders: list[dict[str, Any]]
    fills: list[dict[str, Any]]
    metrics: list[dict[str, Any]]
    decision_traces: list[dict[str, Any]]


class ReplayManualRunRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    run_type: str = Field(default="policy_compare", min_length=1, max_length=64)
    start: datetime | None = None
    end: datetime | None = None
    asset_ids: list[str] | None = None
    condition_ids: list[str] | None = None
    opportunity_ids: list[int] | None = None
    quote_recommendation_ids: list[uuid.UUID] | None = None
    limit: int | None = Field(default=None, ge=1, le=500)


class ReplayTriggerOut(BaseModel):
    run: dict[str, Any]
    rows_inserted: dict[str, Any]
    idempotent_hit: bool


@router.get("/status", response_model=ReplayStatusOut)
async def get_replay_status(db: AsyncSession = Depends(get_db)):
    return await fetch_polymarket_replay_status(db)


@router.get("/runs", response_model=ReplayQueryOut)
async def get_replay_runs(
    run_type: str | None = Query(default=None),
    reason: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_polymarket_replay_runs(
        db,
        run_type=run_type,
        reason=reason,
        start=start,
        end=end,
        limit=limit,
    )
    return ReplayQueryOut(rows=rows, limit=limit)


@router.get("/scenarios", response_model=ReplayQueryOut)
async def get_replay_scenarios(
    run_type: str | None = Query(default=None),
    scenario_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_polymarket_replay_scenarios(
        db,
        run_type=run_type,
        scenario_type=scenario_type,
        condition_id=condition_id,
        asset_id=asset_id,
        start=start,
        end=end,
        limit=limit,
    )
    return ReplayQueryOut(rows=rows, limit=limit)


@router.get("/scenarios/{scenario_id}", response_model=ReplayScenarioDetailOut)
async def get_replay_scenario_detail(scenario_id: int, db: AsyncSession = Depends(get_db)):
    row = await get_polymarket_replay_scenario_detail(db, scenario_id=scenario_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Replay scenario not found")
    return ReplayScenarioDetailOut(**row)


@router.get("/metrics", response_model=ReplayQueryOut)
async def get_replay_metrics(
    run_type: str | None = Query(default=None),
    scenario_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    variant_name: str | None = Query(default=None),
    metric_scope: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_polymarket_replay_metrics(
        db,
        run_type=run_type,
        scenario_type=scenario_type,
        condition_id=condition_id,
        asset_id=asset_id,
        variant_name=variant_name,
        metric_scope=metric_scope,
        start=start,
        end=end,
        limit=limit,
    )
    return ReplayQueryOut(rows=rows, limit=limit)


@router.get("/decision-traces", response_model=ReplayQueryOut)
async def get_replay_decision_traces(
    run_type: str | None = Query(default=None),
    scenario_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    variant_name: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_polymarket_replay_decision_traces(
        db,
        run_type=run_type,
        scenario_type=scenario_type,
        condition_id=condition_id,
        asset_id=asset_id,
        variant_name=variant_name,
        start=start,
        end=end,
        limit=limit,
    )
    return ReplayQueryOut(rows=rows, limit=limit)


@router.get("/policy-summary")
async def get_replay_policy_summary(db: AsyncSession = Depends(get_db)):
    return await fetch_polymarket_replay_policy_summary(db)


@router.post("/trigger", response_model=ReplayTriggerOut)
async def post_replay_trigger(
    payload: ReplayManualRunRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        return await trigger_manual_polymarket_replay(
            session_factory,
            reason=payload.reason,
            run_type=payload.run_type,
            start=payload.start,
            end=payload.end,
            asset_ids=payload.asset_ids,
            condition_ids=payload.condition_ids,
            opportunity_ids=payload.opportunity_ids,
            quote_recommendation_ids=payload.quote_recommendation_ids,
            limit=payload.limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
