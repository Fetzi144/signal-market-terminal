from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import get_db, get_session_factory
from app.research_lab.orchestrator import (
    PRESET_PROFIT_HUNT_V1,
    cancel_research_batch,
    create_research_batch,
    get_latest_research_batch,
    get_research_batch_detail,
    list_research_batches,
    run_research_batch,
)

router = APIRouter(prefix="/api/v1/research", tags=["research"])


class ResearchBatchCreateRequest(BaseModel):
    preset: str = PRESET_PROFIT_HUNT_V1
    window_days: int = Field(default=30, ge=1, le=180)
    max_markets: int = Field(default=500, ge=1, le=5000)
    families: list[str] = Field(
        default_factory=lambda: [
            "default_strategy",
            "kalshi_down_yes_fade",
            "kalshi_low_yes_fade",
            "kalshi_cheap_yes_follow",
            "alpha_factory",
        ]
    )
    start_immediately: bool = True
    window_start: datetime | None = None
    window_end: datetime | None = None


class ResearchBatchCreateOut(BaseModel):
    batch: dict[str, Any]
    idempotent_hit: bool
    started: bool


class ResearchBatchListOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


class ResearchBatchDetailOut(BaseModel):
    batch: dict[str, Any]
    lane_results: list[dict[str, Any]]
    top_blockers: list[dict[str, Any]]
    top_ev_candidates: list[dict[str, Any]]
    data_readiness: dict[str, Any] | None = None


async def _run_research_batch_background(
    session_factory: async_sessionmaker[AsyncSession],
    batch_id: uuid.UUID,
) -> None:
    await run_research_batch(session_factory, batch_id)


@router.post("/batches", response_model=ResearchBatchCreateOut, status_code=201)
async def post_research_batch(
    payload: ResearchBatchCreateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        batch, idempotent_hit = await create_research_batch(
            db,
            preset=payload.preset,
            window_days=payload.window_days,
            max_markets=payload.max_markets,
            families=payload.families,
            window_start=payload.window_start,
            window_end=payload.window_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    started = False
    if payload.start_immediately and batch.status in {"pending", "failed", "completed", "completed_with_warnings"}:
        started = True
        background_tasks.add_task(_run_research_batch_background, session_factory, batch.id)
    detail = await get_research_batch_detail(db, batch.id)
    return ResearchBatchCreateOut(batch=detail["batch"], idempotent_hit=idempotent_hit, started=started)


@router.get("/batches", response_model=ResearchBatchListOut)
async def get_research_batches(
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    return ResearchBatchListOut(rows=await list_research_batches(db, limit=limit), limit=limit)


@router.get("/batches/latest", response_model=ResearchBatchDetailOut | None)
async def get_latest_research_batch_endpoint(db: AsyncSession = Depends(get_db)):
    return await get_latest_research_batch(db)


@router.get("/batches/{batch_id}", response_model=ResearchBatchDetailOut)
async def get_research_batch_endpoint(batch_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    try:
        return await get_research_batch_detail(db, batch_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/cancel", response_model=ResearchBatchDetailOut)
async def cancel_research_batch_endpoint(batch_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    try:
        return await cancel_research_batch(db, batch_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
