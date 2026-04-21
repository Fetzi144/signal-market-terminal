from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.ingestion.polymarket_risk_graph import (
    build_risk_graph,
    create_exposure_snapshot,
    fetch_polymarket_risk_graph_status,
    list_inventory_control_snapshots,
    list_portfolio_exposure_snapshots,
    list_portfolio_optimizer_recommendations,
    list_risk_graph_runs,
    lookup_risk_graph_edges,
    lookup_risk_graph_nodes,
    run_portfolio_optimizer,
)

router = APIRouter(prefix="/api/v1/ingest/polymarket/risk", tags=["ingest"])


class RiskQueryOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


class RiskStatusOut(BaseModel):
    enabled: bool
    on_startup: bool
    interval_seconds: int
    portfolio_optimizer_enabled: bool
    portfolio_optimizer_interval_seconds: int
    advisory_only: bool
    live_disabled_by_default: bool
    last_successful_graph_build_at: datetime | None = None
    last_successful_exposure_snapshot_at: datetime | None = None
    last_successful_optimizer_run_at: datetime | None = None
    last_graph_build_status: str | None = None
    last_exposure_snapshot_status: str | None = None
    last_optimizer_status: str | None = None
    top_concentrated_exposures: list[dict[str, Any]]
    recent_block_reason_counts_24h: dict[str, int]
    maker_budget_used_usd: float | None = None
    maker_budget_usd: float | None = None
    taker_budget_used_usd: float | None = None
    taker_budget_usd: float | None = None
    maker_budget_utilization: float
    taker_budget_utilization: float
    recent_runs: list[dict[str, Any]]


class RiskManualRunRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    snapshot_at: datetime | None = None


class RiskRunOut(BaseModel):
    run: dict[str, Any]
    rows_inserted: dict[str, Any]


@router.get("/status", response_model=RiskStatusOut)
async def get_risk_status(db: AsyncSession = Depends(get_db)):
    return await fetch_polymarket_risk_graph_status(db)


@router.get("/runs", response_model=RiskQueryOut)
async def get_risk_runs(
    run_type: str | None = Query(default=None),
    reason: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_risk_graph_runs(db, run_type=run_type, reason=reason, start=start, end=end, limit=limit)
    return RiskQueryOut(rows=rows, limit=limit)


@router.get("/nodes", response_model=RiskQueryOut)
async def get_risk_nodes(
    node_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_risk_graph_nodes(db, node_type=node_type, condition_id=condition_id, asset_id=asset_id, limit=limit)
    return RiskQueryOut(rows=rows, limit=limit)


@router.get("/edges", response_model=RiskQueryOut)
async def get_risk_edges(
    edge_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_risk_graph_edges(db, edge_type=edge_type, condition_id=condition_id, asset_id=asset_id, limit=limit)
    return RiskQueryOut(rows=rows, limit=limit)


@router.get("/exposure-snapshots", response_model=RiskQueryOut)
async def get_exposure_snapshots(
    node_type: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    strategy_family: str | None = Query(default=None),
    strategy_version_id: int | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_portfolio_exposure_snapshots(
        db,
        node_type=node_type,
        condition_id=condition_id,
        asset_id=asset_id,
        strategy_family=strategy_family,
        strategy_version_id=strategy_version_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RiskQueryOut(rows=rows, limit=limit)


@router.get("/optimizer-recommendations", response_model=RiskQueryOut)
async def get_optimizer_recommendations(
    recommendation_type: str | None = Query(default=None),
    reason_code: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    strategy_family: str | None = Query(default=None),
    strategy_version_id: int | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_portfolio_optimizer_recommendations(
        db,
        recommendation_type=recommendation_type,
        reason_code=reason_code,
        condition_id=condition_id,
        asset_id=asset_id,
        strategy_family=strategy_family,
        strategy_version_id=strategy_version_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RiskQueryOut(rows=rows, limit=limit)


@router.get("/inventory-controls", response_model=RiskQueryOut)
async def get_inventory_controls(
    condition_id: str | None = Query(default=None),
    asset_id: str | None = Query(default=None),
    strategy_family: str | None = Query(default=None),
    strategy_version_id: int | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_inventory_control_snapshots(
        db,
        condition_id=condition_id,
        asset_id=asset_id,
        strategy_family=strategy_family,
        strategy_version_id=strategy_version_id,
        start=start,
        end=end,
        limit=limit,
    )
    return RiskQueryOut(rows=rows, limit=limit)


@router.post("/graph/build", response_model=RiskRunOut)
async def post_graph_build(payload: RiskManualRunRequest, db: AsyncSession = Depends(get_db)):
    return await build_risk_graph(db, reason=payload.reason, scope_json={"manual_api": True, "snapshot_at": payload.snapshot_at})


@router.post("/graph/snapshot", response_model=RiskRunOut)
async def post_exposure_snapshot(payload: RiskManualRunRequest, db: AsyncSession = Depends(get_db)):
    return await create_exposure_snapshot(db, reason=payload.reason, snapshot_at=payload.snapshot_at, scope_json={"manual_api": True})


@router.post("/graph/optimize", response_model=RiskRunOut)
async def post_optimizer_run(payload: RiskManualRunRequest, db: AsyncSession = Depends(get_db)):
    return await run_portfolio_optimizer(db, reason=payload.reason, observed_at=payload.snapshot_at, scope_json={"manual_api": True})
