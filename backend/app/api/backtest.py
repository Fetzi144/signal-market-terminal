"""Backtest CRUD and parameter sweep endpoints."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.backtesting.engine import BacktestEngine
from app.backtesting.sweep import parameter_sweep
from app.db import get_db
from app.models.backtest import BacktestRun, BacktestSignal
from app.models.snapshot import PriceSnapshot

router = APIRouter(prefix="/api/v1/backtests", tags=["backtests"])

MAX_DATE_RANGE_DAYS = 180


# --------------------------------------------------------------------------- #
# Pydantic schemas                                                              #
# --------------------------------------------------------------------------- #

class BacktestCreateRequest(BaseModel):
    name: str
    start_date: datetime
    end_date: datetime
    detector_configs: dict = {}
    rank_threshold: float = 0.5

    @field_validator("rank_threshold")
    @classmethod
    def _validate_rank(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("rank_threshold must be between 0.0 and 1.0")
        return v

    @model_validator(mode="after")
    def _validate_dates(self) -> "BacktestCreateRequest":
        now = datetime.now(timezone.utc)
        start = self.start_date
        end = self.end_date

        # Ensure timezone-aware
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        if end > now:
            raise ValueError("end_date must not be in the future")
        if start >= end:
            raise ValueError("start_date must be before end_date")
        span_days = (end - start).total_seconds() / 86400
        if span_days > MAX_DATE_RANGE_DAYS:
            raise ValueError(f"Date range exceeds maximum of {MAX_DATE_RANGE_DAYS} days")
        return self


class SweepRequest(BaseModel):
    name_prefix: str
    start_date: datetime
    end_date: datetime
    base_detector_configs: dict = {}
    base_rank_threshold: float = 0.5
    sweep_params: dict  # e.g. {"price_move.threshold_pct": [0.03, 0.05], "rank_threshold": [0.5, 0.7]}

    @model_validator(mode="after")
    def _validate_dates(self) -> "SweepRequest":
        now = datetime.now(timezone.utc)
        start = self.start_date
        end = self.end_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if end > now:
            raise ValueError("end_date must not be in the future")
        if start >= end:
            raise ValueError("start_date must be before end_date")
        span_days = (end - start).total_seconds() / 86400
        if span_days > MAX_DATE_RANGE_DAYS:
            raise ValueError(f"Date range exceeds maximum of {MAX_DATE_RANGE_DAYS} days")
        return self


class BacktestRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    created_at: datetime
    start_date: datetime
    end_date: datetime
    detector_configs: dict | None
    rank_threshold: float
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    result_summary: dict | None


class BacktestSignalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    backtest_run_id: uuid.UUID
    signal_type: str
    outcome_id: uuid.UUID | None
    fired_at: datetime
    signal_score: Decimal
    confidence: Decimal
    rank_score: Decimal
    resolved_correctly: bool | None
    price_at_fire: Decimal | None
    details: dict | None


class BacktestSignalListOut(BaseModel):
    signals: list[BacktestSignalOut]
    total: int
    page: int
    page_size: int


# --------------------------------------------------------------------------- #
# Background task helper                                                        #
# --------------------------------------------------------------------------- #

async def _run_backtest_background(run_id: uuid.UUID) -> None:
    """Execute a BacktestRun in the background with its own DB session."""
    from app.db import async_session

    engine = BacktestEngine()
    async with async_session() as session:
        result = await session.execute(
            select(BacktestRun).where(BacktestRun.id == run_id)
        )
        run = result.scalar_one_or_none()
        if run is None:
            return
        try:
            await engine.run(session, run)
        except Exception:
            pass  # engine.run() handles its own error state


# --------------------------------------------------------------------------- #
# Endpoints                                                                     #
# --------------------------------------------------------------------------- #

@router.post("", status_code=201)
async def create_backtest(
    body: BacktestCreateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Create and enqueue a backtest run. Returns immediately with status=pending."""
    # Validate that snapshot data exists for the start_date
    snap_check = await db.execute(
        select(func.count(PriceSnapshot.id)).where(
            PriceSnapshot.captured_at >= body.start_date,
            PriceSnapshot.captured_at <= body.end_date,
        )
    )
    snap_count = snap_check.scalar() or 0
    if snap_count == 0:
        raise HTTPException(
            status_code=422,
            detail="No price snapshot data found in the requested date range.",
        )

    run = BacktestRun(
        id=uuid.uuid4(),
        name=body.name,
        start_date=body.start_date,
        end_date=body.end_date,
        detector_configs=body.detector_configs or None,
        rank_threshold=body.rank_threshold,
        status="pending",
    )
    db.add(run)
    await db.commit()

    background_tasks.add_task(_run_backtest_background, run.id)

    return {"backtest_run_id": str(run.id), "status": "pending"}


@router.get("", response_model=list[BacktestRunOut])
async def list_backtests(db: AsyncSession = Depends(get_db)):
    """List all backtest runs, newest first."""
    result = await db.execute(
        select(BacktestRun).order_by(BacktestRun.created_at.desc())
    )
    return result.scalars().all()


@router.post("/sweep", status_code=201)
async def create_sweep(
    body: SweepRequest,
    db: AsyncSession = Depends(get_db),
):
    """Run a parameter sweep and return all created backtest run IDs."""
    # Validate snapshot data exists
    snap_check = await db.execute(
        select(func.count(PriceSnapshot.id)).where(
            PriceSnapshot.captured_at >= body.start_date,
            PriceSnapshot.captured_at <= body.end_date,
        )
    )
    snap_count = snap_check.scalar() or 0
    if snap_count == 0:
        raise HTTPException(
            status_code=422,
            detail="No price snapshot data found in the requested date range.",
        )

    runs = await parameter_sweep(
        session=db,
        name_prefix=body.name_prefix,
        start_date=body.start_date,
        end_date=body.end_date,
        base_detector_configs=body.base_detector_configs,
        base_rank_threshold=body.base_rank_threshold,
        sweep_params=body.sweep_params,
    )
    await db.commit()

    return {
        "backtest_run_ids": [str(r.id) for r in runs],
        "count": len(runs),
    }


@router.get("/{run_id}", response_model=BacktestRunOut)
async def get_backtest(run_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Get a single backtest run with full result_summary."""
    result = await db.execute(
        select(BacktestRun).where(BacktestRun.id == run_id)
    )
    run = result.scalar_one_or_none()
    if run is None:
        raise HTTPException(404, "Backtest run not found")
    return run


@router.get("/{run_id}/signals", response_model=BacktestSignalListOut)
async def list_backtest_signals(
    run_id: uuid.UUID,
    signal_type: str | None = None,
    resolved_correctly: bool | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """List hypothetical signals from a backtest run with optional filters."""
    # Verify run exists
    run_check = await db.execute(
        select(BacktestRun.id).where(BacktestRun.id == run_id)
    )
    if run_check.scalar_one_or_none() is None:
        raise HTTPException(404, "Backtest run not found")

    query = select(BacktestSignal).where(BacktestSignal.backtest_run_id == run_id)
    count_query = select(func.count(BacktestSignal.id)).where(
        BacktestSignal.backtest_run_id == run_id
    )

    if signal_type:
        query = query.where(BacktestSignal.signal_type == signal_type)
        count_query = count_query.where(BacktestSignal.signal_type == signal_type)
    if resolved_correctly is not None:
        query = query.where(BacktestSignal.resolved_correctly == resolved_correctly)
        count_query = count_query.where(
            BacktestSignal.resolved_correctly == resolved_correctly
        )

    total = (await db.execute(count_query)).scalar() or 0

    query = (
        query.order_by(BacktestSignal.fired_at.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    signals = result.scalars().all()

    return BacktestSignalListOut(
        signals=signals,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.delete("/{run_id}", status_code=204)
async def delete_backtest(run_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Delete a backtest run and all its signals (cascade)."""
    result = await db.execute(
        select(BacktestRun).where(BacktestRun.id == run_id)
    )
    run = result.scalar_one_or_none()
    if run is None:
        raise HTTPException(404, "Backtest run not found")
    await db.delete(run)
    await db.commit()
