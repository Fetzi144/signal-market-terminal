"""Health and observability endpoint."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.ingestion.polymarket_metadata import fetch_polymarket_meta_sync_status
from app.ingestion.polymarket_raw_storage import fetch_polymarket_raw_storage_status
from app.models.ingestion import IngestionRun
from app.models.market import Market
from app.models.signal import Signal

router = APIRouter(prefix="/api/v1", tags=["health"])


class IngestionStatus(BaseModel):
    run_type: str
    last_status: str | None
    last_run: datetime | None
    markets_processed: int | None


class PolymarketPhase2Status(BaseModel):
    enabled: bool
    on_startup: bool
    interval_seconds: int
    include_closed: bool
    page_size: int
    last_successful_sync_at: datetime | None = None
    last_run_status: str | None = None
    recent_param_changes_24h: int
    stale_registry_counts: dict[str, int]
    registry_counts: dict[str, int]
    freshness_seconds: int | None = None


class PolymarketPhase3Status(BaseModel):
    enabled: bool
    projector_last_run_status: str | None = None
    projector_lag: int
    last_projected_raw_event_id: int
    latest_relevant_raw_event_id: int
    last_successful_book_snapshot_at: datetime | None = None
    last_successful_trade_backfill_at: datetime | None = None
    last_successful_oi_poll_at: datetime | None = None
    rows_inserted_24h: dict[str, int]


class HealthOut(BaseModel):
    status: str
    now: datetime
    active_markets: int
    total_signals: int
    unresolved_signals: int
    recent_alerts_24h: int
    alert_threshold: float
    ingestion: list[IngestionStatus]
    polymarket_phase2: PolymarketPhase2Status
    polymarket_phase3: PolymarketPhase3Status


@router.get("/health", response_model=HealthOut)
async def health(db: AsyncSession = Depends(get_db)):
    now = datetime.now(timezone.utc)

    active_markets = (await db.execute(
        select(func.count(Market.id)).where(Market.active.is_(True))
    )).scalar() or 0

    total_signals = (await db.execute(select(func.count(Signal.id)))).scalar() or 0
    unresolved = (await db.execute(
        select(func.count(Signal.id)).where(Signal.resolved.is_(False))
    )).scalar() or 0

    # Count high-rank signals in last 24h (alerts)
    threshold = Decimal(str(settings.alert_rank_threshold))
    recent_alerts = (await db.execute(
        select(func.count(Signal.id)).where(
            Signal.rank_score >= threshold,
            Signal.fired_at >= now - timedelta(hours=24),
        )
    )).scalar() or 0

    # Latest ingestion runs by type
    ingestion_statuses = []
    for run_type in ("market_discovery", "snapshot"):
        result = await db.execute(
            select(IngestionRun)
            .where(IngestionRun.run_type == run_type)
            .order_by(IngestionRun.started_at.desc())
            .limit(1)
        )
        run = result.scalar_one_or_none()
        ingestion_statuses.append(IngestionStatus(
            run_type=run_type,
            last_status=run.status if run else None,
            last_run=run.started_at if run else None,
            markets_processed=run.markets_processed if run else None,
        ))

    polymarket_phase2 = await fetch_polymarket_meta_sync_status(db)
    polymarket_phase3 = await fetch_polymarket_raw_storage_status(db)

    return HealthOut(
        status="ok",
        now=now,
        active_markets=active_markets,
        total_signals=total_signals,
        unresolved_signals=unresolved,
        recent_alerts_24h=recent_alerts,
        alert_threshold=settings.alert_rank_threshold,
        ingestion=ingestion_statuses,
        polymarket_phase2=PolymarketPhase2Status(**{
            key: polymarket_phase2[key]
            for key in (
                "enabled",
                "on_startup",
                "interval_seconds",
                "include_closed",
                "page_size",
                "last_successful_sync_at",
                "last_run_status",
                "recent_param_changes_24h",
                "stale_registry_counts",
                "registry_counts",
                "freshness_seconds",
            )
        }),
        polymarket_phase3=PolymarketPhase3Status(**{
            key: polymarket_phase3[key]
            for key in (
                "enabled",
                "projector_last_run_status",
                "projector_lag",
                "last_projected_raw_event_id",
                "latest_relevant_raw_event_id",
                "last_successful_book_snapshot_at",
                "last_successful_trade_backfill_at",
                "last_successful_oi_poll_at",
                "rows_inserted_24h",
            )
        }),
    )
