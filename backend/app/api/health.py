"""Health and observability endpoint."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.models.ingestion import IngestionRun
from app.models.market import Market
from app.models.signal import Signal

router = APIRouter(prefix="/api/v1", tags=["health"])


class IngestionStatus(BaseModel):
    run_type: str
    last_status: str | None
    last_run: datetime | None
    markets_processed: int | None


class HealthOut(BaseModel):
    status: str
    now: datetime
    active_markets: int
    total_signals: int
    unresolved_signals: int
    recent_alerts_24h: int
    alert_threshold: float
    ingestion: list[IngestionStatus]


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

    return HealthOut(
        status="ok",
        now=now,
        active_markets=active_markets,
        total_signals=total_signals,
        unresolved_signals=unresolved,
        recent_alerts_24h=recent_alerts,
        alert_threshold=settings.alert_rank_threshold,
        ingestion=ingestion_statuses,
    )
