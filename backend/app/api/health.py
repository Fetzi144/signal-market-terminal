"""Health and observability endpoint."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.ingestion.polymarket_book_reconstruction import fetch_polymarket_book_recon_status
from app.ingestion.polymarket_execution_policy import fetch_polymarket_execution_policy_status
from app.ingestion.polymarket_metadata import fetch_polymarket_meta_sync_status
from app.ingestion.polymarket_microstructure import fetch_polymarket_feature_status
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


class PolymarketPhase4Status(BaseModel):
    enabled: bool
    on_startup: bool
    auto_resync_enabled: bool
    stale_after_seconds: int
    resync_cooldown_seconds: int
    max_watched_assets: int
    bbo_tolerance: float
    watched_asset_count: int
    live_book_count: int
    drifted_asset_count: int
    resyncing_asset_count: int
    degraded_asset_count: int
    last_successful_resync_at: datetime | None = None
    recent_incident_count: int
    status_counts: dict[str, int]


class PolymarketPhase5Status(BaseModel):
    enabled: bool
    on_startup: bool
    interval_seconds: int
    lookback_hours: int
    bucket_widths_ms: list[int]
    label_horizons_ms: list[int]
    max_watched_assets: int
    last_successful_feature_run_at: datetime | None = None
    last_successful_label_run_at: datetime | None = None
    recent_feature_rows_24h: int
    recent_label_rows_24h: int
    incomplete_bucket_count_24h: int


class PolymarketPhase6Status(BaseModel):
    enabled: bool
    require_live_book: bool
    default_horizon_ms: int
    passive_lookback_hours: int
    passive_min_label_rows: int
    step_ahead_enabled: bool
    max_cross_slippage_bps: float
    min_net_ev_bps: float
    last_successful_decision_at: datetime | None = None
    recent_decisions_24h: int
    recent_action_mix: dict[str, int]
    recent_invalid_candidates_24h: int
    recent_skip_decisions_24h: int
    recent_avg_est_net_ev_bps: float | None = None


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
    polymarket_phase4: PolymarketPhase4Status
    polymarket_phase5: PolymarketPhase5Status
    polymarket_phase6: PolymarketPhase6Status


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
    polymarket_phase4 = await fetch_polymarket_book_recon_status(db)
    polymarket_phase5 = await fetch_polymarket_feature_status(db)
    polymarket_phase6 = await fetch_polymarket_execution_policy_status(db)

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
        polymarket_phase4=PolymarketPhase4Status(**{
            key: polymarket_phase4[key]
            for key in (
                "enabled",
                "on_startup",
                "auto_resync_enabled",
                "stale_after_seconds",
                "resync_cooldown_seconds",
                "max_watched_assets",
                "bbo_tolerance",
                "watched_asset_count",
                "live_book_count",
                "drifted_asset_count",
                "resyncing_asset_count",
                "degraded_asset_count",
                "last_successful_resync_at",
                "recent_incident_count",
                "status_counts",
            )
        }),
        polymarket_phase5=PolymarketPhase5Status(**{
            key: polymarket_phase5[key]
            for key in (
                "enabled",
                "on_startup",
                "interval_seconds",
                "lookback_hours",
                "bucket_widths_ms",
                "label_horizons_ms",
                "max_watched_assets",
                "last_successful_feature_run_at",
                "last_successful_label_run_at",
                "recent_feature_rows_24h",
                "recent_label_rows_24h",
                "incomplete_bucket_count_24h",
            )
        }),
        polymarket_phase6=PolymarketPhase6Status(**{
            key: polymarket_phase6[key]
            for key in (
                "enabled",
                "require_live_book",
                "default_horizon_ms",
                "passive_lookback_hours",
                "passive_min_label_rows",
                "step_ahead_enabled",
                "max_cross_slippage_bps",
                "min_net_ev_bps",
                "last_successful_decision_at",
                "recent_decisions_24h",
                "recent_action_mix",
                "recent_invalid_candidates_24h",
                "recent_skip_decisions_24h",
                "recent_avg_est_net_ev_bps",
            )
        }),
    )
