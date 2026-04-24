"""Polymarket stream ingestion status and control endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db import get_db, get_session_factory
from app.ingestion.polymarket_book_reconstruction import (
    fetch_polymarket_book_recon_status,
    get_polymarket_reconstructed_top_of_book,
    list_polymarket_book_recon_incidents,
    lookup_polymarket_book_recon_state,
    trigger_manual_polymarket_book_recon_catchup,
    trigger_manual_polymarket_book_recon_resync,
)
from app.ingestion.polymarket_execution_policy import (
    evaluate_polymarket_execution_policy_dry_run,
    fetch_polymarket_execution_action_mix,
    fetch_polymarket_execution_invalidation_reasons,
    fetch_polymarket_execution_policy_status,
    lookup_polymarket_execution_action_candidates,
    lookup_polymarket_execution_decisions,
)
from app.ingestion.polymarket_maker_economics import (
    fetch_polymarket_maker_status,
    lookup_current_reward_state,
    lookup_current_token_fee_state,
    lookup_reward_history,
    lookup_token_fee_history,
)
from app.ingestion.polymarket_metadata import (
    fetch_polymarket_meta_sync_status,
    list_polymarket_meta_sync_runs,
    lookup_polymarket_asset_registry,
    lookup_polymarket_event_registry,
    lookup_polymarket_market_param_history,
    lookup_polymarket_market_registry,
    trigger_manual_polymarket_meta_sync,
)
from app.ingestion.polymarket_microstructure import (
    fetch_polymarket_feature_status,
    list_polymarket_feature_runs,
    lookup_polymarket_alpha_labels,
    lookup_polymarket_book_state_topn,
    lookup_polymarket_microstructure_features,
    lookup_polymarket_passive_fill_labels,
    trigger_manual_polymarket_feature_materialization,
)
from app.ingestion.polymarket_raw_storage import (
    fetch_polymarket_raw_storage_status,
    list_polymarket_raw_capture_runs,
    lookup_polymarket_bbo_events,
    lookup_polymarket_book_deltas,
    lookup_polymarket_book_snapshots,
    lookup_polymarket_open_interest_history,
    lookup_polymarket_trade_tape,
    trigger_manual_polymarket_book_snapshot,
    trigger_manual_polymarket_oi_poll,
    trigger_manual_polymarket_raw_projector,
    trigger_manual_polymarket_trade_backfill,
)
from app.ingestion.polymarket_replay_simulator import fetch_polymarket_replay_status
from app.ingestion.polymarket_stream import (
    ensure_watch_registry_bootstrapped,
    fetch_polymarket_stream_status,
    list_polymarket_incidents,
    list_polymarket_resync_runs,
    list_watch_asset_rows,
    trigger_manual_polymarket_resync,
    upsert_watch_asset,
)
from app.ingestion.structure_engine import fetch_market_structure_status
from app.models.market import Market, Outcome
from app.models.polymarket_stream import PolymarketWatchAsset
from app.strategy_families import build_strategy_family_reviews

router = APIRouter(prefix="/api/v1/ingest/polymarket", tags=["ingest"])


class PolymarketIncidentOut(BaseModel):
    id: uuid.UUID
    created_at: datetime
    incident_type: str
    severity: str
    asset_id: str | None = None
    connection_id: uuid.UUID | None = None
    raw_event_id: int | None = None
    resync_run_id: uuid.UUID | None = None
    details_json: dict[str, Any] | None = None
    resolved_at: datetime | None = None


class PolymarketResyncRunOut(BaseModel):
    id: uuid.UUID
    started_at: datetime
    completed_at: datetime | None = None
    status: str
    reason: str
    connection_id: uuid.UUID | None = None
    requested_asset_count: int
    succeeded_asset_count: int
    failed_asset_count: int
    details_json: dict[str, Any] | None = None


class PolymarketMetaSyncRunOut(BaseModel):
    id: uuid.UUID
    started_at: datetime
    completed_at: datetime | None = None
    status: str
    reason: str
    include_closed: bool
    events_seen: int
    markets_seen: int
    assets_upserted: int
    events_upserted: int
    markets_upserted: int
    param_rows_inserted: int
    error_count: int
    details_json: dict[str, Any] | None = None


class PolymarketMetaSyncStatusOut(BaseModel):
    enabled: bool
    on_startup: bool
    interval_seconds: int
    include_closed: bool
    page_size: int
    last_successful_sync_at: datetime | None = None
    last_run_status: str | None = None
    last_run_started_at: datetime | None = None
    last_run_completed_at: datetime | None = None
    last_run_id: uuid.UUID | None = None
    recent_param_changes_24h: int
    stale_registry_counts: dict[str, int]
    registry_counts: dict[str, int]
    stale_after_seconds: int
    freshness_seconds: int | None = None
    recent_sync_runs: list[PolymarketMetaSyncRunOut]


class PolymarketRawCaptureRunOut(BaseModel):
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


class PolymarketFeatureRunOut(BaseModel):
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


class PolymarketFeatureStatusOut(BaseModel):
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
    recent_runs: list[PolymarketFeatureRunOut]


class PolymarketExecutionPolicyStatusOut(BaseModel):
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


class PolymarketMakerStatusOut(BaseModel):
    enabled: bool
    fee_history_enabled: bool
    reward_history_enabled: bool
    quote_optimizer_enabled: bool
    quote_optimizer_max_notional: float
    quote_optimizer_max_age_seconds: int
    quote_optimizer_require_rewards_data: bool
    quote_optimizer_require_fee_data: bool
    last_fee_sync_at: datetime | None = None
    last_reward_sync_at: datetime | None = None
    last_snapshot_at: datetime | None = None
    last_recommendation_at: datetime | None = None
    fee_history_rows: int
    reward_history_rows: int
    economics_snapshot_rows: int
    quote_recommendation_rows: int
    reward_state_counts: dict[str, int]
    recent_reason_counts_24h: dict[str, int]
    fee_freshness_seconds: int | None = None
    reward_freshness_seconds: int | None = None


class PolymarketRawStorageStatusOut(BaseModel):
    enabled: bool
    book_snapshot_interval_seconds: int
    trade_backfill_enabled: bool
    trade_backfill_on_startup: bool
    trade_backfill_interval_seconds: int
    trade_backfill_lookback_hours: int
    trade_backfill_page_size: int
    oi_poll_enabled: bool
    oi_poll_interval_seconds: int
    retention_days: int
    projector_last_run_status: str | None = None
    projector_last_run_started_at: datetime | None = None
    projector_last_run_completed_at: datetime | None = None
    last_projected_raw_event_id: int
    latest_relevant_raw_event_id: int
    projector_lag: int
    last_successful_book_snapshot_at: datetime | None = None
    last_successful_trade_backfill_at: datetime | None = None
    last_successful_oi_poll_at: datetime | None = None
    book_snapshot_freshness_seconds: int | None = None
    trade_backfill_freshness_seconds: int | None = None
    oi_poll_freshness_seconds: int | None = None
    rows_inserted_24h: dict[str, int]
    recent_capture_runs: list[PolymarketRawCaptureRunOut]


class PolymarketBookReconIncidentOut(BaseModel):
    id: uuid.UUID
    market_dim_id: int | None = None
    asset_dim_id: int | None = None
    condition_id: str
    asset_id: str
    incident_type: str
    severity: str
    raw_event_id: int | None = None
    snapshot_id: int | None = None
    capture_run_id: uuid.UUID | None = None
    exchange_ts: datetime | None = None
    observed_at_local: datetime
    expected_best_bid: float | None = None
    observed_best_bid: float | None = None
    expected_best_ask: float | None = None
    observed_best_ask: float | None = None
    expected_hash: str | None = None
    observed_hash: str | None = None
    details_json: dict[str, Any] | list[Any] | str | None = None
    created_at: datetime


class PolymarketBookReconStateOut(BaseModel):
    id: int
    market_dim_id: int | None = None
    asset_dim_id: int | None = None
    condition_id: str
    asset_id: str
    status: str
    last_snapshot_id: int | None = None
    last_snapshot_source_kind: str | None = None
    last_snapshot_hash: str | None = None
    last_snapshot_exchange_ts: datetime | None = None
    last_applied_raw_event_id: int | None = None
    last_applied_delta_raw_event_id: int | None = None
    last_applied_delta_index: int | None = None
    last_bbo_raw_event_id: int | None = None
    last_trade_raw_event_id: int | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    spread: float | None = None
    depth_levels_bid: int | None = None
    depth_levels_ask: int | None = None
    expected_tick_size: float | None = None
    last_exchange_ts: datetime | None = None
    last_received_at_local: datetime | None = None
    last_reconciled_at: datetime | None = None
    last_resynced_at: datetime | None = None
    drift_count: int
    resync_count: int
    details_json: dict[str, Any] | list[Any] | str | None = None
    created_at: datetime
    updated_at: datetime


class PolymarketBookReconStatusOut(BaseModel):
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
    stale_asset_count: int
    resyncing_asset_count: int
    degraded_asset_count: int
    last_successful_resync_at: datetime | None = None
    recent_incident_count: int
    status_counts: dict[str, int]
    recent_incidents: list[PolymarketBookReconIncidentOut]


class PolymarketReplayStatusOut(BaseModel):
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
    coverage_mode: str
    configured_supported_detectors: list[str]
    supported_detectors: list[str]
    unsupported_detectors: list[str]
    recent_variant_summary: dict[str, dict[str, Any]]
    recent_runs: list[dict[str, Any]]


class StrategyFamilyReviewOut(BaseModel):
    family: str
    label: str
    posture: str
    configured: bool
    review_enabled: bool
    primary_surface: str
    description: str
    disabled_reason: str | None = None


class PolymarketWatchAssetOut(BaseModel):
    id: uuid.UUID
    outcome_id: uuid.UUID
    asset_id: str
    watch_enabled: bool
    watch_reason: str | None = None
    priority: int | None = None
    created_at: datetime
    updated_at: datetime
    market_id: uuid.UUID
    market_platform_id: str
    market_question: str
    market_active: bool
    outcome_name: str


class PaginatedIncidentsOut(BaseModel):
    incidents: list[PolymarketIncidentOut]
    total: int
    page: int
    page_size: int


class PaginatedResyncRunsOut(BaseModel):
    resync_runs: list[PolymarketResyncRunOut]
    total: int
    page: int
    page_size: int


class PaginatedMetaSyncRunsOut(BaseModel):
    sync_runs: list[PolymarketMetaSyncRunOut]
    total: int
    page: int
    page_size: int


class PaginatedWatchAssetsOut(BaseModel):
    watch_assets: list[PolymarketWatchAssetOut]
    total: int
    page: int
    page_size: int


class PaginatedRawCaptureRunsOut(BaseModel):
    capture_runs: list[PolymarketRawCaptureRunOut]
    total: int
    page: int
    page_size: int


class PaginatedFeatureRunsOut(BaseModel):
    feature_runs: list[PolymarketFeatureRunOut]
    total: int
    page: int
    page_size: int


class PolymarketIngestStatusOut(BaseModel):
    enabled: bool
    connected: bool
    connection_started_at: datetime | None = None
    current_connection_id: uuid.UUID | None = None
    last_event_received_at: datetime | None = None
    heartbeat_freshness_seconds: int | None = None
    continuity_status: str
    active_watch_count: int
    watched_asset_count: int
    active_subscription_count: int
    subscribed_asset_count: int
    events_ingested_5m: int
    events_ingested: dict[str, int]
    reconnect_count: int
    resync_count: int
    gap_suspected_count: int
    malformed_message_count: int
    last_resync_at: datetime | None = None
    last_successful_resync_at: datetime | None = None
    last_reconciliation_at: datetime | None = None
    last_error: str | None = None
    last_error_at: datetime | None = None
    updated_at: datetime | None = None
    recent_incidents: list[PolymarketIncidentOut]
    recent_resync_runs: list[PolymarketResyncRunOut]
    metadata_sync: PolymarketMetaSyncStatusOut
    raw_storage: PolymarketRawStorageStatusOut
    book_reconstruction: PolymarketBookReconStatusOut
    features: PolymarketFeatureStatusOut
    execution_policy: PolymarketExecutionPolicyStatusOut
    maker_economics: PolymarketMakerStatusOut
    replay: PolymarketReplayStatusOut
    strategy_families: list[StrategyFamilyReviewOut]
    structure_engine: dict[str, Any]


class PolymarketManualResyncRequest(BaseModel):
    asset_ids: list[str] | None = None
    reason: str = Field(default="manual", min_length=1, max_length=64)


class PolymarketManualResyncOut(BaseModel):
    run_id: uuid.UUID
    asset_ids: list[str]
    requested_asset_count: int
    succeeded_asset_count: int
    failed_asset_count: int
    events_persisted: int
    reason: str
    status: str


class PolymarketManualMetaSyncRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    include_closed: bool | None = None
    asset_ids: list[str] | None = None


class PolymarketManualMetaSyncOut(BaseModel):
    id: uuid.UUID
    started_at: datetime
    completed_at: datetime | None = None
    status: str
    reason: str
    include_closed: bool
    events_seen: int
    markets_seen: int
    assets_upserted: int
    events_upserted: int
    markets_upserted: int
    param_rows_inserted: int
    error_count: int
    details_json: dict[str, Any] | None = None


class PolymarketManualRawProjectorRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    after_raw_event_id: int | None = Field(default=None, ge=0)
    limit: int = Field(default=1000, ge=1, le=5000)


class PolymarketManualRawProjectorOut(BaseModel):
    run_count: int
    last_run: PolymarketRawCaptureRunOut | None = None
    runs: list[PolymarketRawCaptureRunOut]


class PolymarketManualBookSnapshotRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    asset_ids: list[str] | None = None


class PolymarketManualTradeBackfillRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    asset_ids: list[str] | None = None
    condition_ids: list[str] | None = None
    lookback_hours: int | None = Field(default=None, ge=1, le=720)


class PolymarketManualOiPollRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    asset_ids: list[str] | None = None
    condition_ids: list[str] | None = None


class PolymarketManualBookReconResyncRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    asset_ids: list[str] | None = None


class PolymarketManualBookReconCatchupRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    asset_ids: list[str] | None = None


class PolymarketManualFeatureMaterializeRequest(BaseModel):
    reason: str = Field(default="manual", min_length=1, max_length=64)
    asset_ids: list[str] | None = None
    condition_ids: list[str] | None = None
    start: datetime | None = None
    end: datetime | None = None


class PolymarketManualFeatureMaterializeOut(BaseModel):
    status: str
    scope_json: dict[str, Any]
    book_state_run: PolymarketFeatureRunOut
    feature_run: PolymarketFeatureRunOut
    label_run: PolymarketFeatureRunOut


class PolymarketWatchAssetUpsertRequest(BaseModel):
    outcome_id: uuid.UUID | None = None
    asset_id: str | None = None
    watch_enabled: bool = True
    watch_reason: str | None = Field(default=None, max_length=255)
    priority: int | None = None
    bootstrap_from_active_universe: bool = False


class PolymarketWatchAssetPatchRequest(BaseModel):
    watch_enabled: bool | None = None
    watch_reason: str | None = Field(default=None, max_length=255)
    priority: int | None = None


class PolymarketWatchAssetMutationOut(BaseModel):
    watch_assets: list[PolymarketWatchAssetOut]
    created_count: int
    updated_count: int
    bootstrap_created_count: int = 0
    bootstrap_updated_count: int = 0


class PolymarketRegistryQueryOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


class PolymarketParamHistoryQueryOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int
    changed_only: bool


class PolymarketHistoryQueryOut(BaseModel):
    rows: list[dict[str, Any]]
    limit: int


class PolymarketExecutionPolicyDryRunRequest(BaseModel):
    signal_id: uuid.UUID


class PolymarketExecutionPolicyDryRunOut(BaseModel):
    applicable: bool
    policy_version: str | None = None
    reason: str | None = None
    signal_id: str | None = None
    context: dict[str, Any] | None = None
    chosen_reason: str | None = None
    chosen_candidate: dict[str, Any] | None = None
    candidates: list[dict[str, Any]] = Field(default_factory=list)


class PolymarketBookReconStateQueryOut(BaseModel):
    rows: list[PolymarketBookReconStateOut]
    limit: int


class PolymarketBookReconIncidentQueryOut(BaseModel):
    rows: list[PolymarketBookReconIncidentOut]
    limit: int


class PolymarketBookReconActionOut(BaseModel):
    asset_ids: list[str]
    reason: str
    status: str | None = None
    run_id: uuid.UUID | None = None
    requested_asset_count: int | None = None
    succeeded_asset_count: int | None = None
    failed_asset_count: int | None = None
    events_persisted: int | None = None
    asset_count: int | None = None
    live_count: int | None = None
    degraded_count: int | None = None
    results: list[dict[str, Any]] | None = None
    reconstruction: dict[str, Any] | None = None


async def _fetch_watch_asset_row(
    session: AsyncSession,
    watch_asset_id: uuid.UUID,
) -> dict[str, Any] | None:
    result = await session.execute(
        select(PolymarketWatchAsset, Outcome, Market)
        .join(Outcome, PolymarketWatchAsset.outcome_id == Outcome.id)
        .join(Market, Outcome.market_id == Market.id)
        .where(PolymarketWatchAsset.id == watch_asset_id)
    )
    row = result.first()
    if row is None:
        return None
    watch_asset, outcome, market = row
    return {
        "id": watch_asset.id,
        "outcome_id": watch_asset.outcome_id,
        "asset_id": watch_asset.asset_id,
        "watch_enabled": watch_asset.watch_enabled,
        "watch_reason": watch_asset.watch_reason,
        "priority": watch_asset.priority,
        "created_at": watch_asset.created_at,
        "updated_at": watch_asset.updated_at,
        "market_id": market.id,
        "market_platform_id": market.platform_id,
        "market_question": market.question,
        "market_active": market.active,
        "outcome_name": outcome.name,
    }


@router.get("/status", response_model=PolymarketIngestStatusOut)
async def get_polymarket_ingest_status(db: AsyncSession = Depends(get_db)):
    status = await fetch_polymarket_stream_status(db)
    status["raw_storage"] = await fetch_polymarket_raw_storage_status(db)
    status["book_reconstruction"] = await fetch_polymarket_book_recon_status(db)
    status["features"] = await fetch_polymarket_feature_status(db)
    status["execution_policy"] = await fetch_polymarket_execution_policy_status(db)
    status["maker_economics"] = await fetch_polymarket_maker_status(db)
    status["replay"] = await fetch_polymarket_replay_status(db)
    status["strategy_families"] = build_strategy_family_reviews()
    status["structure_engine"] = await fetch_market_structure_status(db)
    return status


@router.get("/incidents", response_model=PaginatedIncidentsOut)
async def get_polymarket_ingest_incidents(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    incidents, total = await list_polymarket_incidents(db, page=page, page_size=page_size)
    return PaginatedIncidentsOut(incidents=incidents, total=total, page=page, page_size=page_size)


@router.get("/resync-runs", response_model=PaginatedResyncRunsOut)
async def get_polymarket_resync_runs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    runs, total = await list_polymarket_resync_runs(db, page=page, page_size=page_size)
    return PaginatedResyncRunsOut(resync_runs=runs, total=total, page=page, page_size=page_size)


@router.get("/meta-sync/status", response_model=PolymarketMetaSyncStatusOut)
async def get_polymarket_meta_sync_status(
    db: AsyncSession = Depends(get_db),
):
    return await fetch_polymarket_meta_sync_status(db)


@router.get("/meta-sync/runs", response_model=PaginatedMetaSyncRunsOut)
async def get_polymarket_meta_sync_runs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    runs, total = await list_polymarket_meta_sync_runs(db, page=page, page_size=page_size)
    return PaginatedMetaSyncRunsOut(sync_runs=runs, total=total, page=page, page_size=page_size)


@router.get("/watch-assets", response_model=PaginatedWatchAssetsOut)
async def get_polymarket_watch_assets(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows, total = await list_watch_asset_rows(db, page=page, page_size=page_size)
    return PaginatedWatchAssetsOut(watch_assets=rows, total=total, page=page, page_size=page_size)


@router.post("/watch-assets", response_model=PolymarketWatchAssetMutationOut)
async def create_or_enable_polymarket_watch_asset(
    body: PolymarketWatchAssetUpsertRequest,
    db: AsyncSession = Depends(get_db),
):
    if body.bootstrap_from_active_universe and (body.outcome_id is not None or body.asset_id is not None):
        raise HTTPException(status_code=400, detail="Bootstrap request cannot target a specific asset")

    bootstrap_created_count = 0
    bootstrap_updated_count = 0
    created_count = 0
    updated_count = 0
    watch_assets: list[dict[str, Any]] = []

    if body.bootstrap_from_active_universe:
        bootstrap = await ensure_watch_registry_bootstrapped(db)
        bootstrap_created_count = bootstrap["created_count"]
        bootstrap_updated_count = bootstrap["updated_count"]
        await db.commit()
    else:
        try:
            existing_result = None
            if body.outcome_id is not None:
                existing_result = await db.execute(
                    select(PolymarketWatchAsset).where(PolymarketWatchAsset.outcome_id == body.outcome_id)
                )
            elif body.asset_id is not None:
                existing_result = await db.execute(
                    select(PolymarketWatchAsset)
                    .join(Outcome, PolymarketWatchAsset.outcome_id == Outcome.id)
                    .where(Outcome.token_id == body.asset_id)
                )
            existing = existing_result.scalars().first() if existing_result is not None else None

            watch_asset = await upsert_watch_asset(
                db,
                outcome_id=body.outcome_id,
                asset_id=body.asset_id,
                watch_enabled=body.watch_enabled,
                watch_reason=body.watch_reason,
                priority=body.priority,
            )
            await db.commit()
            watch_row = await _fetch_watch_asset_row(db, watch_asset.id)
            if watch_row is not None:
                watch_assets.append(watch_row)
            if existing is None:
                created_count = 1
            else:
                updated_count = 1
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return PolymarketWatchAssetMutationOut(
        watch_assets=watch_assets,
        created_count=created_count,
        updated_count=updated_count,
        bootstrap_created_count=bootstrap_created_count,
        bootstrap_updated_count=bootstrap_updated_count,
    )


@router.patch("/watch-assets/{watch_asset_id}", response_model=PolymarketWatchAssetOut)
async def update_polymarket_watch_asset(
    watch_asset_id: uuid.UUID,
    body: PolymarketWatchAssetPatchRequest,
    db: AsyncSession = Depends(get_db),
):
    watch_asset = await db.get(PolymarketWatchAsset, watch_asset_id)
    if watch_asset is None:
        raise HTTPException(status_code=404, detail="Watch asset not found")

    if body.watch_enabled is not None:
        watch_asset.watch_enabled = body.watch_enabled
    if body.watch_reason is not None:
        watch_asset.watch_reason = body.watch_reason
    if body.priority is not None:
        watch_asset.priority = body.priority

    await db.commit()
    watch_row = await _fetch_watch_asset_row(db, watch_asset_id)
    if watch_row is None:
        raise HTTPException(status_code=404, detail="Watch asset not found")
    return PolymarketWatchAssetOut(**watch_row)


@router.post("/resync", response_model=PolymarketManualResyncOut)
async def resync_polymarket_market_data(
    body: PolymarketManualResyncRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        result = await trigger_manual_polymarket_resync(
            session_factory,
            asset_ids=body.asset_ids,
            reason=body.reason,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Polymarket resync failed: {exc}") from exc
    return PolymarketManualResyncOut(**result)


@router.post("/meta-sync", response_model=PolymarketManualMetaSyncOut)
async def sync_polymarket_metadata(
    body: PolymarketManualMetaSyncRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        result = await trigger_manual_polymarket_meta_sync(
            session_factory,
            reason=body.reason,
            include_closed=body.include_closed,
            asset_ids=body.asset_ids,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Polymarket metadata sync failed: {exc}") from exc
    return PolymarketManualMetaSyncOut(**result)


@router.get("/registry/events", response_model=PolymarketRegistryQueryOut)
async def get_polymarket_event_registry(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    event_slug: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_event_registry(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        event_slug=event_slug,
        limit=limit,
    )
    return PolymarketRegistryQueryOut(rows=rows, limit=limit)


@router.get("/registry/markets", response_model=PolymarketRegistryQueryOut)
async def get_polymarket_market_registry(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    event_slug: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_market_registry(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        event_slug=event_slug,
        limit=limit,
    )
    return PolymarketRegistryQueryOut(rows=rows, limit=limit)


@router.get("/registry/assets", response_model=PolymarketRegistryQueryOut)
async def get_polymarket_asset_registry(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    event_slug: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_asset_registry(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        event_slug=event_slug,
        limit=limit,
    )
    return PolymarketRegistryQueryOut(rows=rows, limit=limit)


@router.get("/registry/param-history", response_model=PolymarketParamHistoryQueryOut)
async def get_polymarket_param_history(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    event_slug: str | None = Query(default=None),
    changed_only: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_market_param_history(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        event_slug=event_slug,
        changed_only=changed_only,
        limit=limit,
    )
    return PolymarketParamHistoryQueryOut(rows=rows, limit=limit, changed_only=changed_only)


@router.get("/maker-economics/status", response_model=PolymarketMakerStatusOut)
async def get_polymarket_maker_status(
    db: AsyncSession = Depends(get_db),
):
    return await fetch_polymarket_maker_status(db)


@router.get("/maker-economics/fees/current", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_current_fee_state(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    as_of: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_current_token_fee_state(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        as_of=as_of,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/maker-economics/fees/history", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_fee_history(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_token_fee_history(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/maker-economics/rewards/current", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_current_reward_state(
    condition_id: str | None = Query(default=None),
    as_of: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_current_reward_state(
        db,
        condition_id=condition_id,
        as_of=as_of,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/maker-economics/rewards/history", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_reward_history(
    condition_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_reward_history(
        db,
        condition_id=condition_id,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/reconstruction/status", response_model=PolymarketBookReconStatusOut)
async def get_polymarket_book_reconstruction_status(
    db: AsyncSession = Depends(get_db),
):
    return await fetch_polymarket_book_recon_status(db)


@router.get("/reconstruction/state", response_model=PolymarketBookReconStateQueryOut)
async def get_polymarket_book_reconstruction_state_rows(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_book_recon_state(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        limit=limit,
    )
    return PolymarketBookReconStateQueryOut(rows=[PolymarketBookReconStateOut(**row) for row in rows], limit=limit)


@router.get("/reconstruction/incidents", response_model=PolymarketBookReconIncidentQueryOut)
async def get_polymarket_book_reconstruction_incidents(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    incident_type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await list_polymarket_book_recon_incidents(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        status=status,
        incident_type=incident_type,
        limit=limit,
    )
    return PolymarketBookReconIncidentQueryOut(
        rows=[PolymarketBookReconIncidentOut(**row) for row in rows],
        limit=limit,
    )


@router.get("/reconstruction/top-of-book")
async def get_polymarket_book_reconstruction_top_of_book(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    if not asset_id and not condition_id:
        raise HTTPException(status_code=400, detail="asset_id or condition_id is required")
    row = await get_polymarket_reconstructed_top_of_book(db, asset_id=asset_id, condition_id=condition_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Reconstructed book not found")
    return row


@router.post("/reconstruction/resync", response_model=PolymarketBookReconActionOut)
async def run_polymarket_book_reconstruction_resync(
    body: PolymarketManualBookReconResyncRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        result = await trigger_manual_polymarket_book_recon_resync(
            session_factory,
            asset_ids=body.asset_ids,
            reason=body.reason,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Polymarket reconstruction resync failed: {exc}") from exc
    return PolymarketBookReconActionOut(**result)


@router.post("/reconstruction/catch-up", response_model=PolymarketBookReconActionOut)
async def run_polymarket_book_reconstruction_catch_up(
    body: PolymarketManualBookReconCatchupRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    result = await trigger_manual_polymarket_book_recon_catchup(
        session_factory,
        asset_ids=body.asset_ids,
        reason=body.reason,
    )
    return PolymarketBookReconActionOut(**result, reason=body.reason, status="completed")


@router.get("/features/status", response_model=PolymarketFeatureStatusOut)
async def get_polymarket_feature_pipeline_status(
    db: AsyncSession = Depends(get_db),
):
    return await fetch_polymarket_feature_status(db)


@router.get("/features/runs", response_model=PaginatedFeatureRunsOut)
async def get_polymarket_feature_pipeline_runs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    runs, total = await list_polymarket_feature_runs(db, page=page, page_size=page_size)
    return PaginatedFeatureRunsOut(feature_runs=runs, total=total, page=page, page_size=page_size)


@router.post("/features/materialize", response_model=PolymarketManualFeatureMaterializeOut)
async def run_polymarket_feature_materialization(
    body: PolymarketManualFeatureMaterializeRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    result = await trigger_manual_polymarket_feature_materialization(
        session_factory,
        reason=body.reason,
        asset_ids=body.asset_ids,
        condition_ids=body.condition_ids,
        start=body.start,
        end=body.end,
    )
    return PolymarketManualFeatureMaterializeOut(**result)


@router.get("/features/book-state", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_book_state_rows(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    bucket_width_ms: int | None = Query(default=None, ge=1),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_book_state_topn(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        bucket_width_ms=bucket_width_ms,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/features/rows", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_feature_rows(
    bucket_width_ms: int = Query(default=100, ge=1),
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_microstructure_features(
        db,
        bucket_width_ms=bucket_width_ms,
        asset_id=asset_id,
        condition_id=condition_id,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/features/alpha-labels", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_alpha_label_rows(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    horizon_ms: int | None = Query(default=None, ge=1),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_alpha_labels(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        horizon_ms=horizon_ms,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/features/passive-fill-labels", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_passive_fill_label_rows(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    horizon_ms: int | None = Query(default=None, ge=1),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_passive_fill_labels(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        horizon_ms=horizon_ms,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/execution-policy/status", response_model=PolymarketExecutionPolicyStatusOut)
async def get_polymarket_execution_policy_status(
    db: AsyncSession = Depends(get_db),
):
    return await fetch_polymarket_execution_policy_status(db)


@router.get("/execution-policy/action-candidates", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_execution_action_candidates(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    action_type: str | None = Query(default=None),
    valid: bool | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_execution_action_candidates(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        action_type=action_type,
        valid=valid,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/execution-policy/decisions", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_execution_decisions(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    signal_id: uuid.UUID | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_execution_decisions(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        signal_id=signal_id,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/execution-policy/invalidation-reasons", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_execution_invalidation_reasons(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    rows = await fetch_polymarket_execution_invalidation_reasons(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        start=start,
        end=end,
        limit=limit,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/execution-policy/action-mix", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_execution_action_mix(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    rows = await fetch_polymarket_execution_action_mix(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        start=start,
        end=end,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=len(rows))


@router.post("/execution-policy/dry-run", response_model=PolymarketExecutionPolicyDryRunOut)
async def run_polymarket_execution_policy_dry_run(
    body: PolymarketExecutionPolicyDryRunRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await evaluate_polymarket_execution_policy_dry_run(
            db,
            signal_id=body.signal_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PolymarketExecutionPolicyDryRunOut(**result)


@router.get("/raw/status", response_model=PolymarketRawStorageStatusOut)
async def get_polymarket_phase3_raw_storage_status(
    db: AsyncSession = Depends(get_db),
):
    return await fetch_polymarket_raw_storage_status(db)


@router.get("/raw/runs", response_model=PaginatedRawCaptureRunsOut)
async def get_polymarket_raw_capture_runs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    runs, total = await list_polymarket_raw_capture_runs(db, page=page, page_size=page_size)
    return PaginatedRawCaptureRunsOut(capture_runs=runs, total=total, page=page, page_size=page_size)


@router.post("/raw/project", response_model=PolymarketManualRawProjectorOut)
async def run_polymarket_raw_projector(
    body: PolymarketManualRawProjectorRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    result = await trigger_manual_polymarket_raw_projector(
        session_factory,
        reason=body.reason,
        after_raw_event_id=body.after_raw_event_id,
        limit=body.limit,
    )
    return PolymarketManualRawProjectorOut(**result)


@router.post("/raw/book-snapshots/trigger", response_model=PolymarketRawCaptureRunOut)
async def run_polymarket_manual_book_snapshot(
    body: PolymarketManualBookSnapshotRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        result = await trigger_manual_polymarket_book_snapshot(
            session_factory,
            reason=body.reason,
            asset_ids=body.asset_ids,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Polymarket book snapshot failed: {exc}") from exc
    return PolymarketRawCaptureRunOut(**result)


@router.post("/raw/trade-backfill/trigger", response_model=PolymarketRawCaptureRunOut)
async def run_polymarket_trade_backfill(
    body: PolymarketManualTradeBackfillRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        result = await trigger_manual_polymarket_trade_backfill(
            session_factory,
            reason=body.reason,
            asset_ids=body.asset_ids,
            condition_ids=body.condition_ids,
            lookback_hours=body.lookback_hours,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Polymarket trade backfill failed: {exc}") from exc
    return PolymarketRawCaptureRunOut(**result)


@router.post("/raw/oi-poll/trigger", response_model=PolymarketRawCaptureRunOut)
async def run_polymarket_oi_poll(
    body: PolymarketManualOiPollRequest,
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
):
    try:
        result = await trigger_manual_polymarket_oi_poll(
            session_factory,
            reason=body.reason,
            asset_ids=body.asset_ids,
            condition_ids=body.condition_ids,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Polymarket OI poll failed: {exc}") from exc
    return PolymarketRawCaptureRunOut(**result)


@router.get("/raw/book-snapshots", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_book_snapshots(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    source_kind: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    after_id: int | None = Query(default=None, ge=0),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_book_snapshots(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        source_kind=source_kind,
        start=start,
        end=end,
        limit=limit,
        after_id=after_id,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/raw/book-deltas", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_book_deltas(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    after_id: int | None = Query(default=None, ge=0),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_book_deltas(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        start=start,
        end=end,
        limit=limit,
        after_id=after_id,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/raw/bbo-events", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_bbo_event_rows(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    after_id: int | None = Query(default=None, ge=0),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_bbo_events(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        start=start,
        end=end,
        limit=limit,
        after_id=after_id,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/raw/trade-tape", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_trade_tape_rows(
    asset_id: str | None = Query(default=None),
    condition_id: str | None = Query(default=None),
    source_kind: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    after_id: int | None = Query(default=None, ge=0),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_trade_tape(
        db,
        asset_id=asset_id,
        condition_id=condition_id,
        source_kind=source_kind,
        start=start,
        end=end,
        limit=limit,
        after_id=after_id,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)


@router.get("/raw/oi-history", response_model=PolymarketHistoryQueryOut)
async def get_polymarket_open_interest_rows(
    condition_id: str | None = Query(default=None),
    source_kind: str | None = Query(default=None),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    after_id: int | None = Query(default=None, ge=0),
    db: AsyncSession = Depends(get_db),
):
    rows = await lookup_polymarket_open_interest_history(
        db,
        condition_id=condition_id,
        source_kind=source_kind,
        start=start,
        end=end,
        limit=limit,
        after_id=after_id,
    )
    return PolymarketHistoryQueryOut(rows=rows, limit=limit)
