"""Health and observability endpoint."""
from typing import Any
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.execution.polymarket_control_plane import fetch_pilot_status
from app.execution.polymarket_live_state import fetch_polymarket_live_status
from app.execution.polymarket_pilot_evidence import PolymarketPilotEvidenceService
from app.ingestion.polymarket_book_reconstruction import fetch_polymarket_book_recon_status
from app.ingestion.polymarket_execution_policy import fetch_polymarket_execution_policy_status
from app.ingestion.polymarket_maker_economics import fetch_polymarket_maker_status
from app.ingestion.polymarket_metadata import fetch_polymarket_meta_sync_status
from app.ingestion.polymarket_microstructure import fetch_polymarket_feature_status
from app.ingestion.polymarket_replay_simulator import fetch_polymarket_replay_status
from app.ingestion.polymarket_risk_graph import fetch_polymarket_risk_graph_status
from app.ingestion.polymarket_raw_storage import fetch_polymarket_raw_storage_status
from app.ingestion.structure_engine import fetch_market_structure_status
from app.models.ingestion import IngestionRun
from app.models.market import Market
from app.models.signal import Signal

router = APIRouter(prefix="/api/v1", tags=["health"])
_pilot_evidence = PolymarketPilotEvidenceService()


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


class PolymarketPhase7AStatus(BaseModel):
    enabled: bool
    dry_run: bool
    manual_approval_required: bool
    gateway_reachable: bool
    user_stream_connected: bool
    kill_switch_enabled: bool
    outstanding_live_orders: int
    outstanding_reservations: float
    recent_fills_24h: int
    last_reconcile_success_at: datetime | None = None
    last_user_stream_message_at: datetime | None = None


class PolymarketPhase8AStatus(BaseModel):
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


class PolymarketPhase9Status(BaseModel):
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


class PolymarketPhase10ExposureBucket(BaseModel):
    node_key: str
    node_type: str
    label: str | None = None
    gross_notional_usd: float | None = None
    net_notional_usd: float | None = None
    hedged_fraction: float | None = None


class PolymarketPhase10Status(BaseModel):
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
    top_concentrated_exposures: list[PolymarketPhase10ExposureBucket]
    recent_block_reason_counts_24h: dict[str, int]
    maker_budget_used_usd: float | None = None
    maker_budget_usd: float | None = None
    taker_budget_used_usd: float | None = None
    taker_budget_usd: float | None = None
    maker_budget_utilization: float
    taker_budget_utilization: float


class PolymarketPhase11Status(BaseModel):
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


class PolymarketPhase12Status(BaseModel):
    pilot_enabled: bool
    live_trading_enabled: bool
    pilot_armed: bool
    pilot_paused: bool
    active_pilot_family: str | None = None
    manual_approval_required: bool
    approval_queue_count: int
    heartbeat_status: str
    user_stream_connected: bool
    recent_incident_count_24h: int
    live_shadow_summary: dict[str, Any]
    daily_realized_pnl: dict[str, Any]
    approval_expired_count_24h: int
    recent_guardrail_triggers: list[dict[str, Any]]
    latest_readiness_status: str | None = None
    latest_readiness_generated_at: datetime | None = None
    last_reconcile_success_at: datetime | None = None
    kill_switch_enabled: bool


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
    polymarket_phase7a: PolymarketPhase7AStatus
    polymarket_phase8a: PolymarketPhase8AStatus
    polymarket_phase9: PolymarketPhase9Status
    polymarket_phase10: PolymarketPhase10Status
    polymarket_phase11: PolymarketPhase11Status
    polymarket_phase12: PolymarketPhase12Status


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
    polymarket_phase7a = await fetch_polymarket_live_status(db)
    polymarket_phase8a = await fetch_market_structure_status(db)
    polymarket_phase9 = await fetch_polymarket_maker_status(db)
    polymarket_phase10 = await fetch_polymarket_risk_graph_status(db)
    polymarket_phase11 = await fetch_polymarket_replay_status(db)
    polymarket_phase12_pilot = await fetch_pilot_status(db)
    polymarket_phase12_evidence = await _pilot_evidence.fetch_pilot_evidence_summary(db)

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
        polymarket_phase7a=PolymarketPhase7AStatus(**{
            key: polymarket_phase7a[key]
            for key in (
                "enabled",
                "dry_run",
                "manual_approval_required",
                "gateway_reachable",
                "user_stream_connected",
                "kill_switch_enabled",
                "outstanding_live_orders",
                "outstanding_reservations",
                "recent_fills_24h",
                "last_reconcile_success_at",
                "last_user_stream_message_at",
            )
        }),
        polymarket_phase8a=PolymarketPhase8AStatus(**{
            key: polymarket_phase8a[key]
            for key in (
                "enabled",
                "on_startup",
                "interval_seconds",
                "min_net_edge_bps",
                "require_executable_all_legs",
                "include_cross_venue",
                "allow_augmented_neg_risk",
                "max_groups_per_run",
                "cross_venue_max_staleness_seconds",
                "max_leg_slippage_bps",
                "run_lock_enabled",
                "retention_days",
                "validation_enabled",
                "paper_routing_enabled",
                "paper_require_manual_approval",
                "max_notional_per_plan",
                "min_depth_per_leg",
                "plan_max_age_seconds",
                "link_review_required",
                "last_successful_group_build_at",
                "last_successful_scan_at",
                "last_successful_validation_at",
                "last_successful_paper_plan_at",
                "last_successful_paper_route_at",
                "last_successful_retention_prune_at",
                "last_group_build_status",
                "last_group_build_started_at",
                "last_group_build_duration_seconds",
                "last_scan_status",
                "last_scan_started_at",
                "last_scan_duration_seconds",
                "last_validation_status",
                "last_validation_started_at",
                "last_validation_duration_seconds",
                "last_paper_plan_status",
                "last_paper_plan_started_at",
                "last_paper_plan_duration_seconds",
                "last_paper_route_status",
                "last_paper_route_started_at",
                "last_paper_route_duration_seconds",
                "last_retention_prune_status",
                "last_retention_prune_started_at",
                "last_retention_prune_duration_seconds",
                "recent_actionable_by_type",
                "recent_non_executable_count",
                "informational_augmented_group_count",
                "active_group_counts",
                "active_cross_venue_link_count",
                "informational_only_opportunity_count",
                "blocked_opportunity_count",
                "executable_candidate_count",
                "opportunity_counts_by_type",
                "validation_reason_counts",
                "stale_cross_venue_link_count",
                "skipped_group_count",
                "pending_approval_count",
            )
        }),
        polymarket_phase9=PolymarketPhase9Status(**{
            key: polymarket_phase9[key]
            for key in (
                "enabled",
                "fee_history_enabled",
                "reward_history_enabled",
                "quote_optimizer_enabled",
                "quote_optimizer_max_notional",
                "quote_optimizer_max_age_seconds",
                "quote_optimizer_require_rewards_data",
                "quote_optimizer_require_fee_data",
                "last_fee_sync_at",
                "last_reward_sync_at",
                "last_snapshot_at",
                "last_recommendation_at",
                "fee_history_rows",
                "reward_history_rows",
                "economics_snapshot_rows",
                "quote_recommendation_rows",
                "reward_state_counts",
                "recent_reason_counts_24h",
                "fee_freshness_seconds",
                "reward_freshness_seconds",
            )
        }),
        polymarket_phase10=PolymarketPhase10Status(**{
            key: polymarket_phase10[key]
            for key in (
                "enabled",
                "on_startup",
                "interval_seconds",
                "portfolio_optimizer_enabled",
                "portfolio_optimizer_interval_seconds",
                "advisory_only",
                "live_disabled_by_default",
                "last_successful_graph_build_at",
                "last_successful_exposure_snapshot_at",
                "last_successful_optimizer_run_at",
                "last_graph_build_status",
                "last_exposure_snapshot_status",
                "last_optimizer_status",
                "top_concentrated_exposures",
                "recent_block_reason_counts_24h",
                "maker_budget_used_usd",
                "maker_budget_usd",
                "taker_budget_used_usd",
                "taker_budget_usd",
                "maker_budget_utilization",
                "taker_budget_utilization",
            )
        }),
        polymarket_phase11=PolymarketPhase11Status(**{
            key: polymarket_phase11[key]
            for key in (
                "enabled",
                "on_startup",
                "interval_seconds",
                "default_window_minutes",
                "max_scenarios_per_run",
                "structure_enabled",
                "maker_enabled",
                "risk_adjustments_enabled",
                "require_complete_book_coverage",
                "passive_fill_timeout_seconds",
                "advisory_only",
                "live_disabled_by_default",
                "last_replay_run",
                "last_successful_policy_comparison",
                "recent_scenario_count_24h",
                "recent_coverage_limited_run_count_24h",
                "recent_failed_run_count_24h",
                "recent_variant_summary",
            )
        }),
        polymarket_phase12=PolymarketPhase12Status(
            pilot_enabled=polymarket_phase12_pilot["pilot_enabled"],
            live_trading_enabled=polymarket_phase7a["enabled"],
            pilot_armed=bool(polymarket_phase12_pilot["active_pilot"] and polymarket_phase12_pilot["active_pilot"]["armed"]),
            pilot_paused=bool(polymarket_phase12_pilot["active_run"] and polymarket_phase12_pilot["active_run"]["status"] == "paused"),
            active_pilot_family=polymarket_phase12_pilot["active_pilot"]["strategy_family"] if polymarket_phase12_pilot["active_pilot"] is not None else None,
            manual_approval_required=polymarket_phase12_pilot["manual_approval_required"],
            approval_queue_count=polymarket_phase12_pilot["approval_queue_count"],
            heartbeat_status=polymarket_phase12_pilot["heartbeat_status"],
            user_stream_connected=polymarket_phase7a["user_stream_connected"],
            recent_incident_count_24h=polymarket_phase12_pilot["recent_incident_count_24h"],
            live_shadow_summary=polymarket_phase12_evidence["live_shadow_summary"],
            daily_realized_pnl=polymarket_phase12_evidence["daily_realized_pnl"],
            approval_expired_count_24h=polymarket_phase12_evidence["approval_expired_count_24h"],
            recent_guardrail_triggers=polymarket_phase12_evidence["recent_guardrail_triggers"],
            latest_readiness_status=(
                polymarket_phase12_evidence["latest_readiness_report"]["status"]
                if polymarket_phase12_evidence["latest_readiness_report"] is not None
                else None
            ),
            latest_readiness_generated_at=(
                polymarket_phase12_evidence["latest_readiness_report"]["generated_at"]
                if polymarket_phase12_evidence["latest_readiness_report"] is not None
                else None
            ),
            last_reconcile_success_at=polymarket_phase7a["last_reconcile_success_at"],
            kill_switch_enabled=polymarket_phase12_pilot["kill_switch_enabled"],
        ),
    )
