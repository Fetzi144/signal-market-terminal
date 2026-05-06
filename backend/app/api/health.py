"""Health and observability endpoint."""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.execution.polymarket_autonomy_state import (
    get_latest_demotion_event_by_version,
    summarize_autonomy_state,
)
from app.execution.polymarket_control_plane import fetch_pilot_status
from app.execution.polymarket_live_state import fetch_polymarket_live_status
from app.execution.polymarket_pilot_evidence import PolymarketPilotEvidenceService
from app.ingestion.polymarket_book_reconstruction import fetch_polymarket_book_recon_status
from app.ingestion.polymarket_execution_policy import fetch_polymarket_execution_policy_status
from app.ingestion.polymarket_maker_economics import fetch_polymarket_maker_status
from app.ingestion.polymarket_metadata import fetch_polymarket_meta_sync_status
from app.ingestion.polymarket_microstructure import fetch_polymarket_feature_status
from app.ingestion.polymarket_raw_storage import fetch_polymarket_raw_storage_status
from app.ingestion.polymarket_replay_simulator import fetch_polymarket_replay_status
from app.ingestion.polymarket_risk_graph import fetch_polymarket_risk_graph_status
from app.ingestion.polymarket_stream import fetch_polymarket_stream_status
from app.ingestion.structure_engine import fetch_market_structure_status
from app.models.ingestion import IngestionRun
from app.models.market import Market
from app.models.scheduler_lease import SchedulerLease
from app.models.signal import Signal, SignalEvaluation
from app.models.strategy_registry import StrategyFamilyRegistry, StrategyVersion
from app.paper_trading.analysis import get_overdue_open_trade_count
from app.risk.budgets import build_family_budget_summaries
from app.strategies.registry import (
    get_latest_promotion_evaluation_by_version,
    serialize_strategy_version_snapshot,
)
from app.strategy_families import build_strategy_family_reviews

router = APIRouter(prefix="/api/v1", tags=["health"])
_pilot_evidence = PolymarketPilotEvidenceService()
DEFAULT_SCHEDULER_LEASE_NAME = "default"
MAX_HEALTH_PRICE_CHANGE_PCT = Decimal("9999.9999")


class IngestionStatus(BaseModel):
    run_type: str
    last_status: str | None
    last_run: datetime | None
    markets_processed: int | None


class PolymarketPhase1Status(BaseModel):
    enabled: bool
    connected: bool
    continuity_status: str
    connection_started_at: datetime | None = None
    current_connection_id: str | None = None
    last_event_received_at: datetime | None = None
    heartbeat_freshness_seconds: int | None = None
    watched_asset_count: int
    subscribed_asset_count: int
    reconnect_count: int
    resync_count: int
    gap_suspected_count: int
    malformed_message_count: int
    last_successful_resync_at: datetime | None = None


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
    book_snapshot_freshness_seconds: int | None = None
    trade_backfill_freshness_seconds: int | None = None
    oi_poll_freshness_seconds: int | None = None
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
    stale_asset_count: int
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
    coverage_mode: str
    configured_supported_detectors: list[str]
    supported_detectors: list[str]
    unsupported_detectors: list[str]
    recent_variant_summary: dict[str, dict[str, Any]]


class PolymarketPhase12Status(BaseModel):
    pilot_enabled: bool
    live_trading_enabled: bool
    pilot_armed: bool
    pilot_paused: bool
    active_pilot_family: str | None = None
    strategy_version: dict[str, Any] | None = None
    latest_promotion_evaluation: dict[str, Any] | None = None
    autonomy_state: dict[str, Any] | None = None
    live_submission_gate: dict[str, Any] | None = None
    manual_approval_required: bool
    approval_queue_count: int
    heartbeat_status: str
    user_stream_connected: bool
    recent_incident_count_24h: int
    recent_incidents: list[dict[str, Any]]
    live_shadow_summary: dict[str, Any]
    daily_realized_pnl: dict[str, Any]
    approval_expired_count_24h: int
    recent_guardrail_triggers: list[dict[str, Any]]
    latest_readiness_status: str | None = None
    latest_readiness_generated_at: datetime | None = None
    last_reconcile_success_at: datetime | None = None
    kill_switch_enabled: bool


class StrategyFamilyReviewOut(BaseModel):
    family: str
    label: str
    posture: str
    configured: bool
    review_enabled: bool
    primary_surface: str
    description: str
    disabled_reason: str | None = None
    current_version: dict[str, Any] | None = None
    risk_budget_policy: dict[str, Any] | None = None
    risk_budget_status: dict[str, Any] | None = None
    autonomy_state: dict[str, Any] | None = None


class SchedulerLeaseStatus(BaseModel):
    owner_token: str | None = None
    heartbeat_freshness_seconds: int | None = None
    expires_in_seconds: int | None = None


class DefaultStrategyRuntimeStatus(BaseModel):
    overdue_open_trades: int
    last_resolution_backfill_at: datetime | None = None
    last_resolution_backfill_count: int
    evaluation_clamp_count_24h: int
    last_evaluation_failure_at: datetime | None = None


class RuntimeInvariantStatus(BaseModel):
    key: str
    label: str
    status: str
    detail: str


class HealthOut(BaseModel):
    status: str
    now: datetime
    active_markets: int
    total_signals: int
    unresolved_signals: int
    recent_alerts_24h: int
    alert_threshold: float
    ingestion: list[IngestionStatus]
    polymarket_phase1: PolymarketPhase1Status
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
    scheduler_lease: SchedulerLeaseStatus
    default_strategy_runtime: DefaultStrategyRuntimeStatus
    runtime_invariants: list[RuntimeInvariantStatus]
    strategy_families: list[StrategyFamilyReviewOut]


class HealthSummaryOut(BaseModel):
    status: str
    now: datetime
    active_markets: int
    recent_alerts_24h: int
    ingestion: list[IngestionStatus]
    scheduler_lease: SchedulerLeaseStatus


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_seconds(*, now: datetime, value: datetime | None) -> int | None:
    value_utc = _ensure_utc(value)
    if value_utc is None:
        return None
    return max(0, int((now - value_utc).total_seconds()))


def _expires_in_seconds(*, now: datetime, value: datetime | None) -> int | None:
    value_utc = _ensure_utc(value)
    if value_utc is None:
        return None
    return max(0, int((value_utc - now).total_seconds()))


async def _estimated_postgres_table_count(db: AsyncSession, table_name: str) -> int | None:
    bind = db.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return None
    value = (
        await db.execute(
            text(
                """
                SELECT GREATEST(COALESCE(c.reltuples, 0), 0)::bigint
                FROM pg_class c
                WHERE c.oid = to_regclass(:table_name)
                """
            ),
            {"table_name": table_name},
        )
    ).scalar()
    return int(value) if value is not None else None


async def _estimated_postgres_query_count(
    db: AsyncSession,
    sql: str,
    params: dict[str, Any] | None = None,
) -> int | None:
    bind = db.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return None
    raw_plan = (
        await db.execute(
            text(f"EXPLAIN (FORMAT JSON) {sql}"),
            params or {},
        )
    ).scalar()
    if raw_plan is None:
        return None
    if isinstance(raw_plan, str):
        raw_plan = json.loads(raw_plan)
    plan_root = raw_plan[0] if isinstance(raw_plan, list) and raw_plan else raw_plan
    plan = plan_root.get("Plan") if isinstance(plan_root, dict) else None
    if not isinstance(plan, dict) or plan.get("Plan Rows") is None:
        return None
    return max(0, int(plan["Plan Rows"]))


def _build_runtime_invariants(
    *,
    now: datetime,
    scheduler_lease: SchedulerLeaseStatus,
    runtime_status: DefaultStrategyRuntimeStatus,
) -> list[RuntimeInvariantStatus]:
    invariants: list[RuntimeInvariantStatus] = []
    lease_heartbeat_threshold = max(1, settings.scheduler_lease_renew_interval_seconds * 2)
    lease_observed = any(
        value is not None
        for value in (
            scheduler_lease.owner_token,
            scheduler_lease.heartbeat_freshness_seconds,
            scheduler_lease.expires_in_seconds,
        )
    )

    if not settings.scheduler_enabled and not lease_observed:
        invariants.append(RuntimeInvariantStatus(
            key="scheduler_lease_fresh",
            label="Scheduler Lease Fresh",
            status="not_applicable",
            detail="Scheduler is disabled in this environment.",
        ))
    else:
        owner_token = scheduler_lease.owner_token
        heartbeat_freshness = scheduler_lease.heartbeat_freshness_seconds
        expires_in = scheduler_lease.expires_in_seconds

        if not owner_token:
            lease_status = "failing"
            lease_detail = "Scheduler is enabled but no owner token is registered."
        elif expires_in is None or expires_in <= 0:
            lease_status = "failing"
            lease_detail = (
                f"Lease owner {owner_token} is expired "
                f"(heartbeat {heartbeat_freshness if heartbeat_freshness is not None else '?'}s ago)."
            )
        elif heartbeat_freshness is None or heartbeat_freshness > lease_heartbeat_threshold:
            lease_status = "failing"
            lease_detail = (
                f"Lease owner {owner_token} heartbeat is stale at "
                f"{heartbeat_freshness if heartbeat_freshness is not None else '?'}s "
                f"(threshold {lease_heartbeat_threshold}s)."
            )
        else:
            lease_status = "passing"
            lease_detail = (
                f"Owner {owner_token} heartbeat {heartbeat_freshness}s ago, "
                f"expires in {expires_in}s."
            )

        invariants.append(RuntimeInvariantStatus(
            key="scheduler_lease_fresh",
            label="Scheduler Lease Fresh",
            status=lease_status,
            detail=lease_detail,
        ))

    overdue_trades = runtime_status.overdue_open_trades
    invariants.append(RuntimeInvariantStatus(
        key="overdue_open_trades_zero",
        label="Overdue Open Trades",
        status="passing" if overdue_trades == 0 else "failing",
        detail=(
            "No overdue open trades remain past market end."
            if overdue_trades == 0
            else f"{overdue_trades} overdue open trade(s) remain past market end."
        ),
    ))

    last_evaluation_failure = _ensure_utc(runtime_status.last_evaluation_failure_at)
    evaluation_failure_recent = (
        last_evaluation_failure is not None
        and last_evaluation_failure >= now - timedelta(hours=24)
    )
    invariants.append(RuntimeInvariantStatus(
        key="evaluation_failures_24h_zero",
        label="Evaluation Failures (24h)",
        status="failing" if evaluation_failure_recent else "passing",
        detail=(
            f"Latest evaluation failure at {last_evaluation_failure.isoformat()}."
            if evaluation_failure_recent and last_evaluation_failure is not None
            else "No evaluation failures recorded in the last 24 hours."
        ),
    ))

    return invariants


async def _latest_ingestion_run(
    db: AsyncSession,
    *,
    run_type: str,
    status: str | None = None,
) -> IngestionRun | None:
    query = select(IngestionRun).where(IngestionRun.run_type == run_type)
    if status is not None:
        query = query.where(IngestionRun.status == status)
    query = query.order_by(IngestionRun.started_at.desc(), IngestionRun.finished_at.desc())
    result = await db.execute(query.limit(1))
    return result.scalar_one_or_none()


def _build_scheduler_lease_status(
    *,
    now: datetime,
    scheduler_lease: SchedulerLease | None,
) -> SchedulerLeaseStatus:
    return SchedulerLeaseStatus(
        owner_token=scheduler_lease.owner_token if scheduler_lease is not None else None,
        heartbeat_freshness_seconds=_age_seconds(
            now=now,
            value=scheduler_lease.heartbeat_at if scheduler_lease is not None else None,
        ),
        expires_in_seconds=_expires_in_seconds(
            now=now,
            value=scheduler_lease.expires_at if scheduler_lease is not None else None,
        ),
    )


@router.get("/health/summary", response_model=HealthSummaryOut)
async def health_summary(db: AsyncSession = Depends(get_db)):
    now = datetime.now(timezone.utc)

    active_markets = await _estimated_postgres_query_count(
        db,
        "SELECT 1 FROM markets WHERE active IS TRUE",
    )
    if active_markets is None:
        active_markets = (
            await db.execute(select(func.count(Market.id)).where(Market.active.is_(True)))
        ).scalar() or 0

    threshold = Decimal(str(settings.alert_rank_threshold))
    recent_alerts = await _estimated_postgres_query_count(
        db,
        "SELECT 1 FROM signals WHERE rank_score >= :threshold AND fired_at >= :since",
        {"threshold": threshold, "since": now - timedelta(hours=24)},
    )
    if recent_alerts is None:
        recent_alerts = (
            await db.execute(
                select(func.count(Signal.id)).where(
                    Signal.rank_score >= threshold,
                    Signal.fired_at >= now - timedelta(hours=24),
                )
            )
        ).scalar() or 0

    ingestion_statuses: list[IngestionStatus] = []
    ingestion_failures = False
    ingestion_stale = False
    freshness_thresholds = {
        "market_discovery": max(settings.market_discovery_interval_seconds * 3, 900),
        "snapshot": max(settings.snapshot_interval_seconds * 3, 600),
    }
    for run_type in ("market_discovery", "snapshot"):
        result = await db.execute(
            select(IngestionRun)
            .where(IngestionRun.run_type == run_type)
            .order_by(IngestionRun.started_at.desc())
            .limit(1)
        )
        run = result.scalar_one_or_none()
        latest_success_result = await db.execute(
            select(IngestionRun)
            .where(
                IngestionRun.run_type == run_type,
                IngestionRun.status == "success",
            )
            .order_by(IngestionRun.started_at.desc())
            .limit(1)
        )
        latest_success = latest_success_result.scalar_one_or_none()
        last_run = run.started_at if run else None
        ingestion_statuses.append(
            IngestionStatus(
                run_type=run_type,
                last_status=run.status if run else None,
                last_run=last_run,
                markets_processed=run.markets_processed if run else None,
            )
        )
        if run is None:
            ingestion_failures = True
            continue
        if run.status == "running" and latest_success is not None:
            freshness_source = latest_success
        else:
            if run.status != "success":
                ingestion_failures = True
                continue
            freshness_source = run
        run_age_seconds = _age_seconds(now=now, value=freshness_source.started_at)
        threshold_seconds = freshness_thresholds[run_type]
        if run_age_seconds is None or run_age_seconds > threshold_seconds:
            ingestion_stale = True

    scheduler_lease = await db.get(SchedulerLease, DEFAULT_SCHEDULER_LEASE_NAME)
    scheduler_lease_status = _build_scheduler_lease_status(
        now=now,
        scheduler_lease=scheduler_lease,
    )
    lease_heartbeat_threshold = max(1, settings.scheduler_lease_renew_interval_seconds * 2)
    lease_observed = any(
        value is not None
        for value in (
            scheduler_lease_status.owner_token,
            scheduler_lease_status.heartbeat_freshness_seconds,
            scheduler_lease_status.expires_in_seconds,
        )
    )
    if not settings.scheduler_enabled and not lease_observed:
        lease_failing = False
    else:
        lease_failing = (
            not scheduler_lease_status.owner_token
            or scheduler_lease_status.expires_in_seconds is None
            or scheduler_lease_status.expires_in_seconds <= 0
            or scheduler_lease_status.heartbeat_freshness_seconds is None
            or scheduler_lease_status.heartbeat_freshness_seconds > lease_heartbeat_threshold
        )

    overall_status = "ok"
    if active_markets == 0 or ingestion_failures or ingestion_stale or lease_failing:
        overall_status = "degraded"

    return HealthSummaryOut(
        status=overall_status,
        now=now,
        active_markets=active_markets,
        recent_alerts_24h=recent_alerts,
        ingestion=ingestion_statuses,
        scheduler_lease=scheduler_lease_status,
    )


@router.get("/health", response_model=HealthOut)
async def health(db: AsyncSession = Depends(get_db)):
    now = datetime.now(timezone.utc)

    active_markets = await _estimated_postgres_query_count(
        db,
        "SELECT 1 FROM markets WHERE active IS TRUE",
    )
    if active_markets is None:
        active_markets = (await db.execute(
            select(func.count(Market.id)).where(Market.active.is_(True))
        )).scalar() or 0

    total_signals = await _estimated_postgres_table_count(db, "signals")
    if total_signals is None:
        total_signals = (await db.execute(select(func.count(Signal.id)))).scalar() or 0
    unresolved = await _estimated_postgres_query_count(
        db,
        "SELECT 1 FROM signals WHERE resolved IS FALSE",
    )
    if unresolved is None:
        unresolved = (await db.execute(
            select(func.count(Signal.id)).where(Signal.resolved.is_(False))
        )).scalar() or 0

    # Count high-rank signals in last 24h (alerts)
    threshold = Decimal(str(settings.alert_rank_threshold))
    recent_alerts = await _estimated_postgres_query_count(
        db,
        "SELECT 1 FROM signals WHERE rank_score >= :threshold AND fired_at >= :since",
        {"threshold": threshold, "since": now - timedelta(hours=24)},
    )
    if recent_alerts is None:
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
    polymarket_phase1 = await fetch_polymarket_stream_status(
        db,
        refresh_watch_registry=False,
        include_details=False,
    )
    polymarket_phase3 = await fetch_polymarket_raw_storage_status(db, include_recent_rows=False)
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
    scheduler_lease = await db.get(SchedulerLease, DEFAULT_SCHEDULER_LEASE_NAME)
    overdue_open_trades = await get_overdue_open_trade_count(db)
    latest_backfill_run = await _latest_ingestion_run(db, run_type="resolution_backfill")
    latest_evaluation_failure = await _latest_ingestion_run(
        db,
        run_type="evaluation",
        status="error",
    )
    evaluation_clamp_count = int(
        (
            await db.execute(
                select(func.count(SignalEvaluation.id)).where(
                    SignalEvaluation.evaluated_at >= now - timedelta(hours=24),
                    or_(
                        SignalEvaluation.price_change_pct == MAX_HEALTH_PRICE_CHANGE_PCT,
                        SignalEvaluation.price_change_pct == -MAX_HEALTH_PRICE_CHANGE_PCT,
                    ),
                )
            )
        ).scalar()
        or 0
    )
    strategy_family_reviews = build_strategy_family_reviews()
    budget_summary_by_family = {
        row["family"]: row
        for row in await build_family_budget_summaries(db)
    }
    family_names = [str(row["family"]).strip().lower() for row in strategy_family_reviews]
    current_version_rows = (
        await db.execute(
            select(StrategyVersion, StrategyFamilyRegistry)
            .join(StrategyFamilyRegistry, StrategyFamilyRegistry.id == StrategyVersion.family_id)
            .where(
                StrategyVersion.is_current.is_(True),
                StrategyFamilyRegistry.family.in_(tuple(family_names)),
            )
            .order_by(StrategyFamilyRegistry.family.asc())
        )
    ).all()
    version_rows_by_family = {
        family_row.family: version_row
        for version_row, family_row in current_version_rows
        if version_row.id is not None
    }
    latest_supporting_evaluation_by_version = await get_latest_promotion_evaluation_by_version(
        db,
        version_ids=[int(row.id) for row in version_rows_by_family.values() if row.id is not None],
        include_supporting=True,
    )
    latest_demotion_by_version = await get_latest_demotion_event_by_version(
        db,
        version_ids=[int(row.id) for row in version_rows_by_family.values() if row.id is not None],
    )
    strategy_families = [
        (
            lambda family_review, version_row, budget_row, risk_budget_status: {
                **family_review,
                "current_version": (
                    {
                        **(budget_row.get("current_version") or {}),
                        **(
                            {
                                "version_status": version_row.version_status,
                                "autonomy_tier": version_row.autonomy_tier,
                                "is_current": version_row.is_current,
                                "is_frozen": version_row.is_frozen,
                                "created_at": serialize_strategy_version_snapshot(version_row).get("created_at"),
                                "updated_at": serialize_strategy_version_snapshot(version_row).get("updated_at"),
                            }
                            if version_row is not None
                            else {}
                        ),
                        "autonomy_state": (
                            summarize_autonomy_state(
                                strategy_family=family_review["family"],
                                family_source="current_registry_version" if version_row is not None else "unresolved",
                                strategy_version=serialize_strategy_version_snapshot(version_row) if version_row is not None else None,
                                strategy_version_source="current_registry_version" if version_row is not None else "unresolved",
                                latest_promotion_evaluation=(
                                    latest_supporting_evaluation_by_version.get(int(version_row.id))
                                    if version_row is not None and version_row.id is not None
                                    else None
                                ),
                                latest_demotion_event=(
                                    latest_demotion_by_version.get(int(version_row.id))
                                    if version_row is not None and version_row.id is not None
                                    else None
                                ),
                                risk_budget_status=risk_budget_status,
                                posture=family_review.get("posture"),
                            )
                            if version_row is not None
                            else None
                        ),
                    }
                    if budget_row.get("current_version") is not None or version_row is not None
                    else None
                ),
                "risk_budget_policy": budget_row.get("risk_budget_policy"),
                "risk_budget_status": risk_budget_status,
                "autonomy_state": (
                    summarize_autonomy_state(
                        strategy_family=family_review["family"],
                        family_source="current_registry_version" if version_row is not None else "unresolved",
                        strategy_version=serialize_strategy_version_snapshot(version_row) if version_row is not None else None,
                        strategy_version_source="current_registry_version" if version_row is not None else "unresolved",
                        latest_promotion_evaluation=(
                            latest_supporting_evaluation_by_version.get(int(version_row.id))
                            if version_row is not None and version_row.id is not None
                            else None
                        ),
                        latest_demotion_event=(
                            latest_demotion_by_version.get(int(version_row.id))
                            if version_row is not None and version_row.id is not None
                            else None
                        ),
                        risk_budget_status=risk_budget_status,
                        posture=family_review.get("posture"),
                    )
                    if version_row is not None
                    else None
                ),
            }
        )(
            row,
            version_rows_by_family.get(row["family"]),
            budget_summary_by_family.get(row["family"], {}),
            budget_summary_by_family.get(row["family"], {}).get("risk_budget_status"),
        )
        for row in strategy_family_reviews
    ]
    scheduler_lease_status = _build_scheduler_lease_status(
        now=now,
        scheduler_lease=scheduler_lease,
    )
    default_strategy_runtime = DefaultStrategyRuntimeStatus(
        overdue_open_trades=overdue_open_trades,
        last_resolution_backfill_at=(
            latest_backfill_run.finished_at or latest_backfill_run.started_at
            if latest_backfill_run is not None
            else None
        ),
        last_resolution_backfill_count=(
            latest_backfill_run.markets_processed
            if latest_backfill_run is not None
            else 0
        ),
        evaluation_clamp_count_24h=evaluation_clamp_count,
        last_evaluation_failure_at=(
            latest_evaluation_failure.finished_at or latest_evaluation_failure.started_at
            if latest_evaluation_failure is not None
            else None
        ),
    )
    runtime_invariants = _build_runtime_invariants(
        now=now,
        scheduler_lease=scheduler_lease_status,
        runtime_status=default_strategy_runtime,
    )
    overall_status = "degraded" if any(row.status == "failing" for row in runtime_invariants) else "ok"

    return HealthOut(
        status=overall_status,
        now=now,
        active_markets=active_markets,
        total_signals=total_signals,
        unresolved_signals=unresolved,
        recent_alerts_24h=recent_alerts,
        alert_threshold=settings.alert_rank_threshold,
        ingestion=ingestion_statuses,
        polymarket_phase1=PolymarketPhase1Status(**{
            "enabled": polymarket_phase1["enabled"],
            "connected": polymarket_phase1["connected"],
            "continuity_status": polymarket_phase1["continuity_status"],
            "connection_started_at": polymarket_phase1["connection_started_at"],
            "current_connection_id": (
                str(polymarket_phase1["current_connection_id"])
                if polymarket_phase1["current_connection_id"] is not None
                else None
            ),
            "last_event_received_at": polymarket_phase1["last_event_received_at"],
            "heartbeat_freshness_seconds": polymarket_phase1["heartbeat_freshness_seconds"],
            "watched_asset_count": polymarket_phase1["watched_asset_count"],
            "subscribed_asset_count": polymarket_phase1["subscribed_asset_count"],
            "reconnect_count": polymarket_phase1["reconnect_count"],
            "resync_count": polymarket_phase1["resync_count"],
            "gap_suspected_count": polymarket_phase1["gap_suspected_count"],
            "malformed_message_count": polymarket_phase1["malformed_message_count"],
            "last_successful_resync_at": polymarket_phase1["last_successful_resync_at"],
        }),
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
                "book_snapshot_freshness_seconds",
                "trade_backfill_freshness_seconds",
                "oi_poll_freshness_seconds",
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
                "stale_asset_count",
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
                "coverage_mode",
                "configured_supported_detectors",
                "supported_detectors",
                "unsupported_detectors",
                "recent_variant_summary",
            )
        }),
        polymarket_phase12=PolymarketPhase12Status(
            pilot_enabled=polymarket_phase12_pilot["pilot_enabled"],
            live_trading_enabled=polymarket_phase7a["enabled"],
            pilot_armed=bool(polymarket_phase12_pilot["active_pilot"] and polymarket_phase12_pilot["active_pilot"]["armed"]),
            pilot_paused=bool(polymarket_phase12_pilot["active_run"] and polymarket_phase12_pilot["active_run"]["status"] == "paused"),
            active_pilot_family=polymarket_phase12_pilot["active_pilot"]["strategy_family"] if polymarket_phase12_pilot["active_pilot"] is not None else None,
            strategy_version=polymarket_phase12_pilot["active_strategy_version"] or polymarket_phase12_evidence["strategy_version"],
            latest_promotion_evaluation=(
                polymarket_phase12_pilot["latest_promotion_evaluation"] or polymarket_phase12_evidence["latest_promotion_evaluation"]
            ),
            autonomy_state=polymarket_phase12_pilot.get("active_autonomy_state"),
            live_submission_gate=polymarket_phase12_pilot.get("live_submission_gate"),
            manual_approval_required=polymarket_phase12_pilot["manual_approval_required"],
            approval_queue_count=polymarket_phase12_pilot["approval_queue_count"],
            heartbeat_status=polymarket_phase12_pilot["heartbeat_status"],
            user_stream_connected=polymarket_phase7a["user_stream_connected"],
            recent_incident_count_24h=polymarket_phase12_pilot["recent_incident_count_24h"],
            recent_incidents=polymarket_phase12_pilot["recent_incidents"],
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
        scheduler_lease=scheduler_lease_status,
        default_strategy_runtime=default_strategy_runtime,
        runtime_invariants=runtime_invariants,
        strategy_families=[StrategyFamilyReviewOut(**row) for row in strategy_families],
    )
