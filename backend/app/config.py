from datetime import datetime, timezone

from pydantic import field_validator
from pydantic_settings import BaseSettings


def _parse_positive_int_csv(value: str) -> list[int]:
    parsed: list[int] = []
    for chunk in value.split(","):
        stripped = chunk.strip()
        if not stripped:
            continue
        parsed.append(int(stripped))
    if not parsed:
        raise ValueError("At least one positive integer is required")
    if any(item < 1 for item in parsed):
        raise ValueError("Values must be >= 1")
    return parsed


def _parse_string_csv(value: str) -> list[str]:
    parsed: list[str] = []
    for chunk in value.split(","):
        stripped = chunk.strip()
        if stripped:
            parsed.append(stripped)
    return parsed


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://smt:smt@localhost:5432/smt"

    # Polymarket
    polymarket_api_base: str = "https://clob.polymarket.com"
    polymarket_gamma_base: str = "https://gamma-api.polymarket.com"
    polymarket_data_api_base: str = "https://data-api.polymarket.com"
    polymarket_stream_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Kalshi
    kalshi_api_base: str = "https://api.elections.kalshi.com/trade-api/v2"
    kalshi_enabled: bool = True

    # Connector
    connector_timeout_seconds: float = 30.0

    # Ingestion
    snapshot_interval_seconds: int = 120
    market_discovery_interval_seconds: int = 300
    min_volume_24h: float = 500.0
    market_pagination_cap: int = 100000
    orderbook_sample_size: int = 50
    polymarket_stream_enabled: bool = False
    polymarket_resync_on_startup: bool = True
    polymarket_stream_reconnect_base_seconds: float = 1.0
    polymarket_stream_reconnect_max_seconds: float = 30.0
    polymarket_stream_ping_interval_seconds: int = 10
    polymarket_watch_reconcile_interval_seconds: int = 30
    polymarket_gap_suspect_after_seconds: int = 30
    polymarket_malformed_burst_threshold: int = 5
    polymarket_malformed_burst_window_seconds: int = 30
    polymarket_normalization_enabled: bool = True
    polymarket_watch_bootstrap_from_active_universe: bool = True
    polymarket_meta_sync_enabled: bool = False
    polymarket_meta_sync_on_startup: bool = True
    polymarket_meta_sync_interval_seconds: int = 900
    polymarket_meta_sync_include_closed: bool = False
    polymarket_meta_sync_page_size: int = 200
    polymarket_raw_storage_enabled: bool = False
    polymarket_book_snapshot_interval_seconds: int = 300
    polymarket_trade_backfill_enabled: bool = False
    polymarket_trade_backfill_on_startup: bool = True
    polymarket_trade_backfill_interval_seconds: int = 900
    polymarket_trade_backfill_lookback_hours: int = 24
    polymarket_trade_backfill_page_size: int = 200
    polymarket_oi_poll_enabled: bool = False
    polymarket_oi_poll_interval_seconds: int = 900
    polymarket_raw_retention_days: int = 14
    polymarket_book_recon_enabled: bool = False
    polymarket_book_recon_on_startup: bool = True
    polymarket_book_recon_auto_resync_enabled: bool = True
    polymarket_book_recon_stale_after_seconds: int = 900
    polymarket_book_recon_resync_cooldown_seconds: int = 60
    polymarket_book_recon_max_watched_assets: int = 500
    polymarket_book_recon_bbo_tolerance: float = 0.0
    polymarket_book_recon_bootstrap_lookback_hours: int = 48
    polymarket_features_enabled: bool = False
    polymarket_features_on_startup: bool = True
    polymarket_features_interval_seconds: int = 300
    polymarket_features_lookback_hours: int = 1
    polymarket_feature_buckets_ms: str = "100,1000"
    polymarket_label_horizons_ms: str = "250,1000,5000"
    polymarket_features_max_watched_assets: int = 50
    polymarket_execution_policy_enabled: bool = False
    polymarket_execution_policy_require_live_book: bool = True
    polymarket_execution_policy_default_horizon_ms: int = 1000
    polymarket_execution_policy_passive_lookback_hours: int = 24
    polymarket_execution_policy_passive_min_label_rows: int = 20
    polymarket_execution_policy_max_cross_slippage_bps: float = 150.0
    polymarket_execution_policy_step_ahead_enabled: bool = True
    polymarket_execution_policy_min_net_ev_bps: float = 0.0
    polymarket_structure_engine_enabled: bool = False
    polymarket_structure_on_startup: bool = True
    polymarket_structure_interval_seconds: int = 300
    polymarket_structure_min_net_edge_bps: float = 0.0
    polymarket_structure_require_executable_all_legs: bool = True
    polymarket_structure_include_cross_venue: bool = False
    polymarket_structure_allow_augmented_neg_risk: bool = False
    polymarket_structure_max_groups_per_run: int = 250
    polymarket_structure_cross_venue_max_staleness_seconds: int = 180
    polymarket_structure_max_leg_slippage_bps: float = 150.0
    polymarket_structure_run_lock_enabled: bool = True
    polymarket_structure_retention_days: int = 30
    polymarket_structure_validation_enabled: bool = True
    polymarket_structure_paper_routing_enabled: bool = False
    polymarket_structure_paper_require_manual_approval: bool = True
    polymarket_structure_max_notional_per_plan: float = 100.0
    polymarket_structure_min_depth_per_leg: float = 1.0
    polymarket_structure_plan_max_age_seconds: int = 180
    polymarket_structure_link_review_required: bool = False
    polymarket_live_trading_enabled: bool = False
    polymarket_live_dry_run: bool = True
    polymarket_live_manual_approval_required: bool = True
    polymarket_live_decision_max_age_seconds: int = 300
    polymarket_user_stream_enabled: bool = False
    polymarket_user_stream_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    polymarket_user_stream_reconnect_base_seconds: float = 1.0
    polymarket_user_stream_reconnect_max_seconds: float = 30.0
    polymarket_reconcile_interval_seconds: int = 60
    polymarket_kill_switch_enabled: bool = False
    polymarket_allowlist_markets: str = ""
    polymarket_allowlist_categories: str = ""
    polymarket_max_outstanding_notional_usd: float = 0.0
    polymarket_clob_host: str = "https://clob.polymarket.com"
    polymarket_chain_id: int = 137
    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""
    polymarket_api_passphrase: str = ""
    polymarket_private_key: str = ""
    polymarket_signature_type: int = 2
    polymarket_funder_address: str = ""

    # Multi-Timeframe Analysis
    # Timeframes per detector type (comma-separated). Default is single timeframe.
    price_move_timeframes: str = "30m,1h"     # e.g. "30m,1h,4h"
    volume_spike_timeframes: str = "1h,4h"   # e.g. "1h,4h,24h"
    ofi_timeframes: str = "15m,30m"          # e.g. "15m,30m,1h"

    # Signals — Price Move
    price_move_window_minutes: int = 30
    price_move_threshold_pct: float = 5.0

    # Signals — Volume Spike
    volume_spike_multiplier: float = 2.0
    volume_spike_baseline_hours: int = 24

    # Signals — Spread Change
    spread_change_baseline_hours: int = 24
    spread_change_threshold_ratio: float = 1.5

    # Signals — Liquidity Vacuum
    liquidity_vacuum_baseline_hours: int = 24
    liquidity_vacuum_depth_ratio_threshold: float = 0.4

    # Signals — Deadline Near
    deadline_near_hours: int = 24
    deadline_near_price_threshold_pct: float = 6.0

    # Signals — Order Flow Imbalance
    ofi_threshold: float = 0.3  # minimum abs(OFI) to fire
    ofi_enabled: bool = True
    ofi_min_snapshots: int = 2  # minimum orderbook snapshots needed
    ofi_price_flat_window_minutes: int = 30  # price must be flat in this window

    # Signals — Arbitrage
    arb_spread_threshold: float = 0.025  # minimum spread to fire (2.5 percentage points)
    arb_enabled: bool = True

    # Whale / Smart Money Tracking
    whale_tracking_enabled: bool = False  # off by default (requires Polygon RPC)
    whale_min_volume_usd: float = 100000  # minimum cumulative volume to auto-track
    whale_min_win_rate: float = 0.55  # minimum win rate to auto-track
    whale_signal_min_trade_usd: float = 5000  # minimum trade size to fire signal
    polygon_rpc_url: str = ""  # Polygon RPC endpoint
    whale_scan_interval_seconds: int = 300  # scan every 5 minutes

    # Trading Intelligence (Phase 3 Q2)
    default_bankroll: float = 10000.0
    kelly_multiplier: float = 0.25  # quarter-Kelly
    max_single_position_pct: float = 0.05  # 5% of bankroll per trade
    max_total_exposure_pct: float = 0.30  # 30% of bankroll total
    min_ev_threshold: float = 0.03  # $0.03 minimum EV to surface
    max_cluster_exposure_pct: float = 0.15  # 15% of bankroll per correlated cluster
    drawdown_circuit_breaker_pct: float = 0.15  # pause at -15% from peak
    paper_trading_enabled: bool = True
    shadow_execution_max_staleness_seconds: int = 180
    shadow_execution_max_forward_seconds: int = 30
    shadow_execution_min_fill_pct: float = 0.20
    scheduler_enabled: bool = False
    scheduler_lease_seconds: int = 45
    scheduler_lease_renew_interval_seconds: int = 15
    default_strategy_name: str = "prove_the_edge_default"
    default_strategy_signal_type: str = "confluence"
    default_strategy_start_at: datetime | None = datetime(2026, 4, 13, tzinfo=timezone.utc)
    default_strategy_min_observation_days: int = 14
    default_strategy_preferred_observation_days: int = 30
    strategy_review_lookback_days: int = 30
    strategy_review_recent_mistakes_limit: int = 5
    legacy_benchmark_rank_threshold: float = 0.55

    # Evaluation
    evaluation_interval_seconds: int = 300

    # Alerts
    alert_rank_threshold: float = 0.55
    alert_batch_limit: int = 20
    alert_webhook_url: str | None = None
    alert_telegram_bot_token: str | None = None
    alert_telegram_chat_id: str | None = None
    alert_signal_types: str | None = None  # Comma-separated, None = all types

    # Retention
    retention_price_snapshots_days: int = 30
    retention_orderbook_snapshots_days: int = 14
    retention_signals_days: int = 90

    # Scheduler
    cleanup_interval_hours: int = 6

    # API
    api_rate_limit: str = "60/minute"
    api_key: str | None = None  # Set to require X-API-Key header
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173,http://localhost:4173,http://127.0.0.1:4173"  # Comma-separated origins

    # SSE
    sse_max_connections: int = 50

    # Discord
    alert_discord_webhook_url: str = ""  # Discord webhook URL

    # Push Notifications (VAPID / Web Push)
    push_vapid_private_key: str = ""  # VAPID private key
    push_vapid_public_key: str = ""   # VAPID public key
    push_vapid_email: str = ""        # Contact email for VAPID

    # Webhook Security
    alert_webhook_secret: str = ""  # If set, HMAC-SHA256 sign webhook payloads

    # App
    log_level: str = "INFO"
    log_format: str = "text"  # "text" or "json"

    @field_validator("snapshot_interval_seconds", "market_discovery_interval_seconds", "evaluation_interval_seconds")
    @classmethod
    def intervals_must_be_at_least_30(cls, v: int) -> int:
        if v < 30:
            raise ValueError("Interval must be >= 30 seconds")
        return v

    @field_validator("retention_price_snapshots_days", "retention_orderbook_snapshots_days", "retention_signals_days")
    @classmethod
    def retention_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Retention must be >= 1 day")
        return v

    @field_validator("polymarket_raw_retention_days", "polymarket_structure_retention_days")
    @classmethod
    def polymarket_raw_retention_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Retention must be >= 1 day")
        return v

    @field_validator(
        "price_move_threshold_pct", "volume_spike_multiplier",
        "spread_change_threshold_ratio", "liquidity_vacuum_depth_ratio_threshold",
        "deadline_near_price_threshold_pct",
        "min_volume_24h", "connector_timeout_seconds",
        "shadow_execution_min_fill_pct",
        "polymarket_structure_max_notional_per_plan",
        "polymarket_structure_min_depth_per_leg",
        "polymarket_stream_reconnect_base_seconds",
        "polymarket_stream_reconnect_max_seconds",
        "polymarket_user_stream_reconnect_base_seconds",
        "polymarket_user_stream_reconnect_max_seconds",
    )
    @classmethod
    def thresholds_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Threshold must be > 0")
        return v

    @field_validator("shadow_execution_min_fill_pct")
    @classmethod
    def shadow_execution_min_fill_pct_bounds(cls, v: float) -> float:
        if v <= 0 or v > 1:
            raise ValueError("shadow_execution_min_fill_pct must be > 0 and <= 1")
        return v

    @field_validator("ofi_threshold")
    @classmethod
    def ofi_threshold_bounds(cls, v: float) -> float:
        if v <= 0 or v >= 1:
            raise ValueError("ofi_threshold must be > 0 and < 1")
        return v

    @field_validator("arb_spread_threshold")
    @classmethod
    def arb_spread_threshold_bounds(cls, v: float) -> float:
        if v <= 0 or v >= 1:
            raise ValueError("arb_spread_threshold must be > 0 and < 1")
        return v

    @field_validator("alert_rank_threshold")
    @classmethod
    def alert_rank_threshold_bounds(cls, v: float) -> float:
        if v < 0.0 or v > 1.0:
            raise ValueError("alert_rank_threshold must be between 0.0 and 1.0")
        return v

    @field_validator("legacy_benchmark_rank_threshold")
    @classmethod
    def legacy_benchmark_rank_threshold_bounds(cls, v: float) -> float:
        if v < 0.0 or v > 1.0:
            raise ValueError("legacy_benchmark_rank_threshold must be between 0.0 and 1.0")
        return v

    @field_validator(
        "alert_batch_limit",
        "market_pagination_cap",
        "orderbook_sample_size",
        "cleanup_interval_hours",
        "shadow_execution_max_staleness_seconds",
        "shadow_execution_max_forward_seconds",
        "scheduler_lease_seconds",
        "scheduler_lease_renew_interval_seconds",
        "default_strategy_min_observation_days",
        "default_strategy_preferred_observation_days",
        "strategy_review_lookback_days",
        "strategy_review_recent_mistakes_limit",
        "polymarket_malformed_burst_threshold",
        "polymarket_book_recon_max_watched_assets",
        "polymarket_book_recon_bootstrap_lookback_hours",
        "polymarket_features_lookback_hours",
        "polymarket_features_max_watched_assets",
        "polymarket_execution_policy_default_horizon_ms",
        "polymarket_execution_policy_passive_lookback_hours",
        "polymarket_execution_policy_passive_min_label_rows",
        "polymarket_structure_max_groups_per_run",
        "polymarket_structure_cross_venue_max_staleness_seconds",
        "polymarket_structure_plan_max_age_seconds",
        "polymarket_live_decision_max_age_seconds",
        "polymarket_reconcile_interval_seconds",
        "polymarket_chain_id",
        "polymarket_signature_type",
    )
    @classmethod
    def limits_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Limit must be >= 1")
        return v

    @field_validator(
        "polymarket_execution_policy_max_cross_slippage_bps",
        "polymarket_execution_policy_min_net_ev_bps",
        "polymarket_structure_min_net_edge_bps",
        "polymarket_structure_max_leg_slippage_bps",
    )
    @classmethod
    def polymarket_execution_policy_thresholds_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Execution policy threshold must be >= 0")
        return v

    @field_validator(
        "polymarket_stream_ping_interval_seconds",
        "polymarket_watch_reconcile_interval_seconds",
        "polymarket_gap_suspect_after_seconds",
        "polymarket_malformed_burst_window_seconds",
        "polymarket_meta_sync_interval_seconds",
        "polymarket_book_snapshot_interval_seconds",
        "polymarket_trade_backfill_interval_seconds",
        "polymarket_oi_poll_interval_seconds",
        "polymarket_book_recon_stale_after_seconds",
        "polymarket_book_recon_resync_cooldown_seconds",
        "polymarket_features_interval_seconds",
        "polymarket_structure_interval_seconds",
    )
    @classmethod
    def polymarket_intervals_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Polymarket interval must be >= 1 second")
        return v

    @field_validator("polymarket_feature_buckets_ms", "polymarket_label_horizons_ms")
    @classmethod
    def polymarket_positive_int_csv(cls, v: str) -> str:
        _parse_positive_int_csv(v)
        return v

    @field_validator("polymarket_meta_sync_page_size")
    @classmethod
    def polymarket_meta_sync_page_size_bounds(cls, v: int) -> int:
        if v < 1 or v > 1000:
            raise ValueError("polymarket_meta_sync_page_size must be between 1 and 1000")
        return v

    @field_validator("polymarket_trade_backfill_lookback_hours")
    @classmethod
    def polymarket_trade_backfill_lookback_hours_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("polymarket_trade_backfill_lookback_hours must be >= 1")
        return v

    @field_validator("polymarket_trade_backfill_page_size")
    @classmethod
    def polymarket_trade_backfill_page_size_bounds(cls, v: int) -> int:
        if v < 1 or v > 500:
            raise ValueError("polymarket_trade_backfill_page_size must be between 1 and 500")
        return v

    @field_validator("polymarket_book_recon_bbo_tolerance")
    @classmethod
    def polymarket_book_recon_bbo_tolerance_bounds(cls, v: float) -> float:
        if v < 0 or v >= 1:
            raise ValueError("polymarket_book_recon_bbo_tolerance must be between 0 and 1")
        return v

    @field_validator("polymarket_stream_reconnect_max_seconds")
    @classmethod
    def reconnect_max_must_not_be_lower_than_base(cls, v: float, info) -> float:
        reconnect_base = info.data.get("polymarket_stream_reconnect_base_seconds")
        if reconnect_base is not None and v < reconnect_base:
            raise ValueError("polymarket_stream_reconnect_max_seconds must be >= base")
        return v

    @field_validator("polymarket_user_stream_reconnect_max_seconds")
    @classmethod
    def user_stream_reconnect_max_must_not_be_lower_than_base(cls, v: float, info) -> float:
        reconnect_base = info.data.get("polymarket_user_stream_reconnect_base_seconds")
        if reconnect_base is not None and v < reconnect_base:
            raise ValueError("polymarket_user_stream_reconnect_max_seconds must be >= base")
        return v

    @field_validator("polymarket_max_outstanding_notional_usd")
    @classmethod
    def polymarket_outstanding_notional_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("polymarket_max_outstanding_notional_usd must be >= 0")
        return v

    @field_validator("sse_max_connections")
    @classmethod
    def sse_max_connections_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("sse_max_connections must be >= 1")
        return v

    @property
    def polymarket_feature_bucket_values_ms(self) -> list[int]:
        return _parse_positive_int_csv(self.polymarket_feature_buckets_ms)

    @property
    def polymarket_label_horizon_values_ms(self) -> list[int]:
        return _parse_positive_int_csv(self.polymarket_label_horizons_ms)

    @property
    def polymarket_allowlist_market_values(self) -> list[str]:
        return _parse_string_csv(self.polymarket_allowlist_markets)

    @property
    def polymarket_allowlist_category_values(self) -> list[str]:
        return _parse_string_csv(self.polymarket_allowlist_categories)

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
