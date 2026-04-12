from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://smt:smt@localhost:5432/smt"

    # Polymarket
    polymarket_api_base: str = "https://clob.polymarket.com"
    polymarket_gamma_base: str = "https://gamma-api.polymarket.com"

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
    cors_origins: str = "http://localhost:5173"  # Comma-separated origins

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

    @field_validator(
        "price_move_threshold_pct", "volume_spike_multiplier",
        "spread_change_threshold_ratio", "liquidity_vacuum_depth_ratio_threshold",
        "deadline_near_price_threshold_pct",
        "min_volume_24h", "connector_timeout_seconds",
    )
    @classmethod
    def thresholds_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Threshold must be > 0")
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

    @field_validator("alert_batch_limit", "market_pagination_cap", "orderbook_sample_size", "cleanup_interval_hours")
    @classmethod
    def limits_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Limit must be >= 1")
        return v

    @field_validator("sse_max_connections")
    @classmethod
    def sse_max_connections_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("sse_max_connections must be >= 1")
        return v

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
