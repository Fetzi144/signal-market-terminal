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
    min_volume_24h: float = 5000.0

    # Signals — Price Move
    price_move_window_minutes: int = 30
    price_move_threshold_pct: float = 5.0

    # Signals — Volume Spike
    volume_spike_multiplier: float = 3.0
    volume_spike_baseline_hours: int = 24

    # Signals — Spread Change
    spread_change_baseline_hours: int = 12
    spread_change_threshold_ratio: float = 2.0

    # Signals — Liquidity Vacuum
    liquidity_vacuum_baseline_hours: int = 12
    liquidity_vacuum_depth_ratio_threshold: float = 0.3

    # Signals — Deadline Near
    deadline_near_hours: int = 48
    deadline_near_price_threshold_pct: float = 3.0

    # Evaluation
    evaluation_interval_seconds: int = 300

    # Alerts
    alert_rank_threshold: float = 0.7
    alert_webhook_url: str | None = None
    alert_telegram_bot_token: str | None = None
    alert_telegram_chat_id: str | None = None
    alert_signal_types: str | None = None  # Comma-separated, None = all types

    # Retention
    retention_price_snapshots_days: int = 30
    retention_orderbook_snapshots_days: int = 14
    retention_signals_days: int = 90

    # API
    api_rate_limit: str = "60/minute"
    api_key: str | None = None  # Set to require X-API-Key header
    cors_origins: str = "http://localhost:5173"  # Comma-separated origins

    # App
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
