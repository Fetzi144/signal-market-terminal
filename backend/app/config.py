from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://smt:smt@localhost:5432/smt"

    # Polymarket
    polymarket_api_base: str = "https://clob.polymarket.com"
    polymarket_gamma_base: str = "https://gamma-api.polymarket.com"

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

    # Retention
    retention_price_snapshots_days: int = 30
    retention_orderbook_snapshots_days: int = 14
    retention_signals_days: int = 90

    # App
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
