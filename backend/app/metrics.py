"""Application metrics exposed via /metrics (Prometheus format)."""
from prometheus_client import Counter, Gauge, Histogram

# Ingestion
ingestion_duration = Histogram(
    "smt_ingestion_duration_seconds",
    "Duration of ingestion jobs",
    ["job_type", "platform"],
)

# Signals
signals_fired = Counter(
    "smt_signals_fired_total",
    "Total signals fired",
    ["signal_type"],
)

# Alerts
alerts_sent = Counter(
    "smt_alerts_sent_total",
    "Total alerts dispatched",
    ["channel"],
)

# Active markets gauge
active_markets = Gauge(
    "smt_active_markets",
    "Number of active markets",
    ["platform"],
)

# SSE connections
sse_connections = Gauge(
    "smt_sse_connections",
    "Current SSE subscriber count",
)

# Circuit breaker
circuit_breaker_state = Gauge(
    "smt_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=half-open, 2=open)",
    ["platform"],
)

# Polymarket stream
polymarket_stream_active_subscriptions = Gauge(
    "smt_polymarket_stream_active_subscriptions",
    "Current subscribed Polymarket asset count",
)

polymarket_stream_active_watches = Gauge(
    "smt_polymarket_stream_active_watches",
    "Current watched Polymarket asset count",
)

polymarket_stream_connected = Gauge(
    "smt_polymarket_stream_connected",
    "Current Polymarket stream connection state (1=connected, 0=disconnected)",
)

polymarket_stream_reconnects = Counter(
    "smt_polymarket_stream_reconnects_total",
    "Total Polymarket stream reconnects",
)

polymarket_stream_resyncs = Counter(
    "smt_polymarket_stream_resyncs_total",
    "Total Polymarket resync operations",
)

polymarket_raw_events_ingested = Counter(
    "smt_polymarket_raw_events_ingested_total",
    "Total raw Polymarket events ingested",
    ["provenance", "message_type"],
)

polymarket_resync_runs = Counter(
    "smt_polymarket_resync_runs_total",
    "Total Polymarket resync runs",
    ["reason", "status"],
)

polymarket_gap_suspicions = Counter(
    "smt_polymarket_gap_suspicions_total",
    "Total suspected Polymarket ingest gaps",
    ["reason"],
)

polymarket_malformed_messages = Counter(
    "smt_polymarket_malformed_messages_total",
    "Total malformed Polymarket stream messages",
)

# Polymarket metadata sync / registry
polymarket_meta_sync_runs = Counter(
    "smt_polymarket_meta_sync_runs_total",
    "Total Polymarket metadata sync runs",
    ["reason", "status"],
)

polymarket_meta_sync_failures = Counter(
    "smt_polymarket_meta_sync_failures_total",
    "Total failed or partial Polymarket metadata sync runs",
)

polymarket_meta_events_upserted = Counter(
    "smt_polymarket_meta_events_upserted_total",
    "Total Polymarket event registry rows inserted or updated",
)

polymarket_meta_markets_upserted = Counter(
    "smt_polymarket_meta_markets_upserted_total",
    "Total Polymarket market registry rows inserted or updated",
)

polymarket_meta_assets_upserted = Counter(
    "smt_polymarket_meta_assets_upserted_total",
    "Total Polymarket asset registry rows inserted or updated",
)

polymarket_meta_param_rows_inserted = Counter(
    "smt_polymarket_meta_param_rows_inserted_total",
    "Total Polymarket parameter history rows inserted",
)

polymarket_meta_last_successful_sync_timestamp = Gauge(
    "smt_polymarket_meta_last_successful_sync_timestamp",
    "Unix timestamp of the most recent successful Polymarket metadata sync",
)

polymarket_meta_last_successful_sync_age_seconds = Gauge(
    "smt_polymarket_meta_last_successful_sync_age_seconds",
    "Age in seconds since the most recent successful Polymarket metadata sync",
)

polymarket_meta_registry_stale_rows = Gauge(
    "smt_polymarket_meta_registry_stale_rows",
    "Current stale Polymarket registry row count",
    ["kind"],
)

# Polymarket raw storage / Phase 3
polymarket_raw_projector_runs = Counter(
    "smt_polymarket_raw_projector_runs_total",
    "Total Polymarket raw projector runs",
    ["reason", "status"],
)

polymarket_raw_projector_failures = Counter(
    "smt_polymarket_raw_projector_failures_total",
    "Total failed Polymarket raw projector runs",
)

polymarket_raw_projected_rows = Counter(
    "smt_polymarket_raw_projected_rows_total",
    "Total projected Polymarket raw rows inserted",
    ["table_name", "source_kind"],
)

polymarket_raw_projector_last_success_timestamp = Gauge(
    "smt_polymarket_raw_projector_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket raw projector run",
)

polymarket_raw_projector_lag = Gauge(
    "smt_polymarket_raw_projector_lag",
    "Lag in raw Polymarket event ids between latest relevant raw event and last successful projector watermark",
)

polymarket_book_snapshot_runs = Counter(
    "smt_polymarket_book_snapshot_runs_total",
    "Total Polymarket book snapshot runs",
    ["reason", "status"],
)

polymarket_book_snapshot_failures = Counter(
    "smt_polymarket_book_snapshot_failures_total",
    "Total failed Polymarket book snapshot runs",
)

polymarket_book_snapshot_last_success_timestamp = Gauge(
    "smt_polymarket_book_snapshot_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket book snapshot run",
)

polymarket_trade_backfill_runs = Counter(
    "smt_polymarket_trade_backfill_runs_total",
    "Total Polymarket trade backfill runs",
    ["reason", "status"],
)

polymarket_trade_backfill_failures = Counter(
    "smt_polymarket_trade_backfill_failures_total",
    "Total failed Polymarket trade backfill runs",
)

polymarket_trade_backfill_last_success_timestamp = Gauge(
    "smt_polymarket_trade_backfill_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket trade backfill run",
)

polymarket_oi_poll_runs = Counter(
    "smt_polymarket_oi_poll_runs_total",
    "Total Polymarket open-interest polling runs",
    ["reason", "status"],
)

polymarket_oi_poll_failures = Counter(
    "smt_polymarket_oi_poll_failures_total",
    "Total failed Polymarket open-interest polling runs",
)

polymarket_oi_poll_last_success_timestamp = Gauge(
    "smt_polymarket_oi_poll_last_success_timestamp",
    "Unix timestamp of the most recent successful Polymarket open-interest polling run",
)
