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
