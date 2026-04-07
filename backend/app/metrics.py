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
