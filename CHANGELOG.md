# Changelog

All notable changes to Signal Market Terminal are documented here.

## [0.2.0] - 2026-04-07

### Added
- **Analytics dashboard** — cross-platform comparison cards, signal accuracy table (per type/horizon, color-coded), correlated signals list (same-category signals across platforms)
- **Analytics API** — `platform-summary`, `signal-accuracy`, and `correlated-signals` endpoints
- **Dark/light theme toggle** — CSS variable-based theming with localStorage persistence
- **Circuit breaker** — closed/open/half-open pattern for connector resilience (`failure_threshold=5`, `reset_timeout=300s`); integrated into both Polymarket and Kalshi connectors
- **Prometheus metrics** — `/metrics` endpoint via auto-instrumentation; custom metrics for `signals_fired`, `alerts_sent`, `ingestion_duration`, `active_markets`, `sse_connections`
- **Structured JSON logging** — opt-in via `log_format=json` config
- **Real-time SSE streaming** — `GET /api/v1/events/signals` with `SignalBroadcaster` pub/sub; frontend `useSSE` hook with auto-reconnect; Signal Feed shows live indicator and extends poll to 120s
- **Markets browser** — search, platform filter, sort (updated/volume/end_date/alphabetical), pagination
- **Alerts history page** — type and platform filters, paginated table view
- **Formatted signal detail** — labeled key-value grid replacing raw JSON
- **Nav updated** to: Feed | Markets | Analytics | Alerts | Health

### Changed
- Backend markets endpoint adds `search`, `category`, and `sort_by` params
- Alerts endpoint adds pagination and `market_question` join
- Evaluator timezone handling hardened with `_ensure_utc()` helper

### Fixed
- Polymarket connector: switched to `/markets` endpoint, fixed JSON-string field parsing, reduced batch size, added retry with backoff
- Timezone inconsistencies in signal evaluation horizon comparisons

### Infrastructure
- `docker-compose.prod.yml`: resource limits, named network, JSON logging
- Nginx: SSE proxy support, static asset caching, `/metrics` proxy
- CI: 70% coverage gate via `pytest-cov`, Docker build verification step

### Tests
- 90 total tests (up from 40 in v0.1.0)
- Added circuit breaker tests (8), connector tests (16), ingestion tests (5), alert tests (5), cleanup tests (5), config validation tests (11)

---

## [0.1.0] - 2026-04-07

### Added
- **Polymarket connector** — Gamma API for market discovery, CLOB API for price snapshots and orderbook data
- **Kalshi connector** — REST API with cursor-based pagination and orderbook inversion
- **5 signal detectors** — Price Move (>5% in 30min), Volume Spike (>3x baseline), Spread Change (>2x avg), Liquidity Vacuum (<30% depth), Deadline Near (<48h with >3% move)
- **Signal ranking** — `rank_score = signal_score × confidence × recency_weight`; deduplication per (type, outcome, 15-min window)
- **4-horizon evaluation** — signals evaluated at 15m, 1h, 4h, 24h post-detection
- **Multi-channel alerting** — logger (always on), webhook (POST JSON), Telegram Bot API; threshold configurable via `ALERT_RANK_THRESHOLD`
- **REST API** — signals, markets, alerts, health endpoints with CSV export and platform filtering
- **React frontend** — paginated signal feed with auto-refresh, dark theme, price charts (Recharts)
- **Config validation** — field validators for intervals, retention periods, thresholds, and limits; all hardcoded values extracted to settings
- **Data retention** — scheduled cleanup (snapshots 30d, orderbooks 14d, signals 90d)
- **Docker Compose** — dev and prod configurations
- **CI workflow** — lint (ruff) and test pipeline
- **40 tests** — detectors, ranking, evaluation, API, connectors, integration

[0.2.0]: https://github.com/Fetzi144/signal-market-terminal/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Fetzi144/signal-market-terminal/releases/tag/v0.1.0
