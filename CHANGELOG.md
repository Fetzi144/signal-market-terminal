# Changelog

All notable changes to Signal Market Terminal are documented here.

## [Unreleased]

### Added
- **Default strategy contract** for the prove-the-edge phase: confluence + EV filter + quarter-Kelly + current paper-trading risk guardrails
- **Strategy health endpoint** consolidating paper-trading P&L, CLV, Brier score, benchmark comparison, detector verdicts, and recent mistakes
- **Detector keep/watch/cut review loop** based on recent CLV, hypothetical P&L, calibration, and trade contribution
- **Default strategy docs and review artifact** so weekly analysis has a stable contract and repeatable checklist
- **Phase 13 fail-closed demotion enforcement** records cooling-off demotion events from blocked promotion-eligibility verdicts, blocks live submission for actively demoted versions, and pauses an armed pilot from the supervisor loop.

### Changed
- Paper trading now measures the **default confluence strategy** instead of auto-trading every EV-qualified detector signal
- The `/paper-trading` page is now the primary **strategy health** console for the current phase
- Local dev defaults now support both `localhost` and `127.0.0.1` frontend origins more reliably

### Fixed
- Backend/frontend dev defaults are aligned across API base and CORS settings
- Backend app version metadata and repo docs now reflect the current `v0.4.0` baseline instead of stale `v0.2.0` labels
- Polymarket watch-registry bootstrap now caps Postgres insert batches so large active universes do not crash `/api/v1/health` with asyncpg bind-limit failures
- Default-strategy scoped portfolio/history/metrics/curve reads and `strategy-health` now avoid loading the full live signal universe into memory on every request
- `scripts/smt-monitor.cron` is normalized for LF-only installs so cron can pick it up reliably on Linux hosts

## [0.4.1] - 2026-04-15

### Added
- **Controlled evidence relaunch tooling** via `python -m app.ops.default_strategy_evidence` for recording the evidence boundary, retiring pre-fix runs, explicit run bootstrap, and pending-decision watch checks
- **Frozen evidence metadata** on `strategy_run.contract_snapshot` for `contract_version`, `evidence_boundary`, and `evidence_gate`
- **Pending-decision age watch** in strategy-health plus Prometheus gauges for active pending count and oldest pending age
- **Worker-local Prometheus endpoint** so scheduler-only counters can be scraped from the worker container during relaunch smoke checks
- **Controlled evidence relaunch runbook** and PowerShell orchestration script for the `docker-compose.prod.yml` workflow

### Changed
- Default-strategy bootstrap can now persist explicit release/tag/commit/migration metadata instead of relying on an implicit boundary
- Scheduler no-active-run counting now reflects skipped default-strategy passes even when no new candidate signals are present

### Fixed
- Weekly review artifacts now carry the frozen contract version and evidence-boundary metadata when present

## [0.4.0] - 2026-04-08

### Added
- **Backtesting engine** — replay historical market data with configurable detectors, parameter sweep across threshold/rank combinations, Sharpe ratio and max drawdown metrics
- **Portfolio tracking** — full position lifecycle (open → trade → close) with Decimal-precision P&L, weighted average entry price, partial close support, and "no" side inversion
- **Multi-timeframe detection** — signals fire on 30m, 1h, 4h timeframes with confluence scoring bonus when multiple timeframes agree
- **Web Push notifications** — VAPID-based browser push via service worker; subscribe/unsubscribe API endpoints
- **Performance dashboard** — win rate, Sharpe ratio, accuracy breakdown by signal type and timeframe
- **Order Flow Imbalance (OFI) detector** — 7th signal type based on bid/ask depth imbalance
- **Whale/smart money tracking** — on-chain Polygon trade monitoring for large position detection
- **Mobile-responsive frontend** — PWA-installable with 375px→desktop breakpoints, touch-friendly controls
- **Discord webhook alerts** — rich embed format with market context, rank score, and confidence
- **Analytics expansion** — timeframe accuracy, correlated signals endpoint, platform summary
- **CSV exports** — signals, markets, and portfolio positions exportable as CSV
- **Docker Compose** — full dev/prod setup with Prometheus monitoring

### Fixed
- Portfolio P&L calculation using Decimal instead of Float — eliminates rounding errors
- Backtest signal routing — moved `/sweep` before `/{run_id}` to fix route collision
- Volume spike detector baseline window calculation — detector now produces candidates
- Dedupe collision in alerts test — timeframe differentiation prevents UNIQUE constraint violation

### Security
- Rate limiting on all public endpoints (10 req/sec per IP)
- HMAC signature verification on webhook endpoints
- Web Push uses VAPID keys (RFC 8292)

### Known Limitations
- ML signal scoring deferred to v0.5.0 (insufficient training data)
- Per-timeframe P&L in portfolio view (single aggregate P&L currently)
- No order execution (signal-only tool, manual trade entry)
- Kalshi API requires manual OAuth2 setup per user

### Tests
- **275 tests** (up from 168 in v0.3.0)
- Integration tests: backtest engine (4), portfolio lifecycle (3), alert delivery (5), multi-timeframe confluence (4), missing API endpoints (10)
- Full E2E: snapshot → detect → persist → API → evaluate → resolve pipeline

---

## [0.3.0] - 2026-04-07

### Added
- **Market resolution tracking** — `resolved_correctly` column on signals, backfilled when markets settle via new resolution service (`app/ingestion/resolution.py`)
- **Resolution API** — `resolved_correctly` field in all signal responses, CSV export, and filter (`?resolved_correctly=true`)
- **Ground-truth accuracy** — analytics endpoint now reports resolution-based accuracy alongside price-direction accuracy, with `resolution_rate_pct`
- **Cross-platform arbitrage detector** — 6th signal type; detects price discrepancies across Polymarket and Kalshi via `question_slug` matching
- **Arbitrage UI** — SignalDetail shows side-by-side platform prices, spread, and buy/sell recommendation
- **Dynamic filter options** — Signal Feed and Alerts fetch filter options from `/signals/types` and `/markets/platforms` endpoints
- **Resolution badges** — Signal Feed and Detail show green checkmark (correct), red X (wrong), or grey dot (pending)
- **HMAC webhook signing** — optional `X-SMT-Signature` header on webhook alerts when `alert_webhook_secret` is configured
- **SSE connection cap** — returns 503 when `sse_max_connections` (default 50) is reached; queue overflow forces client reconnect
- **Per-IP rate limiting** — `10/second` per IP on signals endpoint via SlowAPI

### Changed
- Analytics accuracy table uses ground-truth resolution data with color-coded cells (green/yellow/red)
- SSE queue overflow now closes subscriber (forcing reconnect) instead of silently dropping events
- `question_slug` column + index on Market model for cross-platform matching

### Fixed
- **Scheduler broadcast bug** — `persist_signals()` now returns `(count, list[Signal])` instead of just count; SSE broadcasts correct signal objects
- **Off-by-24x math** — removed erroneous `* 24` in `kalshi.py` and `analytics.py`
- **Kalshi cursor pagination** — market discovery now uses `fetch_markets_cursor()` for complete market retrieval
- **N+1 query in MarketDetail** — replaced per-outcome price queries with single window-function query
- **SignalFeed SSE cleanup** — `fetchData` added to `useEffect` dependency array to prevent stale closure

### Infrastructure
- `alert_rank_threshold` validated to [0.0, 1.0] range
- `arb_spread_threshold` and `arb_enabled` config settings with validators
- `sse_max_connections` config setting (default 50, validator ≥ 1)
- FK constraint: `signal.outcome_id` → `outcome.id` with `ondelete="SET NULL"`
- Alembic migrations: `add_resolved_correctly_to_signal`, `add_question_slug_to_market`

### Tests
- **168+ tests** (up from 90 in v0.2.0)
- New: Price Move detector tests (8), Volume Spike tests (8+), ranking/dedupe tests (12), resolution tests (8), arbitrage tests (7), SSE tests (8), webhook HMAC tests (2), config validation tests (16)
- New: Full end-to-end integration test — market → snapshots → detect → persist → API → evaluate → resolve → verify
- New: Arbitrage integration test — cross-platform detection → persist → verify metadata

---

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

[0.4.0]: https://github.com/Fetzi144/signal-market-terminal/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Fetzi144/signal-market-terminal/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Fetzi144/signal-market-terminal/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Fetzi144/signal-market-terminal/releases/tag/v0.1.0
