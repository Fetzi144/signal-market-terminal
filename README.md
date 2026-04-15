# Signal Market Terminal v0.4.1

A prediction-market intelligence platform that ingests market data from **Polymarket** and **Kalshi**, detects unusual market behavior, estimates probabilities, computes expected value, tracks closing line value, and paper-trades a fixed default strategy to prove whether the system has real edge.

This is **not** an auto-trading bot. It is a monitoring, research, and decision-support tool for serious operators. The current product focus is **prove the edge first**: freeze one default strategy, track it honestly, and let the P&L line decide what survives.

## Architecture

```
              +-----------+     +-----------+
              | Polymarket|     |   Kalshi  |
              |  Gamma +  |     |  REST API |
              |  CLOB API |     |  (public) |
              +-----+-----+     +-----+-----+
                    |                 |
                    +--------+--------+
                             |
              +-----------v-----------+
              |       Backend         |
              | (FastAPI API service) |
              |                       |
              |  Ingestion  -> DB     |
              |  Detection  -> Signals|
              |  Evaluation -> Evals  |
              |  Alerting   -> Multi  |
              |  SSE Stream -> Live   |
              |  Analytics  -> Stats  |
              |  Metrics    -> Prom   |
              |  Cleanup    -> Retain |
              +-----------+-----------+
                          |
              +-----------v-----------+
              |  Scheduler Worker     |
              |    (APScheduler)      |
              +-----------+-----------+
                          |
              +-----------v-----------+
              |    PostgreSQL 16      |
              |  markets, outcomes,   |
              |  price_snapshots,     |
              |  orderbook_snapshots, |
              |  signals, evaluations |
              +-----------+-----------+
                          |
              +-----------v-----------+
              |   React Frontend      |
              |  Feed, Markets,       |
              |  Analytics, Alerts,   |
              |  Charts, Health       |
              +-----------------------+
```

## Quick Start

```bash
# Clone and start (development)
cd "Signal Market Terminal"
docker-compose up --build

# The API runs in `backend`; APScheduler jobs run in `worker`
# Wait ~2 minutes for market discovery + first snapshots
# Open http://localhost:5173
```

For production:
```bash
docker-compose -f docker-compose.prod.yml up --build -d
# Open http://localhost
```

For local development without Docker:

```bash
# Start Postgres
docker-compose up db

# Backend
cd backend
cp .env.example .env
pip install -r requirements.txt
alembic upgrade head
set SCHEDULER_ENABLED=false
uvicorn app.main:app --reload

# Scheduler worker (separate terminal)
cd backend
set SCHEDULER_ENABLED=true
python -m app.worker

# Frontend (separate terminal)
cd frontend
npm install
npm run dev
```

Local development supports both `localhost` and `127.0.0.1` frontend origins by default. The frontend API base lives in [frontend/.env.development](<C:/Code/Signal Market Terminal/frontend/.env.development>) and can be overridden with `VITE_API_BASE`.

## Features

### Default Strategy Baseline

The repo now carries one explicit validation path for the "prove the edge" phase:

- **Signal path:** `confluence`
- **Filter:** EV threshold of `>= $0.03/share`
- **Sizing:** quarter-Kelly on a `$10,000` paper bankroll
- **Risk guardrails:** `5%` max single position, `30%` max total exposure, `15%` max cluster exposure, drawdown circuit breaker at `-15%`
- **Primary source of truth:** paper-trading portfolio, P&L curve, strategy-health review, and detector verdicts
- **Immutable run anchor:** the active `strategy_run` record, not a mutable env var
- **Execution realism overlay:** conservative shadow-entry pricing with liquidity flags from stored orderbook snapshots

See [docs/default-strategy.md](<C:/Code/Signal Market Terminal/docs/default-strategy.md>) for the full contract.

### Default Strategy Measurement Rules

The default-strategy measurement stack is intentionally conservative:

- **Read-only verification surfaces:** `GET /api/v1/paper-trading/portfolio?scope=default_strategy`, `GET /api/v1/paper-trading/history?scope=default_strategy`, `GET /api/v1/paper-trading/metrics?scope=default_strategy`, `GET /api/v1/paper-trading/pnl-curve?scope=default_strategy`, and `GET /api/v1/paper-trading/strategy-health` never create a `strategy_run`.
- **Explicit bootstrap only:** if no active run exists, those read paths return a clean `no_active_run` / `bootstrap_required` state. Creating a new run is explicit via `POST /api/v1/paper-trading/default-strategy/bootstrap`.
- **Canonical funnel ledger:** each qualified signal after the run boundary must reconcile into exactly one of `opened_trade`, `skipped`, or `pending_decision`, backed by the run-scoped `ExecutionDecision` ledger.
- **Risk attribution is explicit:** strategy-health and review outputs separate local paper-book blocks from shared/global risk-graph blocks and preserve original upstream reason codes for debugging.
- **Persisted drawdown state:** the drawdown breaker uses stored run equity, high-water mark, and drawdown state rather than reconstructing the breaker from current P&L snapshots.
- **Benchmark honesty:** comparison outputs are split into `signal_level` (`per_share`) and `execution_adjusted` (`usd`) modes so one report never mixes signal-level and trade-level P&L.
- **Replay truth boundary:** replay reports now resolve outcomes from canonical settlement data and label detector support explicitly with `coverage_mode`, `supported_detectors`, and `unsupported_detectors`.

See [docs/default-strategy.md](<C:/Code/Signal Market Terminal/docs/default-strategy.md>) for the measurement contract and [docs/runbooks/default-strategy-remediation.md](<C:/Code/Signal Market Terminal/docs/runbooks/default-strategy-remediation.md>) for the operator runbook.

### Signal Detection (5 families)

| Type | Description | Key Config |
|------|-------------|------------|
| **Price Move** | Outcome price moved >5% in 30min window | `PRICE_MOVE_THRESHOLD_PCT`, `PRICE_MOVE_WINDOW_MINUTES` |
| **Volume Spike** | 24h volume >3x the rolling baseline | `VOLUME_SPIKE_MULTIPLIER`, `VOLUME_SPIKE_BASELINE_HOURS` |
| **Spread Change** | Bid-ask spread widened/narrowed >2x vs 12h avg | `SPREAD_CHANGE_THRESHOLD_RATIO` |
| **Liquidity Vacuum** | Order book depth dropped below 30% of baseline | `LIQUIDITY_VACUUM_DEPTH_RATIO_THRESHOLD` |
| **Deadline Near** | Market within 48h of close showing >3% price move | `DEADLINE_NEAR_HOURS`, `DEADLINE_NEAR_PRICE_THRESHOLD_PCT` |

### Ranking

```
rank_score = signal_score x confidence x recency_weight
```

- **signal_score** (0-1): Raw anomaly strength
- **confidence** (0-1): Trust modifier. Penalized by low volume, low liquidity, thin baseline
- **recency_weight**: 1.0 at 0h, decays to 0.3 at 24h
- **Dedupe**: One signal per (type, outcome, 15-min window)

### Evaluation

Signals are evaluated at 4 horizons: **15m, 1h, 4h, 24h**. The evaluator checks the closest price snapshot to each horizon target and computes `price_change_pct`. Signals are marked `resolved` once all horizons complete.

### Real-Time Updates (SSE)

The backend streams new signal and alert events via Server-Sent Events (`GET /api/v1/events/signals`). The frontend auto-refreshes on incoming events with a green "Live" indicator.

### Cross-Platform Analytics

- **Platform Summary**: Market counts, signal counts, avg rank per platform
- **Signal Accuracy**: Directional accuracy per signal type per horizon
- **Correlated Signals**: Cross-platform signals firing on the same category

### Observability

- **Prometheus metrics** at `/metrics` - auto-instrumented HTTP metrics plus custom counters/gauges for signals, alerts, ingestion, SSE connections
- **Structured JSON logging** in production (`LOG_FORMAT=json`)
- **Circuit breaker** on both connectors (closed/open/half-open states)

### Alerting

Signals with `rank_score >= 0.7` (configurable via `ALERT_RANK_THRESHOLD`) trigger alerts:

| Channel | Config | Description |
|---------|--------|-------------|
| **Logger** | Always on | Structured `ALERT` log lines |
| **Webhook** | `ALERT_WEBHOOK_URL` | POST JSON payload to any URL |
| **Telegram** | `ALERT_TELEGRAM_BOT_TOKEN` + `ALERT_TELEGRAM_CHAT_ID` | Telegram Bot API messages |

Each signal is alerted only once (no re-fires).

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/signals` | Paginated signal feed (filter: `signal_type`, `market_id`, `platform`) |
| GET | `/api/v1/signals/{id}` | Signal detail with evaluations |
| GET | `/api/v1/signals/export/csv` | Export signals as CSV |
| GET | `/api/v1/markets` | Markets list (filter: `search`, `platform`, `category`, sort: `updated`/`volume`/`end_date`/`question`) |
| GET | `/api/v1/markets/{id}` | Market detail with outcomes and latest prices |
| GET | `/api/v1/markets/{id}/snapshots` | Recent price snapshots |
| GET | `/api/v1/markets/{id}/chart-data` | Price time series (1h/6h/24h/7d) |
| GET | `/api/v1/markets/export/csv` | Export markets as CSV |
| GET | `/api/v1/alerts/recent` | Alerted signals (filter: `signal_type`, `platform`) |
| GET | `/api/v1/analytics/platform-summary` | Per-platform stats |
| GET | `/api/v1/analytics/signal-accuracy` | Accuracy per signal type per horizon |
| GET | `/api/v1/analytics/correlated-signals` | Cross-platform correlated signals |
| GET | `/api/v1/events/signals` | SSE stream (new_signal, new_alert events) |
| GET | `/api/v1/health` | System health |
| GET | `/metrics` | Prometheus metrics |

## Configuration

All settings are environment variables. See `backend/.env.example` for the full list with defaults. Validated on startup: intervals >= 30s, retention >= 1 day, thresholds > 0.

Key settings:
- `API_RATE_LIMIT`: Request rate limit (default: `60/minute`)
- `API_KEY`: Optional API key for authenticated access
- `CORS_ORIGINS`: Comma-separated allowed origins
- `KALSHI_ENABLED`: Enable/disable Kalshi connector (default: `true`)
- `LOG_FORMAT`: `text` (dev) or `json` (production)

## Scheduled Jobs

| Job | Interval | Purpose |
|-----|----------|---------|
| Market Discovery | 5 min | Fetch active markets from Polymarket + Kalshi |
| Snapshot Capture | 2 min | Fetch midpoints + orderbooks, persist to DB |
| Signal Detection | 2 min + 10s | Run all 5 detectors, persist + broadcast via SSE |
| Evaluation | 5 min | Evaluate unresolved signals at 15m/1h/4h/24h |
| Cleanup | 6 hours | Delete old snapshots (30d), orderbooks (14d), signals (90d) |

In dev and prod, APScheduler now runs in the dedicated `worker` service. The web/API process should keep `SCHEDULER_ENABLED=false`.

## Tech Stack

- Python 3.12 + FastAPI + SQLAlchemy 2.x + APScheduler
- PostgreSQL 16 (asyncpg)
- React 18 + Vite + React Router + Recharts
- Docker Compose (dev + prod configs)
- Prometheus + structured JSON logging
- GitHub Actions CI (lint + test with 70% coverage gate + Docker build)

## Testing

```bash
cd backend
pip install -r requirements.txt
python -m pytest tests/ -v
```

90+ tests covering connectors (Polymarket + Kalshi), ingestion, all 5 detectors, ranking, evaluation, alerting (webhook/telegram/logger), cleanup, circuit breaker, config validation, API endpoints, and integration.

Canonical repo-root validation commands:

```bash
npm run frontend:install
npm run frontend:validate
npm run secrets:scan
python -m pytest backend/tests/test_api.py backend/tests/test_structure_engine.py backend/tests/test_structure_phase8b_api.py -q
```

The root `package.json` intentionally owns the frontend workflow so CI and local development use the same entrypoints. Secret scanning is handled by `scripts/scan_secrets.py`, which scans tracked text files and supports a per-line allow marker of `secret-scan: allow` for reviewed false positives.

## Changelog

### v0.2.0
- Markets browser page with search, sort, platform filter
- Alerts history page with pagination and filters
- Cross-platform analytics dashboard (platform summary, signal accuracy, correlations)
- Real-time SSE updates (live signal streaming to frontend)
- Prometheus metrics endpoint (`/metrics`)
- Circuit breaker for connector resilience
- Structured JSON logging option
- Dark/light theme toggle
- Config validation with field validators
- Timezone fix in signal evaluator
- 90+ tests (up from 40), 70% coverage gate in CI
- Hardened production Docker (resource limits, named network)
- Enhanced nginx config (SSE proxy, static asset caching)

### v0.1.0
- Initial release: Polymarket + Kalshi connectors, 5 signal detectors, 4-horizon evaluation
- Webhook + Telegram alerting, price charts, CSV export
- Docker dev + prod configs, GitHub Actions CI
