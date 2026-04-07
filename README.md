# Signal Market Terminal

A prediction-market intelligence platform that ingests market data from **Polymarket** and **Kalshi**, detects unusual market behavior, ranks signals, and evaluates signal quality over time. Think "Bloomberg terminal lite for prediction markets."

This is **not** an auto-trading bot. It is a monitoring, research, and decision-support tool for serious operators.

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
              |  (FastAPI + APScheduler)|
              |                       |
              |  Ingestion  -> DB     |
              |  Detection  -> Signals|
              |  Evaluation -> Evals  |
              |  Alerting   -> Multi  |
              |  Cleanup    -> Retain |
              |  API        -> REST   |
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
              |  Signal Feed, Charts, |
              |  Market Detail, Health|
              +-----------------------+
```

## Quick Start

```bash
# Clone and start (development)
cd "Signal Market Terminal"
docker-compose up --build

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
uvicorn app.main:app --reload

# Frontend (separate terminal)
cd frontend
npm install
npm run dev
```

## Signal Types

| Type | Description | Key Config |
|------|-------------|------------|
| **Price Move** | Outcome price moved >5% in 30min window | `PRICE_MOVE_THRESHOLD_PCT`, `PRICE_MOVE_WINDOW_MINUTES` |
| **Volume Spike** | 24h volume >3x the rolling baseline | `VOLUME_SPIKE_MULTIPLIER`, `VOLUME_SPIKE_BASELINE_HOURS` |
| **Spread Change** | Bid-ask spread widened/narrowed >2x vs 12h avg | `SPREAD_CHANGE_THRESHOLD_RATIO` |
| **Liquidity Vacuum** | Order book depth dropped below 30% of baseline | `LIQUIDITY_VACUUM_DEPTH_RATIO_THRESHOLD` |
| **Deadline Near** | Market within 48h of close showing >3% price move | `DEADLINE_NEAR_HOURS`, `DEADLINE_NEAR_PRICE_THRESHOLD_PCT` |

## Ranking

```
rank_score = signal_score x confidence x recency_weight
```

- **signal_score** (0-1): Raw anomaly strength
- **confidence** (0-1): Trust modifier. Penalized by low volume, low liquidity, thin baseline
- **recency_weight**: 1.0 at 0h, decays to 0.3 at 24h
- **Dedupe**: One signal per (type, outcome, 15-min window)

## Evaluation

Signals are evaluated at 4 horizons: **15m, 1h, 4h, 24h**. The evaluator checks the closest price snapshot to each horizon target and computes `price_change_pct`. Signals are marked `resolved` once all horizons complete.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/signals` | Paginated signal feed, filterable by `signal_type` and `market_id` |
| GET | `/api/v1/signals/{id}` | Signal detail with evaluations |
| GET | `/api/v1/signals/export/csv` | Export signals as CSV |
| GET | `/api/v1/markets` | Active markets list |
| GET | `/api/v1/markets/{id}` | Market detail with outcomes and latest prices |
| GET | `/api/v1/markets/{id}/snapshots` | Recent price snapshots for a market |
| GET | `/api/v1/markets/{id}/chart-data` | Price time series for charting (1h/6h/24h/7d ranges) |
| GET | `/api/v1/markets/export/csv` | Export markets as CSV |
| GET | `/api/v1/alerts/recent` | Recent alerted signals |
| GET | `/api/v1/health` | System health: market count, signal count, ingestion status, alert count |

## Alerting

Signals with `rank_score >= 0.7` (configurable via `ALERT_RANK_THRESHOLD`) trigger alerts through multiple channels:

| Channel | Config | Description |
|---------|--------|-------------|
| **Logger** | Always on | Structured `ALERT` log lines |
| **Webhook** | `ALERT_WEBHOOK_URL` | POST JSON payload to any URL |
| **Telegram** | `ALERT_TELEGRAM_BOT_TOKEN` + `ALERT_TELEGRAM_CHAT_ID` | Telegram Bot API messages |

Each signal is alerted only once (no re-fires on subsequent detection cycles).

## Configuration

All settings are environment variables. See `backend/.env.example` for the full list with defaults.

Key API hardening settings:
- `API_RATE_LIMIT`: Request rate limit (default: `60/minute`)
- `API_KEY`: Optional API key for authenticated access (set to enable)
- `CORS_ORIGINS`: Comma-separated allowed origins

## Scheduled Jobs

| Job | Interval | Purpose |
|-----|----------|---------|
| Market Discovery | 5 min | Fetch active markets from Polymarket Gamma API |
| Snapshot Capture | 2 min | Fetch midpoints + orderbooks, persist to DB |
| Signal Detection | 2 min + 10s | Run all 5 detectors, persist signals with dedupe |
| Evaluation | 5 min | Evaluate unresolved signals at 15m/1h/4h/24h |
| Cleanup | 6 hours | Delete old snapshots (30d), orderbooks (14d), signals (90d) |

## Tech Stack

- Python 3.12 + FastAPI + SQLAlchemy 2.x + APScheduler
- PostgreSQL 16
- React 18 + Vite + React Router + Recharts
- Docker Compose (dev + prod configs)
- GitHub Actions CI (lint + test + build)

## Testing

```bash
cd backend
pip install -r requirements.txt
python -m pytest tests/ -v
```

40 tests covering all 5 detectors, ranking, evaluation, API endpoints, and end-to-end integration.

## Roadmap

- [x] Kalshi connector (second market source)
- [ ] WebSocket real-time signal push
- [ ] Signal backtesting framework
- [ ] Performance dashboards and signal accuracy tracking
- [ ] Prometheus metrics endpoint
