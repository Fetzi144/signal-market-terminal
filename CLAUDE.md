# Signal Market Terminal

Real-time prediction market signal detector for Polymarket and Kalshi.

## Stack
- **Backend**: Python 3.12, FastAPI, SQLAlchemy 2 (async), PostgreSQL 16, APScheduler
- **Frontend**: React 18 + Vite, Recharts
- **Infra**: Docker Compose, Prometheus, Alembic migrations

## Version
Current: **v0.3.0** (v0.4.0 sprint in progress — backtesting, OFI, portfolio, whale tracking)

## Directory Structure
```
backend/app/
  main.py          # FastAPI app, lifespan, rate limiting, CORS
  config.py        # Pydantic settings (all via env vars)
  db.py            # Async SQLAlchemy engine
  models/          # SQLAlchemy ORM models (market, signal, snapshot, portfolio, …)
  api/             # FastAPI routers (signals, markets, alerts, SSE, backtest, …)
  signals/         # Signal detectors (base.py + 8 detectors)
  connectors/      # Polymarket + Kalshi API clients with circuit breaker
  ingestion/       # Market discovery + snapshot capture
  backtesting/     # Replay engine + parameter sweep
  jobs/            # APScheduler jobs (snapshot, detect, evaluate, cleanup, …)
  alerts/          # Pluggable alerters (Discord, Telegram, Web Push, Webhook)
frontend/src/
  pages/           # SignalFeed, Markets, Analytics, Backtest, Performance, Portfolio
  components/      # PriceChart, SignalEvaluationBar, PushNotificationToggle
  hooks/           # useSSE (auto-reconnecting EventSource)
```

## Start
```bash
cp backend/.env.example backend/.env   # fill DATABASE_URL, API keys
docker compose up                      # postgres:5433, backend:8001, frontend:5173
```
Manual (no Docker): `uvicorn app.main:app --reload` in `backend/`, `npm run dev` in `frontend/`.

## Tests
```bash
cd backend
pytest                     # 168+ tests, 70% coverage gate enforced in CI
pytest -k "test_signals"   # run specific module
```

## Key Conventions
- All financial values use **`Decimal`** (never `float`)
- Signal deduplication: 15-minute bucket per `(signal_type, outcome_id, timeframe)`
- Rank formula: `signal_score × confidence × recency_weight` (linear decay 0→24h: 1.0→0.3)
- Detectors live in `signals/` and only receive a `SnapshotWindow` — they never query DB directly
- New signal types: subclass `BaseDetector`, register in `jobs/scheduler.py`
- New API route: add router in `api/`, mount in `main.py`
- Migrations: `alembic revision --autogenerate -m "..."` then `alembic upgrade head`
- Config is always via env vars — no hardcoded values in application code
- SSE (not WebSocket) for real-time frontend updates — keep it simple
