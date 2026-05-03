# Signal Market Terminal

Signal Market Terminal (SMT) is a Polymarket-first market-structure and execution-research terminal.

It captures venue state, preserves replayable market truth, measures executable decision quality, and keeps one frozen default strategy as the benchmark for the current "prove the edge" phase. Kalshi remains available as a public cross-venue source for comparison and basis research, but Polymarket is the primary venue focus.

This repo is not an autonomous betting bot. Today it is an operator-facing research, monitoring, and control system with a narrow, fail-closed pilot layer.

## Current posture

- The main goal is still to prove or falsify edge honestly before widening automation.
- The default strategy is frozen. Strategy-health, paper P&L, detector review, and replay-adjusted evidence are the primary decision surfaces.
- Exchange and event time should win over midpoint-only or scheduler-clock inference whenever venue data provides better truth.
- Structure, maker, risk, replay, and pilot layers exist, but they remain conservative and operator-facing first.
- Live trading is disabled by default.
- Pilot mode is disabled by default.
- Manual approval is required by default.
- `exec_policy` is the only currently supported armable pilot family.

## What exists today

### 1. Benchmark evidence loop

- Frozen `confluence` default strategy with explicit `strategy_run` bootstrap
- Pre-trade `ExecutionDecision` audit trail and executable-entry gating
- Paper portfolio, trade history, metrics, P&L curve, and strategy-health surfaces
- Detector keep/watch/cut review support and controlled evidence relaunch tooling
- Honest replay coverage labels and signal-level vs execution-adjusted comparison modes

Primary docs:

- [docs/default-strategy.md](docs/default-strategy.md)
- [docs/runbooks/hetzner-production-ops.md](docs/runbooks/hetzner-production-ops.md)
- [docs/runbooks/default-strategy-remediation.md](docs/runbooks/default-strategy-remediation.md)
- [docs/runbooks/default-strategy-controlled-evidence-relaunch.md](docs/runbooks/default-strategy-controlled-evidence-relaunch.md)

### 2. Polymarket truth stack

- Public Polymarket market-data stream ingest with reconnect, watch reconciliation, and manual resync
- Metadata sync for tick size, min order size, fee state, negative-risk flags, and registry enrichment
- Append-only raw event storage, book snapshots, trade backfill, and open-interest polling
- Deterministic book reconstruction and operator-visible health/status surfaces
- Derived microstructure features and short-horizon labels for research

### 3. Research and execution layers

- Executable EV action policy for cross, post, step-ahead, or skip style decisions
- OMS/EMS foundation, live reconciler, and user-stream handling
- Structure engine with validation and paper-routing controls
- Maker-economics history and advisory quote recommendations
- Risk graph, advisory portfolio optimizer, and inventory controls
- Replay simulator for policy comparison across stored historical truth

### 4. Narrow pilot and control plane

- Pilot config, arming, pause, resume, disarm, and approval workflows
- Kill switch, incidents, guardrail audit trails, scorecards, and readiness reports
- Live-vs-shadow evaluation tied back to stored decision and replay provenance where possible
- Fail-closed defaults that keep the pilot supervised and narrow

## Roadmap status

The repo now has one short active roadmap for near-term Codex work, plus historical buildout records.

Use these docs in this order:

- Current near-term roadmap: [docs/Current roadmaps/codex-active-roadmap.md](docs/Current%20roadmaps/codex-active-roadmap.md)
- Default-strategy measurement contract: [docs/default-strategy.md](docs/default-strategy.md)
- Historical implementation record: [docs/Current roadmaps/polymarket-execution-roadmap.md](docs/Current%20roadmaps/polymarket-execution-roadmap.md) and the phase closeouts in `docs/codex/`

The execution roadmap is no longer just planned work. The repo includes closeout docs from Phase 0 through Phase 12 plus a Phase 12B stabilization pass.

Use these docs as the implementation record:

- [docs/Current roadmaps/codex-active-roadmap.md](docs/Current%20roadmaps/codex-active-roadmap.md)
- [docs/Current roadmaps/polymarket-execution-roadmap.md](docs/Current%20roadmaps/polymarket-execution-roadmap.md)
- [docs/codex/phase-0-closeout.md](docs/codex/phase-0-closeout.md)
- [docs/codex/phase-1-closeout.md](docs/codex/phase-1-closeout.md)
- [docs/codex/phase-2-closeout.md](docs/codex/phase-2-closeout.md)
- [docs/codex/phase-3-closeout.md](docs/codex/phase-3-closeout.md)
- [docs/codex/phase-4-closeout.md](docs/codex/phase-4-closeout.md)
- [docs/codex/phase-5-closeout.md](docs/codex/phase-5-closeout.md)
- [docs/codex/phase-6-closeout.md](docs/codex/phase-6-closeout.md)
- [docs/codex/phase-7a-closeout.md](docs/codex/phase-7a-closeout.md)
- [docs/codex/phase-8a-closeout.md](docs/codex/phase-8a-closeout.md)
- [docs/codex/phase-8b-closeout.md](docs/codex/phase-8b-closeout.md)
- [docs/codex/phase-9-closeout.md](docs/codex/phase-9-closeout.md)
- [docs/codex/phase-10-closeout.md](docs/codex/phase-10-closeout.md)
- [docs/codex/phase-11-closeout.md](docs/codex/phase-11-closeout.md)
- [docs/codex/phase-12-closeout.md](docs/codex/phase-12-closeout.md)
- [docs/codex/phase-12b-stabilization-closeout.md](docs/codex/phase-12b-stabilization-closeout.md)

For agent onboarding, start with [CODEX_START_HERE.md](CODEX_START_HERE.md).

Historical planning docs under `docs/Historic roadmaps/` and the extended roadmap variants are archival context only. They should not drive current scope unless a task explicitly reopens them.

## Architecture

```text
Polymarket public APIs + stream      Kalshi public API
            |                               |
            +---------------+---------------+
                            |
                    Backend API service
                            |
        +-------------------+-------------------+
        |                                       |
   Worker responsibilities                 React frontend
   - scheduler and paper trading           - feed and performance
   - stream ingest and resync              - strategy health and portfolio
   - metadata and raw storage              - markets, analytics, alerts
   - book reconstruction                   - structures
   - features and labels                   - pilot console
   - structure engine                      - live orders
   - risk graph and replay                 - market tape
   - user stream and reconciler            - health
   - pilot supervision
                            |
                         PostgreSQL
```

Backend API entrypoint: `backend/app/main.py`

Worker entrypoint: `backend/app/worker.py`

Frontend entrypoint: `frontend/src/App.jsx`

## Main API areas

The API surface is broader than the original signal-feed product. The main router areas are:

- `/api/v1/signals`
- `/api/v1/markets`
- `/api/v1/alerts`
- `/api/v1/analytics`
- `/api/v1/backtests`
- `/api/v1/performance`
- `/api/v1/paper-trading`
- `/api/v1/events`
- `/api/v1/ingest/polymarket`
- `/api/v1/ingest/polymarket/structure`
- `/api/v1/ingest/polymarket/risk`
- `/api/v1/ingest/polymarket/replay`
- `/api/v1/ingest/polymarket/live`
- `/api/v1/push`
- `/api/v1/health`
- `/metrics`

## Frontend surfaces

The current frontend exposes these operator pages:

- Feed
- Performance
- Strategy Health
- Portfolio
- Markets
- Analytics
- Backtest
- Alerts
- Structures
- Pilot Console
- Live Orders
- Market Tape
- Health

## Safety defaults

The repo remains fail-closed by default. Important defaults include:

| Setting | Default | Meaning |
|---|---|---|
| `POLYMARKET_LIVE_TRADING_ENABLED` | `false` | No live submission unless explicitly enabled |
| `POLYMARKET_LIVE_DRY_RUN` | `true` | Live layer stays in dry-run mode by default |
| `POLYMARKET_LIVE_MANUAL_APPROVAL_REQUIRED` | `true` | Candidate live intents require approval |
| `POLYMARKET_USER_STREAM_ENABLED` | `false` | Authenticated user-stream handling is off unless explicitly enabled |
| `POLYMARKET_PILOT_ENABLED` | `false` | Pilot layer is off by default |
| `POLYMARKET_PILOT_REQUIRE_MANUAL_APPROVAL` | `true` | Pilot approval stays manual by default |
| `POLYMARKET_REPLAY_ENABLED` | `false` | Replay is advisory and off by default |
| `POLYMARKET_EXECUTION_POLICY_ENABLED` | `false` | Execution policy work is present but not globally enabled by default |
| `SCHEDULER_ENABLED` | `false` | The API process should not run scheduler jobs by default |

The default-strategy evidence window is currently anchored by `DEFAULT_STRATEGY_START_AT=2026-04-13T00:00:00+00:00`.

## Quick start

### Canonical environment

The main permanent deployment lives on the Hetzner host `smt-prod-1`, with the checkout at `/opt/signal-market-terminal`.
Use [docs/runbooks/hetzner-production-ops.md](docs/runbooks/hetzner-production-ops.md) for deploys, restarts, health checks, metrics, logs, backups, capture continuity, and the daily operator checklist.

For the production-style stack:

```bash
docker compose -f docker-compose.prod.yml up --build -d
```

Then open `http://localhost`.

### Polymarket capture profile

For a headless Polymarket truth worker:

```bash
cp backend/.env.polymarket-capture.example backend/.env.polymarket-capture
docker compose -f docker-compose.polymarket-capture.yml up -d db worker
docker compose -f docker-compose.polymarket-capture.yml --profile api up -d backend
```

This profile is meant for research truth. It turns on stream continuity, metadata sync, raw storage, trade backfill, open-interest polling, and book reconstruction while leaving features, replay, user stream, pilot, and live trading off initially.

See [docs/runbooks/polymarket-capture.md](docs/runbooks/polymarket-capture.md).

### Oracle free micro profile

For the small scanner-oriented deployment:

```bash
cp backend/.env.oracle-micro.example backend/.env.oracle-micro
docker compose -f docker-compose.oracle-micro.yml up -d db worker
docker compose -f docker-compose.oracle-micro.yml --profile api up -d backend
```

See [docs/runbooks/oracle-free-micro.md](docs/runbooks/oracle-free-micro.md).

### Legacy local Docker flow

Local Docker remains useful for isolated reproduction work, but it is no longer the canonical day-to-day path.

```bash
docker compose up --build
```

The API runs in `backend`, APScheduler jobs run in `worker`, and the frontend is available at `http://localhost:5173`.

### Local development without Docker

```bash
# Start Postgres
docker compose up db

# Backend API
cd backend
cp .env.example .env
pip install -r requirements.txt
alembic upgrade head
set SCHEDULER_ENABLED=false
uvicorn app.main:app --reload

# Worker in a separate terminal
cd backend
set SCHEDULER_ENABLED=true
python -m app.worker

# Frontend in a separate terminal
cd frontend
npm install
npm run dev
```

Local development supports both `localhost` and `127.0.0.1` frontend origins by default. The frontend API base lives in `frontend/.env.development` and can be overridden with `VITE_API_BASE`.

## Validation

Safest first validation command:

```bash
python -m pytest backend/tests/test_config.py -q
```

Common repo-root validation commands:

```bash
npm run frontend:install
npm run frontend:validate
npm run secrets:scan
python -m pytest backend/tests/test_api.py backend/tests/test_structure_engine.py backend/tests/test_structure_phase8b_api.py -q
```

The root `package.json` intentionally owns the frontend workflow so CI and local development use the same entrypoints.

## Tech stack

- Python 3.12
- FastAPI
- SQLAlchemy 2.x
- APScheduler
- PostgreSQL 16 with `asyncpg`
- React 18
- Vite
- React Router
- Recharts
- Docker Compose
- Prometheus metrics
- GitHub Actions CI

## Repository map

- `backend/app/` - API, worker services, ingestion, execution, models, strategy logic
- `backend/tests/` - backend and integration tests
- `frontend/src/` - React application and operator pages
- `docs/codex/` - phase closeouts and implementation progress record
- `docs/runbooks/` - deployment and operational runbooks
- `scripts/` - repository utilities such as secret scanning

## Related docs

- [CHANGELOG.md](CHANGELOG.md)
- [CODEX_START_HERE.md](CODEX_START_HERE.md)
- [docs/Current roadmaps/codex-active-roadmap.md](docs/Current%20roadmaps/codex-active-roadmap.md)
- [docs/default-strategy.md](docs/default-strategy.md)
- [docs/Current roadmaps/polymarket-execution-roadmap.md](docs/Current%20roadmaps/polymarket-execution-roadmap.md)
