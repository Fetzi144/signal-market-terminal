# Handoff: Signal Market Terminal v0.4.0

**Release Date:** 2026-04-08
**Tag:** `v0.4.0`
**Branch:** `develop`

---

## What Was Built (Days 1-14)

### Core Infrastructure (Days 1-2)
- FastAPI backend with async SQLAlchemy, PostgreSQL, Alembic migrations
- React + Vite frontend with Recharts
- Docker Compose dev/prod setup with Prometheus monitoring
- 5 signal detectors: Price Move, Volume Spike, Spread Change, Liquidity Vacuum, Deadline Near
- Signal ranking, deduplication (15-min bucket), 4-horizon evaluation (15m/1h/4h/24h)
- Multi-channel alerting: logger, webhook, Telegram
- REST API with CSV export, SSE real-time streaming

### Features Added (Days 3-12)
| Day | Feature | Key Files |
|-----|---------|-----------|
| 3 | Backtesting engine + frontend | `backtesting/engine.py`, `api/backtest.py`, `pages/Backtest.jsx` |
| 4 | Performance dashboard | `api/performance.py`, `pages/Performance.jsx` |
| 5 | Order Flow Imbalance detector | `signals/order_flow.py` |
| 6 | Portfolio tracker backend | `portfolio/service.py`, `api/portfolio.py` |
| 7 | Portfolio frontend | `pages/Portfolio.jsx` |
| 8 | Discord + Web Push alerts | `alerts/discord_alert.py`, `alerts/push_alert.py` |
| 9 | Whale/smart money tracking | `models/whale.py`, `signals/whale_tracker.py` |
| 10 | Multi-timeframe detection | `signals/base.py` (timeframe support), confluence scoring |
| 11 | Tech debt fixes | Decimal P&L, route collision, volume spike baseline |
| 12 | Mobile-responsive PWA | Responsive CSS, service worker, manifest.json |

### Quality (Days 13-14)
- Fixed dedupe collision bug in alerts test
- 275 tests passing (0 failures), ~70% coverage
- Integration tests: backtest (4), portfolio (3), alerts (5), multi-timeframe (4), API coverage (10)
- CHANGELOG.md with full v0.1.0 → v0.4.0 history

---

## Architecture Overview

```
Client (React PWA)
  ↓ REST / SSE
FastAPI Backend
  ├── Connectors (Polymarket, Kalshi) → market discovery + price snapshots
  ├── Detectors (7 types) → signal candidates
  ├── Ranking → dedupe + rank_score
  ├── Evaluation → 4-horizon price tracking
  ├── Resolution → ground-truth accuracy
  ├── Backtesting → historical replay + parameter sweep
  ├── Portfolio → position lifecycle + P&L
  ├── Alerts → Discord, Telegram, Web Push, Webhook
  └── Analytics → accuracy, correlation, performance
  ↓
PostgreSQL (async via SQLAlchemy 2)
```

---

## How to Run

### Docker (recommended)
```bash
cp backend/.env.example backend/.env  # fill DATABASE_URL, API keys
docker compose up
# backend: http://localhost:8001
# frontend: http://localhost:5173
```

### Manual
```bash
cd backend && uvicorn app.main:app --reload  # port 8001
cd frontend && npm run dev                    # port 5173
```

### Tests
```bash
cd backend && python -m pytest -q --tb=short  # 275 tests
```

---

## Known Limitations

1. **No ML scoring** — deferred to v0.5.0 (insufficient training data for reliable model)
2. **Single aggregate P&L** — no per-timeframe breakdown in portfolio view
3. **Signal-only tool** — no order execution; users place trades manually
4. **Kalshi OAuth2** — requires manual per-user setup (no automated token refresh)
5. **SQLite in tests** — DISTINCT ON warnings (PostgreSQL-only feature, silently ignored)

---

## What's Next (v0.5.0 Candidates)

- ML signal scoring with historical training data
- Per-timeframe P&L breakdown in portfolio
- Automated Kalshi OAuth2 token refresh
- Alert routing rules (per-signal-type channel config)
- Backtesting with portfolio simulation (not just signal accuracy)
- WebSocket upgrade from SSE for bidirectional comms

---

## File Counts

| Area | Files | Tests |
|------|-------|-------|
| Backend Python | ~50 | 275 |
| Frontend JSX | ~15 | — |
| Alembic migrations | 13 | — |
| Docker configs | 2 | — |
| Documentation | ~10 | — |

---

## Git History Summary

```
v0.1.0  Days 1-2   Core infrastructure, 5 detectors, 40 tests
v0.2.0  Days 2-2   Analytics, SSE, circuit breaker, 90 tests
v0.3.0  Days 2-2   Resolution, arbitrage, HMAC, 168 tests
v0.4.0  Days 3-14  Backtesting, portfolio, OFI, whale, PWA, 275 tests
```
