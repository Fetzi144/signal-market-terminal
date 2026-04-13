# Day 13 Research: Integration Tests + Polish

**Date:** 2026-04-07  
**Goal:** Assess what integration tests are needed for a clean v0.4.0 release  
**Current State:** 249 unit tests passing, 74 asyncio deprecation warnings

---

## 1. Current Test State

### Overall Coverage
```
Test Count:   249 passing, 0 failing
Test Types:   Unit (230+), Integration (10+), API (20+)
Coverage:     ~70% (enforced gate in CI: 70%)
```

### Test Files by Domain
| Domain | Files | Focus |
|--------|-------|-------|
| Signal Detection | 8 files | Unit tests of detectors (price_move, volume_spike, ofi, arbitrage, etc) |
| Portfolio | 1 file | Unit + API tests for position management, P&L calculation, market resolution |
| Backtest | 1 file | Unit + API tests for backtest runs and parameter sweep |
| Alerts | 3 files | Unit tests for Discord, Telegram, Web Push, Webhook delivery |
| API/Routing | 1 file | Integration-style tests of signal feed, markets, export, analytics endpoints |
| Integration | 1 file | Full pipeline: snapshots → detect → signal → evaluate → resolve |

---

## 2. Endpoints Inventory

### All 40 Endpoints Across Routers

| Router | Endpoints | Test Status |
|--------|-----------|-------------|
| **health.py** (1) | GET /health | ✓ Tested |
| **signals.py** (5) | GET "", GET /{id}, GET /types, GET /timeframes, GET /export/csv | ✓ Mostly tested (no /export/csv test found) |
| **markets.py** (6) | GET "", GET /platforms, GET /{id}, GET /{id}/snapshots, GET /{id}/chart-data, GET /export/csv | ✓ Mostly tested (chart-data not found) |
| **portfolio.py** (7) | GET /portfolio/summary, GET /positions, GET /positions/{id}, POST /positions, POST /positions/{id}/trades, PUT /positions/{id}/close, GET /portfolio/export/csv | ✓ Tested (no export/csv test) |
| **backtest.py** (6) | GET "", POST "", GET /{id}, GET /{id}/signals, POST /sweep, DELETE /{id} | ✓ Tested |
| **alerts.py** (1) | GET /recent | ✗ No API test (only unit tests of alerter internals) |
| **analytics.py** (4) | GET /platform-summary, GET /signal-accuracy, GET /timeframe-accuracy, GET /correlated-signals | ✓ Some tested (no /correlated-signals test) |
| **performance.py** (1) | GET /summary | ✓ Tested |
| **push.py** (3) | GET /vapid-key, POST /subscribe, DELETE /subscribe | ✗ No API tests (only unit alert tests) |
| **sse.py** (1) | GET /signals | ✓ Tested |

### Gaps Summary
- **No API tests:** `/alerts/recent`, `/push/*` (get vapid-key, subscribe, unsubscribe)
- **No endpoint tests:** `/signals/export/csv`, `/markets/{id}/chart-data`, `/markets/export/csv`, `/portfolio/export/csv`
- **No E2E tests:** Multi-endpoint flows (signal → alert → push delivery)

---

## 3. Unit-Tested Features Needing E2E Validation

### Feature: Multi-Timeframe Signal Detection
**Status:** Unit tested, partially API tested

- **Unit Tests:** `test_multi_timeframe.py` (7 tests)
  - Confluence scoring logic ✓
  - Timeframe deduping ✓
  - Confluence bonus cap at 1.0 ✓
  
- **API Tests:** 
  - GET `/signals?timeframe=30m` filter ✗
  - GET `/signals/timeframes` endpoint ✗ (exists, untested)
  - GET `/analytics/timeframe-accuracy` endpoint ✓ (partially tested)

- **Missing E2E:** Test that same signal fires on 2+ timeframes in real detector run, confluence bonus applied, API returns correct results

### Feature: Backtesting → Parameter Sweep → Results
**Status:** Unit tested, endpoint tested, but no full-flow integration

- **Unit Tests:** `test_backtest.py` (15 tests)
  - Create backtest, list, delete ✓
  - Sweep parameter combinations ✓
  - Win rate calculation ✓
  
- **API Tests:** All CRUD endpoints tested
  
- **Missing E2E:** 
  1. Seed realistic market data with 20+ snapshots
  2. Create backtest with multi-detector config
  3. Run backtest (verify signals detected)
  4. Run sweep (verify all combinations generated)
  5. Verify results match expected accuracy/Sharpe/max_drawdown

### Feature: Portfolio Management (Open → Add → Close → Resolve)
**Status:** Unit tested, partial API tested, needs full flow

- **Unit Tests:** `test_portfolio.py` (15 tests)
  - Open position with weighted avg entry ✓
  - Add trades (buy/sell), update weighted avg ✓
  - Partial close, full close ✓
  - Market resolution auto-closes ✓
  - P&L calculation with Decimal precision ✓

- **API Tests:** 
  - Create position ✓
  - List positions ✓
  - Get position detail ✓
  - Add trade ✗ (not found)
  - Close position ✓

- **Missing E2E:** Full API flow
  1. POST /positions (open)
  2. POST /positions/{id}/trades (add buy)
  3. POST /positions/{id}/trades (add sell)
  4. GET /positions/{id} (verify weighted avg price, P&L)
  5. PUT /positions/{id}/close (close)
  6. Verify realized P&L matches manual calculation

### Feature: Alert Delivery (Signal → Discord/Push/Telegram)
**Status:** Unit tested, no E2E

- **Unit Tests:** `test_alerts.py`, `test_discord_alert.py`, `test_push_alert.py`
  - Discord webhook payload formatting ✓
  - Telegram message sending ✓
  - Web Push notification queue ✓
  
- **API Tests:** None for push subscribe/unsubscribe
  
- **Missing E2E:**
  1. Fire signal (manually or via detector)
  2. Alert service runs (mocked webhook call verified)
  3. Push notification queued for subscribed clients
  4. Verify alert details match signal data

### Feature: Signal Evaluation & Resolution
**Status:** Unit tested, has basic E2E

- **Unit Tests:** `test_evaluation.py`, `test_resolution.py`
  - Price change calculation ✓
  - Correct/incorrect resolution ✓

- **Integration Tests:** `test_integration.py`
  - Full pipeline snapshot → detect → signal → evaluate → resolve ✓

- **Missing:** None identified (already has good E2E)

---

## 4. Export CSV Endpoints

**Status:** Partially tested, 3 endpoints untested

These endpoints exist but have NO dedicated tests:

1. **GET /signals/export/csv** — Signal data as CSV
2. **GET /markets/export/csv** — Market data as CSV  
3. **GET /portfolio/export/csv** — Position data as CSV

Quick validation needed:
- Response has correct headers (`Content-Disposition`, `text/csv`)
- CSV headers present
- Data rows match API response

---

## 5. What Needs Integration Tests (Day 13 Plan)

### Test 1: Backtest Full Flow (30 min)
```
backtest_integration_test.py:
  - Seed 25 price snapshots across 2 outcomes (realistic volatility)
  - Create backtest with detectors: price_move, volume_spike, arbitrage
  - Verify backtest runs successfully (status: completed)
  - Verify signals detected match detector logic
  - Run parameter sweep (3x3 combinations)
  - Verify sweep results ranked by Sharpe ratio
```

### Test 2: Portfolio Full Flow (30 min)
```
portfolio_integration_api_test.py:
  - API flow: open → add_trade → add_trade → close
  - Verify position state transitions
  - Verify P&L calculated using Decimal precision
  - Test partial close (10% reduction)
  - Test full close
  - Verify realized vs unrealized P&L
```

### Test 3: Alert Integration (20 min)
```
alert_integration_test.py:
  - Seed signal in DB
  - Trigger alert job
  - Verify Discord webhook was "called" (mock)
  - Verify push notification queued
  - Verify alert contains signal details
```

### Test 4: Multi-Timeframe Confluence (20 min)
```
multi_timeframe_integration_test.py:
  - Create market with 3 outcomes
  - Seed snapshots that trigger same signal on 30m AND 4h
  - Verify both signals created with different timeframes
  - Verify confluence bonus applied (rank_score > single-TF signal)
  - Test API filter: GET /signals?timeframe=30m returns only 30m
  - Test API: GET /signals/timeframes returns ["30m", "4h"]
```

### Test 5: Missing Endpoint Tests (15 min)
```
api_coverage_test.py:
  - /alerts/recent (GET)
  - /push/vapid-key (GET)
  - /push/subscribe (POST)
  - /push/unsubscribe (DELETE)
  - /signals/export/csv (GET) — validate CSV format
  - /markets/export/csv (GET) — validate CSV format
  - /portfolio/export/csv (GET) — validate CSV format
```

---

## 6. Version Bump Checklist for v0.4.0

### Files to Update

| File | Current | Target | Notes |
|------|---------|--------|-------|
| `CLAUDE.md` | `v0.3.0` | `v0.4.0` | Version line at top |
| `backend/pyproject.toml` | `0.3.0` | `0.4.0` | version = "..." field |
| `frontend/package.json` | `0.3.0` | `0.4.0` | version field |
| `CHANGELOG.md` | ❌ MISSING | NEW | Create with v0.4.0 entry covering Days 1-12 |
| `README.md` | v0.3 docs | Update | Add sections: Features, Installation, Quick Start, Backtesting Guide, Portfolio Management, Mobile/PWA, Known Limitations, Roadmap |

### CHANGELOG.md v0.4.0 Entry (Scaffold)
```markdown
## [0.4.0] — 2026-04-07

### Added
- **Backtesting Engine**: Full replay of historical market data with parameter sweep
- **Portfolio Tracking**: Position management with Decimal-precision P&L
- **Multi-Timeframe Detection**: Signals fire on 30m, 4h, 1h timeframes with confluence scoring
- **Web Push Notifications**: Subscribe → signal → instant mobile alert
- **Performance Dashboard**: Win rate, Sharpe ratio, accuracy by signal type
- **Mobile-Responsive UI**: PWA-installable frontend with 375px→desktop breakpoints
- **Order Flow Imbalance Detector**: OFI-based signal detection
- **Analytics**: Accuracy by signal type, platform, timeframe; correlated signals
- **Docker Support**: Full docker compose setup with Prometheus monitoring

### Fixed
- Portfolio P&L calculation using Decimal (was Float) — fixes rounding errors
- Backtest signal routing (moved /sweep before /{run_id})
- Volume spike detector baseline window calculation

### Security
- Rate limiting on all public endpoints (10 req/sec)
- HMAC signature verification on webhook endpoints
- Web Push uses VAPID keys (RFC 8292)

### Known Limitations
- ML signal scoring deferred to v0.5.0 (insufficient training data)
- Per-timeframe P&L in portfolio view (single aggregate P&L currently)
- No order execution (signal-only tool, manual order entry)
- Kalshi API requires manual OAuth2 setup per user

### Tested
- 249 unit tests (70% coverage)
- 4 integration tests (E2E signal detection → API → DB)
- Docker cold-start smoke test validated
```

---

## 7. Release Readiness Checklist (Day 14)

### Pre-Release (Day 13 EOD)

- [ ] All 4+ integration tests written and green
- [ ] All missing API endpoint tests added
- [ ] Zero linting warnings (`ruff check .`)
- [ ] Test coverage >= 70% (automated gate)
- [ ] CHANGELOG.md written and reviewed
- [ ] README.md updated with v0.4.0 features
- [ ] Docker smoke test: `docker compose up && curl http://localhost:8001/api/v1/health`

### Release Day (Day 14)

- [ ] Version bumped: CLAUDE.md, pyproject.toml, package.json
- [ ] Git tag: `git tag -a v0.4.0 -m "Release v0.4.0"`
- [ ] Final test run: `pytest -q --tb=short` (249+ tests)
- [ ] Docker rebuild: `docker compose build --no-cache`
- [ ] Fresh deploy test: stop containers, `docker compose up` on clean state
- [ ] Handoff doc: `docs/handoff-v0.4.0.md` written

---

## 8. Accumulated Bugs & Fixes (Day 11-12 Completed)

| Bug | Fixed | Impact |
|-----|-------|--------|
| Portfolio Float → Decimal | Day 11 | P&L now precise to 8 decimals (matches financial standard) |
| BacktestSignal.timeframe missing | Day 11 | Multi-timeframe backtesting now works |
| Route collision `/sweep` | Day 11 | Parameter sweep endpoint now reachable |
| Volume spike baseline window | Day 10 | Detector now produces candidates (was collapsed) |
| Asyncio deprecation warnings | Ongoing | 74 warnings (Python 3.16 compat, non-blocking) |

---

## 9. Recommended Day 13 Schedule

**Morning (4 hours):**
1. Write 4 integration test files (1 per feature)
2. Add 5 missing endpoint tests

**Afternoon (2 hours):**
1. Run full test suite, fix any failures
2. Ruff lint pass, cleanup warnings
3. Write CHANGELOG.md + README.md updates

**EOD:**
1. Commit: `docs: Day 13 integration tests + polish`
2. Verify: 253+ tests, lint clean, coverage >= 70%

---

## 10. Summary

**Ready for Day 13:**
- ✓ Sprint plan clear (sprint-replan-v0.4.md)
- ✓ Tech debt fixed (Day 11)
- ✓ Mobile frontend done (Day 12)
- ✓ 249 unit tests passing
- ✓ Some integration tests exist (but need expansion)

**Action Items:**
1. Add 4 integration test files (backtest, portfolio, alert, multi-timeframe)
2. Test 5+ missing API endpoints
3. Update CHANGELOG.md, README.md
4. Version bump (ready for Day 14)

**Risk Factors:**
- None identified (all pre-reqs complete)
