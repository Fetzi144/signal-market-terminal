# Signal Market Terminal — Checkpoint 2026-04-07

## Current Version & Tags

- **Current HEAD**: 3ebcb9e (docs: add CLAUDE.md onboarding file for new agent sessions)
- **Latest Tag**: v0.3.0 (released at commit 5b43ea0)
- **Available Tags**: v0.2.0, v0.3.0
- **Sprint Status**: v0.4.0 in progress

---

## Commits Since v0.3.0

**12 commits built since v0.3.0 release** (5b43ea0):

```
3ebcb9e docs: add CLAUDE.md onboarding file for new agent sessions
890c09a feat: Day 9 — Whale/Smart Money Tracking with on-chain Polygon data
b5de9b5 feat: Day 8 — Discord webhooks and Web Push notifications for alerts
f70743f feat: Day 7 — Portfolio Tracker frontend with position management and P&L charts
a4fef16 feat: Day 6 — Portfolio Position Tracker backend
804cf26 feat: Day 5 — Order Flow Imbalance (OFI) detection signal
605e9cf feat: Day 4 — Performance Dashboard with win rates and detector rankings
8105320 feat: Day 3 — backtesting frontend with charts and sweep comparison
101aefe feat: Day 2 — backtesting API and parameter sweep
c891b67 feat: Day 1 — backtesting replay engine core infrastructure
3ea8e51 docs: add 14-day sprint plan for v0.4.0
```

---

## Sprint Plan vs. Reality: Days DONE vs. OOPEN

### v0.4.0 Sprint Goals (14 Days)

| Day | Feature | Status | Commit |
|-----|---------|--------|--------|
| **Day 1** | Backtesting Engine: Core Replay Infrastructure | ✅ DONE | c891b67 |
| **Day 2** | Backtesting API & Parameter Sweep | ✅ DONE | 101aefe |
| **Day 3** | Backtesting Frontend (Charts, Sweep UI) | ✅ DONE | 8105320 |
| **Day 4** | Performance Dashboard (Win Rates, Detector Ranking) | ✅ DONE | 605e9cf |
| **Day 5** | Order Flow Imbalance (OFI) Detection Signal | ✅ DONE | 804cf26 |
| **Day 6** | Portfolio Position Tracker Backend | ✅ DONE | a4fef16 |
| **Day 7** | Portfolio Tracker Frontend (P&L Charts, Position Mgmt) | ✅ DONE | f70743f |
| **Day 8** | Discord Webhooks & Web Push Notifications | ✅ DONE | b5de9b5 |
| **Day 9** | Whale/Smart Money Tracking (On-chain Polygon) | ✅ DONE | 890c09a |
| **Day 10** | Multi-Timeframe Analysis | ⏳ OPEN | — |
| **Day 11-12** | ML Signal Scoring (Learned Weights) | ⏳ OPEN | — |
| **Day 13** | Mobile-Optimized Frontend | ⏳ OPEN | — |
| **Day 14** | Integration Testing + Release v0.4.0 | ⏳ OPEN | — |

**Progress: 9/14 days complete (64%)**

---

## Test Results

```
FAILED backend/tests/test_volume_spike.py::test_volume_above_3x_fires
FAILED backend/tests/test_volume_spike.py::test_low_baseline_confidence_penalty
FAILED backend/tests/test_volume_spike.py::test_medium_baseline_confidence_penalty
FAILED backend/tests/test_volume_spike.py::test_score_capped_at_one
FAILED backend/tests/test_volume_spike.py::test_log_scaling_higher_spike_higher_score
FAILED backend/tests/test_volume_spike_extra.py::test_volume_spike_fires

6 FAILED | 235 PASSED | 72 warnings
```

**Status**: ⚠️ **Regression detected** — 6 volume spike detector tests failing (likely due to uncommitted scoring changes).

---

## Uncommitted Changes

**8 files modified | 226 insertions (+) | 24 deletions (-):**

```
backend/app/api/signals.py          | 16 ++
backend/app/config.py               |  6 ++
backend/app/models/signal.py        |  6 ⇄ (2 del)
backend/app/ranking/scorer.py       | 64 +++ (major refactor in progress)
backend/app/signals/base.py         | 28 ++
backend/app/signals/order_flow.py   | 45 ⇄ (6 del)
backend/app/signals/price_move.py   | 42 ⇄ (5 del)
backend/app/signals/volume_spike.py | 43 ⇄ (11 del)
```

**Lines Changed**: Majority concentrated in `ranking/scorer.py` (scoring overhaul in progress).

---

## Context from CLAUDE.md

The project maintains strict conventions:

- **Financial values**: Always use `Decimal`, never `float`
- **Signal deduplication**: 15-minute bucket per `(signal_type, outcome_id, timeframe)`
- **Rank formula**: `signal_score × confidence × recency_weight` (linear decay 0→24h: 1.0→0.3)
- **Signal detectors**: Subclass `BaseDetector`, register in `jobs/scheduler.py`
- **Configuration**: Always via env vars, no hardcoded values
- **Frontend**: SSE (not WebSocket) for real-time updates
- **Migrations**: `alembic revision --autogenerate -m "..."` then `alembic upgrade head`
- **Tests**: 168+ tests, 70% coverage gate enforced in CI

**Key Point**: Detectors only receive `SnapshotWindow` — never query DB directly.

---

## Next Logical Step

### ⚠️ Immediate Action Required:

1. **Commit or revert uncommitted scoring changes** — The 6 test failures in `test_volume_spike.py` correlate with changes in `scorer.py` and signal files. These need to be:
   - Either finalized and committed (with tests passing), OR
   - Reverted and re-approached

2. **Inspect the scorer.py refactor** — Determine if the changes are intentional (part of Day 10+ work) or accidental.

### Recommended Next Sprint Days (Pending Test Fix):

- **Day 10**: Multi-Timeframe Analysis — leverage backtesting infrastructure for 1h/4h/24h signals
- **Day 11-12**: ML Signal Scoring — train classifier on historical resolution data + backtest results
- **Day 13**: Mobile-Optimized Frontend — responsive layout + touch-friendly controls
- **Day 14**: Full integration test suite + v0.4.0 release tag

---

## Checkpoint Metadata

- **Generated**: 2026-04-07
- **Working Directory**: `/c/Code/Signal Market Terminal`
- **Git Status**: Clean (only uncommitted code changes, no untracked files)
- **Python Version**: 3.14 (via system)
- **Stack**: FastAPI, SQLAlchemy 2, PostgreSQL, React 18 + Vite, APScheduler
