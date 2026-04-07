# Day 11 Handoff — Tech Debt & Production Hardening

**Date:** 2026-04-07
**Tests:** 249 passed, 0 failed (74 deprecation warnings — asyncio/Python 3.14, no action needed)

---

## What was done

### 1. Migration 012: `timeframe` on `backtest_signals`

- **Migration:** `backend/alembic/versions/012_add_timeframe_to_backtest_signals.py`
  - Adds `timeframe VARCHAR(8) NOT NULL DEFAULT '30m'` to `backtest_signals`
  - Index: `ix_bt_signal_timeframe`
- **Model:** `backend/app/models/backtest.py` — added `timeframe` field to `BacktestSignal`
- **Engine:** `backend/app/backtesting/engine.py` — passes `timeframe="30m"` when creating BacktestSignal instances

**QA Bugs fixed:** BUG-1 (partially — signals.timeframe was already in migration 011, this completes the backtest side)

### 2. Migration 013: Portfolio `Float` -> `Numeric(20,8)`

- **Migration:** `backend/alembic/versions/013_portfolio_float_to_numeric.py`
  - Converts 9 columns from `Float` to `Numeric(20,8)`:
    - `positions`: quantity, avg_entry_price, current_price, unrealized_pnl, exit_price, realized_pnl
    - `trades`: quantity, price, fees

### 3. Float -> Decimal in Portfolio stack

- **Model:** `backend/app/models/portfolio.py` — all 9 financial columns now `Mapped[Decimal]` with `Numeric(20,8)`
- **Service:** `backend/app/portfolio/service.py`
  - All arithmetic uses `Decimal` (no more `float()` casts)
  - Added `_to_decimal()` helper for backward-compatible `float | Decimal` inputs
  - All zero-value constants use `Decimal("0")` instead of `0.0`
- **Router:** `backend/app/api/portfolio.py`
  - Request schemas accept `Decimal` inputs from JSON
  - Response schemas use `float` for JSON serialization (avoids string encoding of Decimals)
- **Tests:** `backend/tests/test_portfolio.py`
  - Updated assertions to use `pytest.approx()` for Decimal/float comparison

**QA Bugs fixed:** BUG-2 (positions table now uses correct types), Sprint replan priority #1 (Float->Decimal P&L)

### 4. Route collision fix: `/backtests/sweep`

- **File:** `backend/app/api/backtest.py`
  - Moved `POST /sweep` endpoint **before** `GET /{run_id}` and other parameterized routes
  - FastAPI now correctly routes `/sweep` instead of treating it as a UUID path parameter

**QA Bugs fixed:** BUG-5 (routing collision causing 422)

---

## What was NOT done (out of scope for Day 11)

- **Docker rebuild + smoke test** — requires running Docker daemon. Migrations are written and ready; run `docker compose build --no-cache && docker compose up` then `alembic upgrade head` to apply.
- **BUG-3** (wrong route `/portfolio/positions` in docs) — this is a documentation issue, not a code bug. Actual routes are `/api/v1/positions` and `/api/v1/portfolio/summary`.
- **BUG-4** (signal types `ofi`, `arbitrage`, `whale` not in DB) — these detectors haven't fired yet. They use internal type names (`order_flow_imbalance`, etc.). Not a bug.

---

## Migrations to run on deploy

```bash
cd backend
alembic upgrade head   # applies 012 + 013
```

Migration 012: adds `timeframe` column to `backtest_signals`
Migration 013: converts 9 Float columns to Numeric(20,8) in `positions` and `trades`

Both migrations are backward-compatible (server_default on new column, in-place type conversion).

---

## Files changed

| File | Change |
|------|--------|
| `backend/alembic/versions/012_add_timeframe_to_backtest_signals.py` | NEW — migration |
| `backend/alembic/versions/013_portfolio_float_to_numeric.py` | NEW — migration |
| `backend/app/models/backtest.py` | Added `timeframe` to BacktestSignal |
| `backend/app/models/portfolio.py` | Float -> Decimal on all financial columns |
| `backend/app/backtesting/engine.py` | Passes `timeframe` when creating BacktestSignal |
| `backend/app/portfolio/service.py` | Decimal arithmetic throughout, `_to_decimal()` helper |
| `backend/app/api/portfolio.py` | Decimal request schemas, float response serialization |
| `backend/app/api/backtest.py` | Moved `/sweep` before `/{run_id}` routes |
| `backend/tests/test_portfolio.py` | Updated assertions for Decimal compatibility |
| `docs/day11-plan.md` | Implementation plan |
| `docs/handoff-day11.md` | This file |

---

## Next: Day 12

Mobile-Responsive Frontend + PWA (per sprint-replan-v0.4.md)
