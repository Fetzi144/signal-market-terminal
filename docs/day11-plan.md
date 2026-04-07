# Day 11 Implementation Plan — Tech Debt & Production Hardening

*Date: 2026-04-07*
*Inputs: docs/sprint-replan-v0.4.md, docs/qa-report-v0.4.md*

---

## Fix 1: Alembic Migration — `timeframe` Column on `signals` + `backtest_signals`

**Problem:** Migration `011_add_signal_timeframe.py` added `timeframe` to `signals` but NOT to `backtest_signals`. The ORM model `BacktestSignal` lacks `timeframe` entirely. QA report BUG-1: `GET /api/v1/signals` → 500 (`column signals.timeframe does not exist`).

**Note:** Migration 011 already handles `signals.timeframe` in the DB. The real issue is:
1. The `backtest_signals` table needs a `timeframe` column too.
2. The `BacktestSignal` ORM model needs the column added.
3. The backtest engine needs to populate `timeframe` when creating signals.

### Changes

| File | Line(s) | Change |
|------|---------|--------|
| `backend/alembic/versions/012_add_timeframe_to_backtest_signals.py` | NEW | `ALTER TABLE backtest_signals ADD COLUMN timeframe VARCHAR(8) NOT NULL DEFAULT '30m'` |
| `backend/app/models/backtest.py` | after L47 | Add `timeframe: Mapped[str] = mapped_column(String(8), nullable=False, default="30m")` |
| `backend/app/backtesting/engine.py` | L107-119 | Add `timeframe="30m"` to `BacktestSignal(...)` constructor (default; detectors don't vary TF in backtest yet) |

---

## Fix 2: Alembic Migration — Portfolio `Float` → `Numeric(20,8)`

**Problem:** Migration `008_add_portfolio_tables.py` created `positions` and `trades` with `sa.Float` columns. CLAUDE.md mandates `Decimal` for all financial values. QA report confirms portfolio tables exist in Docker DB but use Float. Sprint replan identifies this as highest-priority tech debt.

**Note:** Since migration 008 already ran in production Docker, we add a new migration (013) that ALTERs existing columns from Float to Numeric(20,8). We do NOT modify migration 008.

### Changes

| File | Line(s) | Change |
|------|---------|--------|
| `backend/alembic/versions/013_portfolio_float_to_numeric.py` | NEW | `ALTER COLUMN ... TYPE NUMERIC(20,8)` for all 9 Float columns across `positions` (6) and `trades` (3) |

**Columns altered:**
- `positions.quantity` Float → Numeric(20,8)
- `positions.avg_entry_price` Float → Numeric(20,8)
- `positions.current_price` Float → Numeric(20,8)
- `positions.unrealized_pnl` Float → Numeric(20,8)
- `positions.exit_price` Float → Numeric(20,8)
- `positions.realized_pnl` Float → Numeric(20,8)
- `trades.quantity` Float → Numeric(20,8)
- `trades.price` Float → Numeric(20,8)
- `trades.fees` Float → Numeric(20,8)

---

## Fix 3: Float → Decimal in Portfolio Model, Service, and Router

**Problem:** ORM model uses `Float` type hints and SQLAlchemy `Float` columns. Service does `float()` casts. Router Pydantic schemas use `float` types. All must become `Decimal`/`Numeric`.

### Changes

### 3a: Model — `backend/app/models/portfolio.py`

| Line(s) | Change |
|---------|--------|
| L1-8 (imports) | Add `from decimal import Decimal`, add `Numeric` to SQLAlchemy imports, remove `Float` |
| L31 | `quantity: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)` |
| L32 | `avg_entry_price: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)` |
| L33 | `current_price: Mapped[Decimal \| None] = mapped_column(Numeric(20, 8), nullable=True)` |
| L34 | `unrealized_pnl: Mapped[Decimal \| None] = mapped_column(Numeric(20, 8), nullable=True)` |
| L36 | `exit_price: Mapped[Decimal \| None] = mapped_column(Numeric(20, 8), nullable=True)` |
| L37 | `realized_pnl: Mapped[Decimal \| None] = mapped_column(Numeric(20, 8), nullable=True)` |
| L63 | `quantity: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)` |
| L64 | `price: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)` |
| L65 | `fees: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False, default=Decimal("0"))` |

### 3b: Service — `backend/app/portfolio/service.py`

| Line(s) | Change |
|---------|--------|
| L1-3 (imports) | Add `from decimal import Decimal` |
| L22-24 | Change `quantity: float` → `quantity: Decimal`, `price: float` → `price: Decimal` in `open_position()` signature |
| L56-58 | Same for `add_to_position()` signature |
| L88-91 | Same for `close_position()` signature |
| L103 | `pnl = ...` — already works with Decimal (operator overloading) |
| L107 | `(position.realized_pnl or Decimal("0"))` instead of `0.0` |
| L120 | `position.quantity = Decimal("0")` |
| L123 | `position.unrealized_pnl = Decimal("0")` |
| L148 | `pos.current_price = latest_price` (remove `float()` cast — it's already Decimal from DB) |
| L183 | `resolution_price = Decimal("1") if latest_price >= Decimal("0.5") else Decimal("0")` |
| L191 | `(pos.realized_pnl or Decimal("0")) + pnl` |
| L194 | `pos.unrealized_pnl = Decimal("0")` |
| L196 | `pos.quantity = Decimal("0")` |
| L269,277 | Remove `0.0` defaults in coalesce → use `Decimal("0")` or keep SQL coalesce (returns Decimal from Numeric col) |
| L289 | `win_rate` calculation: keep as float (percentage display) |
| L294-295 | Remove `float()` casts — return Decimal values directly |

### 3c: Router — `backend/app/api/portfolio.py`

| Line(s) | Change |
|---------|--------|
| L1-9 (imports) | Add `from decimal import Decimal` |
| L28 | `quantity: Decimal = Field(..., gt=0)` |
| L29 | `price: Decimal = Field(..., ge=0, le=1)` |
| L36 | `quantity: Decimal = Field(..., gt=0)` |
| L37 | `price: Decimal = Field(..., ge=0, le=1)` |
| L38 | `fees: Decimal = Field(default=Decimal("0"), ge=0)` |
| L43 | `quantity: Decimal = Field(..., gt=0)` |
| L44 | `price: Decimal = Field(..., ge=0, le=1)` |
| L45 (ClosePositionRequest) | `fees: Decimal = Field(default=Decimal("0"), ge=0)` (not a typo, not L44) |
| L53 | `quantity: Decimal`, `price: Decimal`, `fees: Decimal` in TradeOut |
| L70-76 | All `float` → `Decimal` in PositionOut |
| L97-98 | `total_unrealized_pnl: Decimal`, `total_realized_pnl: Decimal` in PortfolioSummaryOut |

### 3d: Tests — `backend/tests/test_portfolio.py`

| Line(s) | Change |
|---------|--------|
| L40 | `assert pos.avg_entry_price == Decimal("0.60")` (or keep approx with Decimal) |
| L65 | `assert abs(pos.avg_entry_price - Decimal("0.70")) < Decimal("0.001")` |
| L84,104,etc | Same pattern — use `Decimal` in assertions or keep float-tolerance comparisons (both work since Decimal supports comparison with float) |

**Decision:** Keep existing float-tolerance test assertions (`abs(x - 0.70) < 0.001`). Decimal compares fine with float literals. Only change if tests fail.

---

## Fix 4: Route Fix — `/backtests/sweep` Before `/{run_id}`

**Problem:** QA report BUG-5: `POST /backtests/sweep` returns 422 because FastAPI matches it as `/{run_id}` with `run_id="sweep"`. The `/sweep` route (L275) is defined AFTER `/{run_id}` (L201).

### Changes

| File | Line(s) | Change |
|------|---------|--------|
| `backend/app/api/backtest.py` | L275-end | Move the entire `create_sweep()` function (and its `SweepRequest` usage) to BEFORE L201 (before the first `/{run_id}` route) |

Specifically: cut lines 275-340 (the `@router.post("/sweep", ...)` function) and paste them between the `list_backtests` endpoint (ends ~L198) and the `get_backtest` endpoint (starts L201).

---

## Fix 5: Docker Compose Rebuild + Validate

**Tasks:**
1. Verify `docker-compose.yml` is correct (no missing services/env vars)
2. Verify `.env.example` has all required vars
3. Run `pytest -q` to validate all changes

---

## Execution Order

1. Create migration 012 (backtest_signals.timeframe) + update model + engine
2. Create migration 013 (portfolio Float→Numeric)
3. Update portfolio model/service/router to Decimal
4. Fix backtest route order
5. Run pytest after each step
6. Write handoff doc

---

## Success Criteria

- `pytest -q` passes with 0 failures
- `GET /api/v1/signals` no longer errors on `timeframe` column
- `GET /api/v1/positions` returns correct Decimal-based P&L values
- `POST /api/v1/backtests/sweep` no longer returns 422
- All portfolio financial values stored as `Numeric(20,8)` in DB
