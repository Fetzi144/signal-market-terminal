# Q2 2026 Implementation Plan — From Monitoring Tool to Trading System

**Created:** 2026-04-12
**Baseline:** v0.4.0 (3a51103, detector tuning applied)
**Timeline:** 8 weeks (2026-04-14 to 2026-06-08)
**Goal:** Turn a 47.6% win-rate signal dashboard into a probability-calibrated trading system with verifiable P&L.

---

## Current State (Honest Assessment)

| Metric | Value | What it means |
|--------|-------|---------------|
| Win rate | 47.6% (n=105) | Below coinflip. Not a trading edge. |
| Resolved detectors | 3 of 8 | 5 detectors have zero resolution data |
| Best detector | `price_move` 57.9% (n=57) | Promising but no profitability measure |
| Worst detector | `deadline_near` 34.0% (n=47) | Actively harmful — worse than random |
| CLV tracking | None | Cannot measure if signals beat closing line |
| Probability output | None | Detectors output heuristic scores, not probabilities |
| Position sizing | None | No Kelly, no bankroll, no risk limits |
| Paper trading | None | Cannot simulate P&L without real money |

**The signal model has `price_at_fire` (Decimal) but no `closing_price`.** Resolution service sets `resolved_correctly` boolean but tracks no price data at resolution time. This is the foundation gap.

---

## Phase 1: Measurement Foundation (Weeks 1-2)

**Why first:** Everything downstream (probability calibration, Kelly sizing, paper trading) requires knowing not just *if* a signal was correct, but *how profitable* it would have been. Without CLV, we're optimizing for the wrong metric.

### Task 1.1: CLV Tracking Schema + Migration

**What:** Add columns to track prices at resolution time and compute CLV.

**Files:**
- `backend/app/models/signal.py` — Add columns to `Signal`:
  - `closing_price: Mapped[Decimal | None]` (Numeric(10,6)) — last traded price before market resolution
  - `resolution_price: Mapped[Decimal | None]` (Numeric(10,6)) — final resolution value (1.00 or 0.00)
  - `clv: Mapped[Decimal | None]` (Numeric(10,6)) — `closing_price - price_at_fire` (positive = we signaled before market priced it in)
  - `profit_loss: Mapped[Decimal | None]` (Numeric(10,6)) — `resolution_price - price_at_fire` (actual P&L per share if traded)
- New migration: `alembic revision --autogenerate -m "add CLV and P&L columns to signals"`

**Effort:** 2-3 hours
**Dependencies:** None
**Success criteria:** Migration runs clean, columns exist, existing signals unaffected (all new columns nullable).

---

### Task 1.2: Resolution Service Enhancement

**What:** When a market resolves, capture the closing price and compute CLV + P&L for every signal on that market.

**Files:**
- `backend/app/ingestion/resolution.py` — Extend `resolve_signals()`:
  1. Before setting `resolved_correctly`, fetch the last `PriceSnapshot` for the signal's `outcome_id` before resolution → that's `closing_price`
  2. Set `resolution_price` = 1.00 if outcome won, 0.00 if lost
  3. Compute `clv = closing_price - price_at_fire` (only if both prices exist)
  4. Compute `profit_loss = resolution_price - price_at_fire` for "up" signals; `price_at_fire - resolution_price` for "down" signals
  5. Persist all four new fields

**Effort:** 4-6 hours
**Dependencies:** Task 1.1
**Success criteria:**
- Every newly resolved signal has `closing_price`, `resolution_price`, `clv`, `profit_loss` populated
- Retroactive backfill: run once for existing 105 resolved signals (where snapshot data exists)

---

### Task 1.3: Retroactive CLV Backfill Script

**What:** One-time script to compute CLV for the 105 already-resolved signals using historical price snapshots.

**Files:**
- `backend/app/ingestion/backfill_clv.py` (new) — async script:
  1. Query all signals where `resolved_correctly IS NOT NULL AND clv IS NULL`
  2. For each, find the latest `PriceSnapshot` for that `outcome_id` before the market's resolution time
  3. Compute and persist CLV + P&L

**Effort:** 2-3 hours
**Dependencies:** Task 1.1, Task 1.2
**Success criteria:** CLV computed for ≥80% of resolved signals (some may lack snapshot data).

---

### Task 1.4: Ground Truth Pipeline — Automated Resolution Polling

**What:** The resolution service exists but isn't being called on a schedule. Wire it into the scheduler so resolutions are detected automatically.

**Files:**
- `backend/app/jobs/scheduler.py` — Add a job that runs every 15 minutes:
  1. Poll Polymarket CLOB API for recently resolved markets
  2. Poll Kalshi REST API for recently settled events
  3. Call `resolve_signals()` with the results
- `backend/app/connectors/` — Add resolution-fetching methods to both connectors (check if partially implemented already)

**Effort:** 6-8 hours
**Dependencies:** Task 1.2
**Success criteria:**
- Markets resolved within 1 hour of settlement
- Zero manual intervention needed
- Scheduler job logged in `IngestionRun` table

---

### Task 1.5: Signal Quality Score

**What:** Replace the binary "correct/incorrect" evaluation with a continuous quality score based on profitability.

**Files:**
- `backend/app/api/performance.py` — Extend the performance summary endpoint:
  - Add `avg_clv` per detector type
  - Add `avg_profit_loss` per detector type
  - Add `signal_quality_score = clv * sqrt(n)` (CLV weighted by sample size — rewards both edge and consistency)
  - Add `profit_factor = sum(winning_pnl) / abs(sum(losing_pnl))` per detector

**Effort:** 3-4 hours
**Dependencies:** Tasks 1.1-1.3
**Success criteria:** Performance API returns CLV-based metrics alongside win rate.

---

### Task 1.6: CLV Dashboard Component

**What:** Frontend visualization of CLV data per detector.

**Files:**
- `frontend/src/pages/Performance.jsx` — Add:
  - CLV bar chart per detector type (avg CLV in cents)
  - P&L waterfall chart (cumulative hypothetical P&L over time)
  - Signal quality table with sortable columns
- `frontend/src/components/CLVChart.jsx` (new) — Recharts component

**Effort:** 4-6 hours
**Dependencies:** Task 1.5
**Success criteria:** Performance page shows CLV per detector. Trader can see at a glance which detectors would have made money.

---

### Phase 1 Summary

| Task | Effort | Depends on | Week |
|------|--------|------------|------|
| 1.1 CLV schema + migration | 2-3h | — | 1 |
| 1.2 Resolution service enhancement | 4-6h | 1.1 | 1 |
| 1.3 Retroactive CLV backfill | 2-3h | 1.1, 1.2 | 1 |
| 1.4 Automated resolution polling | 6-8h | 1.2 | 1-2 |
| 1.5 Signal quality score API | 3-4h | 1.1-1.3 | 2 |
| 1.6 CLV dashboard | 4-6h | 1.5 | 2 |
| **Total** | **~25h** | | |

**Phase 1 exit criteria:**
- [ ] Every resolved signal has CLV + P&L computed
- [ ] Resolution pipeline runs automatically every 15 min
- [ ] Performance API returns `avg_clv`, `avg_profit_loss`, `profit_factor` per detector
- [ ] Dashboard shows CLV chart — we can see which detectors *make money*, not just which are "correct"
- [ ] We know the answer to: "If a trader had followed every signal with $1/share, what's the total P&L?"

---

## Phase 2: Probability Engine (Weeks 3-4)

**Why now:** With CLV measuring actual profitability, we can now refactor detectors to output calibrated probabilities — and *verify* whether those probabilities are accurate using the measurement infrastructure from Phase 1.

### Task 2.1: Detector Probability Output Interface

**What:** Extend `BaseDetector` and `SignalCandidate` to support probability estimates.

**Files:**
- `backend/app/signals/base.py`:
  - Add to `SignalCandidate`: `estimated_probability: Decimal | None` (the detector's P(YES|signal_data))
  - Add to `SignalCandidate`: `probability_adjustment: Decimal | None` (delta from market price)
  - The market price IS the prior. Detectors output adjustments, not absolute probabilities.
- `backend/app/models/signal.py` — Add to `Signal`:
  - `estimated_probability: Mapped[Decimal | None]` (Numeric(5,4))
  - `probability_adjustment: Mapped[Decimal | None]` (Numeric(5,4))
- New migration

**Effort:** 3-4 hours
**Dependencies:** Phase 1 complete
**Success criteria:** Schema supports probability output. Existing detectors still work (new fields nullable).

---

### Task 2.2: Outcome-Prior Sensitivity Curve

**What:** A utility that dampens signals near price extremes (94%→97% is noise) and amplifies signals near 50% (48%→51% is significant).

**Files:**
- `backend/app/signals/probability.py` (new):
  ```python
  def prior_sensitivity(market_price: Decimal, raw_adjustment: Decimal) -> Decimal:
      """Apply sensitivity curve: signals near 50% are amplified, signals at extremes dampened."""
      # Sensitivity = 4 * p * (1-p)  → peaks at 0.5 (=1.0), zero at 0/1
      sensitivity = 4 * market_price * (1 - market_price)
      return raw_adjustment * sensitivity
  ```
- This function is called by every detector before returning a probability adjustment.

**Effort:** 2-3 hours
**Dependencies:** Task 2.1
**Success criteria:** Unit tests verify sensitivity(0.50) = 1.0x, sensitivity(0.95) = 0.19x, sensitivity(0.05) = 0.19x.

---

### Task 2.3: Price Move Detector — Probability Refactor

**What:** Convert the highest-performing detector (57.9% win rate) to output probability estimates.

**Files:**
- `backend/app/signals/price_move.py`:
  - Current: outputs `signal_score` based on price change magnitude
  - New: `probability_adjustment = direction * magnitude * calibration_factor`
    - `direction`: +1 for price moving toward YES, -1 for NO
    - `magnitude`: price change as a fraction (e.g., 0.05 for 5% move)
    - `calibration_factor`: initially 1.0, will be tuned from CLV data
  - `estimated_probability = market_price + prior_sensitivity(market_price, probability_adjustment)`
  - Clamp to [0.01, 0.99]
  - Keep `signal_score` and `confidence` for backward compatibility

**Effort:** 4-6 hours
**Dependencies:** Task 2.1, 2.2
**Success criteria:** `price_move` detector emits `estimated_probability` on every signal. Existing score/confidence unchanged.

---

### Task 2.4: Volume Spike + OFI Detectors — Probability Refactor

**What:** Convert the two confirmation-type detectors to probability outputs.

**Files:**
- `backend/app/signals/volume_spike.py`:
  - Volume confirms current trend: `probability_adjustment = trend_direction * volume_ratio * calibration_factor`
  - Volume against trend: contrarian signal (smaller adjustment, opposite direction)
- `backend/app/signals/order_flow.py`:
  - Buy-side OFI → positive adjustment. Sell-side → negative.
  - Flat price + high OFI = informed flow not priced in → larger adjustment
  - `probability_adjustment = ofi_magnitude * (1 - price_change_magnitude) * calibration_factor`

**Effort:** 6-8 hours
**Dependencies:** Task 2.1, 2.2
**Success criteria:** Both detectors emit probabilities. OFI specifically amplifies signal when price hasn't moved yet.

---

### Task 2.5: Remaining Detectors — Probability Stubs

**What:** Add probability output to `deadline_near`, `spread_change`, `liquidity_vacuum`, `arbitrage`. These are *confidence modifiers*, not directional signals — they widen/narrow uncertainty but don't shift probability directly.

**Files:**
- `backend/app/signals/deadline_near.py` — `probability_adjustment = 0` but set a `confidence_modifier` field (deadline urgency increases confidence in other concurrent signals)
- `backend/app/signals/spread_change.py` — `probability_adjustment = 0`, set `uncertainty_modifier` (wider spread = less certainty)
- `backend/app/signals/liquidity_vacuum.py` — same pattern as spread_change
- `backend/app/signals/arbitrage.py` — special case: probability based on price differential between platforms

**Effort:** 4-5 hours
**Dependencies:** Task 2.1
**Success criteria:** All 7 active detectors output probability-related fields. Non-directional ones explicitly mark themselves as modifiers.

---

### Task 2.6: Calibration Tracking

**What:** After each resolution batch, compute per-detector calibration curves — "when we say 70%, is it really 70%?"

**Files:**
- `backend/app/api/performance.py` — New endpoint `GET /api/v1/performance/calibration`:
  - For each detector, bucket signals by `estimated_probability` in 10% bins
  - For each bin: count resolved, count correct, compute `actual_rate = correct/resolved`
  - Return array of `{bin_center, predicted_rate, actual_rate, sample_size}`
  - Brier score per detector: `mean((estimated_probability - actual_outcome)^2)`
- `frontend/src/components/CalibrationPlot.jsx` (new) — Recharts scatter plot: predicted vs actual, with identity line

**Effort:** 6-8 hours
**Dependencies:** Tasks 2.3-2.5, Phase 1 (need resolved signals with probability estimates)
**Success criteria:** Calibration endpoint returns data. We can see if "70% predictions" resolve YES ~70% of the time.

---

### Phase 2 Summary

| Task | Effort | Depends on | Week |
|------|--------|------------|------|
| 2.1 Probability interface + schema | 3-4h | Phase 1 | 3 |
| 2.2 Prior sensitivity curve | 2-3h | 2.1 | 3 |
| 2.3 price_move probability refactor | 4-6h | 2.1, 2.2 | 3 |
| 2.4 volume_spike + OFI refactor | 6-8h | 2.1, 2.2 | 3-4 |
| 2.5 Remaining detectors — stubs | 4-5h | 2.1 | 4 |
| 2.6 Calibration tracking + dashboard | 6-8h | 2.3-2.5 | 4 |
| **Total** | **~30h** | | |

**Phase 2 exit criteria:**
- [ ] Every detector outputs `estimated_probability` alongside existing scores
- [ ] Prior sensitivity curve dampens signals at price extremes (reduces false positives)
- [ ] Calibration endpoint computes Brier score per detector
- [ ] Calibration plot visible on Performance page
- [ ] Baseline Brier scores established for all detectors with ≥20 resolved signals

---

## Phase 3: Trading Intelligence (Weeks 5-6)

**Why now:** With calibrated probabilities and CLV measurement, we can compute expected value and size positions. This is where the system becomes a *trading tool*.

### Task 3.1: Bayesian Signal Fusion Engine

**What:** When multiple detectors fire on the same `outcome_id` within a time window, combine them into a single posterior probability estimate.

**Files:**
- `backend/app/signals/confluence.py` (new):
  ```python
  class ConfluenceEngine:
      WINDOW_MINUTES = 30  # detectors firing within 30min on same outcome
      
      async def fuse(self, signals: list[SignalCandidate], market_price: Decimal) -> SignalCandidate:
          """Bayesian fusion of multiple detector signals."""
          # Prior = market price
          # Each detector's adjustment → likelihood ratio
          # P_posterior ∝ P_prior × LR_1 × LR_2 × ... × LR_n
          # Apply independence correction (discount correlated detectors)
  ```
  - Correlation matrix between detector pairs (initially hardcoded from domain knowledge, later learned from data):
    - `price_move` × `volume_spike`: 0.6 correlation (often co-occur)
    - `price_move` × `ofi`: 0.3 correlation
    - `volume_spike` × `ofi`: 0.4 correlation
    - All others: 0.1 (approximately independent)
  - Output: new `SignalCandidate` with `signal_type = "confluence"`, combined probability, contributing signals list in `details`
- `backend/app/jobs/scheduler.py` — After detection cycle, run confluence engine on signals fired in the last 30 minutes

**Effort:** 8-10 hours
**Dependencies:** Phase 2 (probability outputs needed for fusion)
**Success criteria:**
- Confluence signals fire when ≥2 detectors hit the same outcome within 30 min
- Confluence probability is mathematically derived (Bayesian update), not averaged
- Historical backtest shows confluence signals have higher CLV than any individual detector

---

### Task 3.2: Expected Value Calculation

**What:** For every signal, compute EV = `estimated_probability - market_price`. Only surface signals with EV above a configurable threshold.

**Files:**
- `backend/app/models/signal.py` — Add:
  - `expected_value: Mapped[Decimal | None]` (Numeric(10,6))
- `backend/app/signals/base.py` — Compute EV at signal creation time:
  - `ev = estimated_probability - price_at_fire` (for YES direction)
  - `ev = (1 - estimated_probability) - (1 - price_at_fire)` (for NO direction, simplifies to same formula)
- `backend/app/config.py` — Add `min_ev_threshold: Decimal = Decimal("0.03")` (3 cents)
- `backend/app/jobs/scheduler.py` — Filter: only persist/alert signals where `abs(ev) >= min_ev_threshold`

**Effort:** 4-5 hours
**Dependencies:** Phase 2
**Success criteria:** Every signal has EV. Low-EV signals are filtered. Signal volume drops by ~40-50% while quality improves.

---

### Task 3.3: Kelly Criterion Position Sizing

**What:** For each EV-positive signal, recommend a position size based on Kelly criterion with conservative fractional Kelly.

**Files:**
- `backend/app/signals/kelly.py` (new):
  ```python
  def kelly_size(
      estimated_prob: Decimal,
      market_price: Decimal,
      bankroll: Decimal,
      kelly_fraction: Decimal = Decimal("0.25"),  # quarter-Kelly
      max_position_pct: Decimal = Decimal("0.05"),  # 5% max per position
  ) -> dict:
      edge = estimated_prob - market_price
      odds = (1 - market_price) / market_price
      kelly_f = edge / (1 - market_price)
      raw_size = bankroll * kelly_f * kelly_fraction
      capped_size = min(raw_size, bankroll * max_position_pct)
      return {
          "kelly_fraction_full": kelly_f,
          "kelly_fraction_used": kelly_f * kelly_fraction,
          "recommended_size_usd": max(Decimal("0"), capped_size),
          "edge_pct": edge * 100,
      }
  ```
- `backend/app/config.py` — Add:
  - `default_bankroll: Decimal = Decimal("10000")`
  - `kelly_multiplier: Decimal = Decimal("0.25")`
  - `max_single_position_pct: Decimal = Decimal("0.05")`
  - `max_total_exposure_pct: Decimal = Decimal("0.30")`
- `backend/app/api/signals.py` — Extend signal response with Kelly output when EV > 0

**Effort:** 6-8 hours
**Dependencies:** Task 3.2
**Success criteria:**
- Every EV-positive signal includes recommended position size in USD
- Quarter-Kelly default, configurable
- Max 5% per position, 30% total exposure guardrails enforced

---

### Task 3.4: Risk Management Module

**What:** Portfolio-level risk limits: max exposure, correlated exposure detection, drawdown protection.

**Files:**
- `backend/app/signals/risk.py` (new):
  - `check_exposure(current_positions, new_signal, bankroll)` → approve/reject/reduce size
  - Track total exposure across open signals
  - Basic correlated exposure: group markets by keyword overlap (e.g., "Fed" markets clustered together). Max 15% bankroll in any cluster.
  - Drawdown circuit breaker: if paper-trading P&L drops >15% from peak, halve all recommended sizes
- Wire into signal output: if risk check rejects, signal still fires but sizing recommendation is $0 with reason

**Effort:** 6-8 hours
**Dependencies:** Task 3.3
**Success criteria:** Exposure limits enforced. No single position >5% of bankroll. No cluster >15%. Drawdown protection active.

---

### Task 3.5: Paper Trading Mode

**What:** Simulate trades based on signals without real money. Track hypothetical P&L.

**Files:**
- `backend/app/models/paper_trade.py` (new):
  ```python
  class PaperTrade(Base):
      __tablename__ = "paper_trades"
      id, signal_id, outcome_id
      direction  # "buy_yes" / "buy_no"
      entry_price  # price_at_fire
      size_usd  # Kelly-recommended size
      exit_price  # None until resolved
      pnl  # None until resolved
      status  # "open" / "resolved" / "expired"
      opened_at, resolved_at
  ```
- `backend/app/paper_trading/engine.py` (new):
  - `open_trade(signal)` — Auto-create paper trade for every EV-positive signal (configurable)
  - `resolve_trades(market_resolution)` — When market resolves, compute P&L
  - `get_portfolio_summary()` — Total P&L, open exposure, max drawdown, win rate, Sharpe
- `backend/app/api/paper_trading.py` (new) — REST endpoints:
  - `GET /api/v1/paper-trading/portfolio` — Summary stats
  - `GET /api/v1/paper-trading/trades` — Trade history with filters
  - `GET /api/v1/paper-trading/pnl-curve` — Cumulative P&L over time
- `backend/app/jobs/scheduler.py` — Hook into detection: after signals fire + Kelly sizing, auto-open paper trades
- `frontend/src/pages/PaperTrading.jsx` (new) — Dashboard: P&L curve, open positions, trade history

**Effort:** 12-16 hours
**Dependencies:** Tasks 3.2, 3.3, 3.4
**Success criteria:**
- Paper trades auto-created for EV-positive signals
- P&L computed on resolution
- Cumulative P&L curve visible in dashboard
- This is THE metric that proves the system works

---

### Task 3.6: Enhanced Signal Output Format

**What:** Upgrade the signal API response and SSE events to include the full trading-actionable format.

**Files:**
- `backend/app/api/signals.py` — Extend `SignalResponse`:
  ```python
  class SignalResponse(BaseModel):
      # ... existing fields ...
      estimated_probability: Decimal | None
      market_price: Decimal | None
      expected_value: Decimal | None
      edge_pct: Decimal | None
      recommended_size_usd: Decimal | None
      kelly_fraction: Decimal | None
      direction: str | None  # "BUY YES" / "BUY NO"
      contributing_detectors: list[str] | None  # for confluence signals
  ```
- `frontend/src/pages/SignalFeed.jsx` — Render new format: show EV, edge, recommended size prominently. Color-code by EV magnitude.

**Effort:** 4-6 hours
**Dependencies:** Tasks 3.1-3.3
**Success criteria:** Signal feed shows actionable trading info, not just heuristic scores.

---

### Phase 3 Summary

| Task | Effort | Depends on | Week |
|------|--------|------------|------|
| 3.1 Bayesian fusion engine | 8-10h | Phase 2 | 5 |
| 3.2 Expected value calculation | 4-5h | Phase 2 | 5 |
| 3.3 Kelly criterion sizing | 6-8h | 3.2 | 5 |
| 3.4 Risk management module | 6-8h | 3.3 | 5-6 |
| 3.5 Paper trading mode | 12-16h | 3.2, 3.3, 3.4 | 6 |
| 3.6 Enhanced signal output | 4-6h | 3.1-3.3 | 6 |
| **Total** | **~48h** | | |

**Phase 3 exit criteria:**
- [ ] Confluence engine produces fused signals with higher CLV than individual detectors
- [ ] Every signal shows EV in cents and recommended position size in USD
- [ ] Kelly sizing with quarter-Kelly default and 5%/30% guardrails
- [ ] Paper trading mode live — auto-trades every EV-positive signal
- [ ] Cumulative P&L curve visible in dashboard
- [ ] Signal feed format: "BUY YES @ $0.42, Our estimate: $0.51, Edge: +$0.09, Size: $340"

---

## Phase 4: Validation & Ship (Weeks 7-8)

**Why:** The system is now feature-complete for v0.5.0. These two weeks are for collecting paper-trading data, validating the probability engine, and deciding if we're ready to ship.

### Task 4.1: Data Collection Period (Week 7 — mostly passive)

**What:** Let the system run with paper trading active. Collect data. Monitor.

**Activities:**
- Paper trading auto-executes on all EV-positive signals for 7 days
- Monitor resolution pipeline — ensure markets are being resolved within 1 hour
- Monitor calibration — are probability estimates drifting?
- Daily check: cumulative P&L curve trending up or down?
- Fix any bugs that surface during live operation

**Effort:** 2-3h monitoring/day, ~15h total
**Dependencies:** Phase 3 complete
**Success criteria:** 7 days of clean paper-trading data with no pipeline failures.

---

### Task 4.2: Performance Analysis

**What:** Deep analysis of paper-trading results after 7 days.

**Analysis to produce (document in `docs/paper-trading-analysis-v0.5.md`):
- **P&L Summary:** Total P&L, number of trades, win rate, profit factor
- **CLV Validation:** Average CLV per detector — does positive CLV actually predict profitability?
- **Calibration Report:** Brier score per detector. Calibration plot. Which detectors are over/under-confident?
- **Confluence vs Individual:** Do confluence signals outperform individual detectors?
- **Kelly Effectiveness:** Compare quarter-Kelly sizing vs. flat sizing vs. full-Kelly (simulated)
- **Risk Metrics:** Max drawdown, Sharpe ratio (annualized), worst single day
- **Detector Rankings:** Sort detectors by CLV contribution. Identify candidates for demotion.

**Effort:** 6-8 hours (analysis + documentation)
**Dependencies:** Task 4.1 (7 days of data)
**Success criteria:** Written analysis with clear go/no-go recommendation for v0.5.0 release.

---

### Task 4.3: A/B Comparison — Old Heuristic vs New Probability Engine

**What:** Backtest the same time period with both the old scoring system and the new probability engine. Compare.

**Files:**
- `backend/app/backtesting/engine.py` — Extend to support two modes:
  - Legacy mode: rank by `signal_score * confidence * recency_weight`, no EV filter
  - Probability mode: rank by EV, apply Kelly sizing, compute P&L
- Run both on the same 7-day window. Compare:
  - Signal volume (new should be lower — EV filter removes noise)
  - Win rate (new should be equal or higher)
  - Hypothetical P&L (new should be higher due to sizing)
  - Max drawdown (new should be lower due to risk management)

**Effort:** 6-8 hours
**Dependencies:** Task 4.1
**Success criteria:** Written comparison in paper-trading analysis doc. Probability engine outperforms heuristic on P&L.

---

### Task 4.4: v0.5.0 Release Prep

**What:** Version bump, documentation, tests, Docker validation.

**Files:**
- `backend/pyproject.toml` — version bump to 0.5.0
- `frontend/package.json` — version bump
- `CLAUDE.md` — update version, add probability/CLV/Kelly section to conventions
- `CHANGELOG.md` — v0.5.0 entry
- `docs/handoff-v0.5.md` (new) — What shipped, known limitations, v0.6.0 candidates
- All new modules: minimum 80% test coverage
- Docker: `docker compose build --no-cache && docker compose up` — validate from scratch
- Git tag: `v0.5.0`

**Effort:** 8-10 hours
**Dependencies:** Tasks 4.2, 4.3 (go decision)
**Success criteria:**
- All tests pass (target: 250+)
- Docker cold start works
- Paper trading profitable OR clear documentation of what needs to change
- Tagged v0.5.0 release

---

### Phase 4 Summary

| Task | Effort | Depends on | Week |
|------|--------|------------|------|
| 4.1 Data collection (7 days) | ~15h | Phase 3 | 7 |
| 4.2 Performance analysis | 6-8h | 4.1 | 8 |
| 4.3 A/B comparison | 6-8h | 4.1 | 8 |
| 4.4 Release prep | 8-10h | 4.2, 4.3 | 8 |
| **Total** | **~35h** | | |

**Phase 4 exit criteria:**
- [ ] 7+ days of paper-trading data collected
- [ ] Written performance analysis with P&L, CLV, Brier scores, Sharpe ratio
- [ ] A/B comparison showing probability engine vs. heuristic
- [ ] v0.5.0 tagged and released IF paper trading shows positive trajectory
- [ ] If P&L is negative: documented diagnosis of why + plan for Phase 2.5 recalibration before release

---

## Total Effort Summary

| Phase | Focus | Weeks | Estimated Hours |
|-------|-------|-------|-----------------|
| 1 — Measurement Foundation | CLV, ground truth, quality scores | 1-2 | ~25h |
| 2 — Probability Engine | Detector refactor, calibration | 3-4 | ~30h |
| 3 — Trading Intelligence | Fusion, Kelly, paper trading | 5-6 | ~48h |
| 4 — Validation & Ship | Data collection, analysis, release | 7-8 | ~35h |
| **Total** | | **8 weeks** | **~138h** |

---

## Key Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Not enough resolutions for calibration** | Can't validate probability estimates | Focus on short-duration markets. Lower thresholds already applied (detector tuning 2026-04-12) to increase signal volume. Need ≥200 resolved signals by end of Phase 2. |
| **CLV is negative across all detectors** | No edge to build on | This is valuable information. Kill detectors with persistent negative CLV. Focus resources on `price_move` (57.9%) and any detector that shows positive CLV. |
| **Bayesian fusion overfits to small sample** | Confluence signals look good in backtest, fail live | Use held-out validation. Hardcode conservative correlation estimates rather than learning from limited data. Only deploy fusion after 4+ weeks of individual detector probability data. |
| **Paper trading ≠ real trading (no slippage, no liquidity impact)** | Simulated P&L is optimistic | Track orderbook depth at signal time. Flag trades where recommended size > available liquidity. Report "slippage-adjusted P&L" alongside ideal P&L. |
| **Scope creep from Phase 3** | Paper trading takes longer than expected | Paper trading MVP is auto-open + auto-resolve + P&L curve. Skip trade journal, skip manual trade entry, skip export. Those are v0.6.0. |

---

## What We Are NOT Building in Q2

These are explicitly deferred to prevent scope creep:

- **ML Signal Scoring** — Need 500+ resolved signals first (sprint-replan-v0.4 decision)
- **Execution integration** — No API trading until paper trading proves profitable (roadmap Q4)
- **Strategy marketplace** — No user-facing strategy builder (roadmap Q1 2027)
- **News-to-signal pipeline** — No NLP/news integration (roadmap Q1 2027)
- **Third platform connector** — Polymarket + Kalshi only (roadmap Q1 2027)
- **Smart money wallet scoring** — Requires Polygon RPC, deferred until infrastructure ready
- **Monetization / Stripe** — Don't charge for a product that hasn't proven its edge yet

---

## Success Metric

**One number decides if Q2 was successful:**

> Cumulative paper-trading P&L after 4+ weeks, using default strategy (confluence signals, EV > $0.03, quarter-Kelly sizing, $10K simulated bankroll).

- **Positive and growing:** Q2 succeeded. Ship v0.5.0. Start Q3 (strategy engine, arb, smart money).
- **Flat:** Detectors have no edge over the market. Investigate which detectors have positive CLV, focus there. Delay v0.5.0.
- **Negative:** Something is fundamentally wrong. Diagnose: is it bad probabilities, bad sizing, or bad detectors? Fix before proceeding.

This is the honest assessment. We don't ship v0.5.0 to feel productive. We ship it when the P&L line goes up.
