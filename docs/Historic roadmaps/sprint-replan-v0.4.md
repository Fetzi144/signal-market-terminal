# Sprint Replan v0.4 — Days 11-14

*Written 2026-04-07 after Days 1-10 complete. 249 tests passing.*

---

## Critical Assessment

### 1. Should ML Signal Scoring (Day 11-12) happen now?

**No.** ML scoring is the wrong priority for three reasons:

1. **Insufficient training data.** The ML pipeline requires 200+ resolved signals with `resolved_correctly` labels. The system has been running for ~10 days. Prediction markets resolve over days to weeks. We likely have far fewer than 200 resolved signals — and the ones we have are biased toward fast-resolving markets. Training on this yields a model that overfits to a tiny, non-representative sample.

2. **The formula works fine until proven otherwise.** `signal_score * confidence * recency_weight` is interpretable, debuggable, and tunable via backtesting (which we just built). ML replaces a transparent formula with a black box, and without enough data, the black box will be *worse*. The backtesting + parameter sweep tools (Days 1-3) already let users empirically optimize thresholds — that's the real win.

3. **Opportunity cost.** Two full days on ML means skipping tech debt fixes that will bite us in production and delaying mobile, which traders actually need daily.

**Recommendation:** Defer ML scoring to v0.5.0 once the system has accumulated 500+ resolved signals over 4-6 weeks of real operation. By then the model will have meaningful data and we'll know which features actually matter.

### 2. What's missing that's NOT in the sprint plan?

| Issue | Severity | Details |
|-------|----------|---------|
| **Portfolio uses Float, not Decimal** | High | `Position.quantity`, `avg_entry_price`, `current_price`, `unrealized_pnl`, `exit_price`, `realized_pnl`, `Trade.quantity`, `price`, `fees` are all `Float`. CLAUDE.md mandates `Decimal` for all financial values. Floating-point rounding will corrupt P&L calculations. |
| **BacktestSignal missing timeframe** | Medium | Day 10 added `timeframe` to `Signal`, but `BacktestSignal` (which mirrors Signal) was not updated. Backtesting with multi-timeframe configs will produce incomplete results. |
| **Docker rebuild not documented** | Medium | 10 days of new dependencies (`pywebpush`, new models, new routers) — anyone running via Docker Compose will get stale images. No rebuild instructions in any handoff doc. |
| **No smoke test for Docker** | Medium | `docker compose up` hasn't been validated since Day 0. Migrations, new env vars, new dependencies — high chance of first-run failure. |
| **CHANGELOG.md doesn't exist yet** | Low | Sprint plan says Day 14 updates it, but it should exist before release. |
| **Version in CLAUDE.md still says v0.3.0** | Low | `CLAUDE.md` says "Current: v0.3.0" — should be updated at release. |

### 3. What would a prediction market trader value most?

A trader who uses this tool daily cares about (in order):

1. **Reliability** — The tool runs, doesn't crash, shows correct P&L. Float-based P&L is a trust-killer. If my position shows $47.32 but the real number is $47.3199999997, I stop trusting the tool.

2. **Mobile access** — Prediction market traders check positions on their phone constantly. A desktop-only tool means they still need a spreadsheet or Polymarket's own UI for on-the-go checks. PWA with push notifications is high-impact.

3. **Signal quality visibility** — Already delivered via Performance Dashboard (Day 4) and backtesting (Days 1-3). This is the strongest part of the current build.

4. **Alerts that reach them** — Discord + Push (Day 8) already delivered. Working well.

5. **ML scoring** — Nice to have, but no trader is asking "I wish this used gradient boosting instead of a formula." They want accurate signals, and the formula + backtesting already provides a path to tune accuracy.

### 4. Are 4 more days realistic?

**Yes, if we cut ML and focus on shipping a solid v0.4.0.** The original plan packs 2 days of ML + 1 day mobile + 1 day release into 4 days. That's tight even without tech debt. With the Float->Decimal migration, Docker validation, and accumulated fixes, ML would push us to Day 16+.

**Scope reduction is the right call.** Ship a stable, mobile-friendly v0.4.0 now. Defer ML to v0.5.0.

---

## Revised Plan: Days 11-14

### Day 11: Tech Debt & Production Hardening

**Why:** Fix issues that will cause real bugs in production. None of this is glamorous, but shipping v0.4.0 with Float-based P&L is shipping a known-broken feature.

**Tasks:**

1. **Portfolio Float -> Decimal migration**
   - Change all `Float` columns in `Position` and `Trade` models to `Numeric(20, 8)`
   - Update `portfolio/service.py` to use `Decimal` throughout
   - Alembic migration: `ALTER COLUMN ... TYPE NUMERIC(20,8)`
   - Update portfolio tests to use `Decimal` assertions

2. **Add `timeframe` to BacktestSignal**
   - Add `timeframe: Mapped[str]` column to `BacktestSignal`
   - Alembic migration
   - Update backtest engine to populate timeframe from detector config

3. **Docker validation**
   - `docker compose build --no-cache` and verify startup
   - Ensure all new env vars have defaults or are in `.env.example`
   - Test that `alembic upgrade head` runs cleanly from scratch
   - Document rebuild steps in a brief note

4. **Env var audit**
   - Cross-reference all `settings.*` fields in `config.py` with `.env.example`
   - Add any missing vars with sensible defaults

5. **Run full test suite, fix any failures**

**Deliverables:** Clean data model, working Docker, validated env config, all tests green.

---

### Day 12: Mobile-Responsive Frontend + PWA

**Why:** This is the single highest-impact remaining feature for daily users. Traders live on their phones.

**Tasks:** (Same as original Day 13, now with more breathing room)

1. **Responsive CSS breakpoints** — `768px` and `480px`
2. **Hamburger navigation** on mobile
3. **Card layouts** for Signal Feed and Portfolio on narrow screens
4. **Charts** — responsive with proper aspect ratios
5. **PWA setup** — `manifest.json`, service worker (offline shell only), iOS meta tags
6. **Touch targets** — minimum 44x44px, `touch-action: manipulation`
7. **Priority pages:** Signal Feed (cards), Portfolio (P&L visible without scroll), Performance (hero metrics full-width)

**Deliverables:** All pages usable on 375px screen. PWA installable. Push notifications work on mobile.

---

### Day 13: Integration Tests + Polish

**Why:** Integration tests catch the cross-feature bugs that unit tests miss. The backtest-to-portfolio flow, multi-timeframe confluence, and alert delivery all need end-to-end validation.

**Tasks:**

1. **Backtest integration test** — Seed data, run backtest, verify results, run parameter sweep
2. **Portfolio integration test** — Open position, add trade, refresh prices, close, resolve market
3. **Alert integration test** — Signal fires -> Discord webhook called, push notification queued
4. **Multi-timeframe integration test** — Same signal on 2 timeframes -> confluence bonus applied, API filter works
5. **Fix any accumulated bugs** from test suite run
6. **Ruff lint pass** — zero warnings
7. **Create CHANGELOG.md** with v0.4.0 entry covering all 10 days of features

**Deliverables:** 4 integration test files, all tests passing, lint clean, CHANGELOG written.

---

### Day 14: Release v0.4.0

**Why:** Ship it. A released v0.4.0 with 10 features and solid test coverage beats a hypothetical v0.4.0 with 12 features that never ships.

**Tasks:**

1. **Final test suite run** — all tests pass, coverage >= 80%
2. **Version bump** — `frontend/package.json`, `backend/pyproject.toml`, `CLAUDE.md`
3. **Update README.md** — feature list, backtesting section, portfolio section, mobile note
4. **Docker compose validation** — clean `docker compose up` from scratch, verify all services start
5. **Git tag** — `git tag -a v0.4.0 -m "Signal Market Terminal v0.4.0"`
6. **Write handoff doc** — `docs/handoff-v0.4.0.md` summarizing what shipped, known limitations, and v0.5.0 candidates

**Deliverables:** Tagged v0.4.0 release, updated docs, working Docker deployment.

---

## v0.5.0 Backlog (Deferred from this sprint)

These are explicitly deferred, not forgotten:

| Feature | Why Deferred | When Ready |
|---------|-------------|------------|
| ML Signal Scoring | Need 500+ resolved signals for meaningful training | After 4-6 weeks of v0.4.0 operation |
| ML Inference + Blend | Depends on trained model | After ML training proves value |
| Per-timeframe backtesting P&L | Nice to have, not blocking | v0.5.0 |
| 24h timeframe for long-horizon signals | Needs more data collection first | v0.5.0 |
| Confluence bonus calibration | `0.15` is a placeholder; need real multi-TF data | After v0.4.0 accumulates data |

---

## Summary

| Day | Original Plan | Revised Plan | Rationale |
|-----|--------------|--------------|-----------|
| 11 | ML Feature Extraction + Training | Tech Debt + Production Hardening | Fix Float->Decimal P&L bug, BacktestSignal.timeframe, Docker |
| 12 | ML Inference + Blend | Mobile-Responsive Frontend + PWA | Highest remaining user impact |
| 13 | Mobile Frontend + PWA | Integration Tests + Polish | Moved up from Day 14 to give more time |
| 14 | Integration Tests + Release | Release v0.4.0 | Clean release day without test-writing pressure |

**Net effect:** We trade 2 days of premature ML (that would train on insufficient data) for production hardening + more testing time. The user gets a tool that works correctly on their phone with accurate P&L numbers, instead of a tool with a half-trained ML model nobody trusts.
