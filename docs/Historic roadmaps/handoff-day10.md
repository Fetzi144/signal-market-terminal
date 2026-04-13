# Handoff — Day 10: Multi-Timeframe Analysis & Confluence Scoring

## What Was Built

### Multi-Timeframe Signal Detection
All three core detectors (`PriceMoveDetector`, `VolumeSpikeDetector`, `OrderFlowImbalanceDetector`) now loop over a configurable list of timeframes (e.g. `["30m", "4h"]`) and emit a separate `SignalCandidate` per timeframe. Each candidate carries `timeframe` both as a top-level field and in `details`.

New config keys in `backend/app/config.py`:
- `price_move_timeframes` (default `"30m"`)
- `volume_spike_timeframes` (default `"1h"`)
- `ofi_timeframes` (default `"30m"`)

### Timeframe Column on Signal Model
`Signal` ORM model gains `timeframe: Mapped[str]` (NOT NULL, default `"30m"`). The dedupe unique index `uq_signal_dedupe` now includes `timeframe`, so the same signal type can fire on multiple timeframes without being deduped.

### Confluence Scoring (`scorer.py`)
After `persist_signals` commits new signals, `_apply_confluence_scoring` detects when the same `(signal_type, outcome_id)` fires on ≥2 timeframes in the same 15-min bucket. It applies `CONFLUENCE_BONUS_PER_TF × (n_timeframes - 1)` (currently 0.15 per extra TF) to `rank_score`, capped at 1.000. Results are written back to `signal.details` as `confluence_timeframes` and `confluence_score`.

### API & Frontend
- `GET /api/v1/signals` gains `?timeframe=` filter
- `GET /api/v1/signals/timeframes` returns distinct timeframe values in DB
- `GET /api/v1/analytics/timeframe-accuracy` returns per-timeframe accuracy stats
- `SignalFeed.jsx` has a timeframe dropdown filter
- `Analytics.jsx` shows an "Accuracy by Timeframe" table
- `SignalDetail.jsx` shows the timeframe badge

## Current State

- **Tests**: 249 passing, 0 failing
- **Branch**: `develop`
- **Key commits**:
  - `86777c5` — Day 10 multi-timeframe + confluence scoring (full stack)
  - Latest — fix: remove `session.expire_all()` from async confluence tests

## What Was Fixed Today

1. **`volume_spike.py`** — `_detect_timeframe` now falls back to `settings.volume_spike_baseline_hours` (24) when no explicit `baseline_hours` override is set. The prior code derived `baseline_hours` from the timeframe (`max(1, tf_minutes // 60) = 1h`), which collapsed the baseline window to zero width and produced no candidates.

2. **`test_multi_timeframe.py`** — Removed two `session.expire_all()` calls before attribute access in `test_confluence_across_2_timeframes_applies_bonus` and `test_confluence_across_3_timeframes_capped_at_1`. Calling `expire_all()` then accessing ORM attributes synchronously raises `MissingGreenlet` in async SQLAlchemy — the re-executed `SELECT` already fetches fresh data from the committed transaction.

## What Comes Next

- **Alembic migration**: The `timeframe` column and updated unique index need a migration file (`alembic revision --autogenerate -m "add timeframe to signal"`) before deploying to any environment with an existing database.
- **Timeframe config in `.env`**: Add `PRICE_MOVE_TIMEFRAMES`, `VOLUME_SPIKE_TIMEFRAMES`, `OFI_TIMEFRAMES` to `backend/.env.example` and production env.
- **Tune confluence bonus**: `CONFLUENCE_BONUS_PER_TF = 0.15` is a placeholder — calibrate once real multi-TF signals accumulate in the DB.
- **Day 11 candidates**: Backtesting per timeframe, per-timeframe P&L in the Portfolio view, or adding `24h` to the default timeframe lists for longer-horizon signals.
