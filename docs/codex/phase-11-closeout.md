# Phase 11 Closeout

## Summary

Phase 11 adds a durable Polymarket replay and backtest layer that reuses stored Phase 3 through Phase 10 truth without crossing the live-trading boundary. The repo now supports deterministic historical scenario replay, conservative aggressive/passive fill simulation, cross-variant policy comparison, stored-structure package replay, advisory maker quote replay, and risk-adjusted replay with persisted metrics and decision traces.

The implementation stays offline-only and audit-first. It does not enable unattended live execution, does not broaden the OMS/control plane, does not add new strategy families, and does not start Phase 12 live pilot work.

## What Changed In Phase 11

- Added Phase 11 replay persistence for:
  - `polymarket_replay_runs`
  - `polymarket_replay_scenarios`
  - `polymarket_replay_orders`
  - `polymarket_replay_fills`
  - `polymarket_replay_metrics`
  - `polymarket_replay_decision_traces`
- Added `backend/app/ingestion/polymarket_replay_simulator.py` to:
  - build deterministic replay windows from stored books, deltas, trades, and BBO observations
  - compare midpoint, executable, maker-aware, structure, and risk-adjusted variants on the same tape
  - persist replay-time orders, fills, traces, and rollup metrics
  - expose replay status, run listings, scenario detail, metrics, traces, and manual trigger helpers
- Integrated Phase 11 into:
  - worker scheduling with replay disabled by default
  - health/status serialization
  - a dedicated replay API surface
  - Prometheus metrics
  - the frontend health page for compact replay visibility and a manual replay trigger

## Replay Input Semantics

- Replay inputs are sourced from stored prior-phase truth only:
  - Phase 3 raw snapshots, deltas, BBO events, trade tape, and stored OI where available
  - Phase 4 reconstructed-book identity and checkpoint status
  - Phase 5 passive-fill labels when they already exist
  - Phase 6 execution decisions and action candidates
  - Phase 8 stored structure opportunities, legs, and validations
  - Phase 9 maker-economics snapshots and quote recommendations
  - Phase 10 optimizer recommendations and inventory controls
- Replay windows are seeded from authoritative snapshots and advanced by deterministic delta ordering per asset.
- Coverage gaps are surfaced explicitly in scenario details and metric payloads. The simulator does not silently interpolate missing book truth.

## Fill-Model Semantics And Limitations

- Aggressive fills:
  - deterministic book-walk against visible replay-time depth
  - partial fills when visible depth is insufficient
  - explicit slippage and path traces
- Passive fills:
  - conservative fill logic driven by trade touches, BBO touches, and existing passive-fill labels when available
  - explicit reason codes such as `trade_touch_without_sufficient_label_history`, `bbo_touch_without_sufficient_label_history`, and `timeout_without_touch`
  - finite replay timeout windows with cancel/no-fill outcomes
- Limitations:
  - no hidden queue-position model
  - no optimistic interpolation across missing snapshots or drift windows
  - non-Polymarket structure legs use stored leg estimates only when explicit stored truth exists
  - reward estimates are only applied when stored Phase 9 inputs exist

## Variant-Comparison Semantics

- Phase 11 currently compares these replay variants:
  - `midpoint_baseline`
  - `exec_policy`
  - `maker_policy`
  - `structure_policy`
  - `risk_adjusted`
- Policy-comparison runs evaluate midpoint versus executable action-policy behavior on the same historical tape.
- Maker replay compares executable taker-style behavior against advisory maker quote recommendations, with reward and realism adjustments kept explicit.
- Structure replay compares midpoint package assumptions against stored executable structure-package assumptions and optional risk caps.
- Risk-adjusted replay applies stored Phase 10 recommendations conservatively:
  - block / no-quote
  - reduce-size caps
  - reservation-price shifts

All variant outcomes are persisted with run/scenario provenance and per-variant decision traces.

## What Remains For Phase 12

- Live pilot / control-plane work remains out of scope and was not started here.
- No unattended live execution, auto-approval flow, or new OMS automation was added.
- Replay outputs are still research and operator artifacts, not live routing instructions.
- Any future live pilot should consume these replay artifacts as advisory evidence rather than bypassing them.

## Live-Disabled Default

Yes. The repo is still live-disabled by default.

- `POLYMARKET_LIVE_TRADING_ENABLED` remains `False` by default.
- `POLYMARKET_REPLAY_ENABLED` is also `False` by default.
- `POLYMARKET_REPLAY_ON_STARTUP` is `False` by default.
- Replay remains advisory-only and offline-oriented even when manually triggered.
