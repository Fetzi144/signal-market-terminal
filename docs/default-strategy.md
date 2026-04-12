# Default Strategy Contract

This document freezes the baseline strategy for the **prove the edge** phase.

## Contract

- **Name:** `prove_the_edge_default`
- **Signal path:** `confluence`
- **EV filter:** `abs(expected_value) >= $0.03/share`
- **Sizing:** quarter-Kelly (`0.25`) on a `$10,000` paper bankroll
- **Risk guardrails:**
  - max single position: `5%`
  - max total exposure: `30%`
  - max cluster exposure: `15%`
  - drawdown circuit breaker: `-15%`
- **Execution mode:** paper trading only
- **Primary health surface:** `/api/v1/paper-trading/strategy-health` and the `/paper-trading` frontend route

## Why This Exists

The repo already has CLV tracking, calibrated probabilities, Bayesian confluence, EV output, Kelly sizing, and paper trading. What it lacked was one explicit answer to:

> "Which exact strategy are we measuring when we say we are proving edge?"

This contract removes that ambiguity. During this phase:

- we do **not** expand scope into new detectors or new alpha sources
- we do **not** redefine the baseline week to week
- we let the baseline run long enough to produce an honest track record

## Success Criteria

The baseline should be left unchanged for:

- **minimum window:** 14 days
- **preferred window:** 30 days

Weekly review focuses on:

- cumulative paper-trading P&L
- average CLV
- profit factor
- win rate
- max drawdown
- Brier score
- detector keep/watch/cut verdicts
- comparison vs the legacy rank-threshold benchmark

If the baseline is flat or negative, the next move is to prune detectors, recalibrate probabilities, or raise thresholds before building anything new.
