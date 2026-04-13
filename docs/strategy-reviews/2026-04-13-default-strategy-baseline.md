# Default Strategy Review

**Date:** 2026-04-13  
**Strategy:** `prove_the_edge_default`  
**Run ID:** `018da68b-fc81-4025-8409-06289ebe25f4`  
**Run Status:** `active`  

## Run Metadata

- Run start: 2026-04-13T00:00:00+00:00
- Immutable launch boundary: 2026-04-13T00:00:00+00:00
- Days tracked: 0.0
- Observation status: `live_waiting_for_trades`

## Current Health Snapshot

- Open exposure: $0.00
- Resolved trades: 0
- Cumulative paper P&L: $0.00
- Shadow cumulative P&L: $0.00
- Average CLV: -
- Profit factor: 0.0
- Shadow profit factor: 0.0
- Win rate: 0.0%
- Brier score: -
- Max drawdown: $0.00

## Trade Funnel

- Candidate signals: 1196
- Qualified signals: 870
- Traded signals: 0
- Resolved signals: 0
- Qualified not traded: 870
- Legacy trades excluded: 8

## Skip Reasons

- Total exposure limit reached: 870
- EV below threshold: 326

## Detector Verdicts

- No detector verdicts yet.

## Locked Comparison

| Mode | Resolved Signals | Win Rate | Avg CLV | 1-Share P&L | Max Drawdown | Paper P&L | Brier |
|------|------------------|----------|---------|-------------|--------------|-----------|-------|
| Default Strategy | 0 | 0.0% | - | 0.0c | 0.0c | $0.00 | - |
| Legacy | 0 | 0.0% | - | 0.0c | 0.0c | $0.00 | - |

## Execution Realism Caveat

- Liquidity-constrained trades: 0
- Trades missing orderbook context: 0
- Shadow execution uses a conservative half-spread penalty and near-touch depth checks. It is a realism overlay, not a full market-impact model.

## Empty State

No active-run paper trades have resolved yet. Keep the baseline frozen, watch the skip-reason funnel, and do not change the contract until the run produces measured trades.

