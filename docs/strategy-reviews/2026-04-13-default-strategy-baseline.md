# Default Strategy Review

**Date:** 2026-04-13  
**Strategy:** `prove_the_edge_default`

## Frozen Contract

- Signal path: `confluence`
- EV filter: `abs(expected_value) >= $0.03/share`
- Kelly sizing: quarter-Kelly
- Paper bankroll: `$10,000`
- Risk guardrails: current single-position, total-exposure, cluster-exposure, and drawdown limits

## Weekly Questions

1. Did the default strategy make money this week?
2. Was average CLV positive for the traded path?
3. Did confluence beat the legacy rank-threshold benchmark on 1-share P&L and drawdown?
4. Which detectors earned a `keep`, `watch`, or `cut` verdict?
5. Were there any missing resolutions, broken paper-trade lifecycles, or suspicious data gaps?

## Data Sources

- `/api/v1/paper-trading/strategy-health`
- `/api/v1/paper-trading/portfolio`
- `/api/v1/paper-trading/metrics`
- `/api/v1/paper-trading/pnl-curve`

## Notes

This artifact exists so every review cycle starts from the same contract and the same checklist. If the strategy contract changes, create a new baseline artifact rather than silently editing this one.
