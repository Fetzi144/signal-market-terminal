# Default Strategy Operator Evidence Log

## Frozen Evidence Gate

- Minimum resolved trades: `20`
- Execution-adjusted P&L rule: `positive`
- Max drawdown pct: `0.12`
- CLV rule: `non_negative_or_improving`
- Max Brier score: `0.25`
- Zero funnel integrity failures: `true`
- No hidden shared/global contamination: `true`

## Notes

- Only post-fix runs count as evidence.
- Pre-fix runs and artifacts are historical/debug only.
- Use `v0.4.1` as the first valid post-fix evidence boundary.

## Evidence Boundary
- Recorded at: 2026-04-15T17:12:57.4384942+02:00
- Boundary id: "v0.4.1"
- Release tag: "v0.4.1"
- Commit SHA: "87a4315b81b81365d9ee974aff5b130813757897"
- Alembic revision: "038"
- Contract version: "default_strategy_v0.4.1"
- Note: Only post-fix runs count as evidence. Pre-fix artifacts are historical/debug only.
