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
- Live cutover on `2026-04-18` used explicit unreleased postfix metadata because `v0.4.1` is not present as a local git tag in this workspace.

## Evidence Boundary
- Recorded at: 2026-04-15T17:12:57.4384942+02:00
- Boundary id: "v0.4.1"
- Release tag: "v0.4.1"
- Commit SHA: "87a4315b81b81365d9ee974aff5b130813757897"
- Alembic revision: "038"
- Contract version: "default_strategy_v0.4.1"
- Note: Only post-fix runs count as evidence. Pre-fix artifacts are historical/debug only.

## Evidence Boundary
- Recorded at: 2026-04-18T01:34:45.944901+02:00
- Boundary id: "default-strategy-postfix-2026-04-18"
- Release tag: "unreleased-postfix-2026-04-18"
- Commit SHA: "314c963ecab31a4a789d56d9b79ea887ab2c1e0f"
- Alembic revision: "038"
- Contract version: "default_strategy_postfix_2026_04_18"
- Note: Live relaunch after retiring the contaminated pre-fix default-strategy run.

## Retired Run
- Recorded at: 2026-04-18T01:34:45.944901+02:00
- Retired run id: "64ccf1d7-d9d8-4d0b-853d-77e839f0975f"
- External labels: "pre_fix_invalid_for_evidence", "retired_after_truth_boundary_remediation"
- Note: Pre-fix run retired after missing_execution_decision and risk_state_uninitialized truth-boundary remediation.

## Bootstrapped Evidence Run
- Recorded at: 2026-04-18T01:34:45.944901+02:00
- Run id: "b613c902-dd2c-408b-8079-eefd3c1a0bb9"
- Launch boundary: "2026-04-17T23:34:45.944901+00:00"
- Release tag: "unreleased-postfix-2026-04-18"
- Commit SHA: "314c963ecab31a4a789d56d9b79ea887ab2c1e0f"
- Alembic revision: "038"
- Contract version: "default_strategy_postfix_2026_04_18"
