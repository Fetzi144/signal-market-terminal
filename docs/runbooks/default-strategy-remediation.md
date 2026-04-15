# Default Strategy Remediation Runbook

This runbook covers the post-remediation default-strategy measurement behavior.

## 1. Bootstrap A Run Explicitly

Read paths no longer create a run implicitly.
The scheduler/worker path also no longer creates a run implicitly.

To inspect the current state without mutation:

```bash
curl http://localhost:8000/api/v1/paper-trading/default-strategy/run
```

If the response returns `state = "no_active_run"` and `bootstrap_required = true`, create the run intentionally:

```bash
curl -X POST http://localhost:8000/api/v1/paper-trading/default-strategy/bootstrap ^
  -H "Content-Type: application/json" ^
  -d "{\"launch_boundary_at\":\"2026-04-15T12:00:00Z\"}"
```

Optional fields:

- `launch_boundary_at`
- `bootstrap_started_at`
- `evidence_boundary_id`
- `release_tag`
- `commit_sha`
- `migration_revision`
- `contract_version`
- `evidence_gate`

Use them only when you need an explicit historical boundary. The created `strategy_run.id` becomes the immutable anchor for all subsequent prove-the-edge outputs.

## 2. Verify Health Without Mutating State

These endpoints are safe to call for default-strategy verification:

- `GET /api/v1/paper-trading/default-strategy/run`
- `GET /api/v1/paper-trading/strategy-health`
- `GET /api/v1/paper-trading/portfolio?scope=default_strategy`
- `GET /api/v1/paper-trading/history?scope=default_strategy`
- `GET /api/v1/paper-trading/metrics?scope=default_strategy`
- `GET /api/v1/paper-trading/pnl-curve?scope=default_strategy`

Expected no-run behavior:

- `strategy_run = null`
- `run_state = "no_active_run"`
- `bootstrap_required = true`
- zero-row portfolio/history/metrics outputs

If a verification call creates a run, that is a bug and should be treated as a measurement integrity failure.

## 3. What To Check Once A Run Exists

Minimum operator checks:

- `trade_funnel.conservation_holds` is `true`
- `trade_funnel.integrity_errors` is empty
- `pending_decision` counts come from stored decision rows, not from missing-row inference
- `pending_decision_watch.max_age_seconds` is reviewed so stale pending rows do not accumulate silently
- `risk_blocks.local_paper_book_blocks` and `risk_blocks.shared_global_blocks` are reviewed separately
- `headline.current_equity`, `headline.peak_equity`, and `headline.drawdown_pct` move consistently with resolved trades
- `comparison_modes.signal_level.unit = "per_share"`
- `comparison_modes.execution_adjusted.unit = "usd"`
- any unavailable benchmark fields say so explicitly instead of returning blended totals

Worker-only scheduler metrics are exposed from the worker container on `http://localhost:9101/metrics` by default. Use that endpoint to inspect `smt_default_strategy_scheduler_no_active_run_total` during controlled relaunch smoke checks.

For replay outputs, verify:

- outcomes resolve from canonical settlement data
- `coverage_mode` is understood before treating replay as evidence
- `unsupported_detectors` is empty before claiming full detector coverage

## 4. Why Start A Fresh Run After This Remediation

A fresh run is recommended because the measurement contract is now stricter:

- funnel accounting is run-scoped and decision-ledger based
- risk blocks are separated into local vs shared/global sources
- drawdown state is persisted on the run itself
- benchmark outputs no longer mix signal-level and execution-adjusted units
- replay coverage and settlement truth are explicit

Older runs may still exist, but they were measured under looser semantics. Do not compare pre-remediation and post-remediation runs as if they are interchangeable.

## 5. Evidence Vs Instrumentation

Treat these as evidence:

- run-scoped paper P&L
- reconciled funnel counts
- persisted drawdown state
- explicit risk-block attribution
- honest benchmark modes
- replay outputs with truthful settlement and detector coverage labels

Treat these as instrumentation:

- diagnostic payloads on execution decisions
- shadow-execution overlays
- convenience review narratives
- low-level risk recommendation payloads

Instrumentation helps explain the run. It does not replace the evidence boundary.

For the full post-fix relaunch sequence, use [docs/runbooks/default-strategy-controlled-evidence-relaunch.md](C:/Code/Signal Market Terminal/docs/runbooks/default-strategy-controlled-evidence-relaunch.md).
