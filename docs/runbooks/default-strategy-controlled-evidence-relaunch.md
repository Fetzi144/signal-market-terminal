# Default Strategy Controlled Evidence Relaunch

This runbook operationalizes the `v0.4.1` truth-boundary relaunch for the default strategy.

## Preconditions

- deploy from a clean remediation commit
- annotate that commit as `v0.4.1`
- verify the target database is at Alembic revision `038`
- treat all pre-fix artifacts as historical/debug only

## Operator Paths

Container-local operator actions live in:

```bash
python -m app.ops.default_strategy_evidence --help
```

Supported actions:

- `record-boundary`
- `retire-active-run`
- `bootstrap-run`
- `pending-watch`

## Evidence Boundary Metadata

The first valid post-fix run should be bootstrapped with explicit metadata, for example:

```bash
python -m app.ops.default_strategy_evidence bootstrap-run ^
  --launch-boundary-at 2026-04-15T12:00:00Z ^
  --evidence-boundary-id v0.4.1 ^
  --release-tag v0.4.1 ^
  --commit-sha 87a4315b81b81365d9ee974aff5b130813757897 ^
  --migration-revision 038 ^
  --contract-version default_strategy_v0.4.1 ^
  --use-balanced-gate
```

This metadata is frozen into `strategy_run.contract_snapshot`.

## Smoke Validation Order

Run these in an isolated `docker-compose.prod.yml` clone with `db`, `backend`, and `worker` only:

1. `GET /api/v1/paper-trading/default-strategy/run` returns `no_active_run`
2. `GET /api/v1/paper-trading/strategy-health` returns `bootstrap_required = true`
3. worker metrics show `smt_default_strategy_scheduler_no_active_run_total >= 1`
4. targeted pytest smoke passes:
   - `tests/test_default_strategy_measurement.py::test_default_strategy_read_endpoints_do_not_create_rows_without_active_run`
   - `tests/test_default_strategy_measurement.py::test_default_strategy_run_requires_explicit_bootstrap`
   - `tests/test_default_strategy_measurement.py::test_strategy_health_funnel_reconciles_qualified_opened_skipped_and_pending`
   - `tests/test_default_strategy_measurement.py::test_strategy_health_flags_missing_execution_decision_as_integrity_error`
   - `tests/test_default_strategy_measurement.py::test_strategy_health_uses_persisted_drawdown_state_for_headline`
   - `tests/test_default_strategy_measurement.py::test_strategy_health_never_reports_local_total_exposure_for_shared_global_block`
   - `tests/test_reports.py::test_review_generator_surfaces_shared_global_reasons_and_persisted_drawdown`
   - `tests/test_trading_intelligence_api.py::test_scheduler_does_not_bootstrap_run_or_stamp_metadata_without_active_run`
   - `tests/test_trading_intelligence_api.py::test_scheduler_no_active_run_metric_increments_even_when_no_signals_are_available`
5. retire any pre-fix active run on the live target
6. re-run the low-risk live read checks
7. bootstrap the first valid evidence run with an explicit `launch_boundary_at`

If any check fails, stop and fix it before treating the run as evidence.

## Pending Decision Watch

Daily pending-watch check:

```bash
python -m app.ops.default_strategy_evidence pending-watch --stale-after-seconds 21600
```

Also review `pending_decision_watch` from `GET /api/v1/paper-trading/strategy-health`.

## Automation

For the full host-side workflow on Windows, use:

`scripts/Invoke-ControlledEvidenceRelaunch.ps1`
