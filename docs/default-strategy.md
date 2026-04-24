# Default Strategy Measurement Contract

The default strategy exists to answer one narrow question: does the frozen baseline produce trustworthy evidence of edge once we account for realistic execution and risk controls?

This document defines the measurement surfaces only. It does not authorize new detectors, threshold changes, or broader live-trading scope.

## Read-Only Verification

The following endpoints are read-only when `scope=default_strategy` is used:

- `GET /api/v1/paper-trading/default-strategy/dashboard`
- `GET /api/v1/paper-trading/portfolio?scope=default_strategy`
- `GET /api/v1/paper-trading/history?scope=default_strategy`
- `GET /api/v1/paper-trading/metrics?scope=default_strategy`
- `GET /api/v1/paper-trading/pnl-curve?scope=default_strategy`
- `GET /api/v1/paper-trading/strategy-health`

`GET /api/v1/paper-trading/default-strategy/dashboard` is a read-only snapshot of the same scoped portfolio, metrics, P&L-curve, and strategy-health surfaces. It exists to reduce duplicate dashboard read pressure, not to change benchmark semantics.

These endpoints must never create a `strategy_run`. If there is no active default-strategy run, the read path returns:

- `state = "no_active_run"`
- `bootstrap_required = true`
- empty portfolio, history, metrics, and P&L-curve payloads

Creating a run is explicit:

- `GET /api/v1/paper-trading/default-strategy/run` inspects current run state without mutating anything.
- `POST /api/v1/paper-trading/default-strategy/bootstrap` creates the run anchor intentionally.
- The scheduler/worker path does not auto-bootstrap a run. If no active run exists, default-strategy processing no-ops and leaves signals untouched.

For controlled evidence relaunches, the bootstrap payload should also carry explicit boundary metadata:

- `launch_boundary_at`
- `evidence_boundary_id`
- `release_tag`
- `commit_sha`
- `migration_revision`
- `contract_version`
- `evidence_gate`

## Immutable Run Anchor

Every prove-the-edge output is anchored to one immutable `strategy_run.id`.

- The run boundary is the active run's `started_at`.
- The run state persists drawdown-sensitive fields:
  - `peak_equity`
  - `current_equity`
  - `max_drawdown`
  - `drawdown_pct`
- The drawdown breaker uses those persisted values instead of reconstructing a breaker from whatever trades are currently loaded.

This keeps breaker behavior auditable and reversible.

## Canonical Funnel Ledger

Run-scoped funnel accounting is derived from `ExecutionDecision`, not from `signal.details.default_strategy`.

For every qualified signal after the run boundary, exactly one of the following must be true:

- `opened_trade`
- `skipped`
- `pending_decision`

`pending_decision` is now a stored `ExecutionDecision.decision_status`, not an inferred absence of a row. A qualified signal with no run-scoped decision row is an integrity failure.

`pending_decision_watch` exposes the oldest pending decision timestamp, current pending count, max pending age, stale count, retry window, and reason-code counts so stale pending rows are visible instead of silent.

Retryable execution-context pending decisions are not immortal. The scheduler retries them only within the configured `paper_trading_pending_decision_max_age_seconds` window. Once that window expires, the row is converted to `skipped` with `reason_code = "pending_decision_expired"` while preserving the last retryable reason in decision diagnostics.

Backlog-repair rows are expired again at the end of the same scheduler pass, so historical qualified signals cannot be reintroduced as already-stale pending decisions until the next pass.

The core invariant is:

```text
qualified_signals = opened_trade_signals + skipped_signals + pending_decision_signals
```

Strategy-health surfaces expose `trade_funnel.conservation_holds`. If the system finds impossible states, it records explicit `integrity_errors` instead of silently guessing.

Examples of surfaced integrity failures:

- trade exists without a matching run-scoped execution decision
- decision says `opened` but no trade exists
- trade exists for a decision whose status is not `opened`

## Risk Attribution Semantics

Blocked decisions preserve both the block label used by the default strategy and the original upstream risk recommendation details.

Two categories are surfaced separately:

- `local_paper_book`
  - local exposure caps, cluster caps, invalid sizing, and local book rejections
- `shared_global`
  - shared platform controls, risk-graph blocks, or other cross-strategy/global controls

Strategy-health outputs expose:

- aggregate counts for local vs shared/global blocks
- reason counts by scope
- original reason codes/details inside decision diagnostics

This makes it visible when a run appears to have local risk issues but was actually blocked upstream by shared/global controls.

## Benchmark Modes

Comparison output is intentionally split into two honest modes.

### `signal_level`

- Unit: `per_share`
- Default-strategy cohort: eligible resolved default-strategy signals
- Benchmark cohort: resolved legacy rank-threshold signals

This mode is for cohort-level signal quality, calibration, and 1-share P&L comparison.

### `execution_adjusted`

- Unit: `usd`
- Default-strategy cohort: resolved paper trades for the active run
- Benchmark: currently unavailable for legacy parity in this remediation slice

This mode is for actual run-scoped trade outcomes after paper-trade sizing and execution realism.

These modes must not be merged into one field or one score. If legacy execution-adjusted parity does not exist, the output must say so explicitly rather than imply parity.

## Replay Truth Boundary

Replay outputs now resolve outcomes from canonical market settlement data rather than from the latest observed price marker.

Replay status also exposes explicit coverage metadata:

- `coverage_mode`
- `coverage_scope`
- `global_coverage_mode`
- `configured_supported_detectors`
- `supported_detectors`
- `unsupported_detectors`
- `global_supported_detectors`
- `global_unsupported_detectors`

Interpretation:

- `supported_detectors_only`: the replay window only contained detectors the replay stack currently understands
- `partial_supported_detectors`: only part of observed detector activity was covered
- `unsupported_detectors_only`: the replay output is not evidence for the full system
- `no_detector_activity`: no qualifying detector activity was present in the inspected window

Partial replay coverage is instrumentation, not proof. It must never be presented as full-system evidence.

Default-strategy review health narrows `coverage_mode` to the frozen default-strategy detector scope. The global detector view remains visible as `global_coverage_mode` and `global_unsupported_detectors`, so unrelated historical detector activity does not falsely block a confluence-only default-strategy verdict.

## Operator Verdict Surface

`GET /api/v1/paper-trading/strategy-health` and `GET /api/v1/paper-trading/default-strategy/dashboard` now expose a shared `review_verdict` object for the prove-the-edge phase.

The verdict enum is intentionally narrow:

- `not_ready`
- `watch`
- `keep`
- `cut`

`review_verdict.blockers` is the explicit evidence gate. It surfaces the blocking reason directly instead of expecting operators to infer it from raw funnel or replay fields. Current blocker families are:

- `no_active_run`
- `insufficient_observation_days`
- `stale_pending_decisions`
- `funnel_conservation_failure`
- `integrity_errors`
- `replay_coverage_limited`

`review_verdict.threshold_version` is the stable contract tag for downstream automation/reporting. The current value is `default_strategy_review_v1`.

`review_verdict.reason_code` is the compact machine-readable outcome reason. Current values are:

- `blocked`
- `positive_consensus`
- `negative_consensus`
- `no_resolved_trades`
- `insufficient_consensus`
- `mixed_evidence`

`review_verdict.precedence` is currently `blockers_first`.

Verdict precedence is intentionally strict:

1. If any blocker is present, verdict is `not_ready` even if P&L or CLV looks positive.
2. Otherwise verdict is `keep` only when all of the following are true:
   - `resolved_trades > 0`
   - execution-adjusted default-strategy P&L is positive
   - signal-level default-strategy P&L per share is positive
   - average CLV is positive
3. Otherwise verdict is `cut` only when all of the following are true:
   - `resolved_trades > 0`
   - execution-adjusted default-strategy P&L is negative
   - signal-level default-strategy P&L per share is negative
   - average CLV is negative
4. Otherwise verdict is `watch`.

Examples:

- `not_ready`
  - the run is still inside the minimum observation window
  - a stale pending decision remains open
  - replay coverage is limited
- `watch`
  - blockers are clear but there are still zero resolved trades
  - blockers are clear but one input is flat or missing
  - blockers are clear but P&L and CLV disagree
- `keep`
  - blockers are clear, there is at least one resolved trade, and execution-adjusted P&L, signal-level P&L, and CLV are all positive
- `cut`
  - blockers are clear, there is at least one resolved trade, and execution-adjusted P&L, signal-level P&L, and CLV are all negative

The verdict remains default-strategy scoped. It may use legacy/benchmark comparison data for context, but it must not widen the pilot family, relax fail-closed behavior, or treat pilot/live readiness as prove-the-edge evidence.

## Review Artifacts

The strategy review flow now emits both markdown and JSON artifacts under `docs/strategy-reviews/`, anchored to the active `strategy_run.id`.

- Markdown review: `YYYY-MM-DD-default-strategy-baseline.md`
- JSON review artifact: `YYYY-MM-DD-default-strategy-baseline.json`

Both artifacts should carry the same `review_verdict` payload so operator reads, generated reviews, and downstream automation do not drift into separate verdict semantics.

Review generation should also reuse the shared `comparison_modes` payload from strategy health when it is already available. The read-only health/dashboard surface remains the canonical comparison assembly, and the review generator should not rerun the same measurement query window unless that shared payload is unexpectedly missing.

`GET /api/v1/paper-trading/strategy-health` and `GET /api/v1/paper-trading/default-strategy/dashboard` also expose a read-only `latest_review_artifact` metadata object. It surfaces the newest generated review artifact status, timestamp, recoverable verdict, repo-relative artifact paths, the artifact's stored run/contract references, and a `generation_guidance` block with the canonical backend command/runbook path without generating a review or mutating run state.

`generation_guidance` is intentionally advisory only. It points operators at the manual path:

- run from `backend/`: `python -m app.reports`
- boundary-sensitive workflow: `docs/runbooks/default-strategy-controlled-evidence-relaunch.md`

The read-only health/dashboard surfaces must never execute that command on the operator's behalf.

In production compose, the host checkout's `docs/` directory is mounted into backend and worker containers at `/docs`.
The backend image runs from `/app`, so generated review artifacts resolve to `/docs/...` in-container while still persisting to the host checkout across rebuilds.

Production compose can also run a scheduler-owned review generation job. It generates only when there is an active run and the latest artifact is missing/partial/invalid, mismatched to the active run boundary, or older than active-run activity. Pending-decision expiry timestamps count as execution-decision activity, but the job does not regenerate only because pending decisions are stale.

Generated review artifacts now include two production evidence sections:

- `Live Automation Safety`: records whether live trading and the pilot are fail-closed, plus counts for live orders, fills, reservations, open live lots, active pilot configs, and active pilot runs.
- `Resolution Reconciliation`: records open trades, missing resolutions, overdue open trades, resolved trades/signals, pending-decision backlog, oldest pending age, and representative overdue trade examples.

The JSON artifact carries the same data under `live_safety` and `resolution_reconciliation`. These sections are operator evidence only; they do not arm the pilot, resolve trades, or change strategy state.

## Evidence Freshness Surface

`GET /api/v1/paper-trading/strategy-health` and `GET /api/v1/paper-trading/default-strategy/dashboard` also expose a read-only `evidence_freshness` object.

Its job is narrow: show whether the latest review artifact is still current relative to active-run activity and whether stale pending decisions are already degrading the evidence loop.

Current `evidence_freshness.status` values are:

- `no_active_run`
- `missing_review`
- `fresh`
- `stale`

Important fields:

- `latest_review_generated_at`
- `latest_review_generation_status`
- `review_age_seconds`
- `review_lag_seconds`
- `review_outdated`
- `artifact_identity_status`
- `artifact_identity_summary`
- `artifact_run_matches_active_run`
- `artifact_contract_version_matches_active_run`
- `artifact_evidence_boundary_matches_active_run`
- `last_activity_at`
- `last_activity_kind`
- `pending_decision_count`
- `pending_decision_max_age_seconds`
- `pending_decisions_stale`
- `pending_decision_stale_after_seconds`

Semantics:

- `review_outdated = true` means the newest review artifact predates the newest active-run activity currently visible on the read path.
- `artifact_identity_status = mismatch` means the latest review artifact does not belong to the currently active `strategy_run` and should be treated as stale evidence even if its timestamp is recent.
- `last_activity_kind` currently resolves to the newest of:
  - `strategy_run_started`
  - `paper_trade`
  - `execution_decision` (`decision_at`, plus pending-expiry timestamps recorded in decision details)
- `pending_decisions_stale = true` means pending decisions are past the configured retry window and should be treated as evidence degradation, not as silently acceptable backlog.
- `missing_review` is explicit. The read path must not generate a review just to clear that state.

This surface is instrumentation only. It helps operators understand evidence freshness, but it does not mutate the run, create artifacts, or override the blocker-first `review_verdict` contract.

## Live Submission Gate

Autonomy tier and live submission readiness are separate. Runtime autonomy state includes an additive `submission_gate` object with:

- `state`: `blocked`, `operator_required`, `simulated`, or `permitted`
- `reason_codes`
- `operator_required`
- `live_order_submit_permitted`

`assisted_live` means the strategy lifecycle can support assisted live operation. It does not mean orders can currently submit. Operators should treat `submission_gate.state` and `live_order_submit_permitted` as the runtime safety truth.

## Evidence vs Instrumentation

Evidence is the narrow set of outputs that can support a keep/watch/cut decision on the frozen baseline:

- run-scoped paper P&L
- signal-level cohort comparison
- execution-adjusted trade outcomes
- persisted drawdown state
- reconciled run funnel
- explicit risk attribution
- replay results with truthful coverage labels

Instrumentation is everything that helps explain or debug the evidence but does not replace it:

- diagnostic details on execution decisions
- risk recommendation payloads
- shadow-execution overlays
- detector coverage metadata
- health/readiness convenience summaries

When there is uncertainty, the system should surface it directly rather than infer a cleaner story than the data supports.

## Controlled Evidence Relaunch

Use [docs/runbooks/default-strategy-controlled-evidence-relaunch.md](C:/Code/Signal Market Terminal/docs/runbooks/default-strategy-controlled-evidence-relaunch.md) for the `v0.4.1` relaunch procedure, including:

- pre-fix run retirement
- explicit boundary logging
- prod-compose clone smoke checks
- worker metrics validation
- first valid post-fix bootstrap
