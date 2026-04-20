# Codex Active Roadmap

This is the active near-term roadmap for Codex work in `signal-market-terminal`.

It replaces broad exploratory planning with a short, execution-focused sequence aligned to the repo’s current posture:

- prove or falsify edge honestly before widening automation
- keep the frozen default strategy as the benchmark for the current phase
- preserve fail-closed defaults around live trading, pilot mode, approvals, and guardrails
- keep `exec_policy` as the only armable pilot family unless a later scoped change explicitly broadens that boundary

## Current posture

Codex should preserve all of the following unless a later scoped task explicitly changes them:

- live trading remains disabled by default
- pilot mode remains disabled by default
- manual approval remains required by default
- the default strategy remains frozen for evidence measurement
- read-only strategy-health surfaces must not mutate run state
- replay and pilot evidence must remain honest about coverage limits and comparability

## Completed recent slices

The following hardening work is already in place and should be treated as the new baseline:

1. Default-strategy scoped read-path hardening
   - shared default-strategy snapshot/dashboard surface
   - reduced overlapping read amplification from the paper-trading UI
   - regression coverage for read-only no-run behavior and payload equivalence

2. Default-strategy evidence gate
   - `review_verdict` surfaced through strategy health
   - compact verdict states: `not_ready | watch | keep | cut`
   - explicit blockers for:
     - `no_active_run`
     - `insufficient_observation_days`
     - `stale_pending_decisions`
     - `funnel_conservation_failure`
     - `integrity_errors`
     - `replay_coverage_limited`
   - JSON and Markdown review artifacts
   - compact operator verdict panel in the paper-trading UI

3. Review automation and latest-artifact access
   - latest default-strategy review artifact metadata surfaced through shared read-only health/dashboard payloads
   - compact Paper Trading visibility for latest review status, verdict, timestamp, and artifact paths
   - focused backend coverage for missing, partial, invalid, and unreadable artifact states

4. Verdict-threshold contract hardening
   - review-verdict threshold logic centralized in a dedicated helper
   - stable machine-readable verdict metadata for precedence, threshold version, and reason code
   - explicit docs/examples for blocker precedence and keep/watch/cut transitions

5. Evidence-path observability and alerts
   - shared read-only `evidence_freshness` payload on default-strategy health/dashboard surfaces
   - explicit visibility when the latest review artifact lags active-run activity
   - compact Paper Trading freshness visibility without adding a notification subsystem

6. Read-path/query hardening follow-up
   - default-strategy review generation now reuses the shared strategy-health comparison payload instead of rerunning the same locked comparison query window
   - focused regression coverage prevents the review path from drifting back into duplicate comparison reads

These slices reduced read-path waste and turned the prove-the-edge contract into an explicit operator-visible verdict.

## Active priority order

Codex should work these in order, one narrow slice at a time.

### Priority 1 — Review automation and latest-artifact access

Goal:
Make the evidence verdict operationally useful by ensuring the latest review artifact is easy to generate, locate, and inspect without manual digging.

Why this is first:
The repo now has a shared snapshot path and an explicit review verdict, but the operator loop is still incomplete if artifacts are hard to find or generation is too manual.

Desired outcomes:

- one obvious API and/or UI surface for the latest default-strategy review artifact metadata
- explicit exposure of latest review timestamp, verdict, artifact paths, and generation status
- safe behavior when no review exists yet
- no mutation of strategy-run state from read-only access paths

Suggested scope:

- extend the existing reports/review flow rather than inventing a parallel system
- expose latest artifact metadata through health, dashboard, or a narrow read-only reports endpoint
- optionally add a small operator-facing “latest review” panel if frontend work is minimal

Acceptance criteria:

- operators can discover the latest review artifact without shell access
- no-run behavior stays read-only and explicit
- focused backend tests cover empty state and latest-artifact serialization
- frontend validation only if UI changes

### Priority 2 — Verdict-threshold contract hardening

Goal:
Make the review verdict contract more explicit, stable, and auditable.

Why this is next:
Once operators can see the verdict and artifacts easily, the next risk is ambiguity around what actually causes `watch`, `keep`, or `cut`.

Desired outcomes:

- centralize verdict threshold logic in one narrow module or helper path
- document exact threshold semantics and precedence between blockers vs positive signals
- make serialization stable enough for future automation/reporting without broadening scope

Suggested scope:

- isolate threshold logic from broader health assembly where feasible
- add doc examples for each verdict state
- add regression tests for threshold precedence and edge cases

Acceptance criteria:

- verdict transitions are deterministic and test-covered
- docs clearly explain threshold and precedence behavior
- no pilot/live scope changes

### Priority 3 — Evidence-path observability and alerts

Goal:
Improve operator trust in the evidence loop by surfacing backlog, stale-state, and generation health more clearly.

Why this matters:
A correct verdict is less useful if the underlying evidence path is stale, blocked, or silently degraded.

Desired outcomes:

- metrics or health fields for stale pending decisions, recent review generation, and evidence freshness
- explicit visibility when the review artifact is old relative to current run activity
- narrow alerting-oriented signals without adding a new notification framework

Suggested scope:

- health payload additions
- Prometheus counters/gauges where a metric already fits existing patterns
- optional compact frontend indicators only if very small

Acceptance criteria:

- stale evidence state becomes operator-visible
- no broad alerting subsystem is introduced
- focused tests cover stale/fresh state serialization

### Priority 4 — Read-path/query hardening follow-up

Goal:
Continue reducing expensive or duplicated reads in default-strategy and review surfaces.

Why later:
Recent slices already addressed the most obvious overlap. This should now be a targeted follow-up based on observed hotspots rather than speculative rewrites.

Desired outcomes:

- eliminate any remaining redundant strategy-health/review queries
- prefer shared scope-backed reads over parallel recomputation
- add or document indexes only where clearly justified by measured paths

Suggested scope:

- inspect query-heavy paths tied to review generation and dashboard refreshes
- keep changes narrow and measurable

Acceptance criteria:

- reduced overlap in the changed path
- no benchmark-semantic changes
- updated tests for payload equivalence where appropriate

### Priority 5 — Roadmap and onboarding hygiene

Goal:
Keep Codex aligned with the current mission and prevent drift into older broader roadmaps.

Why this remains important:
The repo has multiple historic planning documents. Codex should have one short active roadmap and explicit deferrals so it does not widen scope accidentally.

Desired outcomes:

- keep this file current when active priorities change
- ensure `CODEX_START_HERE.md` and `README.md` continue to point to the canonical posture
- explicitly mark broad or historical initiatives as deferred where useful

Acceptance criteria:

- no ambiguity about current near-term priorities
- onboarding docs remain aligned with shipped behavior

## Explicitly deferred work

The following are not current priorities and should not be pulled into unrelated slices:

- adding new pilot families
- enabling live trading by default
- reducing approval requirements by default
- semi-automatic or broader autonomous live expansion
- new strategy families unrelated to the current evidence loop
- broad detector expansion
- large frontend redesigns
- SaaS/admin/community/monetization work from older roadmap documents
- replay redesign beyond what is needed for honest coverage surfacing

## Execution rules for Codex

For each task:

1. prefer a plan-first pass
2. implement only one narrow slice
3. preserve fail-closed behavior
4. update docs when behavior changes
5. add focused regression coverage
6. state clearly what was and was not validated

Validation baseline for most slices:

- `python -m pytest backend/tests/test_config.py -q`
- focused backend tests for the changed surface
- `npm run frontend:validate` only if frontend code changed
- `npm run secrets:scan` only when repo-level changes justify it

## Current recommended next task

**Roadmap and onboarding hygiene**

Concrete prompt seed for Codex:

> Align README.md, CODEX_START_HERE.md, and the active roadmap around the current prove-or-falsify posture, and explicitly mark broader historical initiatives as deferred where that prevents scope drift.
