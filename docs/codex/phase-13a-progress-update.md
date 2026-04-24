# Phase 13A Progress Update

## Status

This note records the implementation work completed for Phase 13A.

It does not replace [Phase 13 Proposal - Strategy Lifecycle, Promotion-Grade Evidence, and Bounded Autonomy](</C:/Users/frido/.codex/worktrees/c1cc/Signal Market Terminal/docs/Current roadmaps/phase-13-autonomous-profitability-roadmap.md:1>).

Its purpose is to document what is now implemented in the repo so later milestones can stay aligned with the roadmap instead of reopening already-finished lifecycle groundwork.

Phase 13A is now complete in the repo. The follow-on Milestone 3 slice for hard risk budgets and capital bounds has also landed on top of that lifecycle foundation, so the "next step" for roadmap planning is no longer budget enforcement itself; it is the bounded autonomy control-plane work that depends on those hard rails.

## Implemented In This Pass

### 1. Strategy registry and versioned lifecycle scaffolding

The repo now has persisted first-class entities for:

* `strategy_family`
* `strategy_version`
* `promotion_gate_policy`
* `promotion_evaluation`
* `demotion_event`

The current built-in family posture table remains the seed source, but registry state is now durable and queryable.

### 2. Strategy-version linkage on current evidence lanes

The following evidence surfaces now carry `strategy_version_id` in persistence:

* strategy runs
* paper trades
* replay runs
* live orders
* pilot scorecards
* pilot readiness reports
* pilot incidents
* pilot guardrail events

This keeps the Phase 13A registry tied to real evidence instead of standing apart as metadata only.

### 3. Read-only lifecycle surface

The repo now exposes a narrow `Strategies` API and frontend page for:

* family posture
* current version
* seeded gate policies
* evidence counts by version
* latest promotion evaluation

This is intentionally read-only and does not widen autonomy.

### 4. Promotion-evaluation recording from readiness evidence

Promotion evaluations are now persisted from the existing pilot readiness flow.

Current behavior:

* readiness generation records a durable `promotion_evaluation`
* the evaluation binds to the current strategy version and gate policy version
* provenance includes policy key, config hash, and market-universe hash
* the evaluation records a policy verdict and recommended tier

This is the first real implementation of the roadmap requirement that promotion decisions be durable and provenance-backed.

### 5. Replay-backed promotion evaluations and replay provenance surfacing

Replay runs now also record durable `promotion_evaluation` rows instead of leaving replay as evidence-only metadata.

Current behavior:

* completed and failed replay runs write a replay-gate evaluation tied to family, version, and gate-policy version
* replay evaluations remain conservative in Phase 13A and keep the recommended tier at `shadow_only`
* replay run payloads now surface strategy-version provenance and the recorded promotion-evaluation reference
* the `Strategies` page now distinguishes replay-backed gate evidence from pilot-readiness gate evidence

This keeps replay inside the same lifecycle contract as live readiness evidence without widening autonomy.

### 6. Live API lifecycle metadata exposure

The current live and pilot evidence APIs now expose version and gate metadata directly instead of requiring the operator to cross-reference the registry page manually.

Current behavior:

* live orders, live fills, and live order events now carry `strategy_version` snapshots and the latest version-scoped promotion evaluation when available
* pilot status now exposes the active strategy version and latest gate verdict for the active family
* scorecards and readiness reports now return lifecycle metadata alongside the evidence artifact itself
* the Pilot Console now shows the current lifecycle version and gate verdict while remaining a supervised operator surface

This is still read-only lifecycle surfacing. It does not change submit modes or widen autonomy.

### 7. Cross-surface comparison by strategy version

The lifecycle registry now exposes a compact evidence-alignment summary for each strategy version so replay, live shadow, scorecards, and readiness can be reviewed side by side without hand-matching IDs across pages.

Current behavior:

* every version in the `Strategies` registry now carries an `evidence_alignment` block
* the alignment block summarizes the latest replay run, latest live-shadow evidence, latest scorecard, and latest readiness report for that exact version
* live shadow is summarized conservatively from persisted version-linked live orders and still preserves 24-hour gap and coverage counts
* version rows now also carry the latest version-scoped promotion evaluation so the comparison view can show both the evidence surfaces and the current gate verdict together
* the `Strategies` page now renders a read-only comparison table for each family instead of forcing the operator to cross-reference multiple pages mentally

This keeps Phase 13A read-only while making the lifecycle evidence materially easier to inspect.

### 8. Strategy-version detail drilldown

The registry now includes a focused version-detail path so the operator can drill from the comparison row into the exact artifacts behind that version instead of stopping at summary counts and latest-state snapshots.

Current behavior:

* `GET /api/v1/strategies/versions/{version_id}` now returns a read-only version-detail payload
* the detail payload includes the version snapshot, family reference, recent replay runs, recent live-shadow evaluations, recent scorecards, recent readiness reports, and recent promotion or demotion events for that version
* the `Strategies` page now exposes an inline `Inspect` action from the evidence-alignment table
* the inline detail panel stays inside the registry surface and does not widen autonomy or add new operator controls

This is still Phase 13A lifecycle surfacing, not a rollout or capital-allocation change.

### 9. Pilot incident and guardrail lifecycle attribution

Pilot incidents and guardrail events now behave like durable lifecycle evidence instead of staying as mostly raw Phase 12 audit rows.

Current behavior:

* `polymarket_control_plane_incidents` and `polymarket_pilot_guardrail_events` now persist `strategy_version_id`
* write paths attribute incidents and guardrails on insert from linked live orders first and otherwise fall back conservatively to current family or pilot-run context
* the Phase 13A registry backfill pass now fills missing linkage for older incident and guardrail rows where the family or linked order makes attribution knowable
* incident and guardrail APIs now return the version snapshot plus the latest version-scoped promotion evaluation when available
* the Pilot Console and Health surfaces now show incident and guardrail rows as belonging to a lifecycle version rather than leaving them as detached audit rows

This closes the highest-value remaining Phase 13A evidence-integrity gap without widening autonomy or adding new submit behavior.

### 10. Unified promotion-gate history and rolling-window evidence timeline

The lifecycle backend now records one durable gate timeline per strategy version instead of leaving replay, readiness, scorecards, incidents, and guardrails as partially separate verdict lanes.

Current behavior:

* replay and readiness remain the primary promotion-evaluation kinds surfaced as the default latest gate verdict across existing live and pilot payloads
* scorecards now also write durable supporting `promotion_evaluation` rows tied to the exact scorecard window, version, and gate-policy version
* incidents and guardrails now write durable supporting `promotion_evaluation` rows on insert using conservative rolling 24-hour windows scoped to the exact strategy version
* the Phase 13A registry backfill pass now reconstructs missing scorecard, incident, and guardrail gate-history rows where the source evidence and version linkage are already persisted
* `GET /api/v1/strategies/versions/{version_id}` now returns both the primary promotion evaluations and the full gate-history timeline
* the `Strategies` version-detail panel now shows the unified gate timeline inline instead of forcing operators to infer it from detached source tables

This keeps Phase 13A read-only while finally giving each strategy version a durable, inspectable promotion-gate history rather than only the latest scattered verdicts.

### 11. Production evidence loop hardening

The repo now records the current production-safety questions directly in the default-strategy evidence artifact instead of leaving them as one-off operator notes.

Current behavior:

* `GET /api/v1/paper-trading/strategy-health` exposes a named `resolution_reconciliation` block for open trades, missing resolutions, overdue open trades, pending decisions, and evidence freshness.
* `python -m app.reports` writes the same reconciliation block into the JSON review artifact and renders it in Markdown.
* the generated review now includes a `live_safety` block showing whether live trading and the pilot are fail-closed, plus counts for live orders, fills, reservations, open live position lots, active pilot configs, and active pilot runs.
* paper-trade resolution exceptions now log platform, market, outcome, and settlement direction instead of silently disappearing.
* `/api/v1/signals` accepts `limit` as an operator-friendly alias for `page_size`, and the signal feed has a composite rank/time index for production-sized reads.

This does not arm the pilot, resolve trades by itself, or change the default strategy contract. It makes the evidence loop easier to audit daily while keeping live execution disabled by default.

## What This Still Does Not Do

The repo is still not widening autonomy here.

Specifically, this pass does not:

* add new unattended submit modes
* auto-promote families
* auto-demote families
* add capital-budget enforcement beyond current systems
* add family-level autonomy tiers beyond the current seeded registry values
* generate review artifacts from read-only API requests

That is intentional. Phase 13A is the read-only lifecycle and evidence-integrity slice; the remaining work belongs to later milestones rather than more Phase 13A surface-building.

## Suggested Next Step

The first follow-on Milestone 4 fail-closed enforcement slice has now landed:

* blocked `promotion_eligibility_gate` rows for non-shadow versions now record cooling-off `demotion_event` rows
* live submission checks now fail closed for strategy versions with an active demotion event
* the pilot supervisor now records demotions from blocked eligibility verdicts and pauses the active pilot with a durable control-plane incident

The next best step after this is:

* deploy the production-evidence-loop migration, generate the next default-strategy review artifact from the server evidence, and use the reconciliation output to decide whether paper-trade resolution needs connector-specific repair before any promotion work continues

## Validation Completed

Focused validation for this implementation slice covered:

* backend strategies API tests for both registry and version-detail payloads, including unified gate-history detail
* backend replay and pilot-evidence tests for version-detail drilldown artifacts
* backend control-plane and pilot-evidence tests for scorecard, incident, and guardrail gate-history recording plus lifecycle-aware API serialization
* frontend tests for unified gate-history visibility in `Strategies`, alongside lifecycle-aware incident and guardrail visibility in `Pilot Console` and `Health`
* backend promotion/control-plane tests for demotion-event dedupe, demoted live-submission blocks, and supervisor-triggered pilot pause behavior
* backend evidence-loop tests for signal `limit` aliasing, review artifact safety/reconciliation payloads, strategy-health reconciliation output, and paper-resolution exception logging
