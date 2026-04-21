# Phase 13A Progress Update

## Status

This note records the implementation work completed for the first Phase 13A slices.

It does not replace [Phase 13 Proposal - Strategy Lifecycle, Promotion-Grade Evidence, and Bounded Autonomy](</C:/Users/frido/.codex/worktrees/c1cc/Signal Market Terminal/docs/Current roadmaps/phase-13-autonomous-profitability-roadmap.md:1>).

Its purpose is to document what is now implemented in the repo so the next slice can stay aligned with the roadmap instead of reopening already-finished groundwork.

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

## What This Still Does Not Do

The repo is still not widening autonomy here.

Specifically, this pass does not:

* add new unattended submit modes
* auto-promote families
* auto-demote families
* add capital-budget enforcement beyond current systems
* add family-level autonomy tiers beyond the current seeded registry values
* add direct deep links from version detail into the existing replay, live, and health pages with shared version filters

That is intentional. This pass stays inside Phase 13A rather than jumping into later milestones prematurely.

## Suggested Next Step

The next best step after this pass is:

* add strategy-version filters and deep links across the existing replay, live, and pilot APIs so the new detail panel can hand off into native surfaces without client-side guesswork

## Validation Completed

Focused validation for this implementation slice covered:

* backend strategies API tests for both registry and version-detail payloads
* backend replay and pilot-evidence tests for version-detail drilldown artifacts
* frontend tests for the `Strategies` inspect workflow
