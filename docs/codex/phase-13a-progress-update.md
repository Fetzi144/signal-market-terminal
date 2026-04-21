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

## What This Still Does Not Do

The repo is still not widening autonomy here.

Specifically, this pass does not:

* add new unattended submit modes
* auto-promote families
* auto-demote families
* add capital-budget enforcement beyond current systems
* make replay itself emit promotion evaluations yet

That is intentional. This pass stays inside Phase 13A rather than jumping into later milestones prematurely.

## Suggested Next Step

The next best step after this pass is:

* expose strategy-version metadata and promotion-evaluation references more broadly across existing live and replay APIs

After that, the next milestone-ready step is:

* add replay-backed promotion evaluations so the registry can compare replay and live promotion evidence side by side

After this pass, the next best step becomes:

* expose version and gate metadata more broadly across the live APIs so replay and live evidence can be compared from both directions

## Validation Completed

Focused validation for this implementation slice covered:

* backend strategy-run and strategies API tests
* backend pilot-evidence tests for persisted promotion evaluations
* frontend tests for `Strategies`, `PilotConsole`, and `PaperTrading`
