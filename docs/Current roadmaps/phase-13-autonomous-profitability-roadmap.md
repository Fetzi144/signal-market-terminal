# Phase 13 Proposal - Strategy Lifecycle, Promotion-Grade Evidence, and Bounded Autonomy

## Status

This document is a forward-looking architecture and policy proposal.

It does not replace `docs/Current roadmaps/codex-active-roadmap.md` as the current near-term execution source unless the repo explicitly adopts this phase.

Its purpose is to define what would need to change in Signal Market Terminal to move honestly from:

* supervised research and pilot tooling

to:

* versioned strategy lifecycle management
* promotion-grade evidence
* bounded autonomy for selected families
* explicit promotion, demotion, and rollback semantics

This document is intentionally not a sprint board. It is the strategic parent document for a narrower implementation roadmap such as Phase 13A.

## Why A Separate Phase 13 Proposal Exists

The current repo posture is intentionally narrower:

* the default strategy is a frozen benchmark
* `exec_policy` is the only armable pilot family
* structure remains research-active
* maker remains advisory-only
* cross-venue basis remains disabled
* live trading remains fail-closed and operator-supervised

That posture is still the correct description of the shipped system.

If the repo later chooses to pursue bounded unattended trading, it should do so through an explicit new phase rather than by stretching existing benchmark and pilot semantics beyond what they were designed to prove.

## Core Thesis

Signal Market Terminal should not try to make the frozen default strategy autonomous first.

Instead:

* keep the default strategy as the benchmark truth anchor
* treat `exec_policy` as shared execution infrastructure, not the thing being proven profitable
* promote one versioned strategy family at a time
* target `structure` as the first autonomy lane
* keep `maker` behind stronger replay, queue, and inventory proof
* keep cross-venue disabled until executable hedge routing exists

In this phase, the system earns stronger operating labels only after it passes explicit policy gates. Profitability is not assumed from paper results, replay, or one short live window.

## Phase 13 Objectives

1. Introduce a versioned strategy lifecycle that supports benchmarking, candidate promotion, rollback, and demotion.
2. Upgrade replay and shadow evaluation into promotion-grade evidence inputs.
3. Add bounded autonomy through explicit rollout tiers instead of a binary manual-vs-auto switch.
4. Make capital and risk budgets enforceable before unattended submission widens.
5. Attribute performance honestly across alpha, execution, fees, rewards, inventory, and risk.
6. Preserve fail-closed behavior and append-only auditability as the default even after autonomy is introduced.

## Non-Goals

* no broad multi-family autonomy on day one
* no removal of kill switches, pauses, or demotion paths
* no cross-venue basis until executable hedging exists
* no automatic strategy creation or detector proliferation
* no use of one headline P&L number as the only promotion criterion
* no assumption that a strategy is autonomy-ready merely because it was live-profitable over a short window

## Guiding Principles

### 1. The benchmark stays frozen

The default strategy remains the truth anchor for longitudinal comparison. It should not become the first autonomy candidate.

### 2. Evidence and autonomy are separate concepts

A strategy version may be:

* evidence-eligible but not promotion-eligible
* promotion-eligible but not unattended-eligible
* unattended-eligible at a micro-budget but not at broader capital limits

### 3. Promotion is reversible

Every promotion must have an explicit rollback and demotion path.

### 4. Bounded means actually bounded

No family should enter automatic submission unless enforceable risk and capital ceilings already exist.

### 5. Labels must remain honest

The system should avoid outcome-loaded claims. It should describe what has been proven, under what policy, over what window, with what dependencies.

## Required Conceptual Changes

### New First-Class Entities For The Initial Slice

These are the minimum first-class concepts that should become explicit early:

* `strategy_family`
* `strategy_version`
* `autonomy_tier`
* `promotion_gate_policy`
* `promotion_evaluation`
* `demotion_event`

### Concepts That Can Start Narrower

These may begin as derived fields or lighter-weight persisted artifacts before becoming richer entities:

* `experiment_run`
* `regime_snapshot`
* `profitability_attribution`
* `capital_budget`

### Current Concepts That Need Reframing

* `exec_policy`

  * From: the only armable pilot family
  * To: shared execution infrastructure used by one or more strategy families
* `default_strategy`

  * From: the main thing under review
  * To: the frozen benchmark used to keep the alpha loop honest while autonomy grows elsewhere
* `pilot readiness`

  * From: an operator-facing advisory summary
  * To: one input into explicit promotion and demotion policy

## Target End State

The target end state is not "fully hands-off everywhere."

The target end state is:

* one or more strategy families can trade unattended within bounded capital and scope
* promotion into unattended operation is reversible, audited, and threshold-driven
* demotion is automatic on safety or evidence breaches
* profitability evidence is net of fees and transparent about reward dependence
* replay, shadow, and live evidence stay comparable and coverage-limited where needed
* family failures can be isolated without automatically widening or collapsing the whole platform

## Rollout Tiers

Phase 13 should add a shared autonomy-tier state machine instead of directly enabling full automation.

Recommended tiers:

1. `shadow_only`

   * no live submission
   * replay and shadow only
2. `assisted_live`

   * live intents created
   * manual approval required
3. `bounded_auto_submit`

   * automatic submit within strict family and capital bounds
   * kill-switch and pause semantics remain armed
4. `bounded_unattended`

   * unattended within narrow scope and bounded budget
   * automatic demotion on breaches

`portfolio_auto` should not be part of the initial Phase 13 rollout. It belongs in a later phase after multiple families independently prove themselves.

## Proof Contract For Promotion And Stronger Operating Labels

Phase 13 should define promotion as a policy decision rather than a marketing claim.

Promotion and autonomy should answer three different questions:

1. **Can this version be compared honestly?**
2. **Has this version shown enough evidence to be promoted?**
3. **At what autonomy tier may it operate?**

Promotion and stronger operating labels should require a configurable policy covering all of the following:

* minimum live sample size
* minimum calendar observation window
* positive net realized P&L after fees
* explicit handling of maker rebates and liquidity rewards
* acceptable max drawdown
* acceptable live-vs-shadow gap
* acceptable incident rate
* acceptable replay and shadow coverage quality
* acceptable reconciliation reliability
* acceptable approval-latency or queue-health metrics where relevant
* acceptable concentration and correlation risk

The repo should avoid hardcoding one permanent threshold set in this document.

Instead, Phase 13 should introduce versioned gate policies such as:

* `promotion_gate_policy_v1`
* `promotion_gate_policy_v2`

Every promotion or demotion decision should record which policy version produced it.

## Re-Promotion And Cooling-Off Semantics

Demotion alone is not enough. Phase 13 should also define:

* the exact fallback tier after demotion
* the required cooling-off period
* the fresh evidence window needed for re-promotion
* whether re-promotion is automatic or requires explicit operator approval

Without this, automatic demotion is only half a state machine.

## Provenance Requirements

Every promotion evaluation should bind to immutable provenance that can be reconstructed later.

Recommended provenance set:

* strategy version
* execution policy version
* risk policy version
* promotion gate policy version
* fee schedule version
* reward schedule version
* market-universe hash
* relevant config hash

This should be queryable and durable, not left as scattered operator notes.

## Proposed Milestones

### Milestone 1 - Strategy Lifecycle And Registry

Goal:
Separate benchmarking, experimentation, and live promotion into explicit versioned entities.

Backend changes:

* Add new model and service layer for:

  * `backend/app/models/strategy_registry.py`
  * `backend/app/strategies/registry.py`
  * `backend/app/strategies/promotion.py`
* Refactor:

  * `backend/app/strategy_families.py`
  * `backend/app/default_strategy.py`
  * `backend/app/strategy_runs/service.py`
  * `backend/app/paper_trading/analysis.py`
* Add API surfaces:

  * `backend/app/api/strategies.py`

Frontend changes:

* Add a strategy lifecycle page:

  * `frontend/src/pages/Strategies.jsx`
* Extend:

  * `frontend/src/pages/PaperTrading.jsx`
  * `frontend/src/pages/PilotConsole.jsx`

Acceptance criteria:

* benchmark strategy remains read-only and frozen
* strategy families and versions are persisted, queryable, and auditable
* live orders, paper trades, replay runs, and evidence artifacts carry `strategy_version`
* the UI can distinguish:

  * benchmark
  * candidate
  * promoted
  * demoted

### Milestone 2 - Promotion Gates, Provenance, And Replay-Grade Evidence

Goal:
Turn replay and shadow into trusted promotion inputs and make policy evaluation durable.

Backend changes:

* Expand:

  * `backend/app/ingestion/polymarket_replay_simulator.py`
  * `backend/app/ingestion/polymarket_microstructure.py`
  * `backend/app/ingestion/polymarket_execution_policy.py`
  * `backend/app/ingestion/structure_engine.py`
  * `backend/app/ingestion/polymarket_maker_economics.py`
* Add:

  * `backend/app/replay/calibration.py`
  * `backend/app/replay/coverage_policy.py`
  * `backend/app/execution/promotion_gates.py`

Required capability upgrades:

* replay support for family-scoped promotion candidates
* live-vs-replay parity metrics by family and version
* fee schedule versioning and reward schedule versioning
* stronger coverage semantics for unsupported or weakly modeled scenarios
* durable promotion evaluations tied to explicit policy versions and provenance hashes

Acceptance criteria:

* replay can produce family-scoped promotion summaries
* live-vs-shadow parity is visible by strategy version
* coverage-limited states remain explicit and can block promotion when required
* promotion evidence can be compared across rolling windows, not just one-off runs

### Milestone 3 - Hard Risk Budgets And Capital Bounds

Goal:
Make risk and capital budgets enforceable before unattended submission widens.

Backend changes:

* Expand:

  * `backend/app/ingestion/polymarket_risk_graph.py`
  * `backend/app/execution/polymarket_capital_reservation.py`
  * `backend/app/portfolio/service.py`
  * `backend/app/paper_trading/engine.py`
* Add:

  * `backend/app/risk/budgets.py`
  * `backend/app/risk/regime.py`
  * `backend/app/risk/risk_of_ruin.py`

Required capability upgrades:

* family-level capital budgets
* market, event, entity, and cluster caps
* regime-aware sizing
* concentration and correlation-aware exposure controls
* explicit capacity ceilings
* promotion blocks when size-dependent edge collapses

Acceptance criteria:

* no family can enter automatic submission without enforceable bounded capital policy
* budget breaches can trigger pauses or demotions automatically
* exposure and capacity limits are surfaced in the API and UI

### Milestone 4 - Autonomous Control Plane And Tiered Rollout

Goal:
Replace the current manual pilot posture with a bounded autonomy state machine.

Backend changes:

* Refactor:

  * `backend/app/execution/polymarket_control_plane.py`
  * `backend/app/execution/polymarket_pilot_supervisor.py`
  * `backend/app/execution/polymarket_live_reconciler.py`
  * `backend/app/execution/polymarket_order_manager.py`
  * `backend/app/execution/polymarket_pilot_evidence.py`
  * `backend/app/api/polymarket_live.py`
* Add:

  * `backend/app/execution/autonomy_tiers.py`
  * `backend/app/execution/demotion_rules.py`

Required capability upgrades:

* family-scoped autonomy tiers
* automatic promotion eligibility evaluation
* automatic demotion on gate failure
* micro-budget and bounded-unattended modes
* explicit order provenance linking:

  * strategy version
  * execution policy version
  * risk policy version
  * promotion gate policy version

Acceptance criteria:

* autonomy can be enabled for one family without broadening the whole platform
* every submit path is still fail-closed by default
* demotion and pause events are append-only and operator-visible
* the control plane can answer why a family is or is not autonomous

### Milestone 5 - Honest Profitability Attribution

Goal:
Distinguish alpha edge from execution quality, fee drag, reward subsidy, and inventory carry.

Backend changes:

* Expand:

  * `backend/app/execution/polymarket_pilot_evidence.py`
  * `backend/app/reports/strategy_review.py`
  * `backend/app/api/paper_trading.py`
  * `backend/app/api/polymarket_live.py`
* Add:

  * `backend/app/evaluation/profitability_attribution.py`

Frontend changes:

* Extend:

  * `frontend/src/pages/PaperTrading.jsx`
  * `frontend/src/pages/PilotConsole.jsx`
  * `frontend/src/pages/Health.jsx`

Required attribution outputs:

* gross alpha P&L
* execution drag
* fee drag
* reward contribution
* inventory carry
* slippage and queue loss
* realized vs expected edge
* regime and category breakdowns

Acceptance criteria:

* the operator can see whether a strategy is profitable without rewards
* the operator can see whether a strategy is profitable only in certain categories or regimes
* profitability proof is no longer a single aggregated number with hidden dependencies

### Milestone 6 - Structure Family Bounded Autonomy

Goal:
Promote `structure` to the first bounded autonomous family if and only if it passes the prior gates.

Why structure first:

* it already has research-active posture in the repo
* it is closer to a distinct family than the frozen benchmark
* it avoids conflating benchmark validation with autonomy rollout

Backend changes:

* Expand:

  * `backend/app/ingestion/structure_engine.py`
  * `backend/app/api/polymarket_structure.py`
  * `backend/app/execution/polymarket_control_plane.py`

Frontend changes:

* Extend:

  * `frontend/src/pages/Structures.jsx`
  * `frontend/src/pages/PilotConsole.jsx`

Acceptance criteria:

* structure opportunities can be promoted by version into bounded unattended mode
* promotion and demotion are family-scoped
* paper, replay, shadow, and live views align on the same version identifiers

### Milestone 7 - Maker Family Bounded Autonomy

Goal:
Promote maker only after stronger replay parity, inventory proof, and reward-aware attribution exist.

Backend changes:

* Expand:

  * `backend/app/ingestion/polymarket_maker_economics.py`
  * `backend/app/models/polymarket_maker.py`
  * `backend/app/execution/polymarket_order_manager.py`
  * `backend/app/execution/polymarket_live_reconciler.py`

Frontend changes:

* Add or extend maker-facing UI surfaces under:

  * `frontend/src/pages/Structures.jsx`
  * `frontend/src/pages/PilotConsole.jsx`
  * `frontend/src/pages/Health.jsx`

Acceptance criteria:

* maker promotion is blocked if queue, inventory, or reward assumptions are under-modeled
* reward-aware and reward-free profitability are both visible
* maker demotion rules include stale quote quality, inventory drift, and fill-quality degradation

## What Is Explicitly Deferred Beyond Phase 13

The following should not be part of the initial Phase 13 scope:

* multi-family autonomous portfolio allocation
* broad platform-wide autonomy
* cross-venue autonomous basis trading

Those belong in a later phase after at least two families independently pass bounded-autonomy proof under this model.

## Exact Modules To Change First

If Phase 13 were opened today, the first code areas to change should be:

1. `backend/app/strategy_families.py`

   * replace the static posture table with a persisted strategy-family and strategy-version registry
2. `backend/app/execution/polymarket_control_plane.py`

   * separate family promotion state from the current `exec_policy`-centric pilot model
3. `backend/app/execution/polymarket_pilot_supervisor.py`

   * add automatic gate evaluation and demotion behavior
4. `backend/app/execution/polymarket_pilot_evidence.py`

   * persist promotion evidence, provenance, and profitability attribution by family and version
5. `backend/app/ingestion/polymarket_replay_simulator.py`

   * expand replay support beyond the current narrow benchmark detector model
6. `backend/app/api/polymarket_live.py`

   * expose autonomy tiers, promotion gates, and demotion history
7. `frontend/src/pages/PilotConsole.jsx`

   * evolve from a manual pilot console into a rollout-lifecycle console
8. `frontend/src/pages/PaperTrading.jsx`

   * preserve benchmark visibility while separating it from promoted live candidates

## Proposed Database And Migration Work

Phase 13 will require schema work. Expected migration areas include:

* strategy family registry tables
* strategy version tables
* versioned gate-policy tables
* promotion evaluation tables
* demotion audit tables
* family and version linkage on existing:

  * live orders
  * fills
  * scorecards
  * readiness reports
  * replay runs
  * paper trades

The repo should not fake this with JSON blobs alone. Promotion and autonomy proof need queryable relational structure.

## Frontend Product Changes

The UI should evolve from an operator dashboard into a strategy lifecycle console.

Recommended new or expanded surfaces:

* `Strategies`

  * registry, posture, versions, and promotion state
* `Promotion Gates`

  * gate status, blockers, and policy version
* `Autonomy Timeline`

  * shadow, assisted, bounded auto, unattended, demotion history
* `Profit Attribution`

  * alpha, execution, fees, rewards, and risk breakdown
* `Regime Breakdown`

  * performance by category, market state, and liquidity regime

The benchmark page should remain prominent so the system does not lose its truth anchor while becoming more automated.

## Operational And Security Work Required

Phase 13 is not only a modeling and UI effort.

It also requires stronger unattended-operations guarantees:

* secure server-only credential handling for Polymarket API credentials
* stronger restart and reconnect handling around authenticated user streams
* venue fee and reward schedule versioning
* richer heartbeat and reconcile SLOs
* explicit credential rotation and degraded-mode behavior

These are necessary because unattended submission depends on authenticated trading endpoints and user-channel reliability rather than public market data alone.

## Validation Strategy

Each milestone should add focused regression coverage instead of relying on one end-to-end smoke test.

Recommended validation themes:

* strategy-registry model tests
* promotion-gate policy tests
* replay coverage and calibration tests
* demotion-rule tests
* profitability-attribution tests
* family-scoped API tests
* bounded autonomy control-plane tests
* frontend tests for promotion, demotion, and gate visibility

## Recommended Order Of Adoption

If the repo chooses to adopt Phase 13, the recommended execution order is:

1. Milestone 1
2. Milestone 2
3. Milestone 3
4. Milestone 4
5. Milestone 5
6. Milestone 6
7. Milestone 7

This order is intentional:

* do not add autonomy before registry and versioning
* do not promote before replay and calibration improve
* do not widen submission before hard risk budgets exist
* do not add maker autonomy before attribution can separate reward subsidy from real edge

## Immediate Next Slice If Phase 13 Is Accepted

The first implementation slice should be:

# Phase 13A - Strategy Registry, Gate Policy Versioning, and Read-Only Lifecycle Surfaces

## Goal

Create the minimum durable structure needed for promotion-grade autonomy work without widening autonomy yet.

## Concrete Output

* persisted strategy family and version registry
* strategy-version linkage on live, paper, and replay evidence
* versioned promotion gate policy model
* promotion evaluation records with provenance linkage
* a narrow `Strategies` API
* a compact frontend registry surface

## What Phase 13A Explicitly Does Not Do

* no new autonomous submit modes
* no portfolio allocator
* no broad maker rollout
* no broad strategy generation workflow

## Suggested Initial Files

Backend:

* `backend/app/models/strategy_registry.py`
* `backend/app/strategies/registry.py`
* `backend/app/strategies/promotion.py`
* `backend/app/api/strategies.py`

Refactors:

* `backend/app/strategy_families.py`
* `backend/app/default_strategy.py`
* `backend/app/strategy_runs/service.py`
* `backend/app/paper_trading/analysis.py`

Frontend:

* `frontend/src/pages/Strategies.jsx`
* `frontend/src/pages/PilotConsole.jsx`
* `frontend/src/pages/PaperTrading.jsx`

## Acceptance Criteria For Phase 13A

* benchmark strategy remains frozen and clearly labeled
* strategy families and versions are queryable and auditable
* evidence artifacts can be joined to version identifiers cleanly
* promotion policies are versioned and inspectable
* the UI distinguishes benchmark, candidate, promoted, and demoted states
* no autonomy surface becomes wider merely because the registry exists

## Final Guardrail

The repo should only claim stronger autonomy labels after a strategy family:

* trades live in bounded unattended mode
* remains net profitable after fees across the configured proof window
* stays within drawdown and incident thresholds
* preserves replay, shadow, and reconciliation integrity
* survives demotion rules that are actually armed, not merely documented

Until then, the system should continue to describe itself as a supervised research and pilot platform with bounded live capabilities.
