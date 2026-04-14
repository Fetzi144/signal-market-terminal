# Phase 8A Closeout

## What changed in Phase 8A

Phase 8A adds a structural edge engine that turns event-level payoff constraints into auditable, executable-aware research surfaces without enabling live multi-leg trading.

Key changes:

- Added durable structure storage:
  - `market_structure_groups`
  - `market_structure_group_members`
  - `market_structure_runs`
  - `market_structure_opportunities`
  - `market_structure_opportunity_legs`
  - `cross_venue_market_links`
- Added `backend/app/ingestion/structure_engine.py` to:
  - build event/group constraint registries from Phase 2 Polymarket metadata
  - persist explicit cross-venue mappings
  - scan groups for conservative executable package dislocations
  - persist per-run, per-opportunity, and per-leg audit rows
- Added operator APIs for structure status, runs, groups, members, opportunities, legs, manual build/scan triggers, and explicit cross-venue link management.
- Extended `/api/v1/health`, `/api/v1/ingest/polymarket/status`, Prometheus metrics, and the Health page with a compact Phase 8A section.
- Added hermetic backend tests for neg-risk, parity/complement, cross-venue basis, API wiring, rerun idempotency, executable-leg rejection, and health serialization.

This phase is still research/paper-first. It does not submit live structure bundles, perform on-chain neg-risk conversion, or implement maker-economics optimization.

## Structure opportunity semantics

`neg_risk_direct_vs_basket`

- Built from standard Polymarket neg-risk events in Phase 2 metadata.
- Each persisted opportunity anchors one named outcome.
- Compared package:
  - direct leg: `buy_no(anchor_outcome)`
  - basket legs: `buy_yes(all other named outcomes)`
- The engine records which package is cheaper now and the gross/net edge between the cheaper package and the richer conversion-equivalent package.

`binary_complement`

- Built from explicit binary Yes/No pairs.
- Compared package:
  - `buy_yes`
  - `buy_no`
- Edge is measured against the expected unit payout of `1.0`.
- Actionable only when both legs are executable and the executable bundle cost is below parity after costs.

`event_sum_parity`

- Built from mutually exclusive event outcome sets.
- Compared package:
  - `buy_yes` on every named outcome in the event
- Edge is measured against the expected event-level sum constraint of `1.0`.
- Informational/non-actionable when the event composition is incomplete or augmented in a way this phase does not trust.

`cross_venue_basis`

- Built only from explicit durable links in `cross_venue_market_links`.
- Compared packages:
  - `left_yes + right_no`
  - `right_yes + left_no`
- The cheaper executable package is kept as the detected basis direction.
- Actionable only when the mapped legs are individually executable, fresh enough, and still positive after venue-specific costs.

## How package pricing is computed

Default pricing method is `all_cross_now`.

Meaning:

- every leg is priced using immediate executable estimates from current book state
- no midpoint-only pricing is used
- no passive fill optimism is assumed for package detection

Polymarket leg pricing:

- starts from the Phase 4 reconstructed live book
- uses Phase 2 tick size, min order size, and fee schedule metadata
- walks visible depth conservatively for the requested package size
- uses Phase 6 directional entry semantics:
  - `buy_yes` crosses the ask side
  - `buy_no` uses the economically equivalent no-entry price from the yes-side book
- invalidates a leg when:
  - reconstruction is unreliable
  - touch price is missing
  - visible depth is insufficient
  - fillable size is below min order size
  - slippage exceeds the configured cap

Generic cross-venue leg pricing:

- uses the latest explicit `OrderbookSnapshot` for the linked outcome
- requires the snapshot to be within the configured max staleness window
- prices `buy_yes` from the linked outcome ask book
- prices `buy_no` from an explicit sibling `No` outcome ask book
- applies only explicit fee/min-size settings passed through the mapping surface

Opportunity totals:

- `gross_edge_total` compares package costs before fees
- `net_edge_total` compares package costs after estimated fees
- `gross_edge_bps` / `net_edge_bps` normalize edge versus the cheaper executable reference package
- `executable_all_legs` reflects whether every required leg passed the executable checks
- `actionable` requires:
  - positive executable net edge
  - all legs executable when configured
  - edge above `POLYMARKET_STRUCTURE_MIN_NET_EDGE_BPS`

## How augmented neg-risk outcomes are filtered

Phase 8A treats augmented neg-risk events conservatively.

Implemented rules:

- placeholder outcomes and explicit `Other` outcomes are still stored as visible group members
- those members are marked with explicit roles such as `placeholder` and `other`
- they are not used as normal actionable package legs in this phase
- default config keeps augmented neg-risk actionable detection disabled
- when augmented composition makes the event ambiguous, the group remains informational/non-actionable

Operational effect:

- operators can inspect the full event composition later
- the engine does not silently pretend placeholder or `Other` legs are normal tradeable structure legs

## How cross-venue mappings are sourced and bounded

Cross-venue work is deliberately narrow in Phase 8A.

Implemented rules:

- mappings come only from explicit rows in `cross_venue_market_links`
- no fuzzy text matching or broad entity-resolution engine is used
- the mapping surface stores concrete left/right venue identifiers and optional fee/min-size details
- cross-venue scanning is disabled by default unless explicitly configured
- stale snapshots or missing executable legs prevent actionability

This keeps cross-venue basis historical, queryable, and operator-reviewable without turning Phase 8A into a generalized entity-resolution platform.

## What remains out of scope

Still intentionally not implemented in Phase 8A:

- live multi-leg order submission
- on-chain neg-risk conversion execution
- maker rebates / liquidity rewards optimization
- full portfolio optimizer or risk graph
- replay-quality structural simulator
- unattended strategy automation for structure bundles
- live pilot / production multi-leg rollout
- broad UI redesign or admin tooling expansion

## What the next phase should start next

The next sequenced slice should stay downstream of this research-first foundation:

- maker-economics and quote-quality work for package-level execution realism
- richer risk coordination for multi-leg bundles and shared exposure controls
- simulator/replay work to calibrate structure execution assumptions
- only after those pieces, any explicitly approved live structural pilot path

Phase 8A should remain the auditable detection and visibility layer that those later phases build on.
