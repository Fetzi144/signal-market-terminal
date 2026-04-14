# Phase 10 Closeout

## Summary

Phase 10 adds a durable, explainable risk graph and portfolio optimizer layer on top of the existing Polymarket execution stack. The backend now persists graph nodes and explicit graph edges, builds historical exposure snapshots across paper positions, live-disabled order state, reservations, structure plans, and advisory maker quotes, and emits advisory optimizer recommendations plus inventory-control snapshots that can be consumed by Phase 8 paper routing and the Phase 9 quote optimizer.

The implementation stays conservative and paper-first. It does not place hedges, does not enable live market making, does not add replay simulation, and does not expand into fuzzy entity-resolution or broad control-plane automation. The raw public websocket boundary remains unchanged and the repo remains advisory-only by default.

## What Changed In Phase 10

- Added Phase 10 persistence for:
  - `risk_graph_nodes`
  - `risk_graph_edges`
  - `risk_graph_runs`
  - `portfolio_exposure_snapshots`
  - `portfolio_optimizer_recommendations`
  - `inventory_control_snapshots`
- Added `backend/app/ingestion/polymarket_risk_graph.py` to:
  - build explicit graph nodes and edges from Phase 2 metadata, Phase 8 structure groups, and explicit cross-venue links
  - create conservative exposure snapshots with source provenance
  - compute advisory portfolio recommendations and inventory controls
  - provide health/status summaries plus manual/operator entry points
- Integrated Phase 10 into:
  - worker scheduling
  - ingest/health APIs
  - Phase 8 structure paper-plan risk checks
  - Phase 9 advisory quote recommendations
  - paper-trading risk fallback logic
- Added Prometheus metrics for graph builds, snapshot runs, optimizer runs, concentration, recommendation counts, no-quote totals, timestamps, and maker/taker utilization.

## Graph Node Semantics

- `event`
  - venue-scoped event buckets sourced from Phase 2 metadata.
- `market`
  - market-level buckets keyed by venue and `condition_id`.
- `asset`
  - asset/outcome buckets keyed by venue and asset identity.
  - Phase 10 now reuses real Phase 2 asset ids when they already exist, so structure groups, quote recommendations, reservations, and paper positions land on the same graph nodes.
- `venue`
  - venue-level buckets for Polymarket and explicit cross-venue assets.
- `entity`
  - explicit non-category tags from market metadata.
  - no fuzzy entity resolution was added.
- `category`
  - event category/subcategory and category-like tags.
- `conversion_group`
  - explicit Phase 8 structure groups such as negative-risk baskets, complements, parity groups, and reviewed cross-venue basis groupings.

## Graph Edge Semantics

- `same_event`
  - connects assets, markets, events, and venues through explicit Phase 2 relationships.
- `complement`
  - connects explicit binary complements within the same condition.
- `conversion_equivalent`
  - connects explicit Phase 8 structure-group members to their conversion-group node with signed weights.
- `cross_venue_hedge`
  - connects only explicit/manual cross-venue mappings.
  - hedge relief is applied only when this durable edge exists.
- `same_entity`
  - connects explicit entity-tag nodes to markets/assets.
- `category_link`
  - connects event/category metadata and category-like tags.

All graph edges are persisted with source/provenance metadata so grouping remains auditable.

## Exposure Snapshot Semantics

- Snapshots include, when enabled:
  - open paper positions
  - non-terminal live orders
  - open capital reservations
  - active structure paper orders/plans
  - recent advisory maker quotes
- Each snapshot stores:
  - gross and net notional
  - buy and sell notional
  - share exposure
  - reservation cost
  - hedged fraction
  - source provenance in `details_json`
- Aggregate rows are recorded for:
  - asset
  - market
  - event
  - entity
  - category
  - conversion group
  - venue
- Explicit hedge relief is conservative:
  - complement and cross-venue hedge edges can raise `hedged_fraction`
  - cross-venue relief only applies when the explicit edge exists
  - no fuzzy or text-only hedge matching was introduced

## Optimizer Recommendation Semantics

- Recommendations are advisory-only and deterministic.
- Current recommendation types emitted by Phase 10:
  - `allow`
  - `reduce_size`
  - `block`
  - `hedge_preferred`
  - `skew_quote`
  - `no_quote`
- Current controls consider:
  - event exposure caps
  - entity exposure caps
  - conversion-group exposure caps
  - maker and taker inventory budgets
  - hedge completeness
  - reservation/inventory pressure
- Reason codes stay explicit and stable, including:
  - `event_cap_exceeded`
  - `entity_cap_exceeded`
  - `conversion_group_cap_exceeded`
  - `inventory_toxicity_exceeded`
  - `hedge_incomplete`

Phase 10 intentionally stops at explainable heuristics. It does not implement opaque stochastic optimization, auto-hedging, or a full portfolio rebalancer.

## Inventory-Aware Quote Control Semantics

- Phase 9 quote recommendations now consume Phase 10 inventory controls when the risk graph is enabled.
- The advisory quote optimizer can now:
  - reduce recommended size caps
  - apply reservation-price shifts
  - skew quotes directionally
  - widen into no-quote zones when inventory becomes too toxic
- Maker/taker budget tracking is kept separate.
- Quote actions remain advisory-only:
  - no automatic quote placement
  - no unattended live market-making behavior
  - live trading remains disabled by default

## What Remains For Phase 11

- Replay-quality simulation and historical execution-quality replay remain out of scope and were not started here.
- Queue-position and fill-realism modeling remain limited to the existing conservative assumptions.
- Broader live rollout, unattended execution controls, and control-plane automation remain future work.
- Entity normalization is still explicit/manual; broad fuzzy entity linking remains intentionally out of scope.

## Advisory-Only Default

Yes. The repo is still advisory-only by default.

- Phase 7A live trading remains disabled unless separately enabled.
- Phase 9 quote output remains advisory.
- Phase 10 portfolio and inventory outputs only adjust recommendations and paper-routing approvals; they do not place or manage live trades automatically.
