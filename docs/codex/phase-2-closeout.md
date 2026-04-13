# Phase 2 Closeout

## What Changed

- added a durable Polymarket structural registry layer with:
  - `polymarket_event_dim`
  - `polymarket_market_dim`
  - `polymarket_asset_dim`
  - `polymarket_market_param_history`
  - `polymarket_meta_sync_runs`
- built a `polymarket_meta_sync` worker service that:
  - syncs Gamma events and markets via keyset pagination
  - records audited sync-run rows
  - supports startup, scheduled, and manual sync paths
  - seeds missing watched-asset parameters from `/books` when Gamma metadata is incomplete
- extended the live stream path so `new_market`, `tick_size_change`, and `market_resolved` enrich the registry/history layer without mutating raw append-only `polymarket_market_events`
- added operator APIs, health serialization, metrics, and minimal Health page controls for metadata sync visibility and manual triggering
- backfilled `polymarket_asset_dim.outcome_id` when an existing local `outcomes.token_id` matched the Polymarket asset id

## Core Guarantees

- raw `polymarket_market_events` remains the ingestion truth source and stays append-only
- `polymarket_market_param_history` is append-only and inserts only on first observation or real parameter-state change
- Gamma syncs are idempotent across repeated runs
- stream lifecycle enrichment updates the registry incrementally while preserving the Phase 1B raw/normalized layers
- metadata provenance remains explicit:
  - Gamma updates advance `last_gamma_sync_at`
  - stream lifecycle updates advance `last_stream_event_at`
  - `/books` seeding fills gaps without pretending to be stream truth

## Intentionally Untouched For Phase 3+

- full orderbook delta storage
- trade tape ingestion
- open-interest history
- deterministic L2 reconstruction and gap-perfect replay
- microstructure features, labels, or policy changes
- OMS/EMS, live trading, or new detector logic
- ClickHouse/object-storage archival work
- frontend redesign beyond minimal operator additions

## What Phase 3 Should Start Next

- persist raw book snapshots and deltas explicitly instead of only lifecycle metadata
- add the public trade tape and open-interest capture
- start deterministic in-memory book reconstruction with hash drift detection and resync hooks
- keep the new Phase 2 registry as the dimension layer feeding those raw and replay paths
