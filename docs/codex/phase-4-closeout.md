# Phase 4 Closeout

## What Changed

- added a Phase 4 reconstruction control plane with:
  - `polymarket_book_recon_state`
  - `polymarket_book_recon_incidents`
- built a deterministic in-memory L2 reconstruction service over the Phase 3 raw layer:
  - seeds from the latest authoritative full snapshot
  - replays later book deltas in `raw_event_id` then `delta_index` order
  - reconciles against stored BBO events
  - restores state on worker startup without replaying full history when a recent checkpoint exists
- connected reconstruction to existing watch scope, metadata registry, stream incidents, raw storage projection, and REST resync machinery instead of creating a parallel truth path
- added operator/API/health/metrics coverage for:
  - reconstruction status
  - per-asset state lookup
  - recent reconstruction incidents
  - reconstructed top-of-book lookup
  - manual catch-up / manual resync
- kept REST resync snapshots flowing back into the existing raw snapshot layer with explicit `rest_resync_snapshot` provenance

## Core Guarantees

- `polymarket_market_events` remains the append-only Polymarket truth boundary
- reconstruction is downstream of stored raw truth; it does not block WS ingest or raw projection
- authoritative seed boundaries come from real stored full snapshots, not synthetic snapshots inferred from deltas
- delta replay order is deterministic per asset:
  - `raw_event_id` is the primary replay boundary
  - `delta_index` is preserved within a single `price_change` event
- restart recovery is bounded and idempotent:
  - recent checkpoints are reused
  - only post-seed deltas are replayed
  - repeated replay does not double-apply levels

## Drift And Resync Rules

- reconstruction marks drift conservatively on:
  - missing seed before replay
  - invalid delta application
  - BBO mismatch against the local reconstructed top of book
  - reconnect / gap-suspected stream incidents from the existing Phase 1B control plane
  - stale books that have not been reconciled within the configured bound
- drift and repair behavior is append-only and audible:
  - incidents are written to `polymarket_book_recon_incidents`
  - latest runtime/checkpoint state is written to `polymarket_book_recon_state`
  - resyncs log `resync_started`, `resync_succeeded`, or `resync_failed`
- automatic resync is gated by config and cooldown
- manual resync uses the existing REST `/books` capture path, persists a fresh `rest_resync_snapshot`, then reseeds reconstruction from that new authoritative boundary

## Intentionally Untouched For Later Phases

- replay-quality fill simulation
- execution action policy (`cross`, `post`, `step-ahead`, `skip`)
- executable EV gating
- user-stream ingestion and execution reconciliation
- OMS / live orders / live fills
- maker rebates / reward economics
- microstructure features, labels, or structural-edge logic
- archival redesign, ClickHouse, Parquet, or object-storage replay infrastructure

## What The Next Phase Should Start Next

- build the replay-quality simulator on top of the now-deterministic reconstructed books
- add fill simulation and action-policy logic as downstream consumers of reconstruction state, not as mutations of raw truth
- keep user-stream execution reconciliation and OMS work separate from the reconstruction control plane
- preserve the current repair model: raw truth first, reconstruction second, simulation/policy after that
