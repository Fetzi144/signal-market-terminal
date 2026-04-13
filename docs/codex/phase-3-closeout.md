# Phase 3 Closeout

## What Changed

- added an append-only Polymarket raw storage layer with:
  - `polymarket_raw_capture_runs`
  - `polymarket_book_snapshots`
  - `polymarket_book_deltas`
  - `polymarket_bbo_events`
  - `polymarket_trade_tape`
  - `polymarket_open_interest_history`
- built a resumable raw projector that materializes existing and newly arriving `polymarket_market_events` into specialized Phase 3 tables without moving the raw WS truth boundary
- preserved full-book provenance explicitly:
  - WS `book` events land as `ws_book`
  - REST `/books` snapshots land as `rest_seed_snapshot`, `rest_periodic_snapshot`, `rest_manual_snapshot`, or `rest_resync_snapshot`
- added watched-scope capture jobs for:
  - periodic/manual `/books` snapshots
  - public Data API `/trades` backfill
  - public Data API `/oi` polling
- extended ingest/operator APIs, health serialization, metrics, worker startup, and cleanup so raw storage status and manual actions are visible and auditable
- kept Phase 2 registry bridging in place so new raw rows resolve into `polymarket_market_dim` and `polymarket_asset_dim` whenever that registry state exists

## Core Guarantees

- `polymarket_market_events` remains the canonical append-only WS ingest truth
- all Phase 3 tables are append-only materializations or observed-history tables, not mutable “current state” tables
- raw projection is idempotent and resumable by raw event id
- REST-derived snapshots, trades, and OI samples keep explicit provenance and are not blended into WS truth
- public trade tape dedupes deterministic overlap between WS `last_trade_price` rows and Data API `/trades` backfill
- Phase 2 param-history dedupe remains in force when `/books` snapshots seed or refresh registry parameters

## Intentionally Untouched For Later Phases

- deterministic in-memory L2 reconstruction
- hash drift detection and automated repair loops beyond storing snapshot/hash provenance
- replay simulator and fill simulator
- OMS/EMS, user stream, fills reconciliation, or live execution policy changes
- microstructure features, labels, or structural-edge logic
- ClickHouse, object storage, Parquet export, or archival replay infrastructure

## Retention And Archival For Now

- Phase 3 uses hot-database retention only
- the cleanup job now prunes old Phase 3 raw tables and completed raw capture runs using `POLYMARKET_RAW_RETENTION_DAYS`
- default retention is 14 days unless overridden
- no object-storage or long-horizon archival/export path is introduced in this phase
- later archival work should export from the append-only raw layer rather than replacing it

## What Phase 4 Should Start Next

- build deterministic book reconstruction on top of `polymarket_book_snapshots`, `polymarket_book_deltas`, and `polymarket_bbo_events`
- add book-hash drift checks and resync-repair logic against the stored snapshot provenance
- use the retained public trade tape and OI history as replay inputs, not as execution-state substitutes
- keep the projector/materializer split so reconstruction and simulation remain downstream of raw truth
