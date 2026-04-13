# Phase 2 Build Pack

## Scope freeze

Phase 2 is backend-only. It does exactly two things:

1. Add Polymarket execution-metadata history for parameters that affect fills and economics.
2. Add structural event and market registry layers enriched from Gamma plus stream lifecycle messages.

Do not add:

- order book reconstruction
- trade tape or open-interest storage
- live execution, OMS, or user-stream work
- detector rewrites to consume the new metadata directly
- portfolio graph logic
- new operator UI beyond serializer compatibility if strictly necessary

## Dependency contract

Phase 2 assumes Phase 1A already exists:

- `polymarket_market_events` is the raw append-only source of truth
- `polymarket_stream_status` is already available for ingest health
- the existing `markets` and `outcomes` tables remain the canonical active universe for now

Use these rules throughout:

- exchange time wins when present
- every derived row still preserves `received_at_local`
- raw payloads stay in Phase 1A storage; Phase 2 tables are derived state, not raw replacements

## Migration contract

Assume current Alembic head is `023`. Renumber only if the repo has moved.

### Migration 024 — create `polymarket_market_parameter_history`

Create an append-only history table for execution-critical market parameters.

Columns:

- `id uuid pk`
- `market_id uuid null fk markets.id on delete set null`
- `outcome_id uuid null fk outcomes.id on delete set null`
- `platform_market_id varchar(255) null`
- `condition_id varchar(255) null`
- `token_id varchar(255) null`
- `source_kind varchar(32) not null` (`gamma`, `stream_lifecycle`, `rest_sync`)
- `source_reference varchar(255) null`
- `observed_at_exchange timestamptz null`
- `received_at_local timestamptz not null`
- `tick_size numeric(18,8) null`
- `min_order_size numeric(18,8) null`
- `fee_enabled boolean null`
- `negative_risk_enabled boolean null`
- `details jsonb not null default '{}'`
- `created_at timestamptz not null`

Indexes:

- `(market_id, received_at_local)`
- `(condition_id, received_at_local)`
- `(token_id, received_at_local)`
- `(observed_at_exchange)`
- `(source_kind, received_at_local)`

Behavior:

- append a new row only when a semantic parameter value changes
- do not emit duplicate history rows for identical parameter snapshots
- if only some fields are known from a source, persist the known subset and leave the rest null

### Migration 025 — create `polymarket_event_registry`

Create a current-state registry for Polymarket events.

Columns:

- `id uuid pk`
- `gamma_event_id varchar(255) null unique`
- `event_slug varchar(512) null unique`
- `title text not null`
- `category varchar(128) null`
- `end_date timestamptz null`
- `negative_risk_enabled boolean null`
- `metadata jsonb not null default '{}'`
- `first_seen_at_exchange timestamptz null`
- `last_seen_at_exchange timestamptz null`
- `first_seen_at_local timestamptz not null`
- `last_seen_at_local timestamptz not null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

Indexes:

- `(event_slug)`
- `(end_date)`
- `(last_seen_at_local)`

### Migration 026 — create `polymarket_market_registry`

Create a current-state Polymarket market registry keyed to the existing `markets` table.

Columns:

- `market_id uuid pk fk markets.id on delete cascade`
- `event_registry_id uuid null fk polymarket_event_registry.id on delete set null`
- `platform_market_id varchar(255) not null unique`
- `condition_id varchar(255) null index`
- `question_slug varchar(512) null`
- `accepting_orders boolean null`
- `closed boolean null`
- `archived boolean null`
- `active boolean null`
- `negative_risk_enabled boolean null`
- `fee_enabled boolean null`
- `tick_size numeric(18,8) null`
- `min_order_size numeric(18,8) null`
- `last_lifecycle_message_type varchar(64) null`
- `last_lifecycle_event_time timestamptz null`
- `last_lifecycle_received_at_local timestamptz null`
- `metadata jsonb not null default '{}'`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

Indexes:

- `(condition_id)`
- `(event_registry_id)`
- `(last_lifecycle_received_at_local)`

Behavior:

- this is a mutable current-state sidecar table
- do not overload the generic `markets.metadata` blob with every Polymarket-specific execution field
- keep `markets` and `outcomes` intact; Phase 2 enriches them rather than replacing them

## Application changes

### Models

Create:

- `backend/app/models/polymarket_metadata.py`

Register in:

- `backend/app/models/__init__.py`

### Metadata extraction services

Create:

- `backend/app/ingestion/polymarket_metadata.py`

Responsibilities:

- extract market parameter facts from Gamma payloads
- parse lifecycle-oriented stream messages from Polymarket raw events
- map external identifiers back onto `Market` / `Outcome`
- upsert current-state event and market registries
- append parameter history only on semantic change

### Gamma enrichment path

Reuse the existing Polymarket Gamma client instead of inventing a second universe crawler.

Required behavior:

- enrich only Polymarket markets already present in `markets`
- read `conditionId`, `eventSlug`, `tickSize`, `minOrderSize`, fee flags, and negative-risk flags when available
- resolve Gamma event/group context into `polymarket_event_registry`
- populate or refresh `polymarket_market_registry`
- append to `polymarket_market_parameter_history` when execution-critical parameters materially change

Add a callable entrypoint that can be run from the scheduler, for example:

- `sync_polymarket_metadata(session: AsyncSession) -> dict`

### Stream lifecycle path

Extend the Phase 1A stream ingest flow to interpret lifecycle-style messages without replacing raw persistence.

Rules:

- raw stream frames must still persist first
- lifecycle handlers may update `polymarket_market_registry` after raw persistence
- if a stream message updates execution-critical parameters, append a history row
- if a message cannot be mapped to a known market, log it clearly and skip the derived write; do not fail the worker

### Scheduler integration

Add a dedicated Phase 2 metadata job to `backend/app/jobs/scheduler.py`.

Requirements:

- keep existing market discovery and snapshot jobs unchanged
- run metadata sync on its own interval
- record success/failure in `ingestion_runs`
- keep the job Polymarket-only

Recommended new config:

- `POLYMARKET_METADATA_SYNC_INTERVAL_SECONDS`

## API surface

Only add read APIs needed to inspect the new metadata foundation.

Recommended endpoints:

- `GET /api/v1/ingest/polymarket/metadata/status`
- `GET /api/v1/markets/{market_id}/metadata`

`/markets/{market_id}/metadata` should return:

- event registry summary
- current market-registry fields
- latest execution-critical parameter values
- last metadata update timestamps

Do not add editing surfaces.

## Acceptance tests

Codex should add tests for:

1. Gamma enrichment populates `polymarket_event_registry` for known Polymarket markets.
2. Gamma enrichment populates `polymarket_market_registry` with `condition_id` and current flags.
3. Parameter history appends a row on first observation.
4. Parameter history does not append a duplicate row when values are unchanged.
5. Parameter history appends a new row when tick size, min order size, fee flag, or negative-risk flag changes.
6. Stream lifecycle message updates current registry state after raw persistence succeeds.
7. Unmappable lifecycle messages do not crash the worker.
8. Market metadata API returns registry and latest parameter values for a Polymarket market.
9. Existing generic market listing endpoints still work unchanged.
10. SQLite tests still pass with the new metadata tables.

## Reviewer checklist

Before merge, verify:

- raw Phase 1A payloads remain untouched and append-only
- every derived Phase 2 history row keeps `received_at_local`
- exchange time is used when available for derived metadata timing
- no duplicate parameter-history rows are created for identical values
- the generic `markets` table is not being turned into a Polymarket-only schema
- event and market registry updates remain auditable and source-aware
- no Phase 3+ scope has slipped in
