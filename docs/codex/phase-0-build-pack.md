# Phase 0 Build Pack

## Scope freeze

Phase 0 is backend-only. It does exactly four things:

1. Add explicit timing and source fields to `signals`.
2. Add a first-class `execution_decisions` table.
3. Make execution estimation happen before a paper trade opens.
4. Preserve the current `signal.details.default_strategy` path for strategy-health compatibility.

Do not add:
- WebSocket ingestion
- live OMS/EMS
- new workers
- new detectors
- new strategy families
- frontend work beyond serializer compatibility if strictly necessary

## Migration contract

Assume current Alembic head is `019`. Renumber only if the repo has moved.

### Migration 020 — add signal timing fields

Add nullable columns to `signals`:

- `observed_at_exchange timestamptz null`
- `received_at_local timestamptz null`
- `detected_at_local timestamptz null`
- `source_platform varchar(32) null`
- `source_token_id varchar(128) null`
- `source_stream_session_id uuid null`
- `source_event_hash varchar(128) null`
- `source_event_type varchar(64) null`

Add indexes:

- `(observed_at_exchange)`
- `(received_at_local)`
- `(source_platform, observed_at_exchange)`
- `(source_token_id, observed_at_exchange)`
- `(source_stream_session_id)`

### Migration 021 — create execution_decisions

Create `execution_decisions` with:

- `id uuid pk`
- `signal_id uuid not null fk signals.id on delete cascade`
- `strategy_run_id uuid not null fk strategy_runs.id on delete cascade`
- `decision_at timestamptz not null`
- `decision_status varchar(16) not null` (`opened`, `skipped`)
- `action varchar(32) not null` (`cross`, `skip`)
- `direction varchar(16) null` (`buy_yes`, `buy_no`)
- `ideal_entry_price numeric(18,8) null`
- `executable_entry_price numeric(18,8) null`
- `requested_size_usd numeric(18,4) null`
- `fillable_size_usd numeric(18,4) null`
- `fill_probability numeric(10,6) null`
- `net_ev_per_share numeric(18,8) null`
- `net_expected_pnl_usd numeric(18,8) null`
- `missing_orderbook_context boolean not null default false`
- `stale_orderbook_context boolean not null default false`
- `liquidity_constrained boolean not null default false`
- `fill_status varchar(32) null`
- `reason_code varchar(64) not null`
- `details jsonb not null default '{}'`

Unique constraint:
- `(signal_id, strategy_run_id)`

Indexes:
- `(strategy_run_id, decision_at)`
- `(reason_code)`
- `(fill_status)`

### Migration 022 — link paper trades to execution decisions

Add nullable columns to `paper_trades`:

- `execution_decision_id uuid null fk execution_decisions.id on delete set null`
- `submitted_at timestamptz null`
- `confirmed_at timestamptz null`

Add constraints/indexes:
- unique `(execution_decision_id)`
- index `(submitted_at)`
- index `(execution_decision_id)`

## Application changes

### Models

Update:
- `backend/app/models/signal.py`
- create `backend/app/models/execution_decision.py`
- `backend/app/models/paper_trade.py`
- `backend/app/models/__init__.py`

### SignalCandidate

Extend `backend/app/signals/base.py` with:

- `observed_at_exchange: datetime | None = None`
- `received_at_local: datetime | None = None`
- `source_platform: str | None = None`
- `source_token_id: str | None = None`
- `source_stream_session_id: uuid.UUID | None = None`
- `source_event_hash: str | None = None`
- `source_event_type: str | None = None`

### Detector plumbing

Update detectors to pass the best available timing/source truth.

For current snapshot-based detectors:
- `received_at_local` = latest snapshot `captured_at` used by the signal
- `observed_at_exchange` = `None` for now
- `source_platform` = market platform
- `source_token_id` = outcome token id when available
- `source_event_type` = one of:
  - `price_snapshot`
  - `orderbook_snapshot`
  - `cross_platform_snapshot`
  - `confluence_fusion`

For `confluence.py`:
- `received_at_local` = max of contributing timestamps
- `source_event_type = "confluence_fusion"`

### Signal persistence

In `backend/app/ranking/scorer.py`:

Use:
- `reference_ts = candidate.observed_at_exchange or candidate.received_at_local or now`
- `fired_at = reference_ts`
- `detected_at_local = now`
- `dedupe_bucket = _dedupe_bucket(reference_ts)`

Persist all source fields.

### EV helper

Add to `backend/app/signals/ev.py`:

```python
def compute_directional_ev_full(
    direction: Decimal,
    estimated_probability: Decimal,
    entry_price: Decimal,
) -> dict:
    ...
```

Implementation note: the actual `direction` parameter should be a string (`buy_yes` or `buy_no`) when coded.

Return keys:
- `direction`
- `win_probability`
- `ev_per_share`
- `edge_pct`
- `entry_price`
- `potential_profit`
- `potential_loss`

### Kelly helper

Add to `backend/app/signals/kelly.py`:

```python
def kelly_size_for_trade(
    direction: str,
    estimated_probability: Decimal,
    entry_price: Decimal,
    bankroll: Decimal,
    kelly_fraction: Decimal = Decimal("0.25"),
    max_position_pct: Decimal = Decimal("0.05"),
) -> dict:
    ...
```

Return keys must match the existing size helper.

### Paper-trading engine

In `backend/app/paper_trading/engine.py` add a pre-trade builder:

- `build_execution_decision(...)`

Required order of operations:

1. default-strategy precheck
2. ideal direction + provisional Kelly from current model
3. shadow execution / fillability estimate
4. executable EV recompute at executable entry price
5. executable Kelly recompute
6. cap size by fillable size
7. risk check on actual executable size
8. open or skip

Important behavior change:
- for newly opened trades, `PaperTrade.entry_price` must be the executable entry price
- `PaperTrade.shadow_entry_price` can match executable entry price in Phase 0
- every evaluated signal in the active run should produce one `ExecutionDecision`

### Scheduler integration

In `backend/app/jobs/scheduler.py`:
- preserve `signal.details.default_strategy`
- continue writing the same strategy metadata fields
- add execution-decision persistence without breaking strategy-health

## Phase 0 reason codes

Existing precheck reasons:
- `before_baseline_start`
- `missing_outcome_id`
- `missing_probability`
- `missing_market_price`
- `missing_expected_value`
- `ev_below_threshold`

New execution-gate reasons:
- `execution_missing_orderbook_context`
- `execution_stale_orderbook_context`
- `execution_no_fill`
- `execution_partial_fill_below_minimum`
- `execution_ev_below_threshold`
- `execution_size_zero_after_fill_cap`

Existing engine reasons to preserve:
- `already_recorded`
- `risk_total_exposure`
- `risk_cluster_exposure`
- `risk_rejected`
- `opened`

## Acceptance tests

Codex should add tests for:

1. signal persistence uses candidate timing
2. dedupe bucket uses reference timestamp, not scheduler wall clock
3. confluence carries latest contributing timestamp
4. precheck failure creates a skipped execution decision
5. missing orderbook context creates a skipped execution decision
6. no fill creates a skipped execution decision
7. executable EV failure creates a skipped execution decision
8. opened trades use executable entry price
9. one execution decision exists per `(signal_id, strategy_run_id)`
10. strategy-health endpoint still works unchanged

## Reviewer checklist

Before merge, verify:
- no paper trade opens without an execution decision
- `fired_at` no longer depends on scheduler wall clock when the candidate provides timing
- `signal.details.default_strategy` still works for strategy-health
- newly opened paper trades use executable entry price
- legacy rows with null new fields do not break queries
- SQLite tests still pass
