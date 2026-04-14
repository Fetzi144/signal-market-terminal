# Phase 5 Closeout: Polymarket Microstructure Features and Labels

## What Changed

Phase 5 adds a derived research layer on top of the existing Polymarket truth stack:

- raw WS events remain the append-only truth boundary
- Phase 3 raw storage remains the persisted event tape
- Phase 4 reconstruction remains the mutable live-state operator layer
- Phase 5 now materializes reproducible research outputs:
  - `polymarket_book_state_topn`
  - `polymarket_microstructure_features_100ms`
  - `polymarket_microstructure_features_1s`
  - `polymarket_alpha_labels`
  - `polymarket_passive_fill_labels`
  - `polymarket_feature_runs`

The new service lives in `backend/app/ingestion/polymarket_microstructure.py` and is wired into:

- worker scheduling
- ingest/operator APIs
- health serialization
- Prometheus metrics
- frontend Health page

## Core Guarantees

- Phase 5 does not replace or mutate raw truth, raw storage, or Phase 4 recon state.
- Derived rows are produced from stored authoritative snapshots, deltas, BBO events, trades, parameter history, and recon incidents.
- Materialization is idempotent by natural keys on the derived tables.
- Conservative completeness flags are carried forward instead of inventing smooth or complete-looking rows.
- Passive-fill labels are observational only. They do not model queue position or actual fill probability.

## Exact Semantics

### Replay / Book State

- Historical replay uses authoritative snapshots from the raw storage layer as seed boundaries.
- Deltas are applied in deterministic order using exchange/local time plus `raw_event_id` and `delta_index`.
- A book-state snapshot row at `bucket_start_exchange = T` represents reconstructed state after all replayed events with event-time `<= T`.
- Top-of-book levels are stored compactly as top 5 only; this table is not a mutable full-book copy.

### Feature Buckets

- Feature rows keyed by `bucket_start_exchange = T` summarize flow over `(T, T + bucket_width]`.
- Implemented bucket widths are the first-pass roadmap defaults: `100ms` and `1s`.
- Implemented features include:
  - best bid / ask
  - spread
  - mid
  - microprice
  - top-1 / top-3 / top-5 depth
  - top-1 / top-3 / top-5 imbalance
  - bid/ask add volume
  - bid/ask remove volume
  - buy/sell trade volume and count
  - trade notional
  - last trade price and side
  - book update count
  - BBO update count

Microprice uses top-of-book depth weighting:

- `(best_ask * bid_size_top1 + best_bid * ask_size_top1) / (bid_size_top1 + ask_size_top1)`

Queue imbalance uses:

- `(bid_depth - ask_depth) / (bid_depth + ask_depth)`

Flow attribution is conservative:

- size increases become add volume
- size decreases become remove volume
- zero size removes the level
- no attempt is made to split removes into “cancel” vs “trade” when the source tape cannot prove it

### Completeness Flags

Feature and book-state rows explicitly mark:

- whether the bucket had a trustworthy snapshot seed
- whether the bucket crossed a snapshot / resync boundary
- whether the bucket was affected by reconstruction drift incidents
- whether there were deltas without seed
- whether trade-side coverage was partial
- whether the bucket should be treated as source-coverage partial

### Alpha Labels

- Labels are emitted for fully observed forward horizons only.
- First-pass horizons are `250ms`, `1000ms`, and `5000ms`.
- The anchor row is a feature row.
- `end_mid` is the latest replay marker at or before `anchor + horizon`.
- `mid_return_bps` is forward mid return relative to anchor mid.
- `mid_move_ticks` uses the anchor tick size.
- `up_move`, `down_move`, and `flat_move` are conservative boolean flags from anchor mid vs forward mid.

### Passive-Fill Labels

For an anchor feature row:

- `buy_post_best_bid` assumes a hypothetical passive buy posted at the current best bid
- `sell_post_best_ask` assumes a hypothetical passive sell posted at the current best ask

Labels record observable opportunity only:

- `touch_observed`
- `trade_through_observed`
- `best_price_improved_against_order`
- `adverse_move_after_touch_bps`

They do **not** estimate fill certainty and do **not** simulate queue position.

## Operator / API Surfaces

Phase 5 adds:

- feature pipeline status
- recent feature runs
- manual feature materialization trigger
- book-state lookup
- feature lookup
- alpha-label lookup
- passive-fill-label lookup
- health status summary and manual trigger in the frontend Health page

## What Remains For Later Phases

This phase intentionally does **not** include:

- replay-quality execution simulator
- queue-position or fill simulator
- executable action policy (`cross`, `post`, `step-ahead`, `skip`)
- OMS / EMS / live orders / user stream reconciliation
- maker economics / rebates / rewards logic
- structural-edge engines
- model training or serving

## Recommended Next Phase Start

The next phase should start from this derived layer and build the replay-quality simulator on top of:

- Phase 5 event-time book-state snapshots
- Phase 5 microstructure features
- Phase 5 short-horizon move labels
- conservative passive-fill opportunity labels

That next step should keep Phase 5 outputs as research inputs and should not back-edit the truth boundary or reconstruction checkpoints.
