# Phase 7A Closeout

## What changed in Phase 7A

Phase 7A adds the transactional Polymarket OMS/EMS foundation without enabling unattended live trading.

Key changes:

- Added a new transactional layer that stays separate from the public raw truth boundary:
  - `polymarket_user_events_raw`
  - `live_orders`
  - `live_order_events`
  - `live_fills`
  - `capital_reservations`
  - `polymarket_live_state`
- Added a repo-local gateway wrapper in `backend/app/execution/polymarket_gateway.py` around the Polymarket CLOB client surface.
- Added `backend/app/execution/polymarket_order_manager.py` to turn Phase 6 chosen actions into auditable order intents with idempotent `client_order_id`s.
- Added `backend/app/execution/polymarket_user_stream.py` to capture authenticated user-channel frames server-side into `polymarket_user_events_raw`.
- Added `backend/app/execution/polymarket_live_reconciler.py` to normalize user events, repair state via REST lookup, and keep `live_orders` current.
- Added `backend/app/execution/polymarket_capital_reservation.py` for conservative reservation tracking and oversubscription blocking.
- Added explicit kill-switch and allowlist control through persisted live state plus config defaults.
- Added minimal operator APIs, health serialization, metrics, and a compact Health page Phase 7A section.

This phase still does not implement a live pilot, unattended strategy automation, maker-economics optimization, structural-edge logic, replay simulation, or portfolio optimization.

## Raw truth boundary preserved

`polymarket_market_events` remains the append-only truth boundary for public market data.

Phase 7A introduces a distinct append-only authenticated user-channel capture table, `polymarket_user_events_raw`, and does not mix it with the public market-data raw layer.

## Exact default safety posture

Default config remains conservative:

- `POLYMARKET_LIVE_TRADING_ENABLED=false`
- `POLYMARKET_LIVE_DRY_RUN=true`
- `POLYMARKET_LIVE_MANUAL_APPROVAL_REQUIRED=true`
- `POLYMARKET_USER_STREAM_ENABLED=false`
- `POLYMARKET_KILL_SWITCH_ENABLED=false`
- no credentials are required for dry-run intent creation
- no background live submission path is active unless operators explicitly enable live mode and provide credentials

Operationally, live submission only becomes possible when all of the following are true:

- live trading is explicitly enabled
- dry-run is explicitly disabled
- the kill switch is not enabled
- allowlists pass
- the order has valid reservations
- submit-time validations pass
- required credentials are present

## Order lifecycle semantics

`live_orders` is the current transactional truth for order state, while `live_order_events` is the append-only audit trail of transitions.

Implemented statuses:

- `intent_created`
- `approval_pending`
- `validation_failed`
- `submit_blocked`
- `submission_pending`
- `submitted`
- `live`
- `partially_filled`
- `matched`
- `mined`
- `confirmed`
- `cancel_pending`
- `canceled`
- `expired`
- `rejected`
- `failed`

Current semantics in Phase 7A:

- intent creation is idempotent by `client_order_id`
- manual approval updates the order record and appends an audit event
- dry-run submission appends a simulated submit event without touching the venue
- live submission records gateway acknowledgements and stores `venue_order_id` when present
- cancel is idempotent and first pass replace semantics are cancel-plus-new-intent
- every meaningful internal, gateway, websocket, or reconcile transition appends a `live_order_events` row

## Reconciliation rules

Trust order for lifecycle repair is:

1. append-only raw authenticated inbound events in `polymarket_user_events_raw`
2. direct gateway submit/cancel acknowledgements
3. REST repair (`fetch_order_status`, `fetch_user_trades`) for missed events or restart recovery

Practical rules implemented:

- raw user events are the authoritative inbound audit trail
- raw rows are never collapsed or mutated
- the reconciler replays unreconciled raw user events in order
- REST repair is used to patch current order state when websocket delivery is missed or the process restarts
- `live_orders` keeps the current fast-lookup status
- `live_order_events` records repair transitions as `rest_reconcile`
- `live_fills` is append-only and dedupes conservatively on stable trade identity when available, with fallback fingerprints when it is not

Represented repair outcomes include:

- partial fill
- matched
- mined
- confirmed
- canceled
- expired
- rejected
- failed

## Reservation rules implemented

Reservations are intentionally conservative and durable.

Implemented behavior:

- reservation rows are append-style updates in `capital_reservations`
- buy orders reserve quote notional (`buy_usdc`)
- sell orders reserve share inventory (`sell_shares`)
- intent creation attempts a reservation immediately
- submission promotes reservations from `pending` to `active`
- cancel, reject, fill, and reconcile paths release reservations through new rows instead of in-place mutation
- outstanding notional can be blocked with `POLYMARKET_MAX_OUTSTANDING_NOTIONAL_USD`
- the system prefers over-blocking to under-blocking

## Operator surface added

Added under `/api/v1/ingest/polymarket/live`:

- status
- recent live orders
- recent live order events
- recent fills
- current reservations
- manual approval
- manual rejection
- manual submit
- manual cancel
- kill-switch get/set
- allowlist get/set
- manual reconcile trigger
- user-stream status

Health additions:

- live enabled/disabled
- dry-run/manual approval mode
- gateway reachable/unreachable
- user-stream connected/disconnected
- kill switch on/off
- outstanding live orders
- outstanding reservations
- recent fills count
- last reconcile success

## What remains out of scope

Still intentionally not implemented in Phase 7A:

- unattended live pilot
- full OMS strategy automation
- maker-reward or maker-economics optimization
- structural-edge execution logic
- queue-position or replay-quality simulator
- inventory or portfolio optimizer
- broad admin/SaaS redesign

## What the next phase should start next

The next sequenced slice should build on the Phase 7A foundation without skipping safety:

- richer venue lifecycle handling around replace, retry, and repair semantics
- stronger operator workflows for approvals, cancels, and reconcile monitoring
- broader live-state observability and failure-handling hardening
- only after that, later phases can separately address:
  - maker economics
  - structural edge
  - inventory logic
  - replay/simulator quality
  - any explicitly approved live pilot work
