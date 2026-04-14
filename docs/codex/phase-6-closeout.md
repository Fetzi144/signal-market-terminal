# Phase 6 Closeout

## What changed in Phase 6

Phase 6 adds a deterministic Polymarket execution gate and action-policy baseline on top of the existing paper-trading decision path.

Key changes:

- Added `polymarket_execution_action_candidates` as the append-only audit table for per-action evaluations.
- Extended `execution_decisions` with chosen-action, executable-EV, fee, slippage, and rationale fields.
- Added `backend/app/ingestion/polymarket_execution_policy.py` to:
  - resolve current Polymarket registry, parameter-history, reconstruction, and label context
  - enumerate executable actions
  - estimate executable price, size, fees, slippage, and conservative passive priors
  - choose the best valid action by net executable EV or fall back to `skip`
  - persist candidate rows plus the chosen decision linkage
- Integrated the policy into `backend/app/paper_trading/engine.py` before final paper-trade approval.
- Preserved the existing `shadow_execution` payload so current strategy-health and operator surfaces keep working.
- Added ingest/operator endpoints, health serialization, Prometheus metrics, and a compact Health page Phase 6 section.

This phase does not place live orders. It is still a decision-time execution gate and audit layer only.

## Action type semantics

`cross_now`

- Aggressive executable estimate against visible opposite-side reconstructed depth.
- Evaluated with FAK-like semantics for paper approval only.
- Uses a conservative visible-depth walk and never assumes hidden liquidity.

`post_best`

- Passive resting estimate at the current same-side best quote.
- Uses post-only semantics conceptually.
- Requires reliable reconstructed top-of-book plus sufficient passive-label coverage.

`step_ahead`

- Passive resting estimate one tick better than the current same-side best quote.
- Only valid when the one-tick improvement stays non-crossing and tick-aligned.
- Uses the same conservative passive-label framework as `post_best`; no queue-position simulator is claimed.

`skip`

- Always available as the policy fallback.
- Chosen when all executable actions are invalid, negative, or below the configured minimum net executable EV threshold.

## Executable price, size, and fee estimation

Aggressive `cross_now`

- Starts from the current reconstructed book from Phase 4.
- Walks visible opposite-side levels in price order.
- Computes:
  - fillable notional
  - fillable shares
  - average entry price
  - worst reached price
  - slippage cost versus touch-entry price
- Recomputes target size from Kelly at the walked entry estimate, then caps realized fillability by visible depth.
- Invalidates when:
  - the reconstructed book is not trustworthy
  - there is no visible executable depth
  - fillable shares are below min order size
  - fill probability is below the configured minimum threshold
  - slippage exceeds the configured cross-slippage cap

Passive `post_best` and `step_ahead`

- Start from current reconstructed top-of-book plus Phase 2 tick/min-size parameters.
- Determine a candidate passive price:
  - `post_best`: current same-side best
  - `step_ahead`: one tick better than current same-side best
- Invalidates when:
  - book state is not trustworthy
  - tick size is missing
  - top-of-book is invalid
  - the price is not tick-aligned
  - `step_ahead` would cross
  - target or expected fillable shares fall below min order size
  - passive label coverage is too sparse
  - passive fill probability is too low
- Uses Phase 5 derived data conservatively:
  - passive-fill labels estimate fill probability and adverse selection
  - alpha labels estimate directional opportunity cost while waiting
  - no queue-position certainty is assumed

Fees

- Reads `fees_enabled`, fee schedule, tick size, and min order size from Phase 2 param history.
- Aggressive actions estimate taker fees using the Polymarket-style formula already exposed in metadata:
  - `notional × feeRate × p × (1 - p)`
- Passive actions assume no taker fee and no maker rebate/reward yet.
- Maker rebates, liquidity rewards, and quote optimization remain out of scope.

## When the policy chooses `skip`

The policy chooses `skip` when any of the following is true:

- every non-skip action is invalid
- the best valid non-skip action has non-positive total executable EV
- the best valid non-skip action falls below `POLYMARKET_EXECUTION_POLICY_MIN_NET_EV_BPS`
- the trade cannot credibly satisfy live book, tick-size, min-order-size, or fillability requirements

`skip` decisions are persisted with rationale so operators can see why approval was denied.

## Operator and audit surface added

- `GET /api/v1/ingest/polymarket/execution-policy/status`
- `GET /api/v1/ingest/polymarket/execution-policy/action-candidates`
- `GET /api/v1/ingest/polymarket/execution-policy/decisions`
- `GET /api/v1/ingest/polymarket/execution-policy/invalidation-reasons`
- `GET /api/v1/ingest/polymarket/execution-policy/action-mix`
- `POST /api/v1/ingest/polymarket/execution-policy/dry-run`
- `/api/v1/health` now includes `polymarket_phase6`
- Health UI now shows Phase 6 enablement, recent decisions, action mix, invalid counts, skip counts, and average estimated net EV

## What remains out of scope

Still not implemented in Phase 6:

- live order placement
- OMS / EMS
- user-stream ingestion and reconciliation
- replay-quality simulator
- queue-position simulator
- maker rebate or liquidity-reward optimization
- structural-edge execution
- portfolio optimizer changes
- model training or online model serving

## What the next phase should start next

Per the short execution roadmap, the next sequenced slice should start **Phase 7 — OMS/EMS foundation**:

- explicit order-intent and order-state models
- order lifecycle audit trail
- safe paper/live control-plane seams
- no automatic live placement until reconciliation and higher-fidelity simulation work are ready

Supporting follow-on research still remains important after that:

- replay-quality simulator and queue-aware calibration
- maker-economics engine
- structural edge engine
- user-stream reconciliation and live execution control
