# Polymarket Execution Roadmap

## Thesis

SMT should evolve from a snapshot-based monitoring and paper-trading system into an event-time Polymarket execution stack. The current repo still samples the market too coarsely for true execution-grade alpha:

- snapshots every 120 seconds
- random orderbook sampling
- midpoint-heavy EV and sizing logic
- execution realism applied after the trade decision

The roadmap below fixes those gaps in the right order.

## Core principles

1. Exchange time is the source of truth.
2. Executable EV matters more than midpoint EV.
3. Raw market data must be append-only and replayable.
4. Structural edge and execution edge should be first-class strategy families.
5. Inventory and portfolio risk should be graph-based, not keyword-based.

## Phase 0 — Truth boundary and timing normalization

Goal:
- preserve actual observation timing
- persist decision-time execution estimates
- make execution realism part of the trade gate

Deliverables:
- timing/source fields on `signals`
- `execution_decisions` table
- executable-price-based paper-trade gate
- backward compatibility with strategy-health

## Phase 1 — Public Polymarket market-data plane

Goal:
- ingest event-time market data instead of relying on polling

Deliverables:
- dedicated market stream worker
- raw append-only market event storage
- dynamic subscribe/unsubscribe for watched assets
- resync path from REST orderbook endpoints

## Phase 2 — Metadata sync and structural registry

Goal:
- store the parameters that make Polymarket execution different

Deliverables:
- market parameter history for tick size, min order size, fee-enabled flags, negative-risk flags
- event and market registry enriched from Gamma + stream lifecycle messages

## Phase 3 — Raw orderbook, trade tape, and OI storage

Goal:
- preserve enough state to rebuild books and label fills

Deliverables:
- full-book snapshots
- book deltas
- public trade tape
- open-interest history
- retention and archival policy

## Phase 4 — Deterministic book reconstruction and resync

Goal:
- maintain replay-quality in-memory books per watched asset

Deliverables:
- in-memory L2 reconstruction
- hash mismatch detection
- automatic resync and persistence of resync snapshots

## Phase 5 — Microstructure features and labels

Goal:
- create research-grade features for next-move and fill models

Deliverables:
- microprice
- queue imbalance
- add/cancel/trade flow rates
- short-horizon labels for move prediction and passive fill probability

## Phase 6 — Executable EV gate and action policy

Goal:
- stop approving trades that are not executable

Deliverables:
- action enumeration: cross, post, step-ahead, skip
- executable entry-price estimation
- fillable-size estimation
- net EV after fees/slippage
- policy choosing the best action, not just the best direction

## Phase 7 — OMS/EMS foundation

Goal:
- support live execution safely and auditably

Deliverables:
- Polymarket gateway via official SDK
- order manager
- user-stream consumer
- reconciler
- capital reservation tracking
- kill switch and allowlists

## Phase 8 — Structural edge engine

Goal:
- elevate negative-risk and basket-style edges into first-class strategy paths

Deliverables:
- event graph
- negative-risk conversion pricing
- complement and parity checks
- cross-venue hedgeable basis engine

## Phase 9 — Maker economics engine

Goal:
- exploit Polymarket-native fee, rebate, and liquidity reward mechanics

Deliverables:
- token fee-rate history
- market reward config history
- maker-vs-taker economics estimator
- quote optimizer

## Phase 10 — Risk graph and portfolio optimizer

Goal:
- replace keyword cluster heuristics with real structural exposure management

Deliverables:
- event/entity/conversion-group exposure buckets
- graph-based portfolio optimizer
- inventory-aware quoting controls

## Phase 11 — Replay-quality simulator and backtest expansion

Goal:
- compare midpoint logic, executable logic, maker policy, and structure strategy in one framework

Deliverables:
- orderbook replay
- fill replay
- action-policy replay
- structure replay

## Phase 12 — Live pilot and control plane

Goal:
- run a narrow, supervised live pilot only after the earlier layers are credible

Deliverables:
- execution console
- live orders page
- market tape page
- strategy-specific pilot controls

## Recommended execution order

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4
6. Phase 5
7. Phase 6
8. Phase 7
9. Phase 8
10. Phase 9
11. Phase 10
12. Phase 11
13. Phase 12

## Success condition for the roadmap

SMT should eventually be able to answer:

- what was the true observed market state?
- what action was chosen and why?
- what executable EV existed after costs?
- what fill was realistically available?
- did the edge survive actual execution?
- was the better opportunity directional, maker-based, or structural?
