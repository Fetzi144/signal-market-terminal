# Codex Start Here

This file reflects the repository as it exists now.

Signal Market Terminal is no longer "entering Phase 0". The repo already contains:

- a frozen default-strategy evidence loop for the "prove the edge" phase
- a Polymarket truth and execution stack built across the execution roadmap
- a narrow, supervised pilot control plane plus a Phase 12B stabilization and evidence pass

## Read in this order

1. `README.md`
2. `docs/default-strategy.md`
3. `docs/Current roadmaps/polymarket-execution-roadmap.md`
4. the relevant closeout docs in `docs/codex/` for the subsystem you are changing

Use `docs/codex/` as the shortest accurate progress snapshot:

- `phase-0-closeout.md` through `phase-6-closeout.md` for timing, raw truth, reconstruction, features, and execution policy
- `phase-7a-closeout.md`, `phase-8a-closeout.md`, and `phase-8b-closeout.md` for OMS foundation and structure paper routing
- `phase-9-closeout.md`, `phase-10-closeout.md`, and `phase-11-closeout.md` for maker economics, risk graph, and replay
- `phase-12-closeout.md` and `phase-12b-stabilization-closeout.md` for the live pilot control plane, guardrails, and evidence loop

Do not start from `docs/codex/phase-0-build-pack.md` unless the task explicitly reopens that historical slice.

## Current repo posture

- The canonical deployment target is the Hetzner host `smt-prod-1` at `/opt/signal-market-terminal`.
- Local Docker workflows are legacy and should not be the default path unless the task explicitly needs them.
- The default strategy is frozen and exists to measure edge honestly, not to widen scope.
- `strategy_run` bootstrap is explicit. Read-only strategy-health surfaces must not create a run.
- Exchange and event time are the source of truth when venue data provides them.
- Raw Polymarket market data, user events, and audit trails should stay append-only and replayable.
- Structure, maker, risk, replay, and pilot surfaces exist, but they remain conservative and operator-facing first.
- Live trading is disabled by default.
- Pilot mode is disabled by default.
- Manual approval is required by default.
- `exec_policy` is the only supported armable pilot family in the current slice.

## What already exists

- Benchmark evidence path: frozen confluence default strategy, execution decisions, strategy health, replay-aware evidence, and controlled relaunch tooling
- Truth stack: public stream ingest, metadata sync, raw storage, book reconstruction, trade backfill, open-interest polling, and derived microstructure features
- Execution stack: executable EV gate, OMS/EMS foundation, user stream, reconciler, control plane, and live-vs-shadow evaluation
- Research stack: structure engine, maker economics, risk graph, replay simulator, and operator health and console surfaces

## Change rules

- Preserve the default-strategy measurement contract unless the task explicitly changes that contract and updates docs and tests with it.
- Keep fail-closed behavior around live, pilot, approval, and guardrail paths.
- Prefer additive work that extends the existing truth boundary instead of bypassing it.
- If you touch a subsystem with a closeout doc, read that closeout first and keep the code aligned with it.
- Update docs when behavior changes, especially `README.md`, `docs/default-strategy.md`, and the relevant `docs/codex/phase-*-closeout.md`.

## Quick routing

- Default strategy, paper trading, strategy health:
  - `docs/default-strategy.md`
  - `backend/app/default_strategy.py`
  - `backend/app/paper_trading/`
  - `backend/app/strategy_runs/`
- Polymarket truth pipeline:
  - `backend/app/worker.py`
  - `backend/app/ingestion/`
  - `frontend/src/pages/Health.jsx`
- Live and pilot control plane:
  - `backend/app/execution/`
  - `backend/app/api/polymarket_live.py`
  - `docs/codex/phase-12-closeout.md`
  - `docs/codex/phase-12b-stabilization-closeout.md`
