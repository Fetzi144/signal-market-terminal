# Codex Start Here

This repository is entering a staged upgrade from a snapshot-based research console into an event-time Polymarket execution stack.

## Do not skip ahead

Codex should start with:

1. `docs/codex/phase-0-build-pack.md`
2. `docs/roadmaps/polymarket-execution-roadmap.md`

## Phase 0 objective

Phase 0 fixes the truth boundary before any new alpha work:

- add explicit timing and source fields to `signals`
- create a first-class `execution_decisions` table
- move execution realism in front of paper-trade opening
- preserve current strategy-health compatibility

## Important constraints

- keep the current FastAPI/Postgres/worker architecture
- do not add live trading yet
- do not add WebSocket ingestion yet
- do not add new detectors yet
- do not break `/paper-trading` strategy-health surfaces

## Current weaknesses Phase 0 is meant to address

- `SignalCandidate` does not carry exchange-observation timing
- `persist_signals()` stamps `fired_at` from scheduler time
- paper trades are still approved before executable fill quality is known
- `shadow_execution` is still post-hoc analytics, not a pre-trade gate

Phase 0 is a backend-only correctness phase.
