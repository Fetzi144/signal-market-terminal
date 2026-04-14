# Phase 8B Closeout

## Summary

Phase 8B turns the Phase 8A market-structure layer into a serialized, operator-safe paper-routing pipeline. The engine now acquires a shared lease before every structure cycle, persists conservative validation snapshots, can translate validated opportunities into deterministic paper plans, and records a full audit trail from opportunity to validation to plan to routed leg events without enabling live trading.

The operator surface is also broader and more explainable. Cross-venue manual links now carry governance metadata, health and metrics expose validation and paper-routing state, and the frontend includes a dedicated Structures workflow for reviewing opportunities, reason codes, link provenance, and plan approval/routing state.

## Files Added

- `backend/alembic/versions/032_add_market_structure_validation_and_paper_routing.py`
- `backend/app/ingestion/structure_phase8b.py`
- `backend/app/jobs/lease.py`
- `backend/tests/test_structure_phase8b_api.py`
- `frontend/src/pages/Structures.jsx`
- `frontend/src/pages/Structures.test.jsx`
- `docs/codex/phase-8b-closeout.md`
- `package.json`
- `scripts/scan_secrets.py`

## Files Changed

- `backend/app/api/health.py`
- `backend/app/api/polymarket_structure.py`
- `backend/app/config.py`
- `backend/app/ingestion/structure_engine.py`
- `backend/app/jobs/scheduler.py`
- `backend/app/metrics.py`
- `backend/app/models/__init__.py`
- `backend/app/models/market_structure.py`
- `backend/tests/conftest.py`
- `backend/tests/test_structure_engine.py`
- `frontend/src/App.jsx`
- `frontend/src/api.js`
- `frontend/src/pages/Health.jsx`
- `frontend/src/pages/Health.test.jsx`
- `README.md`
- `Makefile`
- `.github/workflows/ci.yml`
- `.gitignore`

## Migration Added

- `032_add_market_structure_validation_and_paper_routing`
  - extends `cross_venue_market_links` with review and governance metadata
  - adds `market_structure_validations`
  - adds `market_structure_paper_plans`
  - adds `market_structure_paper_orders`
  - adds `market_structure_paper_order_events`
  - adds query and active-plan indexes needed for Phase 8B controls

## Config Variables Added

- `POLYMARKET_STRUCTURE_RUN_LOCK_ENABLED`
- `POLYMARKET_STRUCTURE_RETENTION_DAYS`
- `POLYMARKET_STRUCTURE_VALIDATION_ENABLED`
- `POLYMARKET_STRUCTURE_PAPER_ROUTING_ENABLED`
- `POLYMARKET_STRUCTURE_PAPER_REQUIRE_MANUAL_APPROVAL`
- `POLYMARKET_STRUCTURE_MAX_NOTIONAL_PER_PLAN`
- `POLYMARKET_STRUCTURE_MIN_DEPTH_PER_LEG`
- `POLYMARKET_STRUCTURE_PLAN_MAX_AGE_SECONDS`
- `POLYMARKET_STRUCTURE_LINK_REVIEW_REQUIRED`

## Tests Added/Updated

- Added locking, validation, audit-trail, retention, and degraded-routing coverage to `backend/tests/test_structure_engine.py`
- Added Phase 8B API coverage for detail views, manual validation, paper plan controls, and cross-venue governance filters in `backend/tests/test_structure_phase8b_api.py`
- Extended `backend/tests/conftest.py` to enforce SQLite foreign keys for retention cascade testing
- Updated `frontend/src/pages/Health.test.jsx` for new Phase 8B summaries
- Added `frontend/src/pages/Structures.test.jsx` for the operator review and paper-plan workflow

## Validation Results

Completed validation for the touched Phase 8B surfaces:

- `npm.cmd run frontend:install` passed
- `npm.cmd run frontend:validate` passed
  - Vitest: `2 passed`
  - Vite production build: passed
- `npm.cmd run secrets:scan` passed
- `python -m pytest backend/tests/test_api.py backend/tests/test_structure_engine.py backend/tests/test_structure_phase8b_api.py -q` passed
  - Result: `36 passed`
- Fresh Postgres 16 Alembic verification passed with `python -m alembic upgrade head` against a throwaway container on `127.0.0.1:55432`

## Assumptions / Protocol Uncertainties

- Phase 8B continues to treat structure paper routing as a conservative proof layer. Package sizing stays anchored to the detected `package_size` rather than introducing new optimization logic.
- Unsupported generic cross-venue `buy_no` legs remain explicit block reasons instead of silent inference. Operators must provide explicit/manual mappings when a sibling `No` leg is required.
- Cross-venue review state is enforced conservatively through `approved`, `needs_review`, `expired`, and `disabled`, with expiry also derived from `expires_at` even if the stored review status has not yet been updated.
