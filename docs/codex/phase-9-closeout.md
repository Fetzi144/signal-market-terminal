# Phase 9 Closeout

## Summary

Phase 9 adds a conservative Polymarket maker-economics layer on top of the existing structure scanner and paper-routing stack. The backend now stores time-aware fee and reward histories, exposes current and historical operator views for those inputs, persists explainable maker-vs-taker economics snapshots, and records advisory quote recommendations without enabling live order placement.

The implementation stays additive and paper-first. New APIs, health summaries, metrics, and frontend panels let operators inspect the fee/reward state that drove each advisory result, while the quote optimizer remains deterministic, audit-friendly, and gated by the existing safe defaults.

## Phase 8 Deliverable Status

- `event graph`: complete
  - Implemented by the structure group/member model and build pipeline in `backend/app/ingestion/structure_engine.py` and `backend/app/models/market_structure.py`.
- `negative-risk conversion pricing`: complete
  - Implemented in Phase 8 structure scanning for `neg_risk_direct_vs_basket` opportunities, including per-leg executable estimates and basket-vs-anchor pricing.
- `complement and parity checks`: complete
  - Implemented through `binary_complement` and `event_sum_parity` group and opportunity handling in the structure engine, with coverage in `backend/tests/test_structure_engine.py`.
- `cross-venue hedgeable basis engine`: complete
  - Implemented through explicit/manual cross-venue link governance plus `cross_venue_basis` detection and validation, with API coverage in `backend/tests/test_structure_phase8b_api.py`.

No blocking Phase 8 gap required a structural rewrite before Phase 9. The Phase 9 work reused the existing structure opportunity, validation, and paper-plan surfaces additively.

## Files Added

- `backend/alembic/versions/033_add_polymarket_maker_economics.py`
- `backend/app/ingestion/polymarket_maker_economics.py`
- `backend/app/models/polymarket_maker.py`
- `backend/tests/test_polymarket_maker_economics.py`
- `docs/codex/phase-9-closeout.md`

## Files Changed

- `backend/app/api/health.py`
- `backend/app/api/ingest.py`
- `backend/app/api/polymarket_structure.py`
- `backend/app/config.py`
- `backend/app/ingestion/polymarket_metadata.py`
- `backend/app/metrics.py`
- `backend/app/models/__init__.py`
- `backend/tests/test_polymarket_metadata_sync.py`
- `frontend/src/api.js`
- `frontend/src/pages/Health.jsx`
- `frontend/src/pages/Health.test.jsx`
- `frontend/src/pages/Structures.jsx`
- `frontend/src/pages/Structures.test.jsx`

## Migration Added

- `033_add_polymarket_maker_economics`
  - adds `polymarket_token_fee_rate_history`
  - adds `polymarket_market_reward_config_history`
  - adds `polymarket_maker_economics_snapshots`
  - adds `polymarket_quote_recommendations`
  - adds effective-time and status indexes for current-state lookup, historical lookup, and advisory artifact inspection

## Config Variables Added

- `POLYMARKET_FEE_HISTORY_ENABLED`
- `POLYMARKET_REWARD_HISTORY_ENABLED`
- `POLYMARKET_MAKER_ECONOMICS_ENABLED`
- `POLYMARKET_QUOTE_OPTIMIZER_ENABLED`
- `POLYMARKET_QUOTE_OPTIMIZER_MAX_NOTIONAL`
- `POLYMARKET_QUOTE_OPTIMIZER_MAX_AGE_SECONDS`
- `POLYMARKET_QUOTE_OPTIMIZER_REQUIRE_REWARDS_DATA`
- `POLYMARKET_QUOTE_OPTIMIZER_REQUIRE_FEE_DATA`

## Tests Added/Updated

- Added historical fee/reward persistence and effective-time lookup coverage in `backend/tests/test_polymarket_metadata_sync.py`
- Added Phase 9 structure economics and quote-optimizer coverage in `backend/tests/test_polymarket_maker_economics.py`
- Kept Phase 8B structure API coverage green in `backend/tests/test_structure_phase8b_api.py`
- Updated `frontend/src/pages/Health.test.jsx` for Phase 9 health/operator summaries
- Updated `frontend/src/pages/Structures.test.jsx` for maker economics, quote recommendations, and fee/reward history inspection

## Validation Results

- `python -m pytest backend/tests/test_polymarket_metadata_sync.py backend/tests/test_polymarket_maker_economics.py backend/tests/test_structure_phase8b_api.py -q` passed
  - Result: `9 passed`
- `cmd /c npm test -- src/pages/Health.test.jsx src/pages/Structures.test.jsx` passed
  - Result: `2 passed`
- Fresh Postgres Alembic verification passed with `python -m alembic -c alembic.ini upgrade head`
  - Executed against a scratch database on the running Docker Postgres instance at `localhost:5433`

## Assumptions / Protocol Uncertainties

- Reward normalization is intentionally conservative. The code accepts multiple reward payload field variants and records missing or unknown reward state explicitly instead of inferring unsupported structures.
- Historical maker-economics reconstruction is scoped to persisted structure opportunities plus stored fee/reward history. It is not a replay-quality simulator and does not attempt Phase 11-style historical book replay.
- Quote recommendations remain advisory-only. Even when maker economics beat taker economics, the output is a stored recommendation artifact, not an order-placement workflow.
- The optimizer treats missing fee history more strictly than missing reward history when `POLYMARKET_QUOTE_OPTIMIZER_REQUIRE_FEE_DATA` is enabled, because fee state is required for conservative net-economics estimates.
