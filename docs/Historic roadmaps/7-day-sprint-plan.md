# Signal Market Terminal — 7-Day Sprint Plan

*Based on assessment in `docs/assessment.md`. Target: v0.3.0.*
*Written for Claude Code agents to execute autonomously.*

---

## Sprint Goals

1. Fix all known bugs before adding new features
2. Close critical test gaps (currently ~40% of detector/API logic is untested)
3. Implement market resolution tracking — the analytical foundation everything else builds on
4. Ship one high-impact differentiating feature: **cross-platform arbitrage detector**

---

## Day 1 — Bug Fixes & Database Hardening

### Tasks

#### Fix: Scheduler signal broadcast bug (`app/jobs/scheduler.py:67`)
- Read `scheduler.py` and `app/ranking/scorer.py` to understand what `persist_signals()` returns
- `persist_signals()` returns an integer count of newly-created signals, not a list
- The broadcast currently does `all_candidates[:created]` which slices by count, not by "new vs deduped"
- Fix: `persist_signals()` should return the list of newly-created `Signal` ORM objects (or their IDs), not just a count
- Update `scorer.py` to return `(created_count, new_signals_list)`
- Update `scheduler.py` to broadcast only the `new_signals_list`
- Ensure SSE events include the full signal payload (rank_score, signal_type, market question, outcome)

#### Fix: Off-by-24x math in `kalshi.py:104` and `analytics.py:104`
- In both files, change `hours = max(hours, 1) * 24` to `hours = max(hours, 1)`
- Verify the variable is used as hours (not days) in the surrounding context after the fix

#### Fix: Kalshi cursor pagination in market discovery (`app/ingestion/markets.py`)
- Read `markets.py` and `kalshi.py` to understand the current flow
- Change the Kalshi branch in `discover_markets()` to call `fetch_markets_cursor()` instead of `fetch_markets()`
- Implement a cursor loop: fetch page, upsert, advance cursor, stop when cursor is None
- Keep the existing `market_pagination_cap` safety limit

#### Add: Database constraints (`app/models/`)
- Add FK constraint: `signal.outcome_id` → `outcome.id` with `ondelete="SET NULL"` (keeps signal, nulls the FK if outcome deleted)
- Add `alert_rank_threshold` bounds validator to `config.py`: must be between 0.0 and 1.0 inclusive

### Tests to Write
- `tests/test_scheduler_broadcast.py` — mock `persist_signals()`, assert SSE broadcaster receives correct signal objects, not sliced candidates
- Add to existing connector tests: assert Kalshi market discovery uses cursor pagination and stops at cap

### Deliverables
- `scheduler.py` broadcast bug fixed and tested
- Kalshi fetches all markets beyond offset cap
- Off-by-24x math corrected in 2 files
- FK constraint on `signal.outcome_id`
- `alert_rank_threshold` validated to [0, 1]

---

## Day 2 — Test Coverage: Detectors, Ranking, API

### Context
`test_api.py` is a ~1.5 KB smoke test. Volume Spike and Price Move detectors have zero tests. Ranking/deduplication logic is untested. This day closes those gaps.

### Tasks

#### Write: Price Move detector tests (`tests/test_price_move.py`)
Test cases to cover:
- Price increases above threshold → signal generated with correct score
- Price decreases above threshold → signal generated (direction tracked)
- Price change below threshold → no signal
- Low volume market → confidence penalty applied (confidence < 1.0)
- Sub-penny market (price < 0.01) → `0.01` floor behavior documented/tested
- Insufficient snapshot history → no signal
- Deduplication: same outcome, same 15-min bucket → second call produces no new signal

#### Write: Volume Spike detector tests (`tests/test_volume_spike.py`)
Test cases to cover:
- Volume > 3x baseline → signal generated
- Volume < 3x baseline → no signal
- Insufficient baseline snapshots (< 12) → no signal
- Low baseline volume → confidence penalty applied
- Very high spike multiplier → score capped at 1.0
- Log-scaling behavior: 10x spike scores higher than 3x spike

#### Write: Ranking and deduplication tests (`tests/test_ranking.py`)
Test cases to cover:
- `rank_score = signal_score × confidence × recency_weight` calculated correctly
- Recency weight = 1.0 for brand-new signal
- Recency weight decays toward 0.3 at 24h
- Duplicate signal in same 15-min bucket → returns existing signal, no insert
- Different outcome, same type, same bucket → both inserted
- Same outcome, different type, same bucket → both inserted

#### Write: API integration tests (`tests/test_api.py` — replace stub)
Test cases to cover:
- `GET /api/v1/signals` → 200, returns paginated list with correct schema
- `GET /api/v1/signals?signal_type=price_move` → filters correctly
- `GET /api/v1/signals/{id}` → 200 with evaluations; 404 on missing
- `GET /api/v1/markets` → 200, returns active markets
- `GET /api/v1/markets/{id}` → 200 with outcomes and latest prices
- `GET /api/v1/health` → 200 with expected fields
- `GET /api/v1/signals/export/csv` → 200, content-type text/csv
- `GET /api/v1/markets/export/csv` → 200, content-type text/csv
- Rate limiting: 61st request in a minute → 429

#### Write: SSE broadcaster tests (`tests/test_sse.py`)
Test cases to cover:
- Subscribe → receives events published after subscription
- Queue full (maxsize=100) → oldest events dropped, subscriber stays connected
- Unsubscribe → no longer receives events
- Multiple subscribers → all receive same event
- Keepalive: no events for 30s → comment line sent

### Deliverables
- `tests/test_price_move.py` — ~10 tests
- `tests/test_volume_spike.py` — ~8 tests
- `tests/test_ranking.py` — ~8 tests
- `tests/test_api.py` — replaced with ~12 integration tests
- `tests/test_sse.py` — ~5 tests
- Coverage gate should pass at ≥75% (up from 70%)

---

## Day 3 — Market Resolution Tracking (Part 1: Backend)

### Context
This is the most important analytical feature. When a binary market closes, the platform publishes the winning outcome (YES/NO). Backfilling signals with `resolved_correctly: bool` makes the accuracy metric meaningful. Everything — backtesting, strategy tuning, signal quality assessment — depends on this.

### Tasks

#### Schema: Add `resolved_correctly` to Signal model
- Add column: `resolved_correctly: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)`
- Default `NULL` (not yet resolvable — market still open)
- Write Alembic migration: `alembic revision --autogenerate -m "add_resolved_correctly_to_signal"`
- Review and clean up the autogenerated migration before committing

#### Polymarket: Fetch market resolution
- Read `app/connectors/polymarket.py` and Polymarket Gamma API docs pattern
- Gamma API endpoint for resolved markets: `GET /markets?closed=true&resolved=true`
- Parse `winner` field on each market: maps to outcome `token_id` or `outcome` string
- Add method `fetch_resolved_markets(since_hours: int = 24) -> list[dict]` to `PolymarketConnector`
- Return list of `{platform_id, winning_outcome_id}` dicts

#### Kalshi: Fetch market resolution
- Read `app/connectors/kalshi.py`
- Kalshi API: `GET /markets/{ticker}` returns `result` field when settled (`"yes"` or `"no"`)
- Add method `fetch_resolved_markets(since_hours: int = 24) -> list[dict]` to `KalshiConnector`
- Query recently-closed markets and check `result` field
- Return list of `{platform_id, winning_outcome}` dicts

#### Resolution service (`app/ingestion/resolution.py` — new file)
- `async def resolve_signals(session, platform: str, resolved_markets: list[dict]) -> int:`
  - For each resolved market, look up the `Market` row by `(platform, platform_id)`
  - For each `Signal` linked to an outcome of that market where `resolved_correctly IS NULL`:
    - Determine if the signal predicted movement toward the winning outcome
    - A signal is `resolved_correctly = True` if:
      - `direction == "up"` and the winning outcome's final price > signal's `outcome_price` (signal called the winner moving up, and it won)
      - `direction == "down"` and the winning outcome lost (price collapsed to near 0)
    - Set `signal.resolved_correctly` accordingly
  - Return count of signals resolved
- Handle cases where signal has no direction (spread/liquidity signals): set `resolved_correctly = NULL` (these signal types are not directional)

#### Add resolution job to scheduler
- Add `resolve_markets_job()` to `app/jobs/scheduler.py`
- Schedule: every 15 minutes
- Calls both connectors' `fetch_resolved_markets()`, passes results to `resolve_signals()`
- Logs count of newly resolved signals

### Tests to Write
- `tests/test_resolution.py`:
  - Signal with correct direction on winning outcome → `resolved_correctly = True`
  - Signal with wrong direction → `resolved_correctly = False`
  - Non-directional signal (spread change) → `resolved_correctly` stays NULL
  - Market not yet resolved → signal unchanged
  - Resolution runs twice → idempotent (no double-updates)

### Deliverables
- New `resolved_correctly` column on Signal with migration
- `fetch_resolved_markets()` on both connectors
- `app/ingestion/resolution.py` service
- Resolution job running every 15 minutes in scheduler
- 5+ tests in `test_resolution.py`

---

## Day 4 — Market Resolution Tracking (Part 2: API + Frontend)

### Tasks

#### API: Expose resolution data on signals
- Update `GET /api/v1/signals` response schema to include `resolved_correctly: bool | null`
- Update `GET /api/v1/signals/{id}` to include `resolved_correctly`
- Add filter: `GET /api/v1/signals?resolved_correctly=true` to fetch only correctly-called signals
- Update CSV export to include `resolved_correctly` column

#### API: Update analytics accuracy endpoint
- Read `app/api/analytics.py`
- Replace the current "directional price move" accuracy calculation with ground-truth accuracy:
  - `accuracy = count(resolved_correctly=True) / count(resolved_correctly IS NOT NULL)`
  - Group by `signal_type` and `horizon` (use evaluation data joined with resolution data)
- Add `resolution_rate` field: what % of signals on that type have been resolved so far
- Keep the old price-direction accuracy as `price_direction_accuracy` for comparison
- Add time filter: `?days=30` to limit accuracy window (add index on `signal.created_at` if not present)

#### Frontend: Signal Feed — show resolution badge
- In `SignalFeed.jsx` and `SignalDetail.jsx`, add a resolution badge next to each resolved signal:
  - Green checkmark + "Called it" for `resolved_correctly = true`
  - Red X + "Wrong call" for `resolved_correctly = false`
  - Grey dot + "Pending" for `null`
- Add filter option to Signal Feed: "Show only correct calls"

#### Frontend: Analytics — update accuracy table
- Update the accuracy table in `Analytics.jsx` to use ground-truth accuracy (new API field)
- Add a `Resolution Rate` column showing what % of signals have been resolved
- Add a note explaining the difference between price-direction accuracy and resolution accuracy
- Color-code: green ≥60%, yellow 40–60%, red <40%

#### Frontend: Fix hardcoded filter options
- In `SignalFeed.jsx` and `Alerts.jsx`, replace hardcoded signal type arrays with a `GET /api/v1/signals/types` call
- Add `GET /api/v1/signals/types` endpoint to backend that returns distinct signal_type values from DB
- Same for platform options: fetch from `GET /api/v1/markets/platforms`
- Add `GET /api/v1/markets/platforms` endpoint returning distinct platform values

### Tests to Write
- API tests for `resolved_correctly` filter
- API tests for updated analytics accuracy response schema
- API tests for new `/signals/types` and `/markets/platforms` endpoints

### Deliverables
- `resolved_correctly` surfaced in all signal API responses and CSV export
- Analytics accuracy now uses ground truth, not just price direction
- Signal Feed and Detail show resolution badges
- Filter options fetched from API (not hardcoded)
- `/signals/types` and `/markets/platforms` endpoints

---

## Day 5 — Cross-Platform Arbitrage Detector

### Context
The same real-world question trades on both Polymarket and Kalshi at different prices. Detecting real-time divergences above a configurable threshold is immediately actionable, unique in the market, and builds directly on the two-connector architecture already in place.

### Tasks

#### Schema: Market question fingerprint for cross-platform matching
- Add column `question_slug: str` to `Market` model — a normalized lowercase string with punctuation stripped, used for fuzzy matching across platforms
- Alembic migration: `add_question_slug_to_market`
- Populate in `discover_markets()`: normalize `market.question` → lowercase, strip punctuation, collapse whitespace
- Index on `question_slug` for fast lookup

#### New detector: `app/signals/arbitrage.py`
- Class `ArbitrageDetector(BaseDetector)`
- `detect(session) -> list[SignalCandidate]:`
  - Query all active markets grouped by `question_slug` where `count(distinct platform) > 1`
  - For each group, get the latest midpoint price for each platform's YES outcome
  - Calculate `spread = abs(price_polymarket - price_kalshi)`
  - If `spread >= settings.arb_spread_threshold` (default: 0.04, i.e. 4 percentage points):
    - Create `SignalCandidate` with:
      - `signal_type = "arbitrage"`
      - `signal_score = min(spread / 0.15, 1.0)` (15-point spread = max score)
      - `confidence = 1.0` (price data is objective, no baseline uncertainty)
      - `direction = "up"` on the cheaper platform's outcome (the one to buy)
      - `metadata = {polymarket_price, kalshi_price, spread, question_slug}`
- Register in `app/signals/__init__.py`

#### Config: Add arbitrage settings to `app/config.py`
- `arb_spread_threshold: float = 0.04` — minimum spread to fire a signal (validator: > 0, < 1)
- `arb_enabled: bool = True` — kill switch

#### Scheduler: Add arbitrage detection
- In `scheduler.py`, add `ArbitrageDetector` to the detectors list
- Arbitrage runs on the same 2-minute signal detection cycle
- No separate scheduling needed

#### API + Frontend: Arbitrage signal display
- Arbitrage signals surface automatically in Signal Feed (no code change needed — already filters by signal_type)
- In `SignalDetail.jsx`, add a special "Arbitrage" section when `signal_type == "arbitrage"`:
  - Show Polymarket price vs Kalshi price side-by-side
  - Show spread in percentage points
  - Links to both markets (Polymarket URL + Kalshi URL from market metadata)
- Add "arbitrage" to the signal type filter options (will be automatic once Day 4 fix ships)

### Tests to Write
- `tests/test_arbitrage.py`:
  - Two platforms, same question_slug, spread ≥ threshold → signal generated
  - Two platforms, spread < threshold → no signal
  - Only one platform has the market → no signal
  - Spread exactly at threshold → signal generated (boundary condition)
  - Spread changes next cycle, new dedupe bucket → new signal
  - Score scaling: 4-point spread scores less than 10-point spread

### Deliverables
- `question_slug` column + migration + population in market discovery
- `app/signals/arbitrage.py` — new 6th signal type
- `arb_spread_threshold` + `arb_enabled` config settings
- Arbitrage signals appearing in Signal Feed
- SignalDetail shows both-platform price comparison with links
- 6+ tests in `test_arbitrage.py`

---

## Day 6 — SSE Hardening, N+1 Fix, Security

### Tasks

#### Fix: SSE queue overflow handling
- Read `app/api/sse.py` and `app/api/signals.py` (SSE broadcaster)
- Change queue behavior: when queue is full, log a warning and close the subscriber (force reconnect) rather than silently dropping
- On the frontend `useSSE.js`, the existing auto-reconnect logic will handle the reconnect
- Add `sse_queue_drops_total` Prometheus counter metric incremented on each drop/close
- Add connection limit: if `sse_connections >= settings.sse_max_connections` (default: 50), return 503

#### Fix: N+1 queries in MarketDetail
- Read `app/api/markets.py`, find the `get_market` endpoint
- Replace per-outcome price snapshot queries with a single query:
  - Subquery or window function: `SELECT DISTINCT ON (outcome_id) * FROM price_snapshots WHERE outcome_id = ANY(:ids) ORDER BY outcome_id, captured_at DESC`
  - Use SQLAlchemy's `select().where(PriceSnapshot.outcome_id.in_(outcome_ids))` with a subquery for latest-per-outcome
- Verify the fix with an `EXPLAIN ANALYZE` log statement in development

#### Security: Add HMAC signing to webhook alerts
- Read `app/alerts/webhook_alert.py`
- Add config setting: `alert_webhook_secret: str = ""` — if set, sign the payload
- On each webhook POST, if secret is set:
  - Compute `HMAC-SHA256(secret, json_body)`
  - Add header `X-SMT-Signature: sha256=<hex_digest>`
- Document the signature verification pattern in a comment

#### Security: Add per-IP rate limiting
- Read `app/main.py` and current SlowAPI configuration
- Add a second rate limiter for the signals endpoint: `10/second` per IP in addition to global limit
- Add `X-RateLimit-Remaining` response header for client awareness

#### Security: Bounds-check `alert_rank_threshold`
- In `app/config.py`, add `@field_validator('alert_rank_threshold')` that raises if value not in [0.0, 1.0]
- This was listed as missing in the assessment

#### Add `sse_max_connections` to config
- `sse_max_connections: int = 50` with validator: ≥ 1

### Tests to Write
- SSE: assert that when queue is full, subscriber receives a close event (not silent drop)
- SSE: assert 503 returned when connection limit reached
- Webhook: assert `X-SMT-Signature` header present and correct when secret configured
- Config: assert `alert_rank_threshold = 1.1` raises `ValidationError`
- Config: assert `alert_rank_threshold = -0.1` raises `ValidationError`

### Deliverables
- SSE drops are logged, metered, and force a client reconnect
- SSE connection cap enforced with 503
- MarketDetail loads with a single snapshot query (no N+1)
- Webhook alerts optionally HMAC-signed
- Per-IP rate limiting on signal endpoints
- `alert_rank_threshold` validated to [0, 1]

---

## Day 7 — Polish, Integration Testing, Version Bump to v0.3.0

### Tasks

#### End-to-end integration test (`tests/test_integration.py`)
Write a full-stack integration test that:
1. Creates a market + outcomes in the test DB
2. Inserts price snapshots that would trigger a Price Move signal
3. Runs `detect_and_persist_signals()` directly
4. Asserts the signal was created with correct `rank_score`
5. Calls `GET /api/v1/signals` and asserts the signal appears in the response
6. Calls `GET /api/v1/signals/{id}` and asserts evaluations are present after running evaluator
7. Inserts a resolved market outcome and runs the resolution service
8. Asserts `resolved_correctly` is set on the signal
9. Calls `GET /api/v1/health` and asserts all ingestion fields present

Write a second integration test for the arbitrage detector:
1. Create the same question on two platforms with different prices (spread ≥ threshold)
2. Run `ArbitrageDetector.detect()`
3. Assert arbitrage signal created with correct score, metadata, and both platform prices

#### Frontend: Fix timer cleanup in SignalFeed
- Read `frontend/src/pages/SignalFeed.jsx`
- Ensure `clearInterval` is called in the `useEffect` cleanup function for the auto-refresh timer
- Ensure SSE `useSSE` hook is properly cleaned up on unmount
- Test by toggling routes rapidly in browser and checking for console errors

#### Update CHANGELOG.md
Add v0.3.0 entry with all changes from this sprint:
- Bug fixes: scheduler broadcast, Kalshi pagination, off-by-24x math
- New: market resolution tracking, `resolved_correctly` on signals
- New: cross-platform arbitrage detector (6th signal type)
- New: ground-truth accuracy in analytics
- Improvements: SSE hardening, N+1 fix, HMAC webhook signing, per-IP rate limiting
- Tests: Volume Spike, Price Move, ranking, API integration, SSE, resolution, arbitrage

#### Bump version to 0.3.0
- `frontend/package.json`: `"version": "0.3.0"`
- `backend/pyproject.toml`: `version = "0.3.0"`
- `CHANGELOG.md`: add v0.3.0 section

#### Run full test suite
```bash
cd backend
python -m pytest tests/ -v --tb=short
```
- All tests must pass
- Coverage must be ≥ 75%
- No ruff lint errors: `ruff check app/ tests/`

#### Git tag v0.3.0
```bash
git add -A
git commit -m "release: v0.3.0 — resolution tracking, arb detector, bug fixes, test coverage"
git tag -a v0.3.0 -m "Signal Market Terminal v0.3.0"
```

### Deliverables
- Full end-to-end integration tests passing
- SignalFeed timer cleanup fixed
- CHANGELOG.md updated with v0.3.0 entry
- All version files set to 0.3.0
- Full test suite passing at ≥75% coverage
- v0.3.0 git tag created

---

## Sprint Summary

| Day | Focus | Key Deliverables |
|-----|-------|-----------------|
| 1 | Bug fixes + DB hardening | Scheduler broadcast fixed, Kalshi pagination fixed, FK constraint added |
| 2 | Test coverage | Price Move, Volume Spike, ranking, API, SSE tests — ~45 new tests |
| 3 | Resolution tracking (backend) | Schema migration, connector resolution fetch, resolution service + scheduler job |
| 4 | Resolution tracking (frontend) | Resolution badges, ground-truth accuracy, dynamic filter options |
| 5 | Arbitrage detector | question_slug matching, ArbitrageDetector, config, SignalDetail UI |
| 6 | SSE hardening + security | SSE connection cap, N+1 fix, HMAC webhooks, per-IP rate limit |
| 7 | Polish + release | E2E integration tests, version bump, CHANGELOG, v0.3.0 tag |

## Files That Will Be Created (New)
- `app/signals/arbitrage.py`
- `app/ingestion/resolution.py`
- `tests/test_price_move.py`
- `tests/test_volume_spike.py`
- `tests/test_ranking.py`
- `tests/test_sse.py`
- `tests/test_resolution.py`
- `tests/test_arbitrage.py`
- `tests/test_scheduler_broadcast.py`
- Alembic migration: `add_resolved_correctly_to_signal`
- Alembic migration: `add_question_slug_to_market`

## Files That Will Be Modified (Existing)
- `app/jobs/scheduler.py` — broadcast bug fix, resolution job
- `app/ranking/scorer.py` — return new signals list alongside count
- `app/connectors/kalshi.py` — fix `* 24` math, add `fetch_resolved_markets()`
- `app/connectors/polymarket.py` — add `fetch_resolved_markets()`
- `app/api/analytics.py` — fix `* 24` math, ground-truth accuracy
- `app/api/markets.py` — N+1 fix, add `/platforms` endpoint
- `app/api/signals.py` — `resolved_correctly` filter, add `/types` endpoint
- `app/api/sse.py` — connection cap, forced reconnect on queue full
- `app/alerts/webhook_alert.py` — HMAC signing
- `app/config.py` — arb settings, SSE max connections, threshold validators
- `app/models/` — `resolved_correctly` column, `question_slug` column
- `app/signals/__init__.py` — register ArbitrageDetector
- `app/ingestion/markets.py` — populate `question_slug`, use Kalshi cursor
- `frontend/src/pages/SignalFeed.jsx` — timer cleanup, dynamic filter options
- `frontend/src/pages/SignalDetail.jsx` — resolution badge, arbitrage UI
- `frontend/src/pages/Analytics.jsx` — ground-truth accuracy table
- `frontend/src/pages/Alerts.jsx` — dynamic filter options
- `tests/test_api.py` — replace stub with full integration tests
- `CHANGELOG.md` — v0.3.0 entry
- `frontend/package.json` — version 0.3.0
- `backend/pyproject.toml` — version 0.3.0
