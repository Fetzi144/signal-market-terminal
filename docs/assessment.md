# Signal Market Terminal — Full Project Assessment

*Generated: 2026-04-07. Based on full codebase audit of v0.2.0.*

---

## 1. Production-Ready Features

### Backend Core

- **5 signal detectors** — Price Move (>5% in 30min), Volume Spike (>3x 24h baseline), Spread Change (>2x avg bid-ask), Liquidity Vacuum (<30% orderbook depth), Deadline Near (<48h to close with >3% move). All configurable via environment variables.
- **Signal ranking** — `rank_score = signal_score × confidence × recency_weight`. Recency decays linearly from 1.0 at 0h to 0.3 at 24h. Deduplication enforced per (type, outcome, 15-minute bucket).
- **4-horizon evaluation** — Signals evaluated at 15m, 1h, 4h, 24h post-detection. Uses closest price snapshot within 5-minute tolerance. Timezone-safe via `_ensure_utc()`. Marks signal `resolved` when all horizons complete.
- **Two market connectors** — Polymarket (Gamma API for discovery + CLOB API for prices/orderbooks) and Kalshi (REST with cursor pagination). Both have exponential backoff retry ([1, 2, 4]s), circuit breakers (closed/open/half-open, threshold=5, reset=300s), and configurable batch sizes.
- **Multi-channel alerting** — Logger (always on), webhook (POST JSON), Telegram Bot API. Threshold-gated via `ALERT_RANK_THRESHOLD` (default 0.7). Each signal fires exactly once (no re-fires on subsequent detection cycles).
- **Data retention cleanup** — Scheduled every 6 hours. Snapshots deleted after 30 days, orderbooks after 14 days, resolved signals after 90 days. All retention periods configurable.
- **REST API** — Full suite: signals, markets, alerts, health, analytics, SSE events. All list endpoints paginated (max 200/page), filterable by platform/type/market. CSV export on signals and markets.
- **SSE real-time streaming** — `GET /api/v1/events/signals` with `SignalBroadcaster` pub/sub. Scheduler publishes `new_signal` and `new_alert` events. Keepalive comment every 30s.
- **Config system** — 50+ Pydantic-validated settings. Field validators enforce intervals ≥30s, retention ≥1 day, thresholds >0, limits ≥1. Full env-var + `.env` file override.
- **Observability** — Prometheus auto-instrumentation at `/metrics`. Custom metrics: `signals_fired`, `alerts_sent`, `ingestion_duration`, `active_markets`, `sse_connections`. Structured JSON logging via `LOG_FORMAT=json`.
- **Scheduled jobs** — Market discovery (5 min), snapshot capture (2 min), signal detection (2 min + 10s offset), evaluation (5 min), cleanup (6 hours). All APScheduler-managed with exception logging.
- **Infrastructure** — Docker Compose for dev and prod. Production config adds resource limits (backend 1G/2CPU, db 512M/1CPU, frontend 128M/0.5CPU), named network, Nginx reverse proxy with SSE proxy support and static asset caching, GitHub Actions CI with ruff lint + pytest + 70% coverage gate + Docker build verification.
- **Database** — PostgreSQL 16. SQLAlchemy 2.x async ORM. Proper indexes on `(active, platform)`, `end_date`, `(outcome_id, captured_at)`, `(signal_type, outcome_id, dedupe_bucket)`. Alembic migrations (2 so far).
- **90 tests** — Connectors (Polymarket parse/retry/orderbook, Kalshi parse/cursor/orderbook), ingestion (upsert, outcome update, snapshots), detectors (deadline, liquidity, spread), alerts (webhook, telegram, logger), cleanup, config validation, circuit breaker.

### Frontend

- **Signal Feed** — SSE-powered live updates with green "Live" indicator. Auto-refresh every 120s. Filters by signal type and platform. Paginated. Rank score and confidence displayed per signal.
- **Markets browser** — Search (300ms debounce), platform filter, sort by updated/volume/end_date/alphabetical. Paginated.
- **Market detail** — Interactive Recharts price chart with 1h/6h/24h/7d ranges, multi-outcome lines, volume overlay. Snapshot history table. Related signals list.
- **Signal detail** — Labeled key-value grid (not raw JSON). Evaluation horizon table showing price change % at each horizon. Link back to market.
- **Alerts history** — Paginated table. Filters by signal type and platform.
- **Analytics dashboard** — Platform comparison cards (market count, signal count, avg rank score), signal accuracy table per type per horizon (color-coded), correlated signals list (cross-platform, same category).
- **Dark/light theme toggle** — CSS variable-based, persisted to `localStorage`.
- **Navigation** — Feed | Markets | Analytics | Alerts | Health.

---

## 2. Partially Built or Has Known Bugs

### Bugs

**`scheduler.py:67` — Incorrect signal broadcast (medium-high)**
After signal detection, the code broadcasts `all_candidates[:created]` to SSE subscribers. `created` is an integer count returned by `persist_signals()`, not an index into the deduped list. Newly-created signals may fail to broadcast or wrong signals may be sent to live subscribers.

**`kalshi.py:104` and `analytics.py:104` — Off-by-24x math**
`hours = max(hours, 1) * 24` multiplies hours by 24 erroneously in both files. Low impact on current behavior (values are still positive) but semantically incorrect and will produce wrong results if the variable is used as hours.

### Incomplete or Shallow

**Signal accuracy metric is not ground-truth**
The analytics accuracy calculation measures "did price move in the predicted direction after signal?" — not "was the market signal correct against the actual resolved outcome?" Without market resolution data (whether the event actually happened), the accuracy metric doesn't tell you if the signals are genuinely predictive. This is the most important analytical gap in the system.

**Kalshi cursor pagination unused in market discovery**
`fetch_markets_cursor()` exists and works but `fetch_markets()` falls back to offset-based pagination. Markets beyond the offset cap (5,000) can be silently missed on large datasets.

**SSE queue silently drops events**
Each subscriber gets an `asyncio.Queue(maxsize=100)`. When a slow or lagging client fills the queue, events are dropped with no log entry, no reconnect trigger, and no indication to the client.

**N+1 queries in MarketDetail**
`GET /api/markets/{id}` fetches the latest price snapshot for each outcome in a separate query. Acceptable at current market depth but will degrade noticeably with hundreds of outcomes per market.

**Analytics queries are unbounded**
The accuracy and correlated-signals queries scan all signals with no enforced time-bound index. Will degrade past ~100k signal rows.

### Frontend Gaps

- Signal type and platform filter options are hardcoded strings in JSX rather than fetched from the API — adding a new detector or platform requires a frontend code change.
- No debounce on all filter inputs (some can spam the API on keystroke).
- Timer cleanup not fully guarded on all unmount paths in SignalFeed.
- Zero frontend test coverage (no Jest or Vitest configured).

### Security Gaps

- Nginx config is HTTP-only — no TLS termination configured.
- No HMAC signing on outbound webhook alerts — receivers cannot verify the payload origin.
- No per-IP rate limiting (only a global 60/minute limit).
- `alert_rank_threshold` has no 0–1 bounds validation in `config.py`.
- Telegram bot token not scrubbed from container environment in logs.

### Missing Database Constraints

- `signal.outcome_id` has no foreign key constraint — orphan signals (pointing to deleted outcomes) are possible.
- No CHECK constraints enforcing `signal_score`, `confidence`, `rank_score` to [0, 1] range.
- No soft-delete on markets — deactivated markets remain in the table with `active=false` indefinitely with no archival path.

---

## 3. Roadmap Items Not Started

### On the Official README Roadmap

| Item | Status |
|------|--------|
| WebSocket real-time signal push | Not started. SSE is done; WS would be a protocol upgrade for richer client control. |
| Signal backtesting framework | Not started. No historical replay runner, no parameter sweep, no backtest result storage. |
| Performance dashboards / signal accuracy tracking | Partially started (analytics page exists) but accuracy is shallow without market resolution ground truth. |
| Prometheus metrics endpoint | Endpoint exists via auto-instrumentation. No Grafana dashboards, no alerting rules, no runbook. |

### Missing but Not Documented

- **Market resolution tracking** — Fetching actual YES/NO outcomes when markets close and backfilling `resolved_correctly` on all signals for that market. Foundation for real accuracy measurement.
- **User accounts / saved filters / watchlists** — No authentication, no user model, no persistent preferences.
- **API documentation surfaced to users** — FastAPI's `/docs` and `/redoc` endpoints work but are not linked from the frontend or README.
- **Database backup strategy** — No volume snapshots, no pg_dump automation, no restore runbook.
- **Operations runbook** — No documented procedures for restarting services, recovering from circuit-breaker open state, handling stale ingestion, or rolling back a bad migration.

---

## 4. Technical Debt & Architectural Concerns

### Critical Test Gaps

| Missing Tests | Impact |
|---------------|--------|
| `test_api.py` is ~1.5 KB (smoke test only) | API regressions will go undetected |
| No Volume Spike detector tests | Detector logic untested — confidence penalty logic, baseline calculation |
| No Price Move detector tests | Score normalization, edge cases on low-price markets |
| No ranking / deduplication tests | Dedupe bucket logic, rank score formula correctness |
| No SSE broadcaster tests | Queue overflow, subscriber cleanup, event ordering |
| No scheduler integration tests | Job sequencing, signal broadcast bug above would have been caught |
| No frontend tests | Any component regression is invisible |

### Hardcoded Magic Numbers

Scattered across the detector files without config exposure:

- `price_move.py` — `0.01` price floor (breaks sub-penny markets), `0.3` score normalizer (unexplained)
- `volume_spike.py` — `1000` / `5000` volume cutoffs for confidence penalties
- `deadline_near.py` — 2-hour price window for move detection
- `spread_change.py` / `liquidity_vacuum.py` — Minimum snapshot count inconsistently applied (6 in some detectors, 12 in others)

### APScheduler Is a Scalability Ceiling

All jobs run sequentially in a single process. If snapshot capture takes longer than its 120s interval (slow Polymarket API, large market set, DB contention), it delays signal detection. There is no job parallelism, no distributed execution, no visibility into job queue depth. This is fine for one instance at current market volume but is a hard ceiling if the connector count or market count grows significantly.

### No Event Sourcing / Audit Trail

When a signal's `rank_score` is updated or a market transitions from active to inactive, the previous state is gone. There is no append-only history of signal score changes, no record of which ingestion run changed what. Debugging anomalies (why did this signal disappear?) requires log archaeology.

### SSE Won't Scale Beyond ~50 Concurrent Users

The in-process `SignalBroadcaster` with per-subscriber `asyncio.Queue` holds all subscriber state in memory in the backend process. This pattern breaks under high concurrent load or multi-process deployment. Redis pub/sub or a proper message broker is the right answer if subscriber count grows.

### Alembic Migration Coverage Is Thin

Only 2 migrations exist (initial schema + signal_alerted column). There is no migration for analytics tables, no documented migration testing strategy, and no rollback scripts for existing migrations. Future schema changes have no established path.

### Inconsistent Confidence Penalty Logic

Each of the 5 detectors applies confidence penalties (for low volume, thin baseline, low liquidity) using different hardcoded thresholds with no shared utility. The same market condition gets penalized differently depending on which detector fires. A shared confidence scoring module would make the behavior consistent and testable.

---

## 5. State-of-the-Art Differentiating Features

These are the features that would make Signal Market Terminal genuinely unique — nothing publicly available does all of these together.

### Market Resolution Ground Truth
When a market closes, fetch the actual YES/NO outcome and backfill all historical signals on that market with a `resolved_correctly: bool` field. This transforms the accuracy dashboard from "did price move in the right direction?" into "was the signal actually a leading indicator of the real-world outcome?" Both Polymarket and Kalshi expose resolution data via their APIs. This is the single most important analytical improvement — without it, signal quality is unmeasurable. Everything else (backtesting, strategy tuning) depends on having this.

### Signal Backtesting Engine
Replay historical price snapshots through the detector suite with configurable threshold parameters. Answer: "If I had run rank_score ≥ 0.7 with a 6% price move threshold over the last 90 days, what would the win rate have been, and what was the false positive rate?" Store backtest runs with their parameters and results for comparison. This is the single feature that would attract serious operators — it lets users tune the system empirically rather than by intuition. No public prediction market tool offers this today.

### Cross-Platform Arbitrage Detector
The same real-world question often trades on both Polymarket and Kalshi at different prices. Detect real-time cross-platform price divergences above a configurable spread threshold (e.g., "Will the Fed cut rates in March?" at 62% on Polymarket and 57% on Kalshi — that's a live 5-point arb). Surface these as a dedicated `arbitrage` signal type with links to both markets. No tool currently does this in real time.

### Order Flow Imbalance (OFI) Detection
Polymarket's CLOB API exposes live order book snapshots. Tracking the ratio of buy-side vs sell-side aggression (order flow imbalance) identifies informed trading *before* it moves the midpoint price. This is a standard equity microstructure signal that has never been applied to prediction markets publicly. High OFI on a flat-price market is a leading indicator — price is about to move. Add as a 6th signal type.

### Outcome-Prior Signal Scoring
A price move from 94%→97% is a fundamentally different signal than 48%→51%, even if the percentage-point change is identical. Markets near the extremes are noisier and require larger moves to be meaningful. Markets near 50% are the most informationally sensitive. Incorporate the current probability level as a prior in the signal score: apply a dampening factor for extreme-probability markets and a sensitivity boost for near-50/50 markets. This would meaningfully reduce false positives on near-certain and near-impossible markets.

### Semantic Market Clustering via Embeddings
At market discovery time, generate a small text embedding for each market question (a local sentence-transformer model or a single API call). Cluster semantically related markets (e.g., "Fed rate cut in March", "inflation below 3% in Q1", "10-year yield above 4.5%"). Use these clusters to surface cross-market signal groups in real time: "3 Fed-related markets all showing simultaneous volume spikes" is a far more actionable alert than three independent signals. Current correlated-signals logic uses crude category-string matching — embeddings would make it genuinely powerful.

### Whale Wallet Tracking (Polymarket)
Polymarket is on-chain (Polygon). Large wallets that consistently trade ahead of price moves can be identified by analyzing historical on-chain position data. Index position changes for a tracked set of high-accuracy addresses and generate a `smart_money` signal type when they make significant entries or exits. This has no equivalent in any current public prediction market tool and would be a significant differentiator for sophisticated users.

### Grafana Dashboards + Alerting Rules
Prometheus metrics are already being scraped from `/metrics`. Add:
- Pre-built Grafana dashboards: signal firing rate over time, connector health (request latency, error rate, circuit breaker state), evaluation accuracy drift by signal type, market coverage by platform.
- Alerting rules: no signals fired in 30 minutes → page; circuit breaker open on either connector → page; ingestion last run > 10 minutes ago → warn.
- This would make the system operationally complete in a way that no open-source prediction market tool currently is.

---

## Summary Scorecard

| Area | Current Status | Priority |
|------|---------------|----------|
| Signal detection | Strong — 5 detectors, resilient | Maintain |
| Data ingestion | Good — 2 platforms, circuit-broken | Fix Kalshi pagination |
| REST API | Good — complete, paginated | Add integration tests |
| Frontend | Good — functional, reactive | Fix hardcoded filters |
| Real-time (SSE) | Good — works, limited scale | Fix broadcast bug |
| Testing | Weak — major gaps | Critical sprint item |
| Security | Fair — rate limiting + optional auth, no TLS | Add TLS |
| Scalability | Fair — APScheduler ceiling, no caching | Longer term |
| Analytics accuracy | Shallow — not tied to ground truth | Needs resolution tracking |
| Backtesting | Not started | High differentiator |
| Market resolution | Not started | Foundation for everything |
| Arb detection | Not started | High differentiator |
| OFI detection | Not started | High differentiator |
| Docs / runbook | Minimal | Needed for production ops |

---

## Recommended 7-Day Sprint Focus

**Days 1–2:** Fix the scheduler broadcast bug, close the critical test gaps (Volume Spike, Price Move, ranking, API integration tests), add FK constraint on `signal.outcome_id`.

**Days 3–4:** Implement market resolution tracking — fetch resolved outcomes from both APIs, backfill `resolved_correctly`, update the analytics accuracy calculation to use ground truth.

**Days 5–7:** Pick one differentiating feature. Recommendation: **backtesting engine** (highest operator value, builds directly on the resolution tracking just added) or **arbitrage detector** (fastest to build, immediately visible value, unique in the market).
