# Signal Market Terminal — 2026 Roadmap

*Created: 2026-04-08 · Baseline: v0.4.0 · Author: Product Strategy Review*

---

## Vision

**Signal Market Terminal is the Bloomberg Terminal for prediction markets.**

No tool today combines real-time signal detection, cross-platform aggregation, backtesting, and portfolio tracking for prediction market traders. Platform APIs provide raw data but zero intelligence. Open-source alternatives are fragmented hobby projects. Professional quant techniques (order flow analysis, whale tracking, cross-market correlation) exist in traditional finance but have never been packaged for prediction markets.

SMT fills this gap: an opinionated, signal-first intelligence platform that tells traders **what's moving, why it matters, and whether they should act** — across every major prediction market, in real time, with historically validated accuracy.

**What makes this worth paying for:**
1. **Signal quality you can prove** — backtested, resolution-verified accuracy rates per detector
2. **Cross-platform vision** — see Polymarket, Kalshi, and future platforms in one place
3. **Alpha that compounds** — whale tracking, arbitrage detection, and confluence scoring surface edges no single platform provides
4. **Professional-grade workflow** — screeners, watchlists, alerts, and portfolio P&L in a trader-native interface

---

## Current State (v0.4.0 — April 2026)

| Metric | Value |
|--------|-------|
| Signal detectors | 7 (price_move, volume_spike, spread_change, liquidity_vacuum, deadline_near, order_flow_imbalance, smart_money) |
| Platform connectors | 2 (Polymarket, Kalshi) |
| Backend LOC | ~7,100 Python |
| Frontend LOC | ~4,500 JSX |
| Tests | 275 passing |
| Migrations | 13 |
| API endpoints | 10 route modules |
| Alerting channels | 4 (Discord, Telegram, Web Push, Webhook) |
| Users | 0 (pre-launch) |

**Strengths:** Detector architecture, backtesting engine, multi-channel alerts, PWA mobile support.
**Weaknesses:** No user accounts, resolution pipeline incomplete (0 markets resolved), SSE won't scale past ~50 users, no execution, several unfixed production bugs (see QA report).

---

## Q2 2026 — Foundation & First Users (April–June)

**Theme:** Make it work reliably for 10 real traders. Get feedback. Earn trust.

### Milestone 1: Production Stability (April, ~2 weeks)

**Problem:** v0.4.0 has known DB migration gaps, routing collisions, and the resolution pipeline shows 0 resolved markets despite 3,500+ signals. No trader will trust a tool that shows broken data.

- Fix all QA-report bugs: missing `timeframe` migration, `positions` table, `/backtests/sweep` routing collision
- Complete the resolution pipeline: auto-fetch resolved outcomes from Polymarket & Kalshi, backfill `resolved_correctly` on all historical signals
- Docker Compose validated from clean state (zero-error `docker compose up`)
- Add health-check alerts: ingestion stale > 10min, circuit breaker open, zero signals in 30min
- Migrate SSE broadcaster to Redis pub/sub (removes 50-user ceiling)

**KPIs:** 0 500-errors on any API endpoint · Resolution pipeline processes markets within 1 hour of close · Docker cold-start works first try

### Milestone 2: User Accounts & Personalization (May, ~2 weeks)

**Problem:** Without user accounts, there are no saved filters, no personal watchlists, no per-user alert routing. Every session starts from scratch.

- User model with email/password auth (JWT tokens)
- Watchlists: save markets to named lists, pin to dashboard
- Saved signal filters: persist filter combinations per user
- Per-user alert routing: choose which signal types go to which channels
- API key generation for programmatic access

**KPIs:** User can sign up, configure alerts, and return to their watchlist next session

### Milestone 3: Signal Accuracy Dashboard v2 (May–June, ~2 weeks)

**Problem:** Current accuracy metric is "did price move in the right direction" — not "was the signal correct against the resolved outcome." Without ground-truth accuracy, the tool's credibility is unproven.

- Ground-truth accuracy: signals scored against actual market resolutions
- Per-detector accuracy breakdown with confidence intervals
- Signal P&L simulation: "if you had followed every signal of type X at rank ≥ Y, what would your return be?"
- Historical accuracy trends over time (is the system getting better?)
- Public accuracy page (no login required) — builds credibility

**KPIs:** Ground-truth accuracy available for ≥5 detector types · Accuracy page loads in <2s · At least 100 resolved signals in the dataset

### Milestone 4: Closed Beta Launch (June, ~2 weeks)

**Problem:** Need real trader feedback before building more features.

- Recruit 10-20 active prediction market traders (Discord, Twitter/X, Polymarket community)
- Onboarding flow: guided setup, connect alerts, explain signal types
- Feedback mechanism: in-app feedback button, weekly survey
- Usage analytics: which pages get visited, which signals get clicked, alert open rates
- Landing page with feature overview and waitlist signup

**KPIs:** 10+ active weekly users · NPS score collected · 3+ feature requests documented from real users

**Technical prerequisites:** PostgreSQL with proper backups (pg_dump cron), TLS via Let's Encrypt, rate limiting per user, CORS locked to production domain.

---

## Q3 2026 — Differentiation & API (July–September)

**Theme:** Build the features no one else has. Open the platform to developers.

### Milestone 5: Cross-Platform Arbitrage Engine (July, ~3 weeks)

**Problem:** The same real-world question often trades at different prices on Polymarket and Kalshi. No tool surfaces these opportunities in real time.

- Semantic market matching: embed market questions (sentence-transformers) and cluster semantically identical markets across platforms
- Real-time arbitrage scanner: flag when cross-platform price spread exceeds configurable threshold (e.g., 3+ points)
- Dedicated arbitrage signal type with links to both markets, spread history chart, and estimated profit after fees
- Arbitrage alerts via all channels
- Historical arb opportunity log: how long did each opportunity persist? What was the max spread?

**KPIs:** Arb opportunities detected within 5 minutes of occurrence · False positive rate <20% · At least 10 real arb opportunities surfaced per week

### Milestone 6: Public REST API & Webhooks (July–August, ~2 weeks)

**Problem:** Power users and developers want programmatic access to signals. Bots need real-time signal data.

- Versioned public API (v1) with OpenAPI docs
- API key authentication with rate limiting (free: 100 req/min, paid: 1000 req/min)
- Webhook subscriptions: push new signals to user-defined endpoints (HMAC-signed payloads)
- Signal streaming via WebSocket (upgrade from SSE for bidirectional comms)
- Python SDK package (`pip install signal-market-terminal`)
- API usage dashboard: request counts, latency, error rates per key

**KPIs:** API docs live and discoverable · 5+ external integrations using the API · P99 latency <200ms

### Milestone 7: Advanced Screener & Watchlists (August, ~2 weeks)

**Problem:** Traders need to filter thousands of markets to find the ones worth watching. Current filters are basic (platform, signal type).

- Multi-criteria screener: filter by volume, price range, time-to-close, signal count, detector type, accuracy history, spread
- Column customization: choose which data columns to display
- Screener presets: "High-volume markets closing this week with strong signals"
- Bulk watchlist actions: add screener results to a watchlist
- Screener results exportable as CSV/JSON
- Comparison view: side-by-side market analysis

**KPIs:** Screener query returns in <1s for 5,000+ markets · Users create ≥3 custom screeners on average

### Milestone 8: ML Signal Scoring v1 (September, ~3 weeks)

**Problem:** The linear formula (`signal_score × confidence × recency_weight`) is good but has limits. By Q3, there will be enough resolved signals (~500+) to train a meaningful model.

- Feature extraction: market age, volume trajectory, orderbook shape, time-to-close, cross-detector confluence, prior signal accuracy for this market category
- Gradient-boosted model (XGBoost/LightGBM) trained on resolved signals
- ML score blended with formula score (configurable weight, default 50/50)
- Model performance dashboard: precision/recall curves, feature importance, comparison vs. formula-only
- Automated retraining pipeline (weekly, on new resolution data)
- A/B comparison: formula-only vs. ML-blended accuracy tracked in parallel

**KPIs:** ML-blended accuracy ≥ 5% higher than formula-only · Model retrains weekly without manual intervention · Feature importance is interpretable

**Technical prerequisites:** Redis for caching and pub/sub (from Q2), sufficient resolved signal volume (500+), model artifact storage.

---

## Q4 2026 — Scale, Community & Monetization (October–December)

**Theme:** Turn a tool into a business. Build network effects.

### Milestone 9: Monetization — Pro Tier (October, ~3 weeks)

**Problem:** The tool needs revenue to be sustainable. Traders will pay for alpha.

- **Free tier:** 5 markets watchlisted, basic signal feed (delayed 15 min), 1 alert channel, no API access
- **Pro tier ($29/month):** Unlimited watchlists, real-time signals, all alert channels, full API access (1000 req/min), ML-enhanced scoring, arbitrage alerts, screener presets, export/CSV
- **Team tier ($99/month):** Everything in Pro + shared watchlists, team alert routing, priority support, custom webhook integrations
- Stripe integration for payments
- Usage-gated features (API calls, screener queries, alert volume)
- 14-day free trial for Pro

**KPIs:** 50+ paying Pro users by end of Q4 · MRR > $1,500 · Churn < 10%/month · Free-to-paid conversion > 5%

### Milestone 10: Community & Social Features (October–November, ~3 weeks)

**Problem:** TradingView proved that social features create network effects and retention. Prediction market traders are a tight community.

- Public signal leaderboard: anonymized ranking of signal types by accuracy over last 30/90 days
- User-published watchlists: share a curated market watchlist with commentary
- Signal discussion threads: comment on individual signals (was this a real edge or noise?)
- Reputation system: accuracy score based on user's watchlist/signal track record
- Weekly digest email: top signals, best-performing detectors, notable arb opportunities

**KPIs:** 20+ published watchlists · 100+ signal comments · Weekly digest open rate > 30%

### Milestone 11: Third Platform Connector — Metaculus/PredictIt/Insight (November, ~2 weeks)

**Problem:** More platforms = more arbitrage opportunities = more value for users.

- Evaluate and integrate highest-value third platform (Metaculus for forecast aggregation, or a new real-money exchange if launched)
- Connector follows existing pattern: base class, circuit breaker, retry logic
- Cross-platform arbitrage now covers 3+ platforms
- Market matching quality improves with more data points

**KPIs:** Third connector operational with <5% error rate · Cross-platform coverage increases arb detection by 30%+

### Milestone 12: Operational Excellence (December, ~2 weeks)

**Problem:** Scaling to hundreds of users requires operational maturity.

- Grafana dashboards: signal fire rate, connector health, evaluation accuracy drift, API latency, user activity
- Alerting rules: PagerDuty/OpsGenie integration for system health
- Database optimization: partitioned tables for signals/snapshots, read replicas for API queries
- CDN for frontend assets
- Automated database backups with point-in-time recovery
- Load testing: validate 500 concurrent users
- SOC 2 readiness assessment (if pursuing enterprise customers)

**KPIs:** 99.9% uptime · P99 API latency < 500ms at 500 concurrent users · Zero data loss incidents

---

## Q1 2027 — Market Leadership & Advanced Features (January–March)

**Theme:** Become the definitive platform. Build moats.

### Milestone 13: Execution Integration (January, ~3 weeks)

**Problem:** Showing signals without enabling action is friction. Traders want to act on signals directly.

- One-click trade execution via Polymarket CLOB API (user provides own API keys / wallet)
- Kalshi order placement via their API (OAuth2 flow)
- Position sizing suggestions based on signal strength and Kelly criterion
- Order confirmation with P&L projection
- Trade journal: automatic logging of signal-triggered trades with outcome tracking
- **Safety:** No automated execution — always require user confirmation. Display clear risk warnings.

**KPIs:** 30%+ of Pro users execute trades through SMT · Avg time from signal to trade < 60 seconds

### Milestone 14: News & Event Integration (February, ~2 weeks)

**Problem:** Prediction market prices move on news. Connecting signals to their catalysts makes them actionable.

- News feed aggregation: RSS/API from major news sources, filtered to prediction-market-relevant topics
- Event timeline: overlay news events on price charts
- NLP-based market-news matching: automatically link breaking news to affected markets
- "Why is this moving?" context card on each signal: related news, recent large trades, cross-market activity
- Push alert enhancement: include news context in signal notifications

**KPIs:** 80%+ of signals have at least one contextual data point · Users report higher confidence in signal interpretation

### Milestone 15: Custom Detector SDK (February–March, ~3 weeks)

**Problem:** Power users want to encode their own signal logic. A platform that lets users build is a platform they can't leave.

- Python SDK for writing custom detectors that plug into the SMT pipeline
- Detector marketplace: users can publish and share custom detectors
- Sandboxed execution: custom detectors run in isolated containers
- Backtesting for custom detectors: same engine, same historical data
- Revenue share: detector authors earn a % of subscriptions driven by their detector

**KPIs:** 10+ community-contributed detectors · 3+ detectors with >60% accuracy · SDK documentation rated 4+/5 by users

### Milestone 16: Enterprise & Institutional (March, ~3 weeks)

**Problem:** Hedge funds and research firms are entering prediction markets. They need compliance-grade tooling.

- Multi-user organizations with role-based access (admin, trader, analyst, viewer)
- Audit trail: immutable log of all signal views, trades, and configuration changes
- Compliance exports: CSV/PDF reports for regulatory documentation
- SSO integration (SAML/OIDC)
- SLA-backed uptime guarantees
- Dedicated support channel
- Custom connector development as a service

**KPIs:** 2+ enterprise contracts signed · Enterprise ACV > $10,000 · Zero compliance-related incidents

---

## Success Metrics — Annual Targets

| Metric | Q2 Target | Q3 Target | Q4 Target | Q1 2027 Target |
|--------|-----------|-----------|-----------|----------------|
| Active weekly users | 10 | 50 | 200 | 500 |
| Paying users | 0 | 0 | 50 | 150 |
| MRR | $0 | $0 | $1,500 | $5,000 |
| Signal detectors | 7 | 8+ (ML) | 8+ | 10+ (community) |
| Platform connectors | 2 | 2 | 3 | 3+ |
| Ground-truth accuracy (best detector) | >40% | >50% | >55% | >60% |
| API uptime | 95% | 99% | 99.9% | 99.9% |
| Tests | 275+ | 400+ | 500+ | 600+ |

---

## Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Polymarket API changes/deprecation | High | Medium | Abstraction layer via connector base class; monitor API changelogs; maintain relationships with platform devs |
| Regulatory changes to prediction markets (US) | High | Medium | Kalshi is CFTC-regulated — focus there for US users; international expansion via Polymarket |
| Insufficient resolved signals for ML | Medium | Low (by Q3) | Defer ML until 500+ resolved signals confirmed; formula-only scoring remains strong fallback |
| Competition from platform-native tools | Medium | Low | Platforms are incentivized to keep traders on-platform, not build cross-platform tools — our niche is the aggregation layer |
| User acquisition difficulty | High | Medium | Prediction market community is small but tight — focus on Discord/Twitter, trader testimonials, public accuracy dashboard as credibility proof |
| Single-developer bottleneck | High | High | Open-source core components; prioritize documentation; build community contributors via detector SDK |

---

## Technical Architecture Evolution

```
v0.4.0 (Now)                          v1.0 Target (Q1 2027)
─────────────                          ────────────────────
Single process (APScheduler)     →     Celery workers + Redis broker
In-process SSE broadcaster       →     Redis pub/sub + WebSocket
SQLite in tests                  →     PostgreSQL in tests (testcontainers)
No auth                          →     JWT + API keys + SSO
No caching                       →     Redis cache layer
Single PostgreSQL instance       →     Read replicas + partitioned tables
Manual deployment                →     CI/CD with staging environment
No CDN                           →     CloudFront/Cloudflare for frontend
Prometheus only                  →     Prometheus + Grafana + PagerDuty
```

---

## Principles

1. **Signal quality is the product.** Every feature should ultimately improve the quality, speed, or actionability of signals. If it doesn't, question whether to build it.
2. **Prove it with data.** Every claim about signal accuracy must be backed by resolution-verified ground truth. No "trust us" — show the numbers publicly.
3. **Trader-first design.** Build for the person who checks Polymarket 20 times a day and has $10K+ in active positions. Not for casual observers.
4. **Cross-platform is the moat.** No single platform will build a tool that sends users to competitors. Our value is the aggregated view.
5. **Open core, paid power.** Core signal detection stays accessible. Advanced features (ML scoring, arbitrage, API, execution) justify the subscription.
