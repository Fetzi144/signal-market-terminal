# Signal Market Terminal — 2026 Roadmap

*Rewritten: 2026-04-08 · Baseline: v0.4.0 · Philosophy: Make traders profitable or die trying.*

---

## The Problem with v0.4.0

The current system is a **signal dashboard**, not a **trading system**. It tells you "something is happening" but never tells you "here's how to make money from it."

Specific failures:

1. **Signals have no expected value.** A `signal_score` of 0.7 doesn't map to dollars. The trader still has to guess: is this worth buying? How much? At what price? No answer → no action → no value.

2. **Accuracy is measured wrong.** The evaluator checks "did price move in the predicted direction within 24h?" In binary markets, the only question that matters is: **did the market resolve correctly, and did we buy below fair value?** A signal that catches a 5% uptick that reverts is noise, not alpha.

3. **No position sizing.** Even if signals were perfect, without Kelly criterion or any bankroll logic, a trader following our signals would overbet winners and underbet losers — destroying geometric growth.

4. **No closing line value (CLV) tracking.** CLV is the gold standard metric in any betting/prediction market. It measures: "was the price at the time we signaled better than the closing price?" Positive CLV = long-term profitability regardless of short-term variance. We don't track it.

5. **Detectors are disconnected.** Seven independent detectors fire independently. A price move + volume spike + whale entry on the same market should produce a much stronger signal than any one alone. There's no Bayesian fusion, no confluence that produces a probability estimate.

None of the dashboard features (watchlists, community, screeners, enterprise SSO) matter if the core signals don't make traders money. **Alpha first. Everything else second.**

---

## Vision

**SMT is the quantitative edge for prediction market traders.** It outputs probability estimates better than the market, sizes positions optimally, and proves its edge with auditable track records. Not a dashboard with alerts — a trading brain with a verifiable P&L.

**The one metric that matters:** Cumulative P&L of a trader following SMT signals with recommended position sizes, tracked live, auditable, compared against naive strategies.

---

## Current State (v0.4.0)

| Asset | Status | Alpha Value |
|-------|--------|-------------|
| 7 detectors (price_move, volume_spike, spread, liquidity, deadline, OFI, smart_money) | Working | Low — fire independently, output heuristic scores not probabilities |
| Backtesting engine + parameter sweep | Working | Medium — can replay detectors, but measures wrong thing (price direction, not resolution P&L) |
| 2 connectors (Polymarket CLOB + Kalshi REST) | Working | Infrastructure — necessary but not differentiating |
| Portfolio tracker | Broken (missing migrations) | Zero — can't track P&L if it doesn't run |
| Resolution pipeline | Shows 0 resolved markets | Zero — can't measure anything without ground truth |
| Evaluation system (15m/1h/4h/24h horizons) | Working | Low — measures price movement, not profitability |
| 275 tests, PWA, alerts | Working | Table stakes — not alpha |

---

## Q2 2026 — Build the Measurement Layer (April–June)

**Theme:** You can't improve what you can't measure. Before building any new algorithms, we need the infrastructure to tell us if they actually work.

### Milestone 1: Ground Truth Pipeline (April, 2 weeks)

**Why this first:** Every single thing that follows depends on knowing whether signals were correct. Without resolution data, we're flying blind. The system has 3,500+ signals and 0 resolved markets — that's 10 days of data going to waste.

**What to build:**
- **Resolution fetcher:** Cron job that polls Polymarket and Kalshi for resolved markets. When a market resolves YES/NO, backfill `resolved_correctly` on every signal that fired for that market. This already exists partially in the ingestion module — finish it.
- **Closing Line Value (CLV) tracker:** For every signal, record `price_at_signal` and `closing_price` (the last traded price before resolution). CLV = `closing_price - price_at_signal`. Positive CLV = we signaled before the market priced it in. **CLV is the single most important metric** — it predicts long-term profitability even with small sample sizes.
- **Brier score calculation:** For each detector, compute Brier score = mean of `(forecast_probability - actual_outcome)²`. Lower = better calibrated. This tells us if our probability estimates are actually accurate.
- Fix all QA-report bugs: missing `timeframe` migration, `positions` table, routing collision. Non-negotiable — broken endpoints destroy trust.

**KPIs:**
- Resolution pipeline processing markets within 1 hour of close
- CLV tracked for every signal retroactively
- Brier score computable per detector type
- 0 HTTP 500 errors on any endpoint

**Technical debt also addressed:** Redis pub/sub for SSE (removes 50-user ceiling), Docker cold-start validated.

---

### Milestone 2: Probability-First Detector Refactor (May, 3 weeks)

**Why:** Current detectors output `signal_score ∈ [0,1]` — an arbitrary heuristic. A `price_move` score of 0.8 has no mathematical relationship to the probability of the market resolving YES. Without probability outputs, we can't compute expected value, can't size positions, can't combine signals via Bayes. This is the fundamental architectural shift.

**What to build:**

- **Detector probability output:** Each detector estimates `P(resolve_YES | signal_data)` instead of a heuristic score. This isn't a minor tweak — it requires rethinking what each detector actually predicts:
  - `price_move`: large moves toward YES → higher P(YES). The magnitude and direction of the move map to a probability adjustment from the current market price.
  - `volume_spike`: high volume confirms current price trend → P(YES) ≈ current_price + confirmation_boost. Volume against the trend → contrarian signal.
  - `order_flow_imbalance`: buy-side pressure → P(YES) increases proportionally to OFI magnitude. Flat price + high OFI = informed flow not yet priced in.
  - `smart_money`: whale buys YES → P(YES) increases by amount proportional to whale's historical accuracy.
  - `deadline_near`: no direct probability shift — but increases confidence in other signals (less time for new information to arrive).
  - `spread_change` / `liquidity_vacuum`: no direct probability shift — these are risk/uncertainty modifiers that widen confidence intervals.

- **Probability prior:** The market price IS the prior. Detectors output an adjustment relative to the current market price, not an absolute probability. `P(YES) = market_price + detector_adjustment`. This avoids the trap of ignoring the most important information source (the market itself).

- **Outcome-prior sensitivity:** A move from 94%→97% is noise. A move from 48%→51% is significant. Apply a sensitivity curve based on current market price: signals near 50% get amplified, signals at extremes get dampened. This alone will dramatically reduce false positives.

- **Calibration tracking per detector:** After each resolution, update a running calibration curve. When a detector says "70% likely YES", does it resolve YES ~70% of the time? Calibration plots on the performance dashboard.

**KPIs:**
- Every detector outputs `estimated_probability` alongside the existing score (backward compatible)
- Calibration error < 10% for the top 3 detectors (measured after 200+ resolutions)
- False positive rate drops by ≥ 30% from outcome-prior sensitivity curve

---

### Milestone 3: Bayesian Signal Fusion (May–June, 2 weeks)

**Why:** Seven independent detectors firing independently is wasteful. A `price_move` + `volume_spike` + `smart_money` on the same market at the same time should produce a much stronger signal than any one alone. But right now they're displayed as three separate signals with no combined view. Bayesian fusion combines multiple evidence sources into a single posterior probability — the mathematically correct way to aggregate information.

**What to build:**

- **Confluence engine:** When multiple detectors fire on the same `outcome_id` within a configurable window (e.g., 30 minutes):
  1. Start with the market price as the prior: `P₀ = market_price`
  2. Each detector's probability adjustment is a likelihood ratio
  3. Apply Bayes: `P_posterior ∝ P_prior × LR₁ × LR₂ × ... × LRₙ`
  4. Output: a single fused probability estimate with uncertainty bounds
  
- **Independence correction:** Detectors aren't truly independent (price_move and volume_spike often co-occur). Estimate correlation between detector pairs from historical data. Apply a correlation discount to avoid double-counting evidence.

- **Confluence signal type:** The fused estimate becomes a new first-class signal: `signal_type = "confluence"`. It carries the combined estimated probability, the contributing detector list, and an uncertainty range.

- **Expected Value (EV) calculation:** For every signal (individual or fused):
  ```
  EV = (estimated_prob × $1.00) - market_price
  ```
  A positive EV means the signal suggests the market is mispriced. The magnitude is the edge in cents per share. **Only surface signals with EV > configurable threshold** (default: $0.03, i.e., 3-cent edge). This is the single most important filter — it transforms "interesting things happening" into "opportunities to make money."

**KPIs:**
- Confluence signals have higher CLV than any individual detector (measured retroactively on historical data)
- EV filter reduces signal volume by ≥ 50% while maintaining or improving resolution accuracy
- Fused probability estimates have lower Brier score than individual detectors

---

### Milestone 4: Kelly Criterion Position Sizing (June, 2 weeks)

**Why:** Even with perfect signals, bad position sizing destroys returns. The Kelly criterion is the mathematically optimal betting strategy — it maximizes long-term geometric growth given a known edge and odds. This is not optional for a tool that claims to make traders money.

**What to build:**

- **Kelly calculator:** For each signal with EV > 0:
  ```
  edge = estimated_prob - market_price
  odds = (1 - market_price) / market_price    (for YES bets)
  kelly_fraction = edge / (1 - market_price)   (simplified for binary)
  recommended_size = bankroll × kelly_fraction × kelly_multiplier
  ```
  `kelly_multiplier` defaults to 0.25 (quarter-Kelly — standard conservative practice because full Kelly assumes perfect probability estimates, which we don't have).

- **Bankroll tracking:** User sets their total bankroll. Recommended position sizes are absolute dollar amounts, not percentages. The system tracks current exposure across all open positions and adjusts recommended sizes to stay within risk limits.

- **Risk guardrails:**
  - Max single-position size: 5% of bankroll (even if Kelly says more)
  - Max total exposure: 30% of bankroll
  - Max correlated exposure: 15% of bankroll on semantically related markets (requires market clustering — basic keyword matching first, embeddings later)
  - Minimum edge threshold: don't size positions where EV < $0.02

- **Signal output upgrade:** Every signal now shows:
  ```
  Market: "Will Fed cut rates in June?"
  Direction: BUY YES
  Market Price: $0.42
  Our Estimate: $0.51
  Edge: +$0.09 (21.4%)
  Expected Value: +$0.09/share
  Recommended Size: $340 (2.1% of bankroll)
  Kelly Fraction: 8.4% (using quarter-Kelly)
  ```
  **This is the format that makes traders money.** Not "signal_score: 0.73, confidence: 0.85."

**KPIs:**
- Simulated portfolio (backtest) using Kelly sizing shows positive cumulative P&L
- Max drawdown < 20% of peak bankroll in simulation
- Position sizing recommendations available for every EV-positive signal

---

## Q3 2026 — Alpha Strategies (July–September)

**Theme:** The measurement and probability layer is live. Now build the strategies that generate verifiable edge.

### Milestone 5: Closing Line Value Strategy Engine (July, 3 weeks)

**Why:** CLV is the only metric that reliably predicts long-term profitability in betting markets. A strategy with positive CLV will be profitable over time — it's a mathematical near-certainty with sufficient volume. By July we'll have 3 months of CLV data to analyze.

**What to build:**

- **CLV analysis dashboard:** For every detector and signal type, show:
  - Average CLV (in cents) over 7d, 30d, 90d
  - CLV distribution histogram — is the edge consistent or driven by a few outliers?
  - CLV by market category (politics, crypto, sports, finance) — where does each detector have edge?
  - CLV trend over time — is the edge growing or decaying?

- **Strategy backtester v2:** Rewrite backtesting to measure what matters:
  - Input: detector type, EV threshold, Kelly multiplier, date range
  - Output: cumulative P&L curve, max drawdown, Sharpe ratio, win rate, average CLV, Brier score
  - Not just "how many signals were correct" but "how much money would you have made following this strategy with this position sizing"

- **Strategy combinator:** Test multi-detector strategies:
  - "Follow confluence signals with EV > $0.05 at quarter-Kelly" → what's the P&L?
  - "Follow smart_money signals only on markets with > $50K volume" → what's the P&L?
  - Allow parameter sweeps across: EV threshold, Kelly multiplier, detector filters, market category, volume floor

- **Live strategy tracker:** Paper-trade strategies in real time (no actual execution). Track recommended trades and their outcomes. Build a public track record: "Strategy X has returned +14.3% over 47 trades since July 1."

**KPIs:**
- At least 2 strategies show positive CLV > $0.02 over 90-day backtest
- Live strategy tracker running with daily P&L updates
- Backtest results reproducible and exportable

---

### Milestone 6: Cross-Platform Arbitrage with Execution Path (July–August, 3 weeks)

**Why:** Arbitrage is the purest form of edge — it's risk-free profit when the same event trades at different prices on different platforms. This is the one strategy where you don't need better probability estimates than the market, you just need to be faster than the arb closes.

**What to build:**

- **Semantic market matching:** Use sentence-transformers to embed market questions and match semantically identical markets across Polymarket and Kalshi. "Will the Fed cut rates in June 2026?" on both platforms should be automatically linked.
  
- **Real-time arb scanner:** Every snapshot cycle (2 min), compare matched market prices. Flag when spread > fee-adjusted threshold. Net spread = `|price_A - price_B| - fee_A - fee_B`. Only surface profitable arbs.

- **Arb signal with execution plan:**
  ```
  ARB DETECTED
  Polymarket: "Fed rate cut June" YES @ $0.42
  Kalshi:     "Fed rate cut June" YES @ $0.37
  Spread: $0.05 (after fees: $0.03 net)
  Action: BUY YES on Kalshi @ $0.37, SELL YES on Polymarket @ $0.42
  Guaranteed profit: $0.03/share
  Recommended size: $2,000 (limited by Kalshi orderbook depth)
  Time-to-close estimate: 12 minutes (based on historical spread decay)
  ```

- **Arb decay tracking:** How long do arbs persist? What's the average spread at detection vs. 5min/15min/1h later? This tells us how fast we need to be.

- **One-click execution prep:** API integration with Polymarket CLOB and Kalshi REST for placing orders. User provides API keys. Execution is never automatic — user confirms every trade. But the friction is reduced to a single confirmation click.

**KPIs:**
- Detect ≥ 80% of arb opportunities with spread > $0.03 within 5 minutes
- False positive rate < 10% (wrong market matches)
- Average arb net profit > $0.02/share after fees
- Track cumulative arb P&L (paper-traded initially)

---

### Milestone 7: Smart Money Edge Quantification (August–September, 3 weeks)

**Why:** Polymarket is on Polygon — every trade is on-chain. Wallets with historical accuracy > 60% represent informed money. Following their trades is a proven strategy in sports betting ("following the sharps"). The v0.4.0 whale tracker detects large trades but doesn't measure whether following those trades is actually profitable.

**What to build:**

- **Wallet scoring system:** For every tracked wallet:
  - Historical accuracy: what % of their positions resolved correctly?
  - CLV: did they consistently buy below the closing line?
  - ROI: what's their total return?
  - Recency-weighted: recent performance matters more than 6-month-old trades
  - Tier system: A-tier (>60% accuracy, >$50K volume), B-tier (>55%), C-tier (>50%)

- **Smart money signal upgrade:** When an A-tier wallet takes a position:
  ```
  SMART MONEY SIGNAL
  Wallet: 0x7a3...f2e (A-tier, 64.2% accuracy, 312 resolved trades)
  Action: Bought 5,000 YES shares @ $0.38
  Market: "ETH above $4K by July 1"
  Wallet's historical edge on crypto markets: +$0.07 CLV
  Our estimate incorporating this signal: P(YES) = 0.46 (market says 0.38)
  Edge: +$0.08
  Recommended: BUY YES, $520 (quarter-Kelly)
  ```

- **Wallet discovery pipeline:** Continuously scan Polygon for new wallets that trade prediction markets. Score them against historical resolutions. Auto-promote high-accuracy wallets to tracked set.

- **Counter-signal detection:** Track wallets that are consistently WRONG (< 40% accuracy). Their trades become contrarian signals — fade them.

**KPIs:**
- A-tier wallets maintain > 60% resolution accuracy over rolling 90-day window
- Smart money signals show positive CLV > $0.04
- Following smart money signals at quarter-Kelly shows positive simulated P&L
- Track ≥ 50 wallets with ≥ 100 resolved trades each

---

### Milestone 8: Regime Detection & Adaptive Strategies (September, 2 weeks)

**Why:** Markets behave differently in different phases. A detector optimized for "quiet accumulation" markets will generate false positives during "news event volatility." The system needs to recognize market regimes and adapt.

**What to build:**

- **Market regime classifier:** For each market, classify the current regime:
  - **Quiet/accumulation:** Low volume, tight spread, slow price drift. OFI signals strongest here.
  - **News-driven momentum:** Volume spike + rapid price move. Follow the move — momentum persists in prediction markets due to information cascading.
  - **Mean-reversion/overreaction:** Extreme price move on single event, followed by partial revert. Fade the move — overreaction is common in thin markets.
  - **Convergence:** Market approaching deadline with price between 10-90%. Deadline_near signals most relevant.
  - **Resolved/dead:** Price at >95% or <5%. No signal should fire — the market is decided.

- **Adaptive detector weights:** In momentum regime, weight OFI and smart_money higher. In mean-reversion regime, weight price_move (contrarian direction) higher. In convergence regime, weight deadline_near and spread_change higher.

- **Regime-aware backtesting:** Backtest results segmented by regime. "This strategy works in momentum regimes (+8% ROI) but loses money in mean-reversion (-3% ROI)." → only deploy it in momentum regimes.

**KPIs:**
- Regime classifier has > 70% accuracy (validated against labeled historical data)
- Regime-adaptive strategies outperform static strategies by ≥ 3% ROI in backtests
- Every active market has a current regime label visible in the UI

---

## Q4 2026 — Compounding Edge & Monetization (October–December)

**Theme:** The edge is proven. Now compound it, and charge for it.

### Milestone 9: Execution Integration (October, 3 weeks)

**Why:** Every second between "signal fires" and "order placed" is lost edge. Edge decays — our CLV data will show exactly how fast. Reducing time-to-execution is a direct multiplier on profitability.

**What to build:**
- One-click execution via Polymarket CLOB API and Kalshi REST API
- User-provided API keys / wallet connections (we never custody funds)
- Pre-filled order forms: signal fires → order form populated with recommended direction, size, limit price
- Execution quality tracking: compare fill price vs. signal price. Slippage = lost edge.
- Trade journal: every signal-triggered trade logged with signal data, fill price, resolution outcome, P&L
- **No automated execution.** Every trade requires user confirmation. Display clear risk warnings.

**KPIs:**
- Time from signal to order placed < 30 seconds (vs. minutes today)
- Slippage tracked and averaged < $0.01 per trade
- 30%+ of active users execute trades through SMT

---

### Milestone 10: Monetization — Pay for Alpha (November, 2 weeks)

**Why:** The product is now a proven edge generator with a public track record. Traders pay for alpha.

**Pricing model — simple, aligned with value:**
- **Free:** Signal feed (15-min delay), 3 markets watchlisted, basic accuracy stats, no position sizing
- **Pro ($49/month):** Real-time signals with EV and Kelly sizing, full strategy backtester, live strategy tracker, execution integration, all alert channels, API access (1000 req/min), smart money signals
- **Pro+ ($99/month):** Everything in Pro + arbitrage alerts, custom strategy builder, priority signal delivery (< 10s latency), webhook API, portfolio risk management

**Why $49 not $29:** If the signals have +5% CLV, a trader with a $10K bankroll makes ~$500/month. $49 for that is a no-brainer. Underpricing signals "we're not confident in our own edge."

- Stripe integration, 14-day free trial
- Public track record page (free, no login) — this IS the marketing. "Our confluence strategy returned +22% over 4 months. Verify it yourself."

**KPIs:**
- 30+ paying users by end of Q4
- MRR > $2,000
- Free-to-paid conversion > 8% (higher than typical SaaS because the value prop is directly measurable in dollars)
- Churn < 8%/month (traders stay if they're profitable)

---

### Milestone 11: Portfolio Risk Engine (November–December, 3 weeks)

**Why:** A trader following 20 SMT signals simultaneously needs portfolio-level risk management, not just per-signal sizing.

**What to build:**
- **Correlation-aware sizing:** Markets that are semantically related (multiple Fed-related markets) should have correlated position limits. Use embedding-based market clustering.
- **Portfolio VaR (Value at Risk):** Given all open positions, what's the worst-case 1-day loss at 95% confidence? This requires estimating co-movement between prediction markets.
- **Drawdown protection:** If portfolio drawdown exceeds configurable threshold (e.g., 15%), reduce all recommended sizes by 50% until drawdown recovers.
- **Diversification score:** "Your portfolio is 80% concentrated in US politics markets" → suggest diversification into other categories.
- **P&L attribution:** Which detectors/strategies contributed to returns? Which lost money? This drives improvement.

**KPIs:**
- Portfolio VaR estimates within 20% of realized worst-day losses (backtested)
- Correlated exposure limits prevent > 15% bankroll concentration in any market cluster
- P&L attribution available per-detector and per-strategy

---

### Milestone 12: Self-Improving Feedback Loop (December, 2 weeks)

**Why:** The system should get better over time automatically — not just when a developer pushes code.

**What to build:**
- **Auto-calibration:** Each detector's probability outputs are adjusted weekly based on realized calibration curves. If the price_move detector's "70% probability" events resolve YES only 60% of the time, auto-scale the output down.
- **Detector weight optimization:** Run weekly backtests that optimize the Bayesian fusion weights. If OFI has been generating high CLV this month, increase its weight in the confluence engine.
- **Feature importance tracking:** For the ML scoring model (trained by now on 1000+ resolved signals), track which features drive predictions. Surface drifting feature importance as an alert.
- **Dead detector alert:** If a detector's CLV goes negative for 30 consecutive days, alert and auto-demote it from the confluence engine.

**KPIs:**
- Calibration error improves month-over-month without manual tuning
- At least 1 detector auto-demoted or re-weighted based on performance data
- Overall system Brier score improves ≥ 5% over Q4

---

## Q1 2027 — Compounding Advantages (January–March)

**Theme:** Widen the moat. More data, more strategies, more edge.

### Milestone 13: Third Connector + Expanded Coverage (January, 2 weeks)

- Add highest-value third platform (Insight Prediction, Metaculus, or new regulated exchange)
- More markets = more arb opportunities = more data = better calibration
- Every new platform makes the system better for existing platforms (cross-market signals)

### Milestone 14: News-to-Signal Pipeline (January–February, 3 weeks)

**Why:** Prediction markets move on news. If we can detect relevant news before the market fully prices it in, that's edge.

- RSS/API aggregation from news sources relevant to active markets
- NLP matching: breaking news → affected prediction markets (embedding similarity)
- News-triggered signals: "Reuters reports Fed official hints at June cut" → BUY YES on "Fed rate cut June" if market hasn't moved yet
- Measure: did news-triggered signals have positive CLV? If not, kill the feature.

### Milestone 15: Strategy Marketplace (February–March, 3 weeks)

- Users can publish custom strategies (detector combinations + parameters + sizing rules)
- Live track records for every published strategy — no fake backtests, only forward P&L
- Revenue share: strategy authors earn 20% of subscriptions they drive
- This is the network effect play: strategies compete on P&L, users follow the best ones, authors improve them

### Milestone 16: Automated Paper-Trading Bots (March, 2 weeks)

- Users define a strategy → system paper-trades it automatically → tracks P&L
- "Would this strategy have been profitable over the last month?" answered without risking real capital
- Bridge to eventual semi-automated execution (long-term, requires regulatory clarity)

---

## The One Metric That Decides Everything

**Cumulative P&L of the default recommended strategy, tracked live, auditable, since launch day.**

If this number is positive and growing, everything else follows — users pay, users refer, users stay.
If this number is flat or negative, no amount of screeners, watchlists, community features, or enterprise SSO will save the product.

Every engineering decision, every sprint priority, every feature cut should be evaluated against: **"Does this make the cumulative P&L line go up?"**

---

## Annual Targets

| Metric | Q2 | Q3 | Q4 | Q1 2027 |
|--------|----|----|----|----|
| Resolved signals in dataset | 200+ | 1,000+ | 3,000+ | 5,000+ |
| Best detector CLV (cents) | measured | > +$0.02 | > +$0.04 | > +$0.05 |
| Confluence Brier score | measured | < 0.22 | < 0.20 | < 0.18 |
| Simulated strategy cumulative ROI | measured | > +5% | > +12% | > +20% |
| Live paper-trade P&L | — | tracking | +$X | +$X growing |
| Paying users | 0 | 0 | 30+ | 80+ |
| MRR | $0 | $0 | $2,000+ | $5,000+ |
| Platform connectors | 2 | 2 | 2 | 3 |
| Tests | 300+ | 400+ | 500+ | 600+ |

---

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Our probability estimates are not better than the market** | Fatal — no edge = no product | Measure CLV obsessively from day 1. Kill detectors with negative CLV. The measurement infrastructure in Q2 exists specifically to fail fast on bad strategies. |
| **Insufficient resolution volume for statistical significance** | High — can't prove edge | Focus on short-duration markets (resolve in days, not months). Polymarket has hundreds of markets resolving weekly. |
| **Polymarket/Kalshi API changes** | High | Connector abstraction layer. Monitor changelogs. Build relationships with platform devs. |
| **Overfitting backtests** | High — looks great historically, fails live | Out-of-sample testing mandatory. Live paper-trading before any strategy goes to production. Walk-forward validation. |
| **Regulatory changes to prediction markets** | High | Kalshi is CFTC-regulated (safe harbor). Polymarket is offshore. Support both. |
| **Edge decay — market gets efficient** | Medium | Continuously develop new signal sources (news, on-chain, cross-market). Edge in thin markets decays slower than in equities. |
| **Single-developer bottleneck** | High | Open-source the measurement layer. Strategy marketplace creates community contributors. |

---

## Architecture Evolution

```
v0.4.0 (Dashboard)                    v1.0 Target (Trading System)
──────────────────                     ──────────────────────────
Heuristic scores (0-1)           →     Probability estimates with uncertainty
Independent detectors            →     Bayesian fusion engine
"Did price move?" evaluation     →     CLV + Brier score + resolution P&L
No position sizing               →     Kelly criterion with risk guardrails
Signal feed                      →     EV-ranked actionable trades
In-process SSE                   →     Redis pub/sub + WebSocket
No user model                    →     Auth + bankroll + portfolio tracking
Manual parameter tuning          →     Auto-calibration + adaptive weights
APScheduler (single process)     →     Celery workers for strategy execution
```

---

## Principles

1. **P&L is the only metric that matters.** Every feature is evaluated by its contribution to trader profitability. Vanity metrics (users, page views, signal count) are noise.

2. **Measure before you build.** No new strategy ships without a backtest. No backtest ships without CLV validation. No CLV claim ships without out-of-sample testing.

3. **Kill what doesn't work.** A detector with negative CLV gets demoted, not "improved." Sunk cost is not a reason to keep a bad signal.

4. **Quarter-Kelly, always.** Overconfidence in probability estimates is the #1 killer of quantitative strategies. Conservative sizing with proven edge beats aggressive sizing with uncertain edge.

5. **The market is usually right.** Our probability estimates start with the market price and make adjustments. A detector that consistently says "the market is wrong by 30%" is more likely to be broken than the market is to be wrong by 30%.

6. **Public track record or it didn't happen.** Every strategy's P&L is public, auditable, and starts counting from the day it goes live. No cherry-picked backtests, no hypothetical returns.
