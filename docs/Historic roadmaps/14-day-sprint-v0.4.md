# Signal Market Terminal — 14-Day Sprint Plan

*Based on v0.3.0 codebase audit. Target: v0.4.0.*
*Written for Claude Code agents to execute autonomously.*

---

## Sprint Vision

v0.3.0 delivered a solid signal detection + monitoring platform. v0.4.0 transforms it into a **decision-support system** — one that doesn't just detect signals, but proves their value, learns from history, and gives traders the confidence to act. Every feature in this sprint answers the question: *"Should I trade on this signal?"*

---

## Sprint Goals

1. **Signal Backtesting Engine** — prove signal value with historical data (Days 1–3)
2. **Performance Dashboard** — make signal quality visible at a glance (Day 4)
3. **Order Flow Imbalance Detection** — add the first microstructure signal (Day 5)
4. **Portfolio Position Tracker** — track what the user actually holds (Days 6–7)
5. **Alerting Upgrades** — Discord bot + push notification support (Day 8)
6. **Whale/Smart Money Tracking** — surface large-position movements (Day 9)
7. **Multi-Timeframe Analysis** — signals across different time horizons (Day 10)
8. **ML Signal Scoring** — learn which signals actually predict outcomes (Days 11–12)
9. **Mobile-Optimized Frontend** — make it usable on a phone (Day 13)
10. **Integration Testing + Release** — ship v0.4.0 (Day 14)

---

## Priority Rationale (Nutzer-Impact)

| Rank | Feature | Why This Order |
|------|---------|---------------|
| 1 | Backtesting Engine | **Highest operator value.** Without this, signal thresholds are set by intuition. With it, users can empirically prove "rank ≥ 0.7 with 6% price move = 68% win rate." Builds on resolution tracking from v0.3.0. |
| 2 | Performance Dashboard | **Trust builder.** Users need to see at a glance: "Are these signals actually good?" Feeds directly off backtesting results. |
| 3 | Order Flow Imbalance | **Leading indicator.** OFI detects informed trading *before* price moves — prediction market alpha. Uses existing orderbook data. |
| 4 | Portfolio Tracker | **Closes the loop.** Users detect signals, take positions, but then have no way to track P&L inside the tool. Forces them to use spreadsheets. |
| 5 | Alert Upgrades | **Reach.** Discord is where crypto/prediction market communities live. Push notifications make mobile useful. |
| 6 | Whale Tracking | **Unique differentiator.** No public tool tracks large Polymarket wallets. Requires on-chain data. |
| 7 | Multi-Timeframe | **Depth.** Same signal at 1h and 24h means something different. Adds nuance to existing detectors. |
| 8 | ML Signal Scoring | **Accuracy boost.** Replace hand-tuned rank formula with learned weights. Needs sufficient resolution data to train. |
| 9 | Mobile Frontend | **Accessibility.** Prediction market traders check positions on mobile constantly. |
| 10 | Release | **Quality gate.** Everything tested, documented, tagged. |

---

## Dependency Graph

```
Resolution Tracking (v0.3.0, done)
  │
  ├──▶ Backtesting Engine (Days 1–3)
  │      │
  │      └──▶ Performance Dashboard (Day 4)
  │             │
  │             └──▶ ML Signal Scoring (Days 11–12)
  │
  ├──▶ Portfolio Tracker (Days 6–7) [independent]
  │
  └──▶ Multi-Timeframe Analysis (Day 10) [independent]

Orderbook Snapshots (existing)
  │
  └──▶ Order Flow Imbalance (Day 5) [independent]

Polymarket CLOB / On-chain (external)
  │
  └──▶ Whale Tracking (Day 9) [independent]

Alert System (existing)
  │
  └──▶ Discord + Push (Day 8) [independent]

All Features
  │
  └──▶ Mobile Frontend (Day 13) [depends on all UI work]
        │
        └──▶ Release v0.4.0 (Day 14)
```

---

## Day 1 — Backtesting Engine: Core Replay Infrastructure

**Effort: High (full day) | Depends on: v0.3.0 resolution tracking**

### Context
The backtesting engine replays historical price snapshots through the detector suite with user-configurable parameters. This is the single feature that transforms the tool from "interesting dashboard" into "trading decision support."

### Tasks

#### Schema: Backtest models (`app/models/backtest.py` — new file)
- `BacktestRun` table:
  - `id` (UUID), `name` (user label), `created_at`
  - `start_date`, `end_date` — date range of historical data to replay
  - `detector_configs` (JSONB) — parameter overrides: `{"price_move": {"threshold_pct": 0.06}, "volume_spike": {"multiplier": 4.0}}`
  - `rank_threshold` (float) — minimum rank_score to count as "would have traded"
  - `status` (enum: pending, running, completed, failed)
  - `started_at`, `completed_at`
  - `result_summary` (JSONB) — aggregated stats stored after run completes
- `BacktestSignal` table:
  - `id`, `backtest_run_id` (FK), `signal_type`, `outcome_id` (FK), `fired_at`
  - `signal_score`, `confidence`, `rank_score`
  - `resolved_correctly` (bool, nullable) — did this hypothetical signal call the outcome correctly?
  - `price_at_fire`, `price_at_resolution`
  - `details` (JSONB)
- Write Alembic migration

#### Replay engine (`app/backtesting/engine.py` — new file)
- `class BacktestEngine:`
  - `async def run(session, backtest_run: BacktestRun) -> BacktestResult:`
    - Load all `PriceSnapshot` rows in `[start_date, end_date]`
    - For each 2-minute window (matching production detection interval):
      - Instantiate detectors with overridden config from `detector_configs`
      - Feed the snapshot window to each detector
      - Rank candidates, apply dedupe logic
      - Filter by `rank_threshold`
      - For each signal that passes: look up whether the market resolved, compute `resolved_correctly`
      - Persist as `BacktestSignal` rows
    - Compute summary: win_rate, total_signals, signals_per_day, accuracy_by_type, accuracy_by_horizon, false_positive_rate
    - Store summary in `backtest_run.result_summary`
    - Mark `status = completed`

#### Detector interface: add replay mode
- Modify `BaseDetector` to accept an optional `snapshot_window` parameter
- When `snapshot_window` is provided, detectors query from it instead of the live DB
- This is a minimal change: each detector's `detect()` method gets an optional `snapshots: list[PriceSnapshot] = None` parameter
- If `snapshots` is provided, use those instead of querying `session`

### Deliverables
- `app/models/backtest.py` with `BacktestRun` + `BacktestSignal`
- Alembic migration for backtest tables
- `app/backtesting/engine.py` with replay logic
- `BaseDetector` supports snapshot injection

---

## Day 2 — Backtesting Engine: API + Parameter Sweep

**Effort: High (full day) | Depends on: Day 1**

### Tasks

#### Backtest API (`app/api/backtest.py` — new file)
- `POST /api/v1/backtests` — create + start a backtest run
  - Request body: `{name, start_date, end_date, detector_configs, rank_threshold}`
  - Validates date range (max 180 days, end_date ≤ now, start_date must have snapshot data)
  - Kicks off `BacktestEngine.run()` as a background task (`BackgroundTasks`)
  - Returns `201` with `backtest_run_id` and `status: "pending"`
- `GET /api/v1/backtests` — list all runs with status, sorted by `created_at` desc
- `GET /api/v1/backtests/{id}` — single run with full `result_summary`
- `GET /api/v1/backtests/{id}/signals` — paginated list of hypothetical signals from that run
  - Filters: `signal_type`, `resolved_correctly`
- `DELETE /api/v1/backtests/{id}` — delete run + all its signals

#### Parameter sweep (`app/backtesting/sweep.py` — new file)
- `async def parameter_sweep(session, base_config: dict, sweep_params: dict) -> list[BacktestRun]:`
  - `sweep_params` example: `{"price_move.threshold_pct": [0.03, 0.05, 0.07, 0.10], "rank_threshold": [0.5, 0.6, 0.7, 0.8]}`
  - Generates cartesian product of all parameter combinations
  - Runs each combination as a separate `BacktestRun` (capped at 50 combinations)
  - Returns all runs for comparison
- `POST /api/v1/backtests/sweep` endpoint
  - Body: `{name_prefix, start_date, end_date, sweep_params}`
  - Returns list of created `backtest_run_id`s

### Tests to Write
- `tests/test_backtest.py`:
  - Engine produces signals from historical snapshots
  - Win rate calculation matches manual count
  - Parameter override changes detector behavior (higher threshold → fewer signals)
  - Date range validation rejects future dates
  - Empty date range (no snapshots) → completed with 0 signals
  - Sweep generates correct number of combinations (capped at 50)

### Deliverables
- Full backtest REST API (CRUD + sweep)
- Parameter sweep with cartesian product
- 8+ tests covering engine + API

---

## Day 3 — Backtesting Engine: Frontend

**Effort: Medium-High | Depends on: Day 2**

### Tasks

#### Backtest page (`frontend/src/pages/Backtest.jsx` — new file)
- **Create backtest form:**
  - Date range picker (start/end)
  - Detector parameter sliders with current defaults shown
  - Rank threshold slider (0.0–1.0, step 0.05)
  - "Run Backtest" button → `POST /api/v1/backtests`
  - "Parameter Sweep" toggle that reveals sweep range inputs
- **Backtest list:**
  - Table of all runs: name, date range, status, win rate, signal count, created_at
  - Status badge: pending (spinner), running (progress), completed (green), failed (red)
  - Click to expand → full result summary

#### Backtest result view (`frontend/src/pages/BacktestResult.jsx` — new file)
- **Summary cards:** Win Rate, Total Signals, Signals/Day, False Positive Rate
- **Accuracy by detector type:** Bar chart (Recharts) showing win rate per signal_type
- **Accuracy by horizon:** Grouped bar chart showing win rate at 15m/1h/4h/24h
- **Signal timeline:** Scatter plot of signals over time, colored by resolved_correctly (green/red/grey)
- **Sweep comparison table** (when viewing a sweep):
  - Each row = one parameter combination
  - Columns: parameters used, win rate, signal count, false positive rate
  - Sortable by any column
  - Highlight the best-performing parameter set

#### Navigation
- Add "Backtest" to the navigation bar (between Analytics and Alerts)

### Deliverables
- `Backtest.jsx` — create + list backtests
- `BacktestResult.jsx` — result visualization with charts
- Sweep comparison table
- Navigation updated

---

## Day 4 — Performance Dashboard

**Effort: Medium | Depends on: Day 3 (backtesting data), v0.3.0 resolution data**

### Context
The current Analytics page shows raw accuracy numbers. The Performance Dashboard synthesizes backtesting results + live resolution data into an at-a-glance view that answers: "How good are my signals right now?"

### Tasks

#### Backend: Performance metrics API (`app/api/performance.py` — new file)
- `GET /api/v1/performance/summary` — returns:
  - `overall_win_rate` — % of resolved signals with `resolved_correctly = True` (last 30 days)
  - `win_rate_by_type` — breakdown per signal_type
  - `win_rate_trend` — daily win rate for the last 30 days (for trend chart)
  - `best_detector` — signal type with highest win rate (min 10 resolved signals)
  - `worst_detector` — signal type with lowest win rate
  - `avg_rank_of_winners` — average rank_score of correctly-resolved signals
  - `avg_rank_of_losers` — average rank_score of incorrectly-resolved signals
  - `optimal_threshold` — rank_score cutoff that maximizes win rate (derived from backtesting or live data)
  - `signals_pending_resolution` — count of unresolved signals
  - `total_markets_resolved` — count of markets that have settled

#### Frontend: Performance page (`frontend/src/pages/Performance.jsx` — new file)
- **Hero metrics:** Win Rate (large %), Signals Fired (count), Markets Resolved (count)
- **Win rate trend chart:** Line chart (Recharts) — daily win rate over 30 days with moving average
- **Detector leaderboard:** Ranked table of signal types by win rate, with confidence interval bars
- **Rank score distribution:** Histogram of rank scores for winners vs losers (overlapping)
- **Threshold optimizer:** Visual showing win rate at different rank_score cutoffs (from backtest data or live). "Sweet spot" highlighted.
- **Recent calls:** Last 20 resolved signals with green/red badges

#### Navigation
- Add "Performance" tab (between Feed and Markets — this is the second most important page)

### Tests to Write
- `tests/test_performance.py`:
  - Win rate calculation with mixed resolved/unresolved signals
  - Trend data returns 30 data points
  - Best/worst detector with ties
  - No resolved signals → graceful empty response

### Deliverables
- `app/api/performance.py` with metrics endpoint
- `Performance.jsx` with charts and leaderboard
- 5+ tests

---

## Day 5 — Order Flow Imbalance (OFI) Detection

**Effort: Medium | Depends on: existing orderbook snapshots**

### Context
Orderbook snapshots are already captured every 2 minutes. OFI measures the imbalance between buy-side and sell-side pressure in the order book. A strong buy-side imbalance on a flat-price market is a leading indicator that price is about to move up. This is standard equity microstructure analysis, never applied publicly to prediction markets.

### Tasks

#### New detector: `app/signals/order_flow.py` — new file
- Class `OrderFlowImbalanceDetector(BaseDetector)`
- `detect(session, snapshots=None) -> list[SignalCandidate]:`
  - For each active outcome with >= 3 recent orderbook snapshots:
    - Calculate OFI: `ofi = (bid_depth_change - ask_depth_change) / (bid_depth_change + ask_depth_change)`
    - Use `depth_bid_10pct` and `depth_ask_10pct` fields from `OrderbookSnapshot`
    - Positive OFI = buy pressure increasing, negative = sell pressure increasing
    - If `abs(ofi) >= settings.ofi_threshold` (default: 0.3) AND price has NOT already moved (flat in last 30min):
      - Create `SignalCandidate` with:
        - `signal_type = "order_flow_imbalance"`
        - `signal_score = min(abs(ofi) / 0.6, 1.0)` — OFI of 0.6 = max score
        - `confidence` penalized by low total depth (thin books are noisy)
        - `direction = "up"` if ofi > 0, `"down"` if ofi < 0
        - `metadata = {ofi_value, bid_depth_current, ask_depth_current, bid_depth_previous, ask_depth_previous, price_current}`
- Register in `app/signals/__init__.py`

#### Config
- `ofi_threshold: float = 0.3` — minimum abs(OFI) to fire (validator: > 0, < 1)
- `ofi_enabled: bool = True`
- `ofi_min_snapshots: int = 3` — minimum orderbook snapshots needed
- `ofi_price_flat_window_minutes: int = 30` — price must be flat in this window for OFI to fire

#### Frontend: OFI signal display
- In `SignalDetail.jsx`, when `signal_type == "order_flow_imbalance"`:
  - Show OFI value as a horizontal bar (red left = sell pressure, green right = buy pressure)
  - Show bid vs ask depth comparison
  - Show "Price flat — OFI suggests move incoming" message

### Tests to Write
- `tests/test_order_flow.py`:
  - Strong buy-side imbalance → signal with direction "up"
  - Strong sell-side imbalance → signal with direction "down"
  - Balanced book (OFI near 0) → no signal
  - Price already moved → no signal (not a leading indicator if price caught up)
  - Thin order book → confidence penalty
  - Insufficient snapshots → no signal

### Deliverables
- `app/signals/order_flow.py` — 7th signal type
- OFI config settings
- SignalDetail OFI visualization
- 6+ tests

---

## Day 6 — Portfolio Position Tracker (Backend)

**Effort: High (full day) | Independent — no feature dependencies**

### Context
Users detect signals, decide to trade, but then have no way to track their positions inside Signal Market Terminal. They resort to spreadsheets. Adding position tracking closes the feedback loop: Signal → Trade → Position → P&L → "Was the signal worth it?"

### Tasks

#### Schema: Portfolio models (`app/models/portfolio.py` — new file)
- `Position` table:
  - `id` (UUID), `created_at`, `updated_at`
  - `market_id` (FK → Market), `outcome_id` (FK → Outcome)
  - `platform` (str) — which platform the position is on
  - `side` (enum: "yes", "no") — which side of the binary outcome
  - `quantity` (float) — number of shares/contracts
  - `avg_entry_price` (float) — weighted average entry price
  - `current_price` (float, nullable) — latest price from snapshots
  - `unrealized_pnl` (float, nullable) — `(current_price - avg_entry_price) * quantity`
  - `status` (enum: "open", "closed", "resolved")
  - `exit_price` (float, nullable) — price when closed
  - `realized_pnl` (float, nullable) — `(exit_price - avg_entry_price) * quantity`
  - `notes` (text, nullable) — user annotations
  - `signal_id` (FK → Signal, nullable) — which signal triggered this trade (if any)
- `Trade` table (append-only ledger):
  - `id` (UUID), `position_id` (FK), `created_at`
  - `action` (enum: "buy", "sell")
  - `quantity` (float), `price` (float)
  - `fees` (float, default 0)
- Alembic migration

#### Portfolio service (`app/portfolio/service.py` — new file)
- `async def open_position(session, market_id, outcome_id, platform, side, quantity, price, signal_id=None) -> Position`
- `async def add_to_position(session, position_id, quantity, price) -> Position` — updates avg_entry_price
- `async def close_position(session, position_id, quantity, price) -> Position` — partial or full close
- `async def update_current_prices(session) -> int` — refresh `current_price` and `unrealized_pnl` from latest snapshots
- `async def resolve_positions(session) -> int` — when a market resolves, close all open positions at resolution price ($1 or $0)

#### Scheduled job: Price refresh
- Add `update_portfolio_prices_job()` to scheduler
- Runs every 5 minutes
- Calls `update_current_prices()` to refresh unrealized P&L

#### API (`app/api/portfolio.py` — new file)
- `POST /api/v1/positions` — open a new position
- `GET /api/v1/positions` — list positions (filters: status, platform, market_id)
- `GET /api/v1/positions/{id}` — position detail with trade history
- `POST /api/v1/positions/{id}/trades` — add trade to existing position (buy more / partial sell)
- `PUT /api/v1/positions/{id}/close` — close position at given price
- `GET /api/v1/portfolio/summary` — aggregate: total unrealized P&L, realized P&L, open position count, win rate (of closed positions)
- `GET /api/v1/portfolio/export/csv` — CSV export of all positions + trades

### Tests to Write
- `tests/test_portfolio.py`:
  - Open position → correct avg entry price
  - Add to position → weighted average recalculated
  - Partial close → remaining quantity correct, realized P&L correct
  - Full close → status = "closed", realized P&L computed
  - Market resolution → positions auto-closed at $1 or $0
  - Portfolio summary aggregation
  - CSV export format

### Deliverables
- `app/models/portfolio.py` with Position + Trade
- `app/portfolio/service.py` with CRUD + P&L logic
- `app/api/portfolio.py` with 7 endpoints
- Portfolio price refresh scheduler job
- Alembic migration
- 8+ tests

---

## Day 7 — Portfolio Position Tracker (Frontend)

**Effort: Medium-High | Depends on: Day 6**

### Tasks

#### Portfolio page (`frontend/src/pages/Portfolio.jsx` — new file)
- **Summary cards:** Total Unrealized P&L (green/red), Realized P&L, Open Positions, Win Rate
- **Open positions table:**
  - Columns: Market Question, Side, Qty, Avg Entry, Current Price, Unrealized P&L, Signal Source
  - Sortable by P&L, entry date, quantity
  - Row color: green for positive P&L, red for negative
  - "Close" button → opens close dialog with price input
- **Closed positions table:**
  - Columns: Market, Side, Qty, Entry, Exit, Realized P&L, Duration Held
  - Sortable by P&L, close date
- **Add position form:**
  - Market search (autocomplete from existing markets)
  - Side selector (Yes/No)
  - Quantity + Price inputs
  - Optional: link to a signal that triggered this trade
- **P&L chart:** Line chart of cumulative P&L over time (from trade history)

#### Signal-to-Position flow
- On `SignalDetail.jsx`, add "Track Position" button
  - Pre-fills the position form with market, outcome, and direction from the signal
  - Links the position to the signal via `signal_id`

#### Navigation
- Add "Portfolio" tab (after Performance)

### Deliverables
- `Portfolio.jsx` — full position management UI
- Signal-to-Position quick-add flow
- P&L chart
- Navigation updated

---

## Day 8 — Alerting Upgrades: Discord Bot + Push Notifications

**Effort: Medium | Independent — no feature dependencies**

### Context
Telegram is configured but Discord is where most prediction market communities live. Push notifications make mobile usage viable without keeping the browser open.

### Tasks

#### Discord alerter (`app/alerts/discord_alert.py` — new file)
- Class `DiscordAlerter(BaseAlerter)`
- Uses Discord webhook URL (no bot token needed for webhooks)
- Format: Discord embed with:
  - Color: green (rank > 0.8), yellow (0.6–0.8), red (< 0.6)
  - Title: signal type + direction
  - Fields: market question, rank score, confidence, platform, current price
  - Footer: "Signal Market Terminal" + timestamp
  - For arbitrage signals: show both platform prices + spread
- Config: `alert_discord_webhook_url: str = ""`

#### Push notification support (`app/alerts/push_alert.py` — new file)
- Class `PushAlerter(BaseAlerter)`
- Uses Web Push protocol (VAPID keys)
- Config: `push_vapid_private_key`, `push_vapid_public_key`, `push_vapid_email`
- Store push subscriptions: `PushSubscription` table (`endpoint`, `keys`, `created_at`)
- API endpoint: `POST /api/v1/push/subscribe` — register a browser push subscription
- Payload: compact signal summary (title, body, icon, click URL)

#### Frontend: Push notification opt-in
- In `Health.jsx` or as a global banner: "Enable push notifications" button
- Calls `Notification.requestPermission()`, then subscribes via `POST /api/v1/push/subscribe`
- Shows notification status badge in nav

#### Config additions
- `alert_discord_webhook_url: str = ""` — Discord webhook URL
- `push_vapid_private_key: str = ""` — VAPID private key
- `push_vapid_public_key: str = ""` — VAPID public key
- `push_vapid_email: str = ""` — contact email for VAPID

### Tests to Write
- `tests/test_discord_alert.py`:
  - Formats embed correctly for each signal type
  - Arbitrage signal includes both platform prices
  - Empty webhook URL → alerter skipped
- `tests/test_push_alert.py`:
  - Subscription stored correctly
  - Push sent with correct payload structure

### Deliverables
- `app/alerts/discord_alert.py`
- `app/alerts/push_alert.py` + `PushSubscription` model
- Push subscribe API endpoint
- Frontend push opt-in
- 6+ tests

---

## Day 9 — Whale / Smart Money Tracking

**Effort: High (full day) | Independent — requires Polymarket on-chain data**

### Context
Polymarket runs on Polygon. Large wallets that consistently trade ahead of price moves represent "smart money." Tracking their position changes generates a unique signal type that no public tool currently offers.

### Tasks

#### On-chain data source
- Polymarket's Conditional Token Framework (CTF) contract on Polygon
- Use Polygonscan API or direct RPC to query:
  - Large `Transfer` events on CTF tokens (position changes > $10,000 notional)
  - Map token IDs to outcomes via Polymarket's token registry
- Alternative: Polymarket's activity API if available (check for public endpoints that expose large trades)

#### Whale tracker service (`app/tracking/whale_tracker.py` — new file)
- `WalletProfile` table:
  - `id`, `address` (Polygon address), `label` (optional human name)
  - `total_volume` (cumulative), `win_rate` (of resolved positions), `last_active`
  - `tracked` (bool) — manually flagged or auto-detected
- `WalletActivity` table:
  - `id`, `wallet_id` (FK), `outcome_id` (FK), `action` (buy/sell), `quantity`, `price`, `tx_hash`, `block_number`, `timestamp`
- Auto-detect whales: wallets with > $100k cumulative volume AND > 55% win rate on resolved markets
- `async def scan_recent_activity(session, hours: int = 1) -> list[WalletActivity]:`
  - Fetch large transfers from on-chain data source
  - Map to existing markets/outcomes
  - Persist activities
  - Return new activities for signal generation

#### New detector: `app/signals/smart_money.py` — new file
- Class `SmartMoneyDetector(BaseDetector)`
- Fires when a tracked whale wallet makes a significant entry (buy > $5,000) or exit
- `signal_type = "smart_money"`
- `signal_score` based on wallet's historical win rate
- `confidence` based on position size relative to market liquidity
- `metadata = {wallet_address, wallet_label, action, quantity, price, wallet_win_rate}`

#### Config
- `whale_tracking_enabled: bool = False` — off by default (requires Polygon RPC)
- `whale_min_volume_usd: float = 100000` — minimum cumulative volume to auto-track
- `whale_min_win_rate: float = 0.55` — minimum win rate to auto-track
- `whale_signal_min_trade_usd: float = 5000` — minimum trade size to fire signal
- `polygon_rpc_url: str = ""` — Polygon RPC endpoint

#### Scheduler
- `scan_whale_activity_job()` — runs every 5 minutes (only if `whale_tracking_enabled`)
- Feeds results to `SmartMoneyDetector`

#### Frontend
- In `SignalDetail.jsx` for `smart_money` signals:
  - Show wallet address (truncated with link to Polygonscan)
  - Show wallet's historical stats: volume, win rate, trade count
  - Show the specific trade: buy/sell, quantity, price

### Tests to Write
- `tests/test_whale_tracker.py`:
  - Large trade detected → wallet activity persisted
  - Wallet exceeds volume + win rate thresholds → auto-tracked
  - Tracked wallet buys > $5k → smart_money signal generated
  - Untracked wallet → no signal regardless of trade size
  - Duplicate tx_hash → idempotent

### Deliverables
- `app/tracking/whale_tracker.py` with on-chain scanning
- `app/signals/smart_money.py` — 8th signal type
- `WalletProfile` + `WalletActivity` tables + migration
- Whale config settings
- Scanner scheduler job
- 6+ tests

---

## Day 10 — Multi-Timeframe Analysis

**Effort: Medium | Depends on: existing detectors**

### Context
A price move signal on the 30-minute window means something very different from one on a 4-hour window. Currently all detectors operate on a single timeframe. Multi-timeframe analysis lets users see signal confluence across time horizons, which dramatically increases conviction.

### Tasks

#### Refactor: Configurable timeframes per detector
- Extend `BaseDetector` with `timeframes: list[str]` parameter (e.g., `["30m", "1h", "4h"]`)
- Each detector runs once per configured timeframe
- Add `timeframe` field to `Signal` model + migration
- Signals include their timeframe in metadata and deduplication: `(signal_type, outcome_id, timeframe, dedupe_bucket)`
- Config: `detector_timeframes: list[str] = ["30m"]` — default is current behavior

#### Multi-timeframe detectors
- **Price Move:** Check 30m, 1h, 4h windows
- **Volume Spike:** Check 1h, 4h, 24h baselines
- **OFI:** Check 15m, 30m, 1h orderbook changes
- Other detectors: keep single timeframe (arbitrage is inherently cross-timeframe, deadline is calendar-based)

#### Signal confluence scoring
- When the same signal type fires on the same outcome across multiple timeframes:
  - Create a `confluence_score` bonus: `confluence = 0.15 * (timeframe_count - 1)`
  - Add to rank_score (capped at 1.0)
  - Store `confluence_timeframes: list[str]` in signal metadata
- API: Add `?timeframe=4h` filter to signals endpoint

#### Frontend
- Signal Feed: show timeframe badge on each signal (e.g., "30m", "4h")
- Signal Feed: filter by timeframe
- Signal Detail: if confluence detected, show "Confirmed across 30m + 4h" badge
- Analytics: accuracy breakdown by timeframe

### Tests to Write
- `tests/test_multi_timeframe.py`:
  - Same detector, different timeframes → separate signals
  - Confluence across 2 timeframes → bonus applied
  - Confluence across 3 timeframes → higher bonus, capped at 1.0
  - Timeframe filter on API works correctly

### Deliverables
- `timeframe` field on Signal + migration
- Detectors run across multiple timeframes
- Confluence scoring
- Frontend timeframe badges + filter
- 6+ tests

---

## Day 11 — ML Signal Scoring (Part 1: Feature Engineering + Training)

**Effort: High | Depends on: Days 1–4 (needs resolution data + backtest results)**

### Context
The current rank formula `signal_score × confidence × recency_weight` is hand-tuned. With enough resolved signals, we can train a model to learn which features actually predict correct signals. Even a simple model (logistic regression or gradient boosted trees) will outperform hand-tuned weights.

### Tasks

#### Feature extraction (`app/ml/features.py` — new file)
- `def extract_features(signal: Signal, session) -> dict:`
  - **Signal features:** signal_score, confidence, signal_type (one-hot), direction
  - **Market features:** days_until_close, current_price (probability level), total_volume_24h, total_liquidity
  - **Temporal features:** hour_of_day, day_of_week, minutes_since_last_signal_on_same_market
  - **Orderbook features:** spread, bid_depth_10pct, ask_depth_10pct, ofi (if available)
  - **Cross-signal features:** other_signals_on_same_market_count (last 1h), confluence_timeframe_count
  - **Price context features:** probability_bucket (0-10%, 10-20%, ..., 90-100% — the outcome-prior idea from assessment)
- Total: ~20 features

#### Training pipeline (`app/ml/trainer.py` — new file)
- `class SignalScorer:`
  - `async def prepare_training_data(session, min_signals: int = 200) -> tuple[np.ndarray, np.ndarray]:`
    - Query all signals where `resolved_correctly IS NOT NULL`
    - Extract features for each
    - Labels: `1` if `resolved_correctly = True`, `0` otherwise
    - Return X, y arrays
  - `def train(X, y) -> sklearn.Pipeline:`
    - Train/test split (80/20, stratified)
    - Model: `GradientBoostingClassifier` (scikit-learn) with cross-validation
    - Evaluate: AUC-ROC, precision, recall, F1 on test set
    - Log metrics
    - Return fitted pipeline
  - `def save_model(pipeline, path: str):`
    - Serialize with joblib to `data/models/signal_scorer_v{timestamp}.joblib`
  - `def load_model(path: str) -> sklearn.Pipeline`

#### Dependencies
- Add `scikit-learn`, `joblib`, `numpy` to `requirements.txt`

#### Training job
- `POST /api/v1/ml/train` — trigger model training (admin-only if auth is configured)
  - Returns training metrics: AUC, precision, recall, feature importances
- Add optional scheduled job: retrain weekly (if `ml_auto_retrain: bool = False`)

### Tests to Write
- `tests/test_ml_features.py`:
  - Feature extraction produces expected number of features
  - One-hot encoding for signal_type is correct
  - Missing orderbook data → graceful fallback (0 values)
- `tests/test_ml_trainer.py`:
  - Training with synthetic data produces a valid pipeline
  - Model predictions are between 0 and 1
  - Feature importance extraction works

### Deliverables
- `app/ml/features.py` — 20-feature extraction
- `app/ml/trainer.py` — training pipeline with GBM
- Training API endpoint
- 6+ tests

---

## Day 12 — ML Signal Scoring (Part 2: Inference + Integration)

**Effort: Medium-High | Depends on: Day 11**

### Tasks

#### Inference integration (`app/ml/scorer.py` — new file)
- `class MLSignalScorer:`
  - Loads the latest trained model on startup
  - `async def score(signal: Signal, session) -> float:`
    - Extract features
    - Run through model → predicted probability of `resolved_correctly = True`
    - Return as `ml_score` (0.0–1.0)
  - Falls back to formula-based `rank_score` if no model is available

#### Integration into ranking
- In `app/ranking/scorer.py`:
  - After computing `rank_score`, also compute `ml_score` if model is available
  - Store both on the Signal: add `ml_score: float, nullable` column + migration
  - Config: `ml_scoring_enabled: bool = False` — when enabled, use `ml_score` instead of `rank_score` for alert thresholds and signal ordering
  - Config: `ml_score_blend: float = 0.0` — 0.0 = pure formula, 1.0 = pure ML, 0.5 = equal blend

#### API updates
- Add `ml_score` to signal response schema
- `GET /api/v1/signals?sort=ml_score` — sort by ML score
- `GET /api/v1/ml/status` — returns: model loaded (bool), model version, training date, test metrics, feature importances

#### Frontend: ML integration
- Signal Feed: show `ml_score` alongside `rank_score` when ML is enabled
- Signal Detail: show "ML Confidence" bar alongside rank score
- Performance page: add ML vs Formula comparison chart (win rate when using ML score vs formula)
- ML Status panel (in Health page): model version, last trained, test AUC, top 5 feature importances

### Tests to Write
- `tests/test_ml_scorer.py`:
  - ML score between 0 and 1
  - Fallback to formula when no model loaded
  - Blend calculation correct at 0.0, 0.5, 1.0

### Deliverables
- `app/ml/scorer.py` with inference
- `ml_score` column on Signal + migration
- Blend scoring in ranking
- ML status API endpoint
- Frontend ML indicators
- 4+ tests

---

## Day 13 — Mobile-Optimized Frontend

**Effort: Medium | Depends on: all UI work from Days 1–12**

### Context
Prediction market traders check positions and signals constantly on mobile. The current frontend is desktop-only CSS with no responsive breakpoints.

### Tasks

#### Responsive layout system
- Add CSS breakpoints: `@media (max-width: 768px)` and `@media (max-width: 480px)`
- Navigation: collapse to hamburger menu on mobile
- Tables: switch to card layout on narrow screens (stack columns vertically)
- Charts: reduce margins, make touch-friendly (larger tap targets on tooltips)

#### Priority pages for mobile
1. **Signal Feed** — most-used page
   - Cards instead of table rows on mobile
   - Swipe to dismiss/bookmark (stretch goal)
   - Large rank score badges
   - Pull-to-refresh gesture
2. **Portfolio** — P&L needs to be visible
   - Summary cards stack vertically
   - Position list as cards with P&L prominently displayed
   - Quick close action
3. **Performance** — win rate at a glance
   - Hero metric cards full-width
   - Charts responsive with aspect ratio preservation

#### PWA setup
- Add `manifest.json` with app name, icons, theme color
- Register service worker for offline shell caching (not data — data is always live)
- Add "Add to Home Screen" meta tags
- iOS: add `apple-touch-icon` and `apple-mobile-web-app-capable`

#### Touch interactions
- Increase all tap targets to minimum 44x44px (Apple HIG)
- Add `touch-action: manipulation` to prevent 300ms delay
- Disable double-tap zoom on interactive elements

### Tests to Write
- Manual testing checklist (no automated mobile tests):
  - [ ] Signal Feed renders as cards on 375px width
  - [ ] Navigation hamburger works
  - [ ] Charts resize correctly
  - [ ] Portfolio P&L visible without scrolling
  - [ ] PWA installs from Chrome mobile
  - [ ] Push notifications work on mobile Safari + Chrome

### Deliverables
- Responsive CSS for all pages
- Hamburger navigation
- Card layouts for mobile
- PWA manifest + service worker
- Touch-optimized tap targets

---

## Day 14 — Integration Testing, Polish, Release v0.4.0

**Effort: Medium | Depends on: all previous days**

### Tasks

#### End-to-end integration tests

**Backtest integration test (`tests/test_backtest_integration.py`):**
1. Seed DB with 30 days of price snapshots + resolved markets
2. Run backtest with default parameters
3. Assert signals generated, win rate computed, result_summary populated
4. Run parameter sweep with 2 parameters × 3 values = 6 combinations
5. Assert all 6 runs complete with different results

**Portfolio integration test (`tests/test_portfolio_integration.py`):**
1. Seed DB with active market + price snapshots
2. Open a position linked to a signal
3. Add a trade (buy more)
4. Run price refresh → assert unrealized P&L updated
5. Close position → assert realized P&L correct
6. Resolve market → assert remaining positions auto-closed

**ML integration test (`tests/test_ml_integration.py`):**
1. Seed DB with 300+ resolved signals
2. Run training → assert model saved, metrics returned
3. Score a new signal with ML → assert ml_score between 0 and 1
4. Assert blend scoring works correctly

#### Fix any accumulated bugs
- Run full test suite, fix failures
- Run ruff linter, fix any issues
- Check all new API endpoints respond correctly
- Verify SSE still works with new signal types

#### Update documentation
- `CHANGELOG.md` — v0.4.0 entry with all changes
- `README.md` — update feature list, add backtesting and portfolio sections
- Update API endpoint list in README

#### Version bump
- `frontend/package.json`: `"version": "0.4.0"`
- `backend/pyproject.toml`: `version = "0.4.0"`

#### Full test suite
```bash
cd backend
python -m pytest tests/ -v --tb=short
ruff check app/ tests/
```
- All tests pass
- Coverage ≥ 80% (up from 75%)
- No lint errors

#### Git tag
```bash
git tag -a v0.4.0 -m "Signal Market Terminal v0.4.0"
```

### Deliverables
- 3 integration test files
- All tests passing at ≥80% coverage
- CHANGELOG.md + README.md updated
- v0.4.0 tagged

---

## Sprint Summary

| Day | Focus | Effort | Key Deliverables |
|-----|-------|--------|-----------------|
| 1 | Backtesting: Core engine | High | BacktestRun/Signal models, replay engine, detector replay mode |
| 2 | Backtesting: API + sweep | High | CRUD API, parameter sweep, 8+ tests |
| 3 | Backtesting: Frontend | Med-High | Backtest page, result visualization, sweep comparison |
| 4 | Performance Dashboard | Medium | Win rate trends, detector leaderboard, threshold optimizer |
| 5 | Order Flow Imbalance | Medium | 7th signal type (OFI), orderbook imbalance detection |
| 6 | Portfolio: Backend | High | Position/Trade models, P&L service, 7 API endpoints |
| 7 | Portfolio: Frontend | Med-High | Position management UI, P&L chart, signal-to-position flow |
| 8 | Alert upgrades | Medium | Discord webhooks, Web Push notifications |
| 9 | Whale tracking | High | On-chain scanning, 8th signal type (smart_money) |
| 10 | Multi-timeframe | Medium | Configurable timeframes, confluence scoring |
| 11 | ML scoring: Training | High | Feature extraction, GBM training pipeline |
| 12 | ML scoring: Inference | Med-High | Inference integration, blend scoring, ML status UI |
| 13 | Mobile frontend | Medium | Responsive CSS, PWA, touch optimization |
| 14 | Release v0.4.0 | Medium | Integration tests, polish, version bump, tag |

## New Files Created

### Backend
- `app/models/backtest.py` — BacktestRun, BacktestSignal
- `app/models/portfolio.py` — Position, Trade
- `app/backtesting/engine.py` — Replay engine
- `app/backtesting/sweep.py` — Parameter sweep
- `app/api/backtest.py` — Backtest CRUD + sweep
- `app/api/performance.py` — Performance metrics
- `app/api/portfolio.py` — Position management
- `app/signals/order_flow.py` — OFI detector
- `app/signals/smart_money.py` — Whale signal detector
- `app/tracking/whale_tracker.py` — On-chain scanner
- `app/alerts/discord_alert.py` — Discord webhook alerter
- `app/alerts/push_alert.py` — Web Push alerter
- `app/portfolio/service.py` — Portfolio business logic
- `app/ml/features.py` — Feature extraction
- `app/ml/trainer.py` — Model training pipeline
- `app/ml/scorer.py` — Inference integration
- 6 Alembic migrations (backtest, portfolio, wallet, timeframe, ml_score, push_subscription)

### Frontend
- `frontend/src/pages/Backtest.jsx`
- `frontend/src/pages/BacktestResult.jsx`
- `frontend/src/pages/Performance.jsx`
- `frontend/src/pages/Portfolio.jsx`
- `frontend/public/manifest.json`
- `frontend/public/sw.js`

### Tests
- `tests/test_backtest.py`
- `tests/test_performance.py`
- `tests/test_order_flow.py`
- `tests/test_portfolio.py`
- `tests/test_discord_alert.py`
- `tests/test_push_alert.py`
- `tests/test_whale_tracker.py`
- `tests/test_multi_timeframe.py`
- `tests/test_ml_features.py`
- `tests/test_ml_trainer.py`
- `tests/test_ml_scorer.py`
- `tests/test_backtest_integration.py`
- `tests/test_portfolio_integration.py`
- `tests/test_ml_integration.py`

## Files Modified

- `app/signals/base.py` — replay mode, timeframe support
- `app/signals/__init__.py` — register OFI + smart_money detectors
- `app/models/__init__.py` — register new models
- `app/ranking/scorer.py` — ML score blend
- `app/config.py` — ~20 new settings
- `app/jobs/scheduler.py` — 3 new jobs (portfolio refresh, whale scan, ML retrain)
- `app/main.py` — new routers (backtest, performance, portfolio, ML)
- `app/alerts/__init__.py` — register Discord + Push alerters
- `frontend/src/App.jsx` — new routes
- `frontend/src/pages/SignalDetail.jsx` — OFI visualization, smart_money display
- `frontend/src/pages/SignalFeed.jsx` — timeframe badges, ML score
- `frontend/src/pages/Health.jsx` — ML status, push notification opt-in
- `frontend/src/pages/Analytics.jsx` — timeframe accuracy breakdown
- `frontend/src/index.css` — responsive breakpoints
- `frontend/index.html` — PWA meta tags
- `backend/requirements.txt` — scikit-learn, joblib, numpy, pywebpush
- `CHANGELOG.md` — v0.4.0 entry
- `README.md` — updated feature list
