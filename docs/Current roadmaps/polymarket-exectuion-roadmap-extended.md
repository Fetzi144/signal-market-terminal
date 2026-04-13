## Thesis

The right roadmap is not “add more detectors.” It is to turn SMT from a snapshot-based research console into an **event-time Polymarket execution stack**. Today the repo is still a FastAPI/Postgres research tool, not an auto-trader; it snapshots every 120 seconds, randomly samples only 50 orderbooks, stores coarse append-only price/orderbook snapshots, and computes shadow execution only after EV, Kelly, and risk approval. Polymarket, meanwhile, exposes a much richer surface: public CLOB market WebSocket data, authenticated user WebSocket updates, public trades and open interest, richer order types, tick-size changes, fee schedules, maker rebates, and negative-risk event metadata.     ([Polymarket Dokumentation][1])

My strong recommendation is to prioritize three platform-native edges in this order: first, **execution-quality edge** by moving from midpoint logic to executable-price logic; second, **structural edge** in negative-risk and event-linked baskets; third, **maker edge** by explicitly modeling fees, rebates, and liquidity incentives. The current default confluence strategy remains useful as a baseline, but it should stop being the center of the architecture.  ([Polymarket Dokumentation][2])

## Phase 0 — Correctness hardening and truth boundary

Do this first, before any new alpha work.

The repo currently loses too much temporal truth. `SignalCandidate` has no exchange-observation timestamp, and `persist_signals()` stamps `fired_at` using scheduler time rather than the actual observation time. The paper engine also computes EV, Kelly sizing, and risk first, and only then builds `shadow_execution`, which means “tradability” is still downstream analytics rather than a pre-trade gate.   

Build these changes immediately:

* Split time into `observed_at_exchange`, `received_at_local`, `detected_at`, `decided_at`, `submitted_at`, and `confirmed_at`.
* Replace `fired_at` as a catch-all with explicit semantics.
* Promote execution estimation to a first-class pre-trade step: compute executable price, fillable size, and net EV **before** Kelly and risk.
* Add canonical resolution events. Right now the connectors return flat resolved-market payloads, while `_resolve_paper_trades()` still expects per-outcome arrays; even if this happens to work elsewhere, the shape boundary is fragile and should be normalized into one internal format.   

Success gate for Phase 0: every signal and trade can be traced from exchange observation through decision and, later, actual order/fill lifecycle with unambiguous timestamps.

## Phase 1 — Build an event-time Polymarket data plane

This is the foundation. Without it, every later “strategy” improvement is partially blind.

Polymarket’s market channel gives you the live tape you are currently missing: `book`, `price_change`, `tick_size_change`, `last_trade_price`, `best_bid_ask`, `new_market`, and `market_resolved`. The orderbook endpoint also exposes `tick_size`, `min_order_size`, `neg_risk`, `hash`, and last-trade price, and the docs explicitly say to use WebSockets instead of polling for live data. The Data API adds public `/trades` and `/oi` endpoints. ([Polymarket Dokumentation][3])

Create four services.

First, a `polymarket_market_stream` service. It should subscribe to the market WebSocket for:

* every open position
* every hedge leg for open positions
* every market in the active signal watchlist
* every market in any targeted maker-incentive or negative-risk event group

Use dynamic subscribe/unsubscribe rather than reconnecting, because the market channel supports changing subscriptions live. ([Polymarket Dokumentation][4])

Second, a `polymarket_meta_sync` service. It should combine Gamma market/event discovery with `new_market` and `market_resolved` messages from the market WebSocket so you always have fresh metadata such as token IDs, tick sizes, fee schedules, tags, and negative-risk flags. The `new_market` payload already includes fields like `order_price_min_tick_size`, `fees_enabled`, and `fee_schedule`. ([Polymarket Dokumentation][5])

Third, a `polymarket_backfill` service. Use:

* `GET /book` or batch `POST /books` for resync and snapshot seeding,
* `GET /prices-history` only for coarse historical backfill and overnight recovery,
* Data API `/trades` for historical trade tape,
* Data API `/oi` for open-interest history.
  Do **not** treat `prices-history` as microstructure truth, because it is an aggregated time-series endpoint with interval and fidelity controls. ([Polymarket Dokumentation][4])

Fourth, once live trading begins, a `polymarket_user_stream` service. The user channel is server-side only and gives order and trade lifecycle events. That is the canonical live execution tape you need to reconcile what you thought you sent with what Polymarket actually matched, mined, confirmed, retried, or failed. ([Polymarket Dokumentation][6])

## Phase 2 — Storage redesign: raw, normalized, derived, transactional

Keep Postgres as the transactional system of record because the repo is already built around Postgres 16 and SQLAlchemy, but stop trying to make the current snapshot tables carry the whole research burden. Right now you only have append-only `price_snapshots` and `orderbook_snapshots` with coarse depth fields. That is not enough for replay-quality execution research.  

Use a four-layer storage model.

**Dimension layer**

* `pm_event_dim`
* `pm_market_dim`
* `pm_asset_dim`
* `pm_market_param_history`

Store immutable and slowly changing metadata here: event slug, condition ID, asset ID, outcome name, category, `negRisk`, `feesEnabled`, fee schedule, tick size, min order size, incentive parameters, resolution state.

**Raw event layer**

* `pm_ws_market_raw`
* `pm_ws_user_raw`
* `pm_book_snapshot`
* `pm_book_delta`
* `pm_bbo_event`
* `pm_trade_tape`
* `pm_tick_size_event`
* `pm_market_lifecycle_event`

Each row should carry `event_ts_exchange`, `recv_ts_local`, `ingest_ts_db`, `asset_id`, `condition_id`, `stream_session_id`, and any `hash` provided by Polymarket. Store raw payloads compressed and append-only so you can always replay the exact tape later.

**Derived research layer**

* `pm_book_state_topn`
* `pm_microstructure_features_100ms`
* `pm_microstructure_features_1s`
* `pm_alpha_labels`
* `pm_fill_estimates`
* `pm_execution_decisions`
* `pm_reward_estimates`
* `pm_constraint_violations`

This is where you compute queue imbalance, microprice, trade-sign flow, realized short-horizon returns, and fill/adverse-selection labels.

**Transactional live-trading layer**

* `live_orders`
* `live_order_events`
* `live_fills`
* `position_lots`
* `capital_reservations`
* `neg_risk_conversions`

This layer mirrors the actual order manager and should never be mixed with raw market-data storage.

For retention, keep hot raw data in Postgres for roughly 14–30 days, derived features for a few months, and archive raw events plus periodic full-book snapshots to Parquet in object storage for long-run research reproducibility. If the event volume becomes painful, add ClickHouse or another columnar store later; do not add it before the event-time capture is working.

## Phase 3 — Deterministic book reconstruction and replay-quality simulator

The simulator should stop pretending that midpoint and “half-spread shadow” are enough.

Polymarket’s live market feed gives you a clean basis for deterministic reconstruction: `book` snapshots when you subscribe, `price_change` deltas when orders are placed or cancelled, `last_trade_price` events when trades print, and `best_bid_ask` updates. The `book` and orderbook REST response include a `hash`, which is exactly what you should use for drift detection and resync. ([Polymarket Dokumentation][3])

Build the simulator like this:

* Maintain an in-memory L2 book per subscribed asset.
* Seed from `book` on subscribe.
* Apply `price_change` deltas in event order.
* Reconcile with `best_bid_ask` and `hash`.
* If drift appears or the stream reconnects, refresh from `/book` and continue.
* Persist periodic full-book snapshots every 1–5 minutes and on every resync; persist deltas in between.

Then replace the current execution model with a real one.

Polymarket supports GTC, GTD, FOK, FAK, and post-only behavior. Post-only orders that would cross are rejected. Tick sizes can change dynamically, especially when price moves above 0.96 or below 0.04, and using stale tick rules causes rejections. Insert responses may also come back `live`, `matched`, `delayed`, or `unmatched`, while the user channel later advances actual trades through `MATCHED`, `MINED`, `CONFIRMED`, `RETRYING`, and `FAILED`. ([Polymarket Dokumentation][7])

So the simulator and OMS need to support:

* aggressive FOK/FAK book-walk fills
* passive GTC/GTD/post-only resting orders
* queue position at placement
* partial fill logic
* delayed insert states
* cancel/replace
* final mined/confirmed/failed outcomes

At that point, every trade decision should evaluate multiple actions:

* cross now
* post at best price
* step ahead by one tick
* wait or skip

and choose the action with the highest **net executable EV**, not the highest midpoint EV.

## Phase 4 — Fee-aware, rebate-aware, and reward-aware execution economics

You are currently leaving a Polymarket-native edge on the table.

Polymarket’s fee model is not flat. Taker fees apply only on fee-enabled markets, with a fee formula `C × feeRate × p × (1 - p)`. Makers pay no fees. Some categories also pay maker rebates, and fee-enabled markets expose `feesEnabled`; the fee-rate endpoint can be queried per token, while the official SDKs automatically pull the fee rate into the signed order payload. ([Polymarket Dokumentation][2])

Separate from maker rebates, Polymarket also runs liquidity-incentive programs for passive quoting. The docs specify `min_incentive_size`, `max_incentive_spread`, a two-sided scoring framework, and minute-level random sampling for reward calculations. That means maker edge on Polymarket is not just spread capture; it is spread capture plus expected reward minus adverse selection and inventory cost. ([Polymarket Dokumentation][8])

This deserves its own strategy family.

Build a `maker_economics_engine` that computes:

* expected spread capture
* expected taker-fee avoidance
* expected maker rebate
* expected liquidity reward
* fill probability
* expected adverse selection after fill
* inventory penalty

Then produce a net maker EV for each passive quote candidate.

This is especially valuable because the current repo’s default strategy ignores these economics entirely, even though Polymarket has now made them structurally relevant on a meaningful subset of markets.  ([Polymarket Dokumentation][2])

## Phase 5 — Structural edge engine: negative risk, complements, and event baskets

This is where I think the biggest durable edge probably sits.

Polymarket’s negative-risk markets let a No share in one outcome convert into Yes shares in all other outcomes within the event, making the structure capital-efficient. Augmented negative-risk events add placeholders and an explicit “Other,” and the docs explicitly say to trade only named outcomes and to avoid placeholder outcomes until named. ([Polymarket Dokumentation][9])

The repo’s current “arbitrage” detector is still just a one-sided cross-platform cheap-leg signal. It is not a full paired structural engine. 

So build a separate `structure` module with three engines.

**A. Negative-risk basket engine**
Represent event-level payoff constraints and conversion paths explicitly. Price:

* direct Yes
* direct No
* converted No-to-Yes basket equivalents
* event-wide implied probability vectors

Look for violations between direct and conversion-implied prices after fees and executable slippage.

**B. Complement and parity engine**
For plain binary markets, enforce complement and basket consistency across related markets. Build constraint graphs at the event level, not just at the market level.

**C. Cross-venue hedgeable basis engine**
Compare Polymarket versus Kalshi or other venues only when both legs are executable with acceptable depth, latency, and fee profile. Stop calling one-sided cheap-leg longs “arbitrage.”

This structural engine should become a first-class strategy path with its own run IDs, replay framework, and review console.

## Phase 6 — Microstructure modeling roadmap

Once the event-time tape exists, build the models in this order.

First, a **queue imbalance baseline**. The literature is clear that bid/ask queue imbalance has significant predictive power for the direction of the next mid-price movement, and simple logistic models already outperform a null benchmark, especially in large-tick settings. ([SSRN][10])

Second, a **microprice model**. Recent work on high-resolution microprice estimation argues that future short-horizon price can be estimated more robustly by combining spread, best-bid/ask imbalance, and higher-rank orderbook imbalances rather than relying only on top-of-book. ([IDEAS/RePEc][11])

Third, a **passive-fill hazard model**. Predict the chance a post-only or resting quote gets filled within 1s, 5s, 30s, and before alpha decay. Features should include:

* queue ahead
* queue behind
* recent cancellations ahead
* recent same-side additions
* last trade sign and size
* spread
* tick regime
* time to resolution
* market category
* fee/reward flags

Fourth, an **alpha half-life model**. Every signal should carry an estimate of how quickly its edge decays. If the half-life is 300 ms, you should never post passively. If it is 20 seconds, posting may dominate crossing.

Fifth, an **action-policy model**. This chooses among cross now, FAK, FOK, post best bid/ask, step ahead, or skip. This is the first model that should be directly optimized on **net executable P&L**, not accuracy.

Only after these baselines are strong should you explore more complex models such as Hawkes-style flow clustering or deep LOB models.

## Phase 7 — Inventory-aware and quote-skewing logic

If you want to make markets intelligently rather than just cross spreads, the right theoretical starting point is inventory-aware market making.

Avellaneda and Stoikov frame market making as choosing bid/ask quotes around a reservation price that shifts with inventory and execution intensity. Fodra and Labadie extend that idea with directional views and non-symmetric quoting, allowing quotes to lean with expected drift while still controlling inventory risk. ([ResearchGate][12])

So implement:

* reservation-price adjustment
* inventory penalties by event graph, not by keyword overlap
* directional quote skew when microprice and structural signals align
* separate maker and taker inventory budgets
* explicit no-quote zones in toxic conditions

This should replace the repo’s current keyword-overlap cluster heuristic as the core live inventory logic. 

## Phase 8 — Live OMS/EMS and control plane

Only after the simulator and economic models are credible should you turn on live execution.

Polymarket trading requires L1/L2 authentication, signed orders, server-side user-channel use, and balance/allowance checks. The docs also note per-market order placement limits tied to available balance and open reserved order size. ([Polymarket Dokumentation][13])

The live control plane should therefore include:

* `execution_gateway` using the official SDK
* `order_manager` with idempotency, cancel/replace, and retry rules
* `capital_reservation_service` that mirrors Polymarket’s per-market reservation rules
* `fill_reconciler` that trusts user-channel and REST order/trade queries
* emergency kill switch
* market/category allowlists
* maximum daily loss and maximum outstanding notional

Rollout should go:
manual approval -> semi-automatic on one narrow market family -> fully automatic only after the live-shadow gap is acceptably small.

## Phase 9 — Risk graph and portfolio optimizer

The current risk layer groups exposure via keyword overlap in market questions. That is too weak for Polymarket, where exposure is often driven by event structure, complements, negative-risk conversion, and cross-platform linkage rather than wording similarity. 

Replace it with a graph.

Nodes:

* event
* market / condition ID
* asset / token ID
* venue
* named entity
* negative-risk event group

Edges:

* complement
* same-event
* conversion-equivalent
* hedgeable cross-venue pair
* historical return covariance
* common underlying entity

Then optimize portfolio decisions on:

* marginal executable EV
* marginal inventory risk
* fill quality
* capital reservation cost
* hedge completeness

## What not to build yet

Do **not** spend the next month adding more detectors, UI polish, or broad SaaS features.

The current repo still says the objective is to prove whether the frozen strategy has real edge, and that is still the right mentality. But “prove edge” now needs to move one layer down: from detector truth to **execution truth**.  

## A concrete 90-day sequence

Weeks 1–2: Phase 0.
Fix timestamps, canonical resolution events, executable pre-trade gate, and deterministic candidate-market orderbook capture.

Weeks 3–5: Phases 1–2.
Launch market and metadata streams, raw event storage, book reconstruction, and replay harness.

Weeks 6–8: Phase 4 plus Phase 6 baselines.
Add fee/rebate/reward economics, then build queue-imbalance, microprice, and fill-hazard models.

Weeks 9–11: Phase 5.
Build the negative-risk and structural basket engine.

Weeks 12–13: Phases 7–8.
Add inventory-aware quote skew, OMS/EMS, and user-channel reconciliation.

Weeks 14–15: Pilot.
Run one narrow live strategy family:

* either reward-aware maker quoting in an incentive-rich category
* or structural negative-risk baskets
* or execution-aware directional confluence if it still dominates after the earlier work

## The roadmap’s payoff

If you follow this roadmap in order, SMT will stop answering only “was the signal smart?” and start answering the much more valuable questions:

* Could I actually get filled?
* Was crossing or posting better?
* Did fees or rebates change the trade from bad to good?
* Is the real moat directional alpha, maker economics, or structural conversion/arbitrage?
* Which Polymarket-native mechanisms deserve capital?

That is the roadmap I would trust to find the biggest edge.

The clean next step is for me to turn this into a build sheet with specific DB migrations, new services, and file/module changes for your repo.

[1]: https://docs.polymarket.com/market-data/overview "https://docs.polymarket.com/market-data/overview"
[2]: https://docs.polymarket.com/trading/fees "https://docs.polymarket.com/trading/fees"
[3]: https://docs.polymarket.com/market-data/websocket/market-channel "https://docs.polymarket.com/market-data/websocket/market-channel"
[4]: https://docs.polymarket.com/trading/orderbook "https://docs.polymarket.com/trading/orderbook"
[5]: https://docs.polymarket.com/developers/CLOB/introduction "https://docs.polymarket.com/developers/CLOB/introduction"
[6]: https://docs.polymarket.com/market-data/websocket/user-channel "https://docs.polymarket.com/market-data/websocket/user-channel"
[7]: https://docs.polymarket.com/developers/CLOB/orders/orders "https://docs.polymarket.com/developers/CLOB/orders/orders"
[8]: https://docs.polymarket.com/market-makers/liquidity-rewards "https://docs.polymarket.com/market-makers/liquidity-rewards"
[9]: https://docs.polymarket.com/developers/neg-risk/overview "https://docs.polymarket.com/developers/neg-risk/overview"
[10]: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2702117 "Queue Imbalance as a One-Tick-Ahead Price Predictor in a Limit Order Book by Martin Gould, Julius Bonart :: SSRN"
[11]: https://ideas.repec.org/p/arx/papers/2411.13594.html "High resolution microprice estimates from limit orderbook data using hyperdimensional vector Tsetlin Machines"
[12]: https://www.researchgate.net/publication/24086205_High_Frequency_Trading_in_a_Limit_Order_Book "(PDF) High Frequency Trading in a Limit Order Book"
[13]: https://docs.polymarket.com/developers/CLOB/trades/trades-data-api "https://docs.polymarket.com/developers/CLOB/trades/trades-data-api"
