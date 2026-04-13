# Market Scope Audit — Signal Market Terminal

**Date:** 2026-04-10  
**Health endpoint:** `GET /api/v1/health`  
**Findings:** We are covering ~8.5% of available Polymarket markets and ~1.5% of available Kalshi markets.

---

## 1. Current Coverage

| Platform   | Markets in DB | Last discovery run (markets_processed) |
|------------|:-------------:|:--------------------------------------:|
| Polymarket | 3,795         | 155 (last run, single pass)            |
| Kalshi     | 596           | included in above                      |
| **Total**  | **4,391**     |                                        |

Snapshot job processes **1,176 markets** per 2-minute cycle (active, high-signal subset of DB).

---

## 2. Real Platform Scale

| Platform   | Total active/open markets | Source                                    |
|------------|:-------------------------:|-------------------------------------------|
| Polymarket | **~51,904**               | Gamma API offset pagination (ceiling at ~51,904, `active=true&closed=false`) |
| Kalshi     | **~39,260**               | Trade API cursor pagination, `status=open&mve_filter=exclude` (40 full pages of 1,000) |

---

## 3. Filters Cutting Coverage

### 3.1 `market_pagination_cap: 5000` — the biggest bottleneck

Both connectors share this cap, set in `config.py:23`:

- **Polymarket** (`_paginate_offset`): caps at `offset >= 5000`, i.e. 50 pages × 100 = **5,000 markets scanned** out of ~51,904. We see only the first ~10% of the sorted result set.
- **Kalshi** (`_paginate_kalshi`): `pages >= (5000 // 200) = 25 pages` × 200 = **5,000 markets scanned** out of ~39,260. We see only the first ~13% of the sorted result set.

This single setting is responsible for the majority of missed coverage.

### 3.2 `min_volume_24h: 5000.0` (USD) — secondary filter

Applied in `ingestion/markets.py:80-81` and `ingestion/markets.py:99-100` to every market before upsert:

```python
if rm.volume_24h is not None and rm.volume_24h < settings.min_volume_24h:
    continue
```

- Of the ~5,000 Polymarket markets scanned, **3,795 pass** the $5k filter (~76%).
- Of the ~5,000 Kalshi markets scanned, **596 pass** the $5k filter (~12%). Kalshi markets tend to have much lower individual volume because they split events into many binary sub-markets (e.g. every CS2 match becomes 2 separate markets).

Note: markets with `volume_24h = None` are **always admitted** — the filter only triggers on non-null volume. This means zero-volume markets with missing data can slip through.

### 3.3 `mve_filter=exclude` in Kalshi connector — hard exclusion

Set in `connectors/kalshi.py:97`:
```python
"mve_filter": "exclude",  # exclude multivariate/parlay markets (mostly zero-volume)
```

This correctly excludes Kalshi's multivariate/parlay products, which are combination bets. Without the filter, the Kalshi API returns the same ~39k+ markets — they appear to be the same data (the unfiltered test with 5 pages also returned 1,000/page). The filter is safe to keep.

### 3.4 No category or geographic filters

Neither connector applies category, topic, or geographic exclusions. All market types (politics, sports, crypto, entertainment, science) are eligible if they pass the volume threshold.

---

## 4. Platforms NOT Covered

| Platform      | Type               | Real money? | Public API? | Why not covered                                           |
|---------------|--------------------|:-----------:|:-----------:|-----------------------------------------------------------|
| **PredictIt** | US political binary | Yes         | Yes (no auth) | Not implemented. `GET https://www.predictit.org/api/marketdata/all/` returns all ~300 active markets as one JSON blob. Easy to add. |
| **Smarkets**  | UK political/sports exchange | Yes | Yes (account required) | Not implemented. Excellent UK politics coverage. Requires OAuth. |
| **Betfair Exchange** | Sports/politics betting | Yes | Yes (account required) | Not implemented. Largest prediction exchange globally. High liquidity. Requires API key + account. |
| **Metaculus** | Forecasting aggregation | No (play)  | Yes (no auth) | No orderbook, no CLOB — community probability forecasts only. Not tradeable. Useful only for calibration/cross-reference. |
| **Manifold Markets** | Forecasting         | No (play $ ) | Yes         | Play money only, no real trading signals. |
| **Augur v2**  | Decentralized (Polygon) | Yes     | Yes (on-chain) | Very low liquidity, largely dormant as of 2026. Requires Polygon RPC integration. |

---

## 5. Recommendations for Maximum Scope

### Priority 1 — Remove the pagination cap (high impact, zero new code)

Change `market_pagination_cap` from `5000` to a much higher value (e.g. `100000`). This alone would expose all ~51,904 Polymarket and ~39,260 Kalshi markets to the volume filter.

**Concern:** Discovery runtime. At 100ms/request average with 100 markets/page, scanning 52k Polymarket markets = 520 requests ≈ ~52s per discovery cycle (currently runs every 300s). Kalshi at 1,000/page = 40 requests ≈ ~4s. Both fit within the 300s discovery interval.

**Concern:** DB size. Even at 100% admission (no volume filter), 90k markets × ~2 outcomes each = ~180k outcome rows. PostgreSQL handles this trivially.

### Priority 2 — Lower the volume threshold for Kalshi (medium impact)

Kalshi's per-market volumes are structurally lower because events are split into many binary sub-markets (e.g. 30 NBA game winners = 60 markets each with $200/day volume). Lowering `min_volume_24h` to `500` for Kalshi (or making the threshold platform-aware) would admit far more of the ~5,000 currently scanned Kalshi markets (from 596 → likely 2,000–3,000+).

### Priority 3 — Add PredictIt connector (low effort, real-money markets)

PredictIt has ~300 active real-money US political prediction markets. The public API endpoint returns all markets in a single call with no authentication:

```
GET https://www.predictit.org/api/marketdata/all/
```

Returns: `{ markets: [ { id, name, url, contracts: [ { id, name, bestBuyYesCost, bestBuyNoCost, ... } ] } ] }`

Effort estimate: ~200 lines for a new `PredictItConnector` following the existing pattern. No orderbook depth available (bid/ask only), but price move and volume spike signals work with this data.

### Priority 4 — Smarkets / Betfair (higher effort, highest liquidity)

Both require account registration and OAuth/API key setup. Betfair has the deepest orderbooks of any prediction market globally. Worth adding for arbitrage detection between Betfair and Polymarket/Kalshi. Effort: 1–2 days each including auth flow.

---

## 6. Summary Table

| Action                                   | Effort | Markets unlocked     | Signal quality impact       |
|------------------------------------------|--------|---------------------|-----------------------------|
| Raise `market_pagination_cap` to 100000 | 1 line | +~48k Polymarket, +~34k Kalshi | High — more markets = more signals |
| Lower Kalshi `min_volume_24h` to 500    | 1 line | +~1,500–2,500 Kalshi | Medium — more low-volume signals, more noise |
| Add PredictIt connector                 | ~1 day | +~300 markets       | Medium — US political depth  |
| Add Smarkets connector                  | ~2 days | +~500–1,000 markets | Medium — UK political depth  |
| Add Betfair connector                   | ~2 days | +~2,000 markets     | High — deep orderbooks, arb opportunities |

The single highest-leverage action is **raising `market_pagination_cap`** — it requires one config change and unlocks 10× the current market scope without any new code.
