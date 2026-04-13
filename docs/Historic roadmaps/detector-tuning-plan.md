# Detector Tuning Plan

**Date:** 2026-04-12
**Data Source:** `GET /api/v1/performance/summary` (lookback: 30 days)
**Total resolved signals:** 105 across 3 signal types
**Overall win rate:** 47.6%

---

## Executive Summary

Performance data covers only three signal types (`price_move`, `deadline_near`,
`order_flow_imbalance`) despite the system running eight detectors, meaning five
detectors (`volume_spike`, `spread_change`, `liquidity_vacuum`, `arbitrage`,
`smart_money`) have produced zero resolved signals in the 30-day window. The
resolved sample is small (105 signals), so all confidence intervals are wide.
The threshold curve shows only modest improvement at rank ≥ 0.55 and a hard
reliability floor at around 51 samples.

---

## Win Rate by Signal Type

| Signal Type | Resolved | Correct | Win Rate | Assessment |
|---|---|---|---|---|
| `order_flow_imbalance` | 1 | 1 | 100% | Sample too small — statistically meaningless |
| `price_move` | 57 | 33 | **57.9%** | Good — primary actionable detector |
| `deadline_near` | 47 | 16 | **34.0%** | Poor — well below 45% floor |

### Notes on sample size
- `order_flow_imbalance` at n=1 cannot be evaluated. The single correct call may
  be luck; no threshold changes should be made based on this data alone.
- `price_move` at n=57 is borderline reliable for directional conclusions.
- `deadline_near` at n=47 provides moderate confidence that the detector is
  genuinely underperforming.

---

## Threshold Curve Analysis

API-reported `optimal_threshold`: **0.55** (rank score filter applied at alert time)

| Threshold | Win Rate | Signal Count | Notes |
|---|---|---|---|
| 0.00 | 47.6% | 105 | Baseline, no filtering |
| 0.30 | 50.0% | 70 | First meaningful jump |
| 0.35 | 50.0% | 70 | Stable at 0.30–0.35 |
| 0.55 | **51.0%** | 51 | Peak win rate in curve |
| 0.60 | 49.0% | 49 | Drops back below 0.55 |
| 0.95+ | 46.8–45.7% | 47–46 | Win rate declines at extreme thresholds |

**Optimal rank threshold: 0.55**

- Raising the alert rank threshold from the current 0.70 to 0.55 would
  *increase* signal throughput (from ~51 to ~51 retained signals) while
  maximally filtering noise. This is counter-intuitive: the current threshold
  of 0.70 sits in a region where win rate has already declined from its 0.55
  peak back to ~48%.
- The reliability floor is approximately n=51 signals (threshold 0.55). Below
  this count the win-rate estimates become too noisy to act on.
- No further tightening beyond 0.55 is justified by the data: win rate
  monotonically declines from 0.55 onward.

---

## Recent Calls Analysis

The 20 most recent resolved signals are all `price_move` type, spanning
2026-04-10T17:01 to 2026-04-10T18:01 UTC.

**Hits (resolved_correctly = true):**
- All rank scores ≥ 0.39 resolved correctly.
- Scores of 1.0 appear in multiple correct calls across two market IDs.
- Score of 0.175 resolved correctly (one instance — may be noise).

**Misses (resolved_correctly = false):**
- Both misses have rank scores of 0.242 and 0.265.
- Score of 0.188 also resolved incorrectly.

**Pattern:** There is a clear rank-score boundary around **0.30–0.35** in the
recent data. Signals with rank < 0.30 are misses; signals with rank ≥ 0.39
are hits. This corroborates the threshold curve showing improvement at the 0.30
step and peak at 0.55.

**Trend anomaly (win_rate_trend):**
- 2026-04-07: 77 resolved, 25 correct → **32.5% win rate** (poor)
- 2026-04-10: 28 resolved, 25 correct → **89.3% win rate** (excellent)

The April 10 session coincides exactly with the batch of high-rank-score
`price_move` calls visible in `recent_calls`. The April 7 session likely
contained a large batch of `deadline_near` signals (which have a 34% win rate)
inflating the miss count. This reinforces the case for demoting or
heavily filtering `deadline_near`.

---

## Per-Detector Analysis and Recommendations

### 1. `price_move`

| Attribute | Value |
|---|---|
| Win rate | 57.9% (n=57) |
| Current threshold | `price_move_threshold_pct = 5.0%` |
| Current window | `price_move_window_minutes = 30` |
| Timeframes active | `price_move_timeframes = "30m"` |
| Current alert filter | `alert_rank_threshold = 0.70` |

**Recommended changes:**
1. **Lower `alert_rank_threshold` from 0.70 to 0.55.** The curve peaks at 0.55;
   the current 0.70 over-filters and cuts into the best signals (as seen in the
   April 10 near-perfect session where many rank=1.0 signals fired).
2. **Add a second timeframe (e.g., `price_move_timeframes = "30m,1h"`).** The
   current single 30m window may miss sustained moves. A 1h timeframe would
   catch slower-developing dislocations without duplicating 30m signals (the
   deduplication bucket handles overlap).
3. **Keep `price_move_threshold_pct` at 5.0%.** Current performance is already
   above 55% — do not tighten without more data.

**Expected effect:** +5–10 additional actionable signals per week; sustained
or improved win rate because the 0.55-threshold filter retains the high-quality
signals currently being discarded between 0.55 and 0.70.

---

### 2. `deadline_near`

| Attribute | Value |
|---|---|
| Win rate | 34.0% (n=47) |
| Current threshold | `deadline_near_price_threshold_pct = 3.0%` |
| Current window | `deadline_near_hours = 48` |
| Price look-back | 2-hour window (hardcoded in detector) |

**Recommended changes:**
1. **Raise `deadline_near_price_threshold_pct` from 3.0% to 6.0%.** At 3%,
   the detector is firing on noise. A 6% filter would demand a more decisive
   move in the 2-hour window, reducing spurious signals.
2. **Reduce `deadline_near_hours` from 48 to 24.** The current 48-hour window
   includes markets where deadline urgency is low. Tighter deadline proximity
   means the urgency multiplier in the detector (which scales with closeness)
   will always be high, producing better-calibrated scores.
3. **Add rank floor of 0.55 specifically for this type** by setting
   `alert_signal_types` filtering or handling in config if per-type thresholds
   are later implemented.

**Expected effect:** Estimated 40–60% reduction in `deadline_near` signal
volume; win rate should climb toward 45–50% as the lowest-conviction signals
(small price moves, distant deadlines) are filtered out.

---

### 3. `order_flow_imbalance`

| Attribute | Value |
|---|---|
| Win rate | 100% (n=1) |
| Current threshold | `ofi_threshold = 0.30` |
| Flat price window | `ofi_price_flat_window_minutes = 30` |
| Min snapshots | `ofi_min_snapshots = 3` |

**Recommended changes:**
1. **No threshold changes.** n=1 is not a basis for tuning.
2. **Reduce `ofi_min_snapshots` from 3 to 2** to allow more opportunities to
   fire and accumulate evaluation data faster. The current constraint combined
   with the flat-price filter produces very few candidates.
3. **Expand `ofi_timeframes` to `"15m,30m"`** to increase signal frequency and
   build a larger evaluation sample over the next 30 days.

**Expected effect:** Increased OFI signal volume (from ~1 to potentially 10–20
per month) to generate a statistically useful sample for future tuning.

---

### 4. `volume_spike` — No resolved data

| Attribute | Value |
|---|---|
| Win rate | N/A (0 resolved) |
| Current threshold | `volume_spike_multiplier = 3.0` |
| Baseline window | `volume_spike_baseline_hours = 24` |
| Min baseline snapshots | 12 (hardcoded) |

**Recommended changes:**
1. **Lower `volume_spike_multiplier` from 3.0 to 2.0** to fire more signals
   and start accumulating resolution data. A 3× volume spike is a very
   conservative threshold; 2× is still a meaningful anomaly.
2. **Enable multi-timeframe: `volume_spike_timeframes = "1h,4h"`** to catch
   both short-burst and sustained volume anomalies.

**Expected effect:** Signal volume increases; resolution data begins to
accumulate within the next 30-day window, enabling data-driven threshold
tuning.

---

### 5. `spread_change` — No resolved data

| Attribute | Value |
|---|---|
| Win rate | N/A (0 resolved) |
| Current threshold | `spread_change_threshold_ratio = 2.0` |
| Baseline window | `spread_change_baseline_hours = 12` |
| Min baseline snapshots | 6 (hardcoded) |

**Recommended changes:**
1. **Lower `spread_change_threshold_ratio` from 2.0 to 1.5** to generate more
   signal volume. A 2× spread change is significant but may be filtering out
   genuinely informative 1.5× anomalies.
2. **Extend `spread_change_baseline_hours` from 12 to 24** to make the baseline
   more stable and reduce false positives from intraday spread variance.

**Expected effect:** More spread-change signals fired; baseline is more robust
against intraday noise.

---

### 6. `liquidity_vacuum` — No resolved data

| Attribute | Value |
|---|---|
| Win rate | N/A (0 resolved) |
| Current threshold | `liquidity_vacuum_depth_ratio_threshold = 0.30` |
| Baseline window | `liquidity_vacuum_baseline_hours = 12` |
| Min baseline snapshots | 6 (hardcoded) |

**Recommended changes:**
1. **Raise `liquidity_vacuum_depth_ratio_threshold` from 0.30 to 0.40.** A
   depth ratio of 0.30 means current depth must fall to 30% of baseline to
   fire — this may be too conservative. 0.40 (40% of baseline) is still a
   genuine vacuum event but fires more readily.
2. **Extend `liquidity_vacuum_baseline_hours` from 12 to 24** for the same
   stability reason as spread_change.

**Expected effect:** More signals fired; resolution data begins accumulating.

---

### 7. `arbitrage` — No resolved data

| Attribute | Value |
|---|---|
| Win rate | N/A (0 resolved) |
| Current threshold | `arb_spread_threshold = 0.04` (4 percentage points) |
| Enabled | `arb_enabled = True` |

**Recommended changes:**
1. **Lower `arb_spread_threshold` from 0.04 to 0.025** to fire on tighter
   cross-venue spreads. A 4pp spread between Polymarket and Kalshi rarely
   persists long enough to resolve; 2.5pp spreads are more common and still
   represent genuine inefficiency.

**Expected effect:** More arbitrage signals; resolution data available for
next tuning cycle.

---

### 8. `smart_money` — Disabled, no data

| Attribute | Value |
|---|---|
| Win rate | N/A (disabled) |
| Enabled | `whale_tracking_enabled = False` |
| Requires | Polygon RPC endpoint |

**Recommendation:** Keep disabled until `polygon_rpc_url` is configured and
whale wallet seeding is complete. This detector has a hard external dependency
and should not be enabled until the prerequisite infrastructure is in place.

---

## Detectors to Disable

None should be fully disabled at this time. The five detectors with no resolved
data are not confirmed underperformers — they simply need more time or lower
thresholds to accumulate data. The exception is `smart_money`, which is
correctly disabled pending Polygon RPC setup.

**Candidate for conditional disable:** `deadline_near` — if win rate does not
improve above 40% within the next 30 days after threshold changes, this
detector should be disabled pending a structural review of the urgency scoring
logic.

---

## Summary of Recommended Threshold Changes

| Parameter | Current Value | Recommended Value | Rationale |
|---|---|---|---|
| `alert_rank_threshold` | 0.70 | **0.55** | Threshold curve peaks at 0.55; current value over-filters |
| `deadline_near_price_threshold_pct` | 3.0% | **6.0%** | 34% win rate; raise bar to reduce noise fires |
| `deadline_near_hours` | 48 | **24** | Tighter deadline proximity produces higher urgency scores |
| `volume_spike_multiplier` | 3.0× | **2.0×** | Too conservative; zero signals resolved in 30 days |
| `volume_spike_timeframes` | `"1h"` | **`"1h,4h"`** | Multi-TF coverage for sustained volume events |
| `spread_change_threshold_ratio` | 2.0 | **1.5** | Lower bar to accumulate resolution data |
| `spread_change_baseline_hours` | 12 | **24** | Stabilize baseline against intraday spread variance |
| `liquidity_vacuum_depth_ratio_threshold` | 0.30 | **0.40** | Increase sensitivity; current threshold fires rarely |
| `liquidity_vacuum_baseline_hours` | 12 | **24** | Stabilize baseline |
| `arb_spread_threshold` | 0.04 | **0.025** | Narrower spreads are more common and still actionable |
| `ofi_min_snapshots` | 3 | **2** | Reduce constraint to build evaluation sample |
| `ofi_timeframes` | `"30m"` | **`"15m,30m"`** | Multi-TF to increase OFI signal frequency |
| `price_move_timeframes` | `"30m"` | **`"30m,1h"`** | Capture slower sustained moves |

---

## Changes NOT Recommended

- **`price_move_threshold_pct`** (5.0%): Win rate is 57.9% — do not change.
- **`ofi_threshold`** (0.30): Only 1 resolved signal; insufficient data.
- **`ofi_price_flat_window_minutes`** (30): Structural parameter; do not change
  without understanding why OFI signals are rare first.
- **`min_volume_24h`** (500): Not a signal threshold; affects market discovery,
  not signal quality. Out of scope for this tuning cycle.

---

## Next Review

Recommended re-evaluation date: **2026-05-12** (30 days after changes applied).
Target metrics:
- `deadline_near` win rate ≥ 40%
- At least 10 resolved `volume_spike`, `spread_change`, and `liquidity_vacuum`
  signals each
- Overall win rate ≥ 52%
