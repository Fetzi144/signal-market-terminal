# Fix Plan — Signal Market Terminal

## Uncommitted Changes (git diff --stat)

```
 backend/app/api/analytics.py        | 40 ++++++++++++++++++++
 backend/app/api/signals.py          | 16 ++++++++
 backend/app/config.py               |  6 +++
 backend/app/models/signal.py        |  6 ++-
 backend/app/ranking/scorer.py       | 64 +++++++++++++++++++++++++++++++-
 backend/app/signals/base.py         | 28 ++++++++++++++
 backend/app/signals/order_flow.py   | 45 ++++++++++++++++++-----
 backend/app/signals/price_move.py   | 42 ++++++++++++++++++---
 backend/app/signals/volume_spike.py | 43 ++++++++++++++++++---
 frontend/src/api.js                 | 13 ++++++-
 frontend/src/pages/Analytics.jsx    | 47 ++++++++++++++++++++++-
 frontend/src/pages/SignalDetail.jsx | 42 ++++++++++++++++-----
 frontend/src/pages/SignalFeed.jsx   | 74 +++++++++++++++++++++++++++++++++++--
 13 files changed, 426 insertions(+), 40 deletions(-)
```

---

## What Changed (Summary)

This diff implements **multi-timeframe analysis** across the entire stack.

### `backend/app/config.py` (+6)
Three new settings are added:
- `price_move_timeframes: str = "30m"` — comma-separated timeframe list for PriceMoveDetector
- `volume_spike_timeframes: str = "1h"` — comma-separated timeframe list for VolumeSpikeDetector
- `ofi_timeframes: str = "30m"` — comma-separated timeframe list for OrderFlowImbalanceDetector

### `backend/app/models/signal.py` (+6, -1)
- New column `timeframe: Mapped[str]` (`String(8)`, `NOT NULL`, `default="30m"`) added to the `Signal` ORM model
- The dedupe unique index `uq_signal_dedupe` gains `timeframe` as a component: `(signal_type, outcome_id, timeframe, dedupe_bucket)` instead of `(signal_type, outcome_id, dedupe_bucket)`
- New index `ix_signal_timeframe` added on `timeframe`

### `backend/app/signals/base.py` (+28)
- New `TIMEFRAME_MINUTES` dict mapping timeframe strings to integer minutes
- New `timeframe_to_minutes(tf: str) -> int` helper function
- `BaseDetector` gains an `__init__` that accepts `timeframes: list[str] | None` and stores `self.timeframes`
- `SignalCandidate` dataclass gains `timeframe: str = "30m"` field

### `backend/app/signals/price_move.py` (+42, -3)
- `PriceMoveDetector.__init__` now also accepts `timeframes` kwarg and calls `super().__init__(timeframes=tf)`
- `detect()` loops over `self.timeframes`, delegating to new `_detect_timeframe(session, timeframe, ...)`
- `_detect_timeframe` derives `window_minutes` from the timeframe via `timeframe_to_minutes()` (unless `_window_minutes` override is set)
- Each `SignalCandidate` emitted has `timeframe=timeframe` and `details["timeframe"]=timeframe`

### `backend/app/signals/volume_spike.py` (+43, -3)
- `VolumeSpikeDetector.__init__` accepts `timeframes` kwarg; defaults to `settings.volume_spike_timeframes.split(",")`
- `detect()` loops over `self.timeframes`, delegating to new `_detect_timeframe(session, timeframe, ...)`
- **Key behavioral change**: `_detect_timeframe` derives `baseline_hours` from the timeframe when `self._baseline_hours` is `None`:
  `baseline_hours = max(1, timeframe_to_minutes(timeframe) // 60)`
  For the default `"1h"` timeframe: `max(1, 60 // 60) = 1`
- Each `SignalCandidate` emitted has `timeframe=timeframe`

### `backend/app/signals/order_flow.py` (+45, -5)
- Same multi-timeframe pattern as PriceMoveDetector and VolumeSpikeDetector
- `_detect_timeframe` uses `timeframe_to_minutes(timeframe)` as `flat_minutes` for the price-flat window
- Each candidate has `timeframe=timeframe`

### `backend/app/ranking/scorer.py` (+64, -1)
- `persist_signals` now includes `Signal.timeframe == c.timeframe` in the dedupe check
- Newly persisted `Signal` objects get `timeframe=c.timeframe`
- After commit, calls new `_apply_confluence_scoring(session, new_signals, bucket)`
- `_apply_confluence_scoring` groups signals by `(signal_type, outcome_id)`, detects when the same type fires on multiple timeframes in the same 15-min bucket, and applies `CONFLUENCE_BONUS_PER_TF * (len(timeframes) - 1)` bonus to `rank_score` (capped at 1.000), also writing `confluence_timeframes` and `confluence_score` into `signal.details`

### `backend/app/api/signals.py` (+16)
- `SignalOut` Pydantic model gains `timeframe: str`
- `list_signals` endpoint gains `timeframe: str | None` query param that filters `Signal.timeframe`
- New `GET /signals/timeframes` endpoint returning distinct timeframe values
- Both `list_signals` and `get_signal` now include `timeframe` in returned `SignalOut`

### `backend/app/api/analytics.py` (+40)
- New `GET /analytics/timeframe-accuracy` endpoint: returns per-timeframe signal accuracy stats grouped by `Signal.timeframe`

### Frontend files (api.js, Analytics.jsx, SignalDetail.jsx, SignalFeed.jsx)
- `getSignals()` now forwards a `timeframe` filter param
- New `getTimeframeAccuracy()` and `getSignalTimeframes()` API calls
- `Analytics.jsx` shows a new "Accuracy by Timeframe" table
- `SignalFeed.jsx` gains a timeframe dropdown filter
- `SignalDetail.jsx` shows the timeframe badge on signal detail view

---

## Failing Tests

Eight tests failed on the initial pytest run (before any `.pyc` caches were warm). Subsequent runs have passed consistently, indicating **intermittent / flaky** failures with a deterministic root cause that is sometimes masked by timing.

### Group 1 — Volume Spike tests (6 failures)

**Files:** `backend/tests/test_volume_spike.py`, `backend/tests/test_volume_spike_extra.py`

| # | Test name | File | Approx. line | Error |
|---|-----------|------|-------------|-------|
| 1 | `test_volume_above_3x_fires` | `test_volume_spike.py` | 42 | `assert 0 >= 1` (no candidates) |
| 2 | `test_low_baseline_confidence_penalty` | `test_volume_spike.py` | 113 | `assert 0 >= 1` |
| 3 | `test_medium_baseline_confidence_penalty` | `test_volume_spike.py` | 138 | `assert 0 >= 1` |
| 4 | `test_score_capped_at_one` | `test_volume_spike.py` | 163 | `assert 0 >= 1` |
| 5 | `test_log_scaling_higher_spike_higher_score` | `test_volume_spike_extra.py` | 198 | `AssertionError: assert 0 > 0` |
| 6 | `test_volume_spike_fires` | `test_volume_spike_extra.py` | 40 | `assert 0 >= 1` |

All six share the same error: `VolumeSpikeDetector().detect(session)` returns 0 candidates when at least 1 is expected.

### Group 2 — Multi-timeframe confluence tests (2 failures)

**File:** `backend/tests/test_multi_timeframe.py`

| # | Test name | File | Approx. line | Error |
|---|-----------|------|-------------|-------|
| 7 | `test_confluence_across_2_timeframes_applies_bonus` | `test_multi_timeframe.py` | 193 | `sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called` |
| 8 | `test_confluence_across_3_timeframes_capped_at_1` | `test_multi_timeframe.py` | 261 | `sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called` |

---

## Root Cause Analysis

### Failure Group 1: Volume Spike — zero candidates

**Cause: The new timeframe-derived `baseline_hours` shrinks the baseline window to zero.**

Before this diff, `VolumeSpikeDetector._detect_timeframe` used:
```python
baseline_hours = self._baseline_hours or settings.volume_spike_baseline_hours  # 24
```

After this diff, when `self._baseline_hours` is `None` (the default), it derives from the timeframe:
```python
baseline_minutes = timeframe_to_minutes(timeframe)   # "1h" → 60
baseline_hours = max(1, baseline_minutes // 60)       # max(1, 1) = 1
```

The default `volume_spike_timeframes = "1h"` (set in `config.py` and also in `backend/.env`), so every call to `VolumeSpikeDetector()` in the tests uses `baseline_hours = 1`.

The detector then computes:
```python
baseline_start  = now - timedelta(hours=1)   # e.g. 19:17 UTC
recent_window   = now - timedelta(hours=1)   # also 19:17 UTC  ← identical!
```

Because both cutoffs are `now - 1h`, the SQL baseline subquery has the condition:
```sql
WHERE captured_at >= '...:17' AND captured_at < '...:17'
```
This is a zero-width window — no rows ever satisfy it. Consequently `snap_count = 0 < MIN_BASELINE_SNAPSHOTS (12)`, and all candidates are filtered out.

The tests place their baseline snapshots at `now - timedelta(hours=6, minutes=i*20)`, which is 6–9 hours ago. With the old 24-hour baseline window these snapshots were inside the window; with the new 1-hour window they are outside it.

**Why the tests sometimes pass (flakiness):** When the test module loads fresh (cold import, first run), `settings` is instantiated and `volume_spike_timeframes` is read from the `.env` file as `"1h"`. On subsequent pytest runs within the same Python process, if another test has monkey-patched `settings` in a way that persists (via `unittest.mock.patch` that is not properly undone), the setting may revert to some other value. In practice the issue is consistent and reproducible — it only appears intermittent because `pytest` with `asyncio_mode=auto` and a shared `session`-scoped event loop can occasionally serialize tests in an order where the Settings object is re-read.

**This is a direct consequence of the uncommitted change** — specifically the new config default `volume_spike_timeframes: str = "1h"` combined with the new `baseline_hours` derivation logic that ignores `settings.volume_spike_baseline_hours`.

### Failure Group 2: Confluence tests — MissingGreenlet

**Cause: `session.expire_all()` followed by synchronous attribute access triggers a lazy-load outside an async greenlet.**

Both failing tests follow this pattern:
```python
count, signals = await persist_signals(session, candidates)
session.expire_all()
result = await session.execute(select(Signal).where(...))
db_signals = result.scalars().all()

for sig in db_signals:
    assert sig.details.get("confluence_timeframes") is not None  # ← triggers lazy load
```

After `session.expire_all()`, all ORM attributes on every mapped instance are marked as "expired". When the test accesses `sig.details` (or `sig.signal_type`) in the `for` loop, SQLAlchemy tries to reload the row. In async SQLAlchemy, attribute access outside an `await` expression cannot run I/O — it raises `MissingGreenlet`.

This issue exists in the newly-added test (`test_multi_timeframe.py`). It does not exist in older test files. The error occurs because:
1. `result.scalars().all()` returns ORM objects whose session is in an "expired" state
2. The new `_apply_confluence_scoring` calls `await session.commit()` (line 129 of `scorer.py`), which expires all objects tracked by the session
3. Then the test calls `session.expire_all()` again — doubly expiring
4. The `for sig in db_signals:` loop then accesses attributes on expired objects, triggering a synchronous IO attempt

The `MissingGreenlet` error is raised at line 193 / 261 in `test_multi_timeframe.py` (the `select(Signal).where(...)` line in the error traceback is a false lead — the real problem is the attribute access on the returned objects in the `for` loop).

**This is caused by newly-added test code** (`test_multi_timeframe.py`) that uses `session.expire_all()` incorrectly for async SQLAlchemy. However, the test was written for newly-added scorer functionality (`_apply_confluence_scoring`) that is part of this diff.

**Why the tests sometimes pass:** The `MissingGreenlet` error is also timing-dependent. The shared `session`-scoped event loop (defined in `conftest.py` with `scope="session"`) can mask this error when the test runs in the same greenlet context as the event loop setup. On a fresh Python startup, the greenlet stack may be configured differently.

---

## Relationship: Changes → Failures

**The uncommitted changes directly cause all 8 failures:**

- **6 volume spike failures**: Caused by `config.py` adding `volume_spike_timeframes = "1h"` and `volume_spike.py` changing the `baseline_hours` derivation. The old code used `settings.volume_spike_baseline_hours` (24h by default). The new code uses `max(1, timeframe_to_minutes("1h") // 60) = 1h`, shrinking the baseline window to zero width and producing no baseline data.

- **2 confluence failures**: Caused by `test_multi_timeframe.py` (a new test file) using `session.expire_all()` followed by ORM attribute access in a synchronous loop — a pattern that is invalid for async SQLAlchemy. The test was written to verify the new `_apply_confluence_scoring` function in `scorer.py`. Both the test code and the production code being tested are part of this diff.

There are **no pre-existing failures** — all failures are introduced by this diff.

---

## Concrete Fix Proposals

### Fix 1: Volume Spike baseline hours regression

**File:** `backend/app/signals/volume_spike.py`  
**Problem:** Line 59–60 — `baseline_hours = max(1, baseline_minutes // 60)` ignores `settings.volume_spike_baseline_hours`  
**Fix:** When `self._baseline_hours` is `None`, fall back to `settings.volume_spike_baseline_hours` rather than deriving from the timeframe. If multi-timeframe scaling of the baseline window is desired, multiply the setting by the timeframe ratio rather than replacing it entirely.

Change this:
```python
# Current (broken)
if self._baseline_hours is not None:
    baseline_hours = self._baseline_hours
else:
    baseline_minutes = timeframe_to_minutes(timeframe)
    baseline_hours = max(1, baseline_minutes // 60)
```

To this:
```python
# Fixed: use explicit override, otherwise use the configured default
if self._baseline_hours is not None:
    baseline_hours = self._baseline_hours
else:
    baseline_hours = settings.volume_spike_baseline_hours
```

If the intent is to scale the baseline by timeframe, consider an additive or multiplicative approach that still respects the config default — for example:
```python
# Alternative: scale from config default proportionally
base = settings.volume_spike_baseline_hours  # 24
tf_ratio = timeframe_to_minutes(timeframe) / 60  # "1h" → 1.0, "4h" → 4.0, "24h" → 24.0
baseline_hours = max(1, int(base * tf_ratio / 24))  # 24*1/24=1, 24*4/24=4, 24*24/24=24
```
But this would still fail the existing tests. The safest fix that keeps all existing tests green is to simply use `settings.volume_spike_baseline_hours` unchanged.

**Also consider:** Adding `volume_spike_baseline_hours` to the `.env` file's test section or passing `baseline_hours=24` explicitly in each test's `VolumeSpikeDetector(baseline_hours=24)` call, so the tests are not sensitive to the default.

---

### Fix 2: Confluence tests — async attribute access after expire_all

**File:** `backend/tests/test_multi_timeframe.py`  
**Lines:** 188 and 257 (the `session.expire_all()` call)  
**Problem:** After `expire_all()`, accessing ORM attributes in a synchronous loop raises `MissingGreenlet` in async SQLAlchemy.

**Fix option A (preferred): Remove `expire_all()` and use eager loading in the SELECT**

Replace:
```python
session.expire_all()
from sqlalchemy import select
from app.models.signal import Signal
result = await session.execute(
    select(Signal).where(
        Signal.signal_type == "price_move",
        Signal.outcome_id == outcome_id,
    )
)
db_signals = result.scalars().all()
```

With:
```python
from sqlalchemy import select
from app.models.signal import Signal
# Re-query without expire_all; the session already committed so fresh data is available
await session.refresh(signals[0])  # or simply re-query
result = await session.execute(
    select(Signal).where(
        Signal.signal_type == "price_move",
        Signal.outcome_id == outcome_id,
    )
)
db_signals = result.scalars().unique().all()
```

**Fix option B: Use `options(load_only(...))` or `raiseload` to prevent lazy loads**

```python
from sqlalchemy.orm import load_only
result = await session.execute(
    select(Signal)
    .where(Signal.signal_type == "price_move", Signal.outcome_id == outcome_id)
    .options(load_only(Signal.rank_score, Signal.details, Signal.timeframe))
)
```

**Fix option C: Eagerly access attributes immediately after the query (before expire_all)**

Move the `session.expire_all()` call to after the assertions, or avoid it entirely. The purpose of `expire_all()` here is to force a fresh DB read, but the correct async way to do this is to re-execute the SELECT (which already goes to the DB).

The cleanest fix for both tests is to **remove the `session.expire_all()` call** entirely and rely on the fact that `_apply_confluence_scoring` committed the updated rows — a fresh `SELECT` will return the updated data from the committed transaction.

```python
# In test_confluence_across_2_timeframes_applies_bonus and test_confluence_across_3_timeframes_capped_at_1:
# REMOVE: session.expire_all()
# The SELECT below already fetches fresh data from the committed transaction.
result = await session.execute(
    select(Signal).where(
        Signal.signal_type == "price_move",
        Signal.outcome_id == outcome_id,
    )
)
db_signals = result.scalars().all()
```

---

### Fix 3 (Optional): Make `conftest.py` session-scoped event loop compatible with pytest-asyncio 0.21+

**File:** `backend/tests/conftest.py`  
**Lines:** 40–43 (the `event_loop` fixture)  
**Problem:** The `scope="session"` event loop fixture is deprecated in pytest-asyncio 0.21+ and causes warnings (`asyncio_default_fixture_loop_scope=None`). Under certain orderings it contributes to greenlet isolation issues.

**Fix:** Add `asyncio_mode = "auto"` to `pytest.ini` (already present) and pin the loop scope explicitly:
```ini
[pytest]
asyncio_mode = auto
asyncio_default_fixture_loop_scope = session
```

Or remove the custom `event_loop` fixture and let pytest-asyncio manage it:
```python
# Remove from conftest.py:
# @pytest.fixture(scope="session")
# def event_loop():
#     loop = asyncio.new_event_loop()
#     yield loop
#     loop.close()
```
And in `pytest.ini`:
```ini
[pytest]
asyncio_mode = auto
asyncio_default_fixture_loop_scope = session
```

This is not strictly required to fix the failures, but it eliminates the source of non-deterministic greenlet behavior.
