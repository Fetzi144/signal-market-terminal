"""Tests for Day 2: Backtest API and parameter sweep."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from app.backtesting.sweep import _build_combinations, _flat_to_detector_configs, MAX_SWEEP_COMBINATIONS
from app.models.backtest import BacktestRun, BacktestSignal
from tests.conftest import make_market, make_outcome, make_price_snapshot


# --------------------------------------------------------------------------- #
# Helpers                                                                       #
# --------------------------------------------------------------------------- #

def _make_run(session, **kwargs):
    now = datetime.now(timezone.utc)
    defaults = dict(
        id=uuid.uuid4(),
        name="test run",
        start_date=now - timedelta(days=7),
        end_date=now - timedelta(hours=1),
        rank_threshold=0.5,
        status="completed",
        result_summary={"total_signals": 3, "win_rate": 0.667},
    )
    defaults.update(kwargs)
    run = BacktestRun(**defaults)
    session.add(run)
    return run


def _make_bt_signal(session, run_id, **kwargs):
    now = datetime.now(timezone.utc)
    defaults = dict(
        id=uuid.uuid4(),
        backtest_run_id=run_id,
        signal_type="price_move",
        fired_at=now - timedelta(hours=2),
        signal_score=Decimal("0.600"),
        confidence=Decimal("0.800"),
        rank_score=Decimal("0.480"),
        resolved_correctly=True,
    )
    defaults.update(kwargs)
    sig = BacktestSignal(**defaults)
    session.add(sig)
    return sig


# --------------------------------------------------------------------------- #
# Unit tests: sweep helpers                                                     #
# --------------------------------------------------------------------------- #

def test_build_combinations_single_param():
    combos = _build_combinations({"price_move.threshold_pct": [0.03, 0.05, 0.07]})
    assert len(combos) == 3
    assert combos[0] == {"price_move.threshold_pct": 0.03}


def test_build_combinations_cartesian_product():
    combos = _build_combinations({
        "price_move.threshold_pct": [0.03, 0.07],
        "rank_threshold": [0.5, 0.6, 0.7],
    })
    assert len(combos) == 6  # 2 × 3


def test_build_combinations_empty():
    combos = _build_combinations({})
    assert combos == [{}]


def test_build_combinations_capped_at_50():
    """Verify sweep is capped when cartesian product exceeds MAX_SWEEP_COMBINATIONS."""
    # 10 × 10 = 100 > 50
    combos = _build_combinations({
        "a": list(range(10)),
        "b": list(range(10)),
    })
    # _build_combinations itself returns all combos; capping happens in parameter_sweep
    assert len(combos) == 100  # raw combos, capping is in parameter_sweep


def test_flat_to_detector_configs_dot_notation():
    configs, rank = _flat_to_detector_configs({
        "price_move.threshold_pct": 0.07,
        "volume_spike.multiplier": 4.0,
        "rank_threshold": 0.6,
    })
    assert configs == {
        "price_move": {"threshold_pct": 0.07},
        "volume_spike": {"multiplier": 4.0},
    }
    assert rank == 0.6


def test_flat_to_detector_configs_no_rank():
    configs, rank = _flat_to_detector_configs({"price_move.threshold_pct": 0.05})
    assert rank is None
    assert configs["price_move"]["threshold_pct"] == 0.05


# --------------------------------------------------------------------------- #
# API: CRUD                                                                     #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_list_backtests_empty(client):
    resp = await client.get("/api/v1/backtests")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_get_backtest_not_found(client):
    resp = await client.get(f"/api/v1/backtests/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_backtest_not_found(client):
    resp = await client.delete(f"/api/v1/backtests/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_and_get_backtest(client, session):
    run = _make_run(session)
    await session.commit()

    resp = await client.get("/api/v1/backtests")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["id"] == str(run.id)
    assert data[0]["status"] == "completed"
    assert data[0]["result_summary"]["win_rate"] == 0.667

    resp2 = await client.get(f"/api/v1/backtests/{run.id}")
    assert resp2.status_code == 200
    assert resp2.json()["name"] == "test run"


@pytest.mark.asyncio
async def test_delete_backtest(client, session):
    run = _make_run(session)
    sig = _make_bt_signal(session, run.id)
    await session.commit()

    resp = await client.delete(f"/api/v1/backtests/{run.id}")
    assert resp.status_code == 204

    resp2 = await client.get(f"/api/v1/backtests/{run.id}")
    assert resp2.status_code == 404


@pytest.mark.asyncio
async def test_list_signals_for_run(client, session):
    run = _make_run(session)
    _make_bt_signal(session, run.id, signal_type="price_move", resolved_correctly=True)
    _make_bt_signal(session, run.id, signal_type="volume_spike", resolved_correctly=False)
    await session.commit()

    resp = await client.get(f"/api/v1/backtests/{run.id}/signals")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert len(data["signals"]) == 2


@pytest.mark.asyncio
async def test_list_signals_filter_by_type(client, session):
    run = _make_run(session)
    _make_bt_signal(session, run.id, signal_type="price_move")
    _make_bt_signal(session, run.id, signal_type="volume_spike")
    await session.commit()

    resp = await client.get(f"/api/v1/backtests/{run.id}/signals?signal_type=price_move")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["signals"][0]["signal_type"] == "price_move"


@pytest.mark.asyncio
async def test_list_signals_filter_by_resolved(client, session):
    run = _make_run(session)
    _make_bt_signal(session, run.id, resolved_correctly=True)
    _make_bt_signal(session, run.id, resolved_correctly=False)
    _make_bt_signal(session, run.id, resolved_correctly=None)
    await session.commit()

    resp = await client.get(f"/api/v1/backtests/{run.id}/signals?resolved_correctly=true")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1

    resp2 = await client.get(f"/api/v1/backtests/{run.id}/signals?resolved_correctly=false")
    assert resp2.json()["total"] == 1


@pytest.mark.asyncio
async def test_list_signals_run_not_found(client):
    resp = await client.get(f"/api/v1/backtests/{uuid.uuid4()}/signals")
    assert resp.status_code == 404


# --------------------------------------------------------------------------- #
# API: Create backtest — validation                                             #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_create_backtest_future_end_date(client):
    now = datetime.now(timezone.utc)
    resp = await client.post("/api/v1/backtests", json={
        "name": "future test",
        "start_date": (now - timedelta(days=10)).isoformat(),
        "end_date": (now + timedelta(days=1)).isoformat(),
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_backtest_date_range_too_large(client):
    now = datetime.now(timezone.utc)
    resp = await client.post("/api/v1/backtests", json={
        "name": "too wide",
        "start_date": (now - timedelta(days=200)).isoformat(),
        "end_date": (now - timedelta(hours=1)).isoformat(),
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_backtest_no_snapshots(client):
    now = datetime.now(timezone.utc)
    resp = await client.post("/api/v1/backtests", json={
        "name": "no data",
        "start_date": (now - timedelta(days=10)).isoformat(),
        "end_date": (now - timedelta(hours=1)).isoformat(),
    })
    assert resp.status_code == 422
    assert "No price snapshot data" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_create_backtest_starts_background_task(client, session):
    """When snapshot data exists, create endpoint returns 201 with pending status."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    now = datetime.now(timezone.utc)
    make_price_snapshot(session, outcome.id, 0.5, captured_at=now - timedelta(days=3))
    await session.commit()

    # Patch the background task so we don't actually run the engine
    with patch("app.api.backtest._run_backtest_background", new_callable=AsyncMock):
        resp = await client.post("/api/v1/backtests", json={
            "name": "bg task test",
            "start_date": (now - timedelta(days=5)).isoformat(),
            "end_date": (now - timedelta(hours=1)).isoformat(),
        })

    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "pending"
    assert "backtest_run_id" in data


# --------------------------------------------------------------------------- #
# API: Sweep — validation                                                       #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_sweep_no_snapshots(client):
    now = datetime.now(timezone.utc)
    resp = await client.post("/api/v1/backtests/sweep", json={
        "name_prefix": "sweep test",
        "start_date": (now - timedelta(days=10)).isoformat(),
        "end_date": (now - timedelta(hours=1)).isoformat(),
        "sweep_params": {"rank_threshold": [0.5, 0.6]},
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_sweep_correct_combination_count(client, session):
    """Sweep with 2×2 = 4 combinations should create 4 runs."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    now = datetime.now(timezone.utc)
    make_price_snapshot(session, outcome.id, 0.5, captured_at=now - timedelta(days=3))
    await session.commit()

    with patch("app.backtesting.sweep.BacktestEngine") as mock_engine_cls:
        mock_engine = mock_engine_cls.return_value
        mock_engine.run = AsyncMock(side_effect=lambda session, run: _complete_run(run))

        resp = await client.post("/api/v1/backtests/sweep", json={
            "name_prefix": "sweep",
            "start_date": (now - timedelta(days=5)).isoformat(),
            "end_date": (now - timedelta(hours=1)).isoformat(),
            "sweep_params": {
                "price_move.threshold_pct": [0.05, 0.07],
                "rank_threshold": [0.5, 0.6],
            },
        })

    assert resp.status_code == 201
    data = resp.json()
    assert data["count"] == 4
    assert len(data["backtest_run_ids"]) == 4


def _complete_run(run: BacktestRun):
    """Helper: mark a run as completed (used in mocks)."""
    from datetime import datetime, timezone
    run.status = "completed"
    run.result_summary = {"total_signals": 0, "win_rate": 0.0}
    run.completed_at = datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_sweep_capped_at_50(client, session):
    """10×10 = 100 combinations should be capped at 50."""
    market = make_market(session)
    outcome = make_outcome(session, market.id)
    now = datetime.now(timezone.utc)
    make_price_snapshot(session, outcome.id, 0.5, captured_at=now - timedelta(days=3))
    await session.commit()

    with patch("app.backtesting.sweep.BacktestEngine") as mock_engine_cls:
        mock_engine = mock_engine_cls.return_value
        mock_engine.run = AsyncMock(side_effect=lambda session, run: _complete_run(run))

        resp = await client.post("/api/v1/backtests/sweep", json={
            "name_prefix": "big sweep",
            "start_date": (now - timedelta(days=5)).isoformat(),
            "end_date": (now - timedelta(hours=1)).isoformat(),
            "sweep_params": {
                "rank_threshold": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                "price_move.threshold_pct": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10],
            },
        })

    assert resp.status_code == 201
    data = resp.json()
    assert data["count"] == MAX_SWEEP_COMBINATIONS
    assert len(data["backtest_run_ids"]) == MAX_SWEEP_COMBINATIONS


# --------------------------------------------------------------------------- #
# Engine: win rate calculation                                                  #
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_engine_win_rate_manual_count(session):
    """Verify win_rate matches a manually counted set of signals."""
    from app.backtesting.engine import BacktestEngine

    engine = BacktestEngine()

    # Build signals manually: 3 correct, 1 incorrect, 1 unresolved
    signals = []
    for i, (resolved, correct) in enumerate([
        (True, True), (True, True), (True, True), (True, False), (False, None)
    ]):
        sig = BacktestSignal(
            id=uuid.uuid4(),
            backtest_run_id=uuid.uuid4(),
            signal_type="price_move",
            fired_at=datetime.now(timezone.utc) - timedelta(hours=i),
            signal_score=Decimal("0.600"),
            confidence=Decimal("0.800"),
            rank_score=Decimal("0.500"),
            resolved_correctly=correct if resolved else None,
        )
        signals.append(sig)

    summary = engine._compute_summary(signals)
    assert summary["total_signals"] == 5
    assert summary["resolved_signals"] == 4
    assert summary["correct_signals"] == 3
    assert summary["win_rate"] == round(3 / 4, 4)
    assert summary["false_positive_rate"] == round(1 / 4, 4)


@pytest.mark.asyncio
async def test_engine_empty_signals(session):
    """Empty signal list returns zero-filled summary."""
    from app.backtesting.engine import BacktestEngine

    engine = BacktestEngine()
    summary = engine._compute_summary([])
    assert summary["total_signals"] == 0
    assert summary["win_rate"] == 0.0
    assert summary["signals_per_day"] == 0.0
