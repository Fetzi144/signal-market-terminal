"""Integration test: Backtest full cycle — seed → run → sweep → verify."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.backtesting.engine import BacktestEngine
from app.backtesting.sweep import parameter_sweep
from app.db import get_db
from app.main import app
from app.models.backtest import BacktestRun, BacktestSignal
from tests.conftest import make_market, make_orderbook_snapshot, make_outcome, make_price_snapshot, make_signal


def _make_naive(dt):
    """Strip tzinfo for SQLite compatibility."""
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def _seed_price_history(session, outcome_id, base_price, num_snapshots, start, interval_minutes=2):
    """Create price snapshots with a 20% ramp in the second half."""
    for i in range(num_snapshots):
        t = start + timedelta(minutes=i * interval_minutes)
        if i < num_snapshots // 2:
            price = base_price
        else:
            progress = (i - num_snapshots // 2) / (num_snapshots // 2)
            price = base_price + Decimal(str(round(float(base_price) * 0.20 * progress, 4)))
        make_price_snapshot(session, outcome_id, price, captured_at=t,
                            volume_24h=Decimal("50000"), liquidity=Decimal("20000"))


@pytest.mark.asyncio
async def test_backtest_engine_run_and_verify(session: AsyncSession):
    """Seed data → run engine → verify signals detected and summary computed."""
    market = make_market(session, question="Will BTC hit $100k?")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    # Use recent naive timestamps so detector's datetime.now() window aligns
    now_naive = _make_naive(datetime.now(timezone.utc))
    start = now_naive - timedelta(minutes=50)
    end = now_naive
    _seed_price_history(session, outcome.id, Decimal("0.40"), 25, start)
    await session.flush()

    run = BacktestRun(
        id=uuid.uuid4(),
        name="test-full-cycle",
        start_date=start,
        end_date=end,
        detector_configs={"price_move": {"threshold_pct": 3.0, "window_minutes": 20}},
        rank_threshold=0.0,
        status="pending",
    )
    session.add(run)
    await session.flush()

    engine = BacktestEngine()
    result = await engine.run(session, run)

    assert run.status == "completed"
    assert "win_rate" in result
    assert "accuracy_by_type" in result
    assert "total_signals" in result
    assert isinstance(result["total_signals"], int)

    # Verify DB persistence
    bt_signals = (await session.execute(
        select(BacktestSignal).where(BacktestSignal.backtest_run_id == run.id)
    )).scalars().all()
    assert len(bt_signals) == result["total_signals"]

    for sig in bt_signals:
        assert sig.signal_type == "price_move"
        assert sig.rank_score > 0


@pytest.mark.asyncio
async def test_backtest_sweep_combinations(session: AsyncSession):
    """Parameter sweep produces multiple runs with different configs."""
    market = make_market(session, question="Sweep test market")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now_naive = _make_naive(datetime.now(timezone.utc))
    start = now_naive - timedelta(minutes=50)
    end = now_naive
    _seed_price_history(session, outcome.id, Decimal("0.40"), 25, start)
    await session.flush()

    sweep_params = {
        "price_move.threshold_pct": [3.0, 5.0, 8.0],
        "rank_threshold": [0.0, 0.3],
    }

    runs = await parameter_sweep(
        session,
        name_prefix="sweep-test",
        start_date=start,
        end_date=end,
        base_detector_configs={"price_move": {"window_minutes": 20}},
        base_rank_threshold=0.1,
        sweep_params=sweep_params,
    )

    assert len(runs) == 6
    completed = [r for r in runs if r.status == "completed"]
    assert len(completed) == 6

    # Each run has a result summary
    for r in completed:
        assert r.result_summary is not None
        assert "total_signals" in r.result_summary


@pytest.mark.asyncio
async def test_strategy_comparison_replays_default_and_legacy_paths(session: AsyncSession):
    market_end = datetime.now(timezone.utc) - timedelta(hours=1)
    market = make_market(session, question="Strategy replay market", end_date=market_end)
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    confluence_time = datetime.now(timezone.utc) - timedelta(hours=4)
    legacy_time = confluence_time + timedelta(minutes=30)

    make_orderbook_snapshot(
        session,
        outcome.id,
        spread="0.0200",
        depth_bid="800",
        depth_ask="900",
        captured_at=confluence_time,
        bids=[["0.39", "500"]],
        asks=[["0.41", "600"]],
    )

    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="confluence",
        timeframe="30m",
        fired_at=confluence_time,
        dedupe_bucket=confluence_time.replace(minute=(confluence_time.minute // 15) * 15, second=0, microsecond=0),
        signal_score=Decimal("0.850"),
        confidence=Decimal("0.900"),
        rank_score=Decimal("0.765"),
        details={"market_question": market.question, "outcome_name": outcome.name},
        estimated_probability=Decimal("0.6500"),
        price_at_fire=Decimal("0.400000"),
        expected_value=Decimal("0.250000"),
        resolution_price=Decimal("1.000000"),
        closing_price=Decimal("0.700000"),
    )
    make_signal(
        session,
        market.id,
        outcome.id,
        signal_type="price_move",
        timeframe="30m",
        fired_at=legacy_time,
        dedupe_bucket=legacy_time.replace(minute=(legacy_time.minute // 15) * 15, second=0, microsecond=0),
        signal_score=Decimal("0.700"),
        confidence=Decimal("0.850"),
        rank_score=Decimal("0.595"),
        details={"direction": "up", "market_question": market.question, "outcome_name": outcome.name},
        estimated_probability=Decimal("0.6300"),
        price_at_fire=Decimal("0.420000"),
        expected_value=Decimal("0.210000"),
        resolved_correctly=True,
        resolution_price=Decimal("1.000000"),
        closing_price=Decimal("0.720000"),
    )
    await session.flush()

    run = BacktestRun(
        id=uuid.uuid4(),
        name="strategy-comparison",
        start_date=confluence_time - timedelta(minutes=10),
        end_date=market_end + timedelta(minutes=10),
        detector_configs={"_replay_mode": "strategy_comparison"},
        rank_threshold=0.55,
        status="pending",
    )
    session.add(run)
    await session.flush()

    engine = BacktestEngine()
    result = await engine.run(session, run)

    assert run.status == "completed"
    assert result["replay_mode"] == "strategy_comparison"
    assert result["comparison"]["default_strategy"]["resolved_trades"] == 1
    assert result["comparison"]["legacy"]["resolved_trades"] == 1
    assert result["comparison"]["default_strategy"]["cumulative_pnl"] > 0
    assert result["comparison"]["legacy"]["cumulative_pnl"] > 0
    assert result["total_signals"] == 1

    bt_signals = (await session.execute(
        select(BacktestSignal).where(BacktestSignal.backtest_run_id == run.id)
    )).scalars().all()
    assert len(bt_signals) == 2
    replay_paths = {signal.details["replay"]["replay_path"] for signal in bt_signals}
    assert replay_paths == {"default_strategy", "legacy"}


@pytest.mark.asyncio
async def test_backtest_api_crud(engine):
    """POST /backtests creates, GET lists, DELETE removes — no actual run needed."""
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db():
        async with async_sess() as sess:
            yield sess

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)

    async with async_sess() as session:
        market = make_market(session, question="API backtest test")
        outcome = make_outcome(session, market.id, name="Yes")
        now = datetime.now(timezone.utc)
        _seed_price_history(session, outcome.id, Decimal("0.40"), 10,
                            _make_naive(now - timedelta(hours=1)))
        await session.commit()

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # List (initially may have items from other tests)
        resp = await client.get("/api/v1/backtests")
        assert resp.status_code == 200

        # Manually create a run in DB to test GET detail
        async with async_sess() as session:
            run = BacktestRun(
                id=uuid.uuid4(),
                name="api-crud-test",
                start_date=_make_naive(now - timedelta(hours=1)),
                end_date=_make_naive(now),
                detector_configs={"price_move": {"threshold_pct": 5.0}},
                rank_threshold=0.5,
                status="completed",
                result_summary={"total_signals": 5, "win_rate": 0.6},
            )
            session.add(run)
            await session.commit()
            run_id = str(run.id)

        # Get single
        resp = await client.get(f"/api/v1/backtests/{run_id}")
        assert resp.status_code == 200
        detail = resp.json()
        assert detail["name"] == "api-crud-test"
        assert detail["result_summary"]["total_signals"] == 5

        # Get signals for the run (empty since we didn't run the engine)
        resp = await client.get(f"/api/v1/backtests/{run_id}/signals")
        assert resp.status_code == 200

        # Delete
        resp = await client.delete(f"/api/v1/backtests/{run_id}")
        assert resp.status_code in (200, 204)

        # Verify deleted
        resp = await client.get(f"/api/v1/backtests/{run_id}")
        assert resp.status_code == 404

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_backtest_high_threshold_filters(session: AsyncSession):
    """A very high rank threshold filters out weak signals."""
    market = make_market(session, question="High threshold test")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now_naive = _make_naive(datetime.now(timezone.utc))
    start = now_naive - timedelta(minutes=50)
    end = now_naive
    # Tiny price move → weak signal
    for i in range(25):
        t = start + timedelta(minutes=i * 2)
        price = Decimal("0.50") + Decimal(str(round(0.005 * i / 25, 4)))
        make_price_snapshot(session, outcome.id, price, captured_at=t,
                            volume_24h=Decimal("50000"), liquidity=Decimal("20000"))
    await session.flush()

    run = BacktestRun(
        id=uuid.uuid4(),
        name="high-threshold",
        start_date=start,
        end_date=end,
        detector_configs={"price_move": {"threshold_pct": 1.0, "window_minutes": 20}},
        rank_threshold=0.99,
        status="pending",
    )
    session.add(run)
    await session.flush()

    engine = BacktestEngine()
    result = await engine.run(session, run)

    assert run.status == "completed"
    assert result["total_signals"] == 0
