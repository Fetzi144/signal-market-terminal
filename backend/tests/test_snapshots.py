from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config import settings
from app.connectors.base import RawOrderbook
from app.ingestion.snapshots import capture_snapshots
from app.models.execution_decision import ExecutionDecision
from app.models.ingestion import IngestionRun
from app.models.paper_trade import PaperTrade
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.strategy_runs.service import ensure_active_default_strategy_run
from tests.conftest import make_market, make_outcome, make_polymarket_watch_asset, make_price_snapshot, make_signal


class _FakeSnapshotConnector:
    def __init__(self):
        self.midpoint_batches: list[list[str]] = []
        self.orderbook_tokens: list[str] = []

    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, Decimal]:
        self.midpoint_batches.append(list(token_ids))
        return {token_id: Decimal("0.550000") for token_id in token_ids}

    async def fetch_orderbook(self, token_id: str) -> RawOrderbook:
        self.orderbook_tokens.append(token_id)
        return RawOrderbook(
            token_id=token_id,
            bids=[["0.54", "100"]],
            asks=[["0.56", "100"]],
            spread=Decimal("0.020000"),
        )

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_capture_snapshots_limits_polymarket_to_watch_enabled_assets(session, monkeypatch):
    watched_market = make_market(session, platform="polymarket", question="Watched market")
    watched_outcome = make_outcome(session, watched_market.id, token_id="pm-watched")

    disabled_market = make_market(session, platform="polymarket", question="Disabled market")
    disabled_outcome = make_outcome(session, disabled_market.id, token_id="pm-disabled")

    unwatched_market = make_market(session, platform="polymarket", question="Unwatched market")
    make_outcome(session, unwatched_market.id, token_id="pm-unwatched")
    await session.flush()

    make_polymarket_watch_asset(session, watched_outcome.id, "pm-watched", watch_enabled=True)
    make_polymarket_watch_asset(session, disabled_outcome.id, "pm-disabled", watch_enabled=False)
    await session.commit()

    connector = _FakeSnapshotConnector()
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["polymarket"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda _platform: connector)
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "polymarket_snapshot_max_watched_assets", 10)

    count = await capture_snapshots(session)

    assert count == 1
    assert connector.midpoint_batches == [["pm-watched"]]
    assert connector.orderbook_tokens == ["pm-watched"]

    price_snapshots = (await session.execute(select(PriceSnapshot))).scalars().all()
    orderbook_snapshots = (await session.execute(select(OrderbookSnapshot))).scalars().all()
    runs = (await session.execute(select(IngestionRun))).scalars().all()

    assert len(price_snapshots) == 1
    assert price_snapshots[0].outcome_id == watched_outcome.id
    assert len(orderbook_snapshots) == 1
    assert orderbook_snapshots[0].outcome_id == watched_outcome.id
    assert len(runs) == 1
    assert runs[0].run_type == "snapshot"
    assert runs[0].status == "success"


@pytest.mark.asyncio
async def test_capture_snapshots_includes_open_polymarket_paper_trades(session, monkeypatch):
    watched_market = make_market(session, platform="polymarket", question="Watched market")
    watched_outcome = make_outcome(session, watched_market.id, token_id="pm-watched")

    held_market = make_market(session, platform="polymarket", question="Held market")
    held_outcome = make_outcome(session, held_market.id, token_id="pm-held")
    held_signal = make_signal(session, held_market.id, held_outcome.id, signal_type="confluence")
    await session.flush()

    make_polymarket_watch_asset(session, watched_outcome.id, "pm-watched", watch_enabled=True)
    session.add(
        PaperTrade(
            signal_id=held_signal.id,
            outcome_id=held_outcome.id,
            market_id=held_market.id,
            direction="buy_yes",
            entry_price=Decimal("0.500000"),
            size_usd=Decimal("25.00"),
            shares=Decimal("50.0000"),
            status="open",
        )
    )
    await session.commit()

    connector = _FakeSnapshotConnector()
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["polymarket"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda _platform: connector)
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "polymarket_snapshot_max_watched_assets", 1)

    count = await capture_snapshots(session)

    assert count == 2
    assert connector.midpoint_batches == [["pm-watched", "pm-held"]]
    assert connector.orderbook_tokens == ["pm-watched", "pm-held"]

    price_snapshots = (await session.execute(select(PriceSnapshot))).scalars().all()
    assert {snapshot.outcome_id for snapshot in price_snapshots} == {
        watched_outcome.id,
        held_outcome.id,
    }


@pytest.mark.asyncio
async def test_capture_snapshots_refreshes_open_trades_on_disabled_platforms(session, monkeypatch):
    held_market = make_market(session, platform="kalshi", question="Held Kalshi market")
    held_outcome = make_outcome(session, held_market.id, token_id="KXTEST:yes")
    held_signal = make_signal(session, held_market.id, held_outcome.id, signal_type="confluence")
    await session.flush()

    session.add(
        PaperTrade(
            signal_id=held_signal.id,
            outcome_id=held_outcome.id,
            market_id=held_market.id,
            direction="buy_yes",
            entry_price=Decimal("0.500000"),
            size_usd=Decimal("25.00"),
            shares=Decimal("50.0000"),
            status="open",
        )
    )
    await session.commit()

    polymarket_connector = _FakeSnapshotConnector()
    kalshi_connector = _FakeSnapshotConnector()
    connectors = {"polymarket": polymarket_connector, "kalshi": kalshi_connector}
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["polymarket"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda platform: connectors[platform])
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "polymarket_snapshot_max_watched_assets", 1)

    count = await capture_snapshots(session)

    assert count == 1
    assert polymarket_connector.midpoint_batches == []
    assert kalshi_connector.midpoint_batches == [["KXTEST:yes"]]

    price_snapshots = (await session.execute(select(PriceSnapshot))).scalars().all()
    assert len(price_snapshots) == 1
    assert price_snapshots[0].outcome_id == held_outcome.id


@pytest.mark.asyncio
async def test_capture_snapshots_prefers_top_liquidity_polymarket_watch_assets(session, monkeypatch):
    high_volume_market = make_market(
        session,
        platform="polymarket",
        question="High volume market",
        last_volume_24h=Decimal("1000000"),
        last_liquidity=Decimal("500000"),
    )
    high_volume_outcome = make_outcome(session, high_volume_market.id, token_id="pm-high")

    low_volume_market = make_market(
        session,
        platform="polymarket",
        question="Low volume market",
        last_volume_24h=Decimal("1000"),
        last_liquidity=Decimal("500"),
    )
    low_volume_outcome = make_outcome(session, low_volume_market.id, token_id="pm-low")
    await session.flush()

    make_polymarket_watch_asset(session, high_volume_outcome.id, "pm-high", watch_enabled=True)
    make_polymarket_watch_asset(session, low_volume_outcome.id, "pm-low", watch_enabled=True)
    await session.commit()

    connector = _FakeSnapshotConnector()
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["polymarket"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda _platform: connector)
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "polymarket_snapshot_max_watched_assets", 1)

    count = await capture_snapshots(session)

    assert count == 1
    assert connector.midpoint_batches == [["pm-high"]]
    assert connector.orderbook_tokens == ["pm-high"]


@pytest.mark.asyncio
async def test_capture_snapshots_limits_kalshi_to_short_horizon_liquid_universe(session, monkeypatch):
    now = datetime.now(timezone.utc)
    liquid_market = make_market(
        session,
        platform="kalshi",
        question="Liquid short horizon",
        end_date=now + timedelta(days=7),
        last_volume_24h=Decimal("25000"),
        last_liquidity=Decimal("10000"),
    )
    liquid_outcome = make_outcome(session, liquid_market.id, token_id="kalshi-liquid")

    thin_market = make_market(
        session,
        platform="kalshi",
        question="Thin short horizon",
        end_date=now + timedelta(days=7),
        last_volume_24h=Decimal("100"),
        last_liquidity=Decimal("10"),
    )
    make_outcome(session, thin_market.id, token_id="kalshi-thin")

    far_market = make_market(
        session,
        platform="kalshi",
        question="Liquid long horizon",
        end_date=now + timedelta(days=120),
        last_volume_24h=Decimal("50000"),
        last_liquidity=Decimal("50000"),
    )
    make_outcome(session, far_market.id, token_id="kalshi-far")
    await session.commit()

    connector = _FakeSnapshotConnector()
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["kalshi"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda _platform: connector)
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "kalshi_snapshot_full_universe_enabled", False)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_active_outcomes", 1)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_market_horizon_days", 45)

    count = await capture_snapshots(session)

    assert count == 1
    assert connector.midpoint_batches == [["kalshi-liquid"]]
    assert connector.orderbook_tokens == ["kalshi-liquid"]

    price_snapshots = (await session.execute(select(PriceSnapshot))).scalars().all()
    assert len(price_snapshots) == 1
    assert price_snapshots[0].outcome_id == liquid_outcome.id


@pytest.mark.asyncio
async def test_capture_snapshots_includes_open_kalshi_trade_beyond_active_cap(session, monkeypatch):
    now = datetime.now(timezone.utc)
    liquid_market = make_market(
        session,
        platform="kalshi",
        question="Liquid short horizon",
        end_date=now + timedelta(days=7),
        last_volume_24h=Decimal("25000"),
        last_liquidity=Decimal("10000"),
    )
    make_outcome(session, liquid_market.id, token_id="kalshi-liquid")

    held_market = make_market(
        session,
        platform="kalshi",
        question="Held long horizon",
        end_date=now + timedelta(days=120),
        last_volume_24h=Decimal("1"),
        last_liquidity=Decimal("1"),
    )
    held_outcome = make_outcome(session, held_market.id, token_id="kalshi-held")
    held_signal = make_signal(session, held_market.id, held_outcome.id, signal_type="price_move")
    await session.flush()
    session.add(
        PaperTrade(
            signal_id=held_signal.id,
            outcome_id=held_outcome.id,
            market_id=held_market.id,
            direction="buy_no",
            entry_price=Decimal("0.150000"),
            size_usd=Decimal("25.00"),
            shares=Decimal("166.6667"),
            status="open",
        )
    )
    await session.commit()

    connector = _FakeSnapshotConnector()
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["kalshi"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda _platform: connector)
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "kalshi_snapshot_full_universe_enabled", False)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_active_outcomes", 1)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_market_horizon_days", 45)

    count = await capture_snapshots(session)

    assert count == 2
    assert connector.midpoint_batches == [["kalshi-held", "kalshi-liquid"]]
    assert connector.orderbook_tokens == ["kalshi-held", "kalshi-liquid"]


@pytest.mark.asyncio
async def test_capture_snapshots_includes_pending_kalshi_decision_beyond_active_cap(session, monkeypatch):
    now = datetime.now(timezone.utc)
    strategy_run = await ensure_active_default_strategy_run(session, bootstrap_started_at=now - timedelta(days=1))
    liquid_market = make_market(
        session,
        platform="kalshi",
        question="Liquid short horizon",
        end_date=now + timedelta(days=7),
        last_volume_24h=Decimal("25000"),
        last_liquidity=Decimal("10000"),
    )
    make_outcome(session, liquid_market.id, token_id="kalshi-liquid")

    pending_market = make_market(
        session,
        platform="kalshi",
        question="Pending long horizon",
        end_date=now + timedelta(days=120),
        last_volume_24h=Decimal("1"),
        last_liquidity=Decimal("1"),
    )
    pending_outcome = make_outcome(session, pending_market.id, token_id="kalshi-pending")
    pending_signal = make_signal(session, pending_market.id, pending_outcome.id, signal_type="price_move")
    await session.flush()
    session.add(
        ExecutionDecision(
            signal_id=pending_signal.id,
            strategy_run_id=strategy_run.id,
            decision_at=now - timedelta(minutes=1),
            decision_status="pending_decision",
            action="skip",
            reason_code="pending_decision",
            details={},
        )
    )
    await session.commit()

    connector = _FakeSnapshotConnector()
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["kalshi"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda _platform: connector)
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "kalshi_snapshot_full_universe_enabled", False)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_active_outcomes", 1)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_market_horizon_days", 45)
    monkeypatch.setattr(settings, "paper_trading_pending_decision_max_age_seconds", 900)

    count = await capture_snapshots(session)

    assert count == 2
    assert connector.midpoint_batches == [["kalshi-pending", "kalshi-liquid"]]
    assert connector.orderbook_tokens == ["kalshi-pending", "kalshi-liquid"]


@pytest.mark.asyncio
async def test_capture_snapshots_skips_unchanged_price_until_heartbeat(session, monkeypatch):
    now = datetime.now(timezone.utc)
    market = make_market(
        session,
        platform="kalshi",
        question="Unchanged market",
        end_date=now + timedelta(days=7),
        last_volume_24h=Decimal("1000"),
        last_liquidity=Decimal("1000"),
    )
    outcome = make_outcome(session, market.id, token_id="kalshi-unchanged")
    make_price_snapshot(
        session,
        outcome.id,
        Decimal("0.550000"),
        captured_at=now - timedelta(minutes=5),
        volume_24h=Decimal("1000"),
        liquidity=Decimal("1000"),
    )
    await session.commit()

    connector = _FakeSnapshotConnector()
    from app.ingestion import snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "get_enabled_platforms", lambda: ["kalshi"])
    monkeypatch.setattr(snapshots_module, "get_connector", lambda _platform: connector)
    monkeypatch.setattr(snapshots_module.random, "sample", lambda seq, n: list(seq)[:n])
    monkeypatch.setattr(settings, "orderbook_sample_size", 10)
    monkeypatch.setattr(settings, "kalshi_snapshot_full_universe_enabled", False)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_active_outcomes", 10)
    monkeypatch.setattr(settings, "kalshi_snapshot_max_market_horizon_days", 45)
    monkeypatch.setattr(settings, "snapshot_price_heartbeat_seconds", 900)
    monkeypatch.setattr(settings, "snapshot_price_change_epsilon", 0.000001)
    monkeypatch.setattr(settings, "snapshot_volume_liquidity_change_ratio", 0.25)

    count = await capture_snapshots(session)

    assert count == 0
    assert connector.midpoint_batches == [["kalshi-unchanged"]]
    price_snapshots = (await session.execute(select(PriceSnapshot))).scalars().all()
    assert len(price_snapshots) == 1
