from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config import settings
from app.connectors.base import RawOrderbook
from app.ingestion.snapshots import capture_snapshots
from app.models.ingestion import IngestionRun
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from tests.conftest import make_market, make_outcome, make_polymarket_watch_asset


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
