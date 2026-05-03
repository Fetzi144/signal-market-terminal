from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.models.polymarket_stream import PolymarketMarketEvent
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from app.reports.scanner_storage import (
    build_scanner_storage_snapshot,
    run_scanner_storage_retention,
)
from tests.conftest import make_market, make_outcome


@pytest.mark.asyncio
async def test_scanner_storage_dry_run_and_apply_prunes_expired_rows(session, monkeypatch):
    import app.reports.scanner_storage as scanner_storage_module

    monkeypatch.setattr(scanner_storage_module.settings, "retention_price_snapshots_days", 1)
    monkeypatch.setattr(scanner_storage_module.settings, "retention_orderbook_snapshots_days", 1)
    monkeypatch.setattr(scanner_storage_module.settings, "retention_signals_days", 1)
    monkeypatch.setattr(scanner_storage_module.settings, "polymarket_raw_retention_days", 1)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    old = now - timedelta(days=3)
    fresh = now - timedelta(hours=1)
    market = make_market(session, platform="polymarket", platform_id="storage-test")
    outcome = make_outcome(session, market.id, name="Yes", token_id="token-storage")
    await session.flush()

    session.add_all(
        [
            PriceSnapshot(outcome_id=outcome.id, price=Decimal("0.500000"), captured_at=old),
            PriceSnapshot(outcome_id=outcome.id, price=Decimal("0.510000"), captured_at=fresh),
            OrderbookSnapshot(outcome_id=outcome.id, bids={}, asks={}, captured_at=old),
            OrderbookSnapshot(outcome_id=outcome.id, bids={}, asks={}, captured_at=fresh),
            PolymarketMarketEvent(
                venue="polymarket",
                provenance="stream",
                channel="market",
                message_type="price_change",
                asset_id="token-storage",
                received_at_local=old,
                payload={"price": "0.50"},
            ),
            PolymarketMarketEvent(
                venue="polymarket",
                provenance="stream",
                channel="market",
                message_type="price_change",
                asset_id="token-storage",
                received_at_local=fresh,
                payload={"price": "0.51"},
            ),
        ]
    )
    await session.commit()

    dry_run = await run_scanner_storage_retention(session, apply=False, include_raw_events=True, as_of=now)

    candidates = dry_run["snapshot"]["candidate_rows_by_table"]
    assert dry_run["mode"] == "dry_run"
    assert candidates["price_snapshots"] == 1
    assert candidates["orderbook_snapshots"] == 1
    assert candidates["polymarket_market_events"] == 1

    result = await run_scanner_storage_retention(
        session,
        apply=True,
        include_raw_events=True,
        batch_size=1,
        as_of=now,
    )

    assert result["deleted"]["price_snapshots"] == 1
    assert result["deleted"]["orderbook_snapshots"] == 1
    assert result["deleted"]["polymarket_market_events"] == 1
    assert len((await session.execute(select(PriceSnapshot))).scalars().all()) == 1
    assert len((await session.execute(select(OrderbookSnapshot))).scalars().all()) == 1
    assert len((await session.execute(select(PolymarketMarketEvent))).scalars().all()) == 1

    after = await build_scanner_storage_snapshot(session, as_of=now)
    assert after["candidate_rows_by_table"]["price_snapshots"] == 0
