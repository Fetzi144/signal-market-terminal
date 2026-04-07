"""Tests for ingestion logic: market upsert, outcome update, snapshot creation."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.connectors.base import RawMarket, RawOutcome
from app.ingestion.markets import _upsert_market
from app.models.market import Market, Outcome
from app.models.snapshot import PriceSnapshot
from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
class TestMarketUpsert:
    async def test_insert_new_market(self, session):
        rm = RawMarket(
            platform="polymarket",
            platform_id="new-mkt-1",
            slug="new-mkt-1",
            question="Will it snow?",
            category="Weather",
            end_date="2025-12-31T00:00:00Z",
            active=True,
            outcomes=[
                RawOutcome(platform_outcome_id="new-mkt-1_0", name="Yes", token_id="tok_yes", price=Decimal("0.60")),
                RawOutcome(platform_outcome_id="new-mkt-1_1", name="No", token_id="tok_no", price=Decimal("0.40")),
            ],
            volume_24h=Decimal("10000"),
            liquidity=Decimal("5000"),
            metadata={"key": "value"},
        )
        await _upsert_market(session, rm)
        await session.flush()

        result = await session.execute(
            select(Market).where(Market.platform_id == "new-mkt-1")
        )
        market = result.scalar_one()
        assert market.question == "Will it snow?"
        assert market.active is True

        outcome_result = await session.execute(
            select(Outcome).where(Outcome.market_id == market.id)
        )
        outcomes = outcome_result.scalars().all()
        assert len(outcomes) == 2
        assert {o.name for o in outcomes} == {"Yes", "No"}

    async def test_update_existing_market(self, session):
        """Upsert should update question, active, volume on existing market."""
        market = make_market(session, platform="polymarket", platform_id="existing-1", question="Old question")
        make_outcome(session, market.id, platform_outcome_id="existing-1_0", name="Yes", token_id="old_tok")
        await session.flush()

        rm = RawMarket(
            platform="polymarket",
            platform_id="existing-1",
            slug="existing-1",
            question="Updated question",
            category="Updated",
            end_date=None,
            active=False,
            outcomes=[
                RawOutcome(platform_outcome_id="existing-1_0", name="Yes Updated", token_id="new_tok", price=None),
            ],
            volume_24h=Decimal("20000"),
            liquidity=Decimal("8000"),
            metadata={},
        )
        await _upsert_market(session, rm)
        await session.flush()

        result = await session.execute(
            select(Market).where(Market.platform_id == "existing-1")
        )
        market = result.scalar_one()
        assert market.question == "Updated question"
        assert market.active is False
        assert market.last_volume_24h == Decimal("20000")

        outcome_result = await session.execute(
            select(Outcome).where(Outcome.market_id == market.id)
        )
        outcome = outcome_result.scalar_one()
        assert outcome.name == "Yes Updated"
        assert outcome.token_id == "new_tok"

    async def test_upsert_adds_new_outcome(self, session):
        """If a new outcome appears in the raw data, it should be created."""
        market = make_market(session, platform="polymarket", platform_id="grow-mkt")
        make_outcome(session, market.id, platform_outcome_id="grow-mkt_0", name="Yes")
        await session.flush()

        rm = RawMarket(
            platform="polymarket",
            platform_id="grow-mkt",
            slug="grow-mkt",
            question="Growing outcomes",
            category=None,
            end_date=None,
            active=True,
            outcomes=[
                RawOutcome(platform_outcome_id="grow-mkt_0", name="Yes", token_id="t0", price=None),
                RawOutcome(platform_outcome_id="grow-mkt_1", name="No", token_id="t1", price=None),
            ],
            volume_24h=None,
            liquidity=None,
            metadata={},
        )
        await _upsert_market(session, rm)
        await session.flush()

        outcome_result = await session.execute(
            select(Outcome).where(Outcome.market_id == market.id)
        )
        outcomes = outcome_result.scalars().all()
        assert len(outcomes) == 2


@pytest.mark.asyncio
class TestSnapshotCreation:
    async def test_price_snapshot_stored(self, session):
        """Verify price snapshots are correctly stored and queryable."""
        market = make_market(session)
        outcome = make_outcome(session, market.id)
        now = datetime.now(timezone.utc)
        make_price_snapshot(session, outcome.id, price="0.55", captured_at=now, volume_24h=Decimal("1000"))
        await session.flush()

        result = await session.execute(
            select(PriceSnapshot).where(PriceSnapshot.outcome_id == outcome.id)
        )
        snap = result.scalar_one()
        assert snap.price == Decimal("0.55")
        assert snap.volume_24h == Decimal("1000")

    async def test_multiple_snapshots_ordered(self, session):
        """Multiple snapshots for same outcome should be queryable in order."""
        from datetime import timedelta
        market = make_market(session)
        outcome = make_outcome(session, market.id)
        now = datetime.now(timezone.utc)

        make_price_snapshot(session, outcome.id, price="0.50", captured_at=now - timedelta(hours=2))
        make_price_snapshot(session, outcome.id, price="0.55", captured_at=now - timedelta(hours=1))
        make_price_snapshot(session, outcome.id, price="0.60", captured_at=now)
        await session.flush()

        result = await session.execute(
            select(PriceSnapshot)
            .where(PriceSnapshot.outcome_id == outcome.id)
            .order_by(PriceSnapshot.captured_at.desc())
        )
        snaps = result.scalars().all()
        assert len(snaps) == 3
        assert float(snaps[0].price) == 0.60  # most recent first
