"""Tests for data cleanup/retention logic."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app.jobs.cleanup import cleanup_old_data
from app.models.signal import Signal, SignalEvaluation
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot
from tests.conftest import (
    make_market,
    make_orderbook_snapshot,
    make_outcome,
    make_price_snapshot,
    make_signal,
)


@pytest.mark.asyncio
class TestCleanup:
    async def test_old_price_snapshots_deleted(self, session):
        """Price snapshots older than retention period should be deleted."""
        market = make_market(session)
        outcome = make_outcome(session, market.id)
        now = datetime.now(timezone.utc)

        # Old snapshot (beyond 30-day default retention)
        make_price_snapshot(session, outcome.id, price="0.50", captured_at=now - timedelta(days=31))
        # Fresh snapshot (within retention)
        make_price_snapshot(session, outcome.id, price="0.55", captured_at=now - timedelta(hours=1))
        await session.flush()

        counts = await cleanup_old_data(session)
        assert counts["price_snapshots"] == 1

        result = await session.execute(
            select(PriceSnapshot).where(PriceSnapshot.outcome_id == outcome.id)
        )
        remaining = result.scalars().all()
        assert len(remaining) == 1
        assert remaining[0].price == Decimal("0.55")

    async def test_fresh_data_preserved(self, session):
        """Data within retention period should not be deleted."""
        market = make_market(session)
        outcome = make_outcome(session, market.id)
        now = datetime.now(timezone.utc)

        make_price_snapshot(session, outcome.id, price="0.50", captured_at=now - timedelta(days=1))
        make_price_snapshot(session, outcome.id, price="0.55", captured_at=now)
        await session.flush()

        counts = await cleanup_old_data(session)
        assert counts["price_snapshots"] == 0
        assert counts["orderbook_snapshots"] == 0
        assert counts["signals"] == 0

    async def test_old_resolved_signals_deleted(self, session):
        """Resolved signals older than retention should be cleaned up with evaluations."""
        market = make_market(session)
        outcome = make_outcome(session, market.id)
        now = datetime.now(timezone.utc)
        old_time = now - timedelta(days=91)

        signal = make_signal(
            session, market.id, outcome.id,
            fired_at=old_time,
            dedupe_bucket=old_time,
            resolved=True,
        )
        # Add an evaluation for this signal
        eval_ = SignalEvaluation(
            id=uuid.uuid4(),
            signal_id=signal.id,
            horizon="1h",
            price_at_eval=Decimal("0.55"),
            price_change=Decimal("0.05"),
            price_change_pct=Decimal("10.0"),
            evaluated_at=old_time + timedelta(hours=1),
        )
        session.add(eval_)
        await session.flush()

        counts = await cleanup_old_data(session)
        assert counts["signals"] == 1
        assert counts["signal_evaluations"] == 1

        # Verify signal is gone
        result = await session.execute(select(Signal).where(Signal.id == signal.id))
        assert result.scalar_one_or_none() is None

    async def test_unresolved_signals_kept(self, session):
        """Unresolved signals should not be deleted even if old."""
        market = make_market(session)
        outcome = make_outcome(session, market.id)
        old_time = datetime.now(timezone.utc) - timedelta(days=91)

        signal = make_signal(
            session, market.id, outcome.id,
            fired_at=old_time,
            dedupe_bucket=old_time,
            resolved=False,  # not resolved
        )
        await session.flush()

        counts = await cleanup_old_data(session)
        assert counts["signals"] == 0

        result = await session.execute(select(Signal).where(Signal.id == signal.id))
        assert result.scalar_one_or_none() is not None

    async def test_old_orderbook_snapshots_deleted(self, session):
        """Orderbook snapshots use shorter retention (14 days default)."""
        market = make_market(session)
        outcome = make_outcome(session, market.id)
        now = datetime.now(timezone.utc)

        make_orderbook_snapshot(session, outcome.id, spread="0.02", captured_at=now - timedelta(days=15))
        make_orderbook_snapshot(session, outcome.id, spread="0.03", captured_at=now)
        await session.flush()

        counts = await cleanup_old_data(session)
        assert counts["orderbook_snapshots"] == 1
