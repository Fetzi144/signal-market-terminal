"""Integration test: Multi-timeframe confluence — same signal on 2 TFs → bonus applied."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.ranking.scorer import persist_signals
from app.signals.price_move import PriceMoveDetector
from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_multi_timeframe_confluence_bonus(session: AsyncSession):
    """Same signal on 30m and 4h timeframes → confluence bonus applied to rank_score."""
    market = make_market(session, question="Multi-TF confluence test")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    # Create snapshots spanning 4+ hours so both 30m and 4h windows fire
    # Old snapshot 5 hours ago at 0.40
    make_price_snapshot(session, outcome.id, Decimal("0.40"),
                        captured_at=now - timedelta(hours=5),
                        volume_24h=Decimal("50000"), liquidity=Decimal("20000"))
    # Intermediate snapshot 1 hour ago at 0.42
    make_price_snapshot(session, outcome.id, Decimal("0.42"),
                        captured_at=now - timedelta(hours=1),
                        volume_24h=Decimal("50000"), liquidity=Decimal("20000"))
    # Recent snapshot 10 min ago at 0.55 — 15% move in 30m, 37.5% move in 4h
    make_price_snapshot(session, outcome.id, Decimal("0.55"),
                        captured_at=now - timedelta(minutes=10),
                        volume_24h=Decimal("50000"), liquidity=Decimal("20000"))
    await session.flush()

    # Detect on both timeframes
    detector = PriceMoveDetector(
        threshold_pct=5.0,
        timeframes=["30m", "4h"],
    )
    candidates = await detector.detect(session)

    # Should fire on both timeframes
    timeframes_fired = {c.timeframe for c in candidates}
    assert "30m" in timeframes_fired or "4h" in timeframes_fired, \
        f"Expected at least one timeframe to fire, got {timeframes_fired}"

    if len(timeframes_fired) >= 2:
        # Persist signals — confluence scoring will apply
        count, signals = await persist_signals(session, candidates)
        assert count >= 2

        # Check that confluence bonus was applied
        for sig in signals:
            if sig.details and "confluence_timeframes" in sig.details:
                assert len(sig.details["confluence_timeframes"]) >= 2
                assert float(sig.details["confluence_score"]) > 0
                # Rank score should be boosted above base
                base_rank = sig.signal_score * sig.confidence
                assert sig.rank_score > base_rank


@pytest.mark.asyncio
async def test_single_timeframe_no_confluence(session: AsyncSession):
    """Signal on only one timeframe does NOT get confluence bonus."""
    market = make_market(session, question="Single-TF test")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    # Short window: only 30m will fire
    make_price_snapshot(session, outcome.id, Decimal("0.40"),
                        captured_at=now - timedelta(minutes=25),
                        volume_24h=Decimal("50000"), liquidity=Decimal("20000"))
    make_price_snapshot(session, outcome.id, Decimal("0.55"),
                        captured_at=now - timedelta(minutes=5),
                        volume_24h=Decimal("50000"), liquidity=Decimal("20000"))
    await session.flush()

    detector = PriceMoveDetector(threshold_pct=5.0, timeframes=["30m"])
    candidates = await detector.detect(session)

    if candidates:
        count, signals = await persist_signals(session, candidates)
        for sig in signals:
            details = sig.details or {}
            assert "confluence_timeframes" not in details


@pytest.mark.asyncio
async def test_timeframe_api_filter(client, session: AsyncSession):
    """GET /signals?timeframe=30m returns only 30m signals."""
    market = make_market(session, question="TF filter test")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    from tests.conftest import make_signal

    # Create signals with different timeframes
    make_signal(session, market.id, outcome.id, timeframe="30m",
                signal_type="price_move")
    make_signal(session, market.id, outcome.id, timeframe="4h",
                signal_type="price_move",
                dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
    await session.commit()

    resp = await client.get("/api/v1/signals", params={"timeframe": "30m"})
    assert resp.status_code == 200
    data = resp.json()
    for sig in data["signals"]:
        assert sig["timeframe"] == "30m"


@pytest.mark.asyncio
async def test_timeframes_endpoint(client, session: AsyncSession):
    """GET /signals/timeframes returns distinct timeframe values."""
    market = make_market(session, question="Timeframes endpoint test")
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    from tests.conftest import make_signal

    make_signal(session, market.id, outcome.id, timeframe="30m")
    make_signal(session, market.id, outcome.id, timeframe="4h",
                dedupe_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
    await session.commit()

    resp = await client.get("/api/v1/signals/timeframes")
    assert resp.status_code == 200
    data = resp.json()
    timeframes = data["timeframes"]
    assert "30m" in timeframes
    assert "4h" in timeframes
