"""Tests for the ArbitrageDetector."""
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from tests.conftest import make_market, make_outcome, make_price_snapshot


@pytest.mark.asyncio
async def test_arb_signal_generated_when_spread_above_threshold(session):
    """Two platforms, same question_slug, spread >= threshold -> signal generated."""
    slug = "will it rain tomorrow"
    m1 = make_market(session, platform="polymarket", question="Will it rain tomorrow?", question_slug=slug)
    m2 = make_market(session, platform="kalshi", question="Will it rain tomorrow?", question_slug=slug)
    await session.flush()

    o1 = make_outcome(session, m1.id, name="Yes")
    o2 = make_outcome(session, m2.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, o1.id, price=0.40, captured_at=now)
    make_price_snapshot(session, o2.id, price=0.50, captured_at=now)
    await session.flush()

    from app.signals.arbitrage import ArbitrageDetector
    detector = ArbitrageDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1
    c = candidates[0]
    assert c.signal_type == "arbitrage"
    assert c.confidence == Decimal("1.000")
    assert c.details["direction"] == "up"
    assert c.details["buy_platform"] == "polymarket"
    assert c.details["sell_platform"] == "kalshi"
    assert c.details["spread"] == "0.1000"


@pytest.mark.asyncio
async def test_no_signal_when_spread_below_threshold(session):
    """Two platforms, spread < threshold -> no signal."""
    slug = "will it snow"
    m1 = make_market(session, platform="polymarket", question="Will it snow?", question_slug=slug)
    m2 = make_market(session, platform="kalshi", question="Will it snow?", question_slug=slug)
    await session.flush()

    o1 = make_outcome(session, m1.id, name="Yes")
    o2 = make_outcome(session, m2.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, o1.id, price=0.50, captured_at=now)
    make_price_snapshot(session, o2.id, price=0.52, captured_at=now)  # spread = 0.02 < 0.04
    await session.flush()

    from app.signals.arbitrage import ArbitrageDetector
    detector = ArbitrageDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_no_signal_single_platform(session):
    """Only one platform has the market -> no signal."""
    slug = "will bitcoin hit 100k"
    make_market(session, platform="polymarket", question="Will Bitcoin hit 100k?", question_slug=slug)
    await session.flush()

    from app.signals.arbitrage import ArbitrageDetector
    detector = ArbitrageDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_signal_at_exact_threshold(session):
    """Spread exactly at threshold -> signal generated (boundary condition)."""
    slug = "will eth flip btc"
    m1 = make_market(session, platform="polymarket", question="Will ETH flip BTC?", question_slug=slug)
    m2 = make_market(session, platform="kalshi", question="Will ETH flip BTC?", question_slug=slug)
    await session.flush()

    o1 = make_outcome(session, m1.id, name="Yes")
    o2 = make_outcome(session, m2.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, o1.id, price=0.50, captured_at=now)
    make_price_snapshot(session, o2.id, price=0.54, captured_at=now)  # spread = 0.04 == threshold
    await session.flush()

    from app.signals.arbitrage import ArbitrageDetector
    detector = ArbitrageDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 1


@pytest.mark.asyncio
async def test_score_scaling(session):
    """Wider spread scores higher than narrower spread."""
    slug_narrow = "narrow spread market"
    m1n = make_market(session, platform="polymarket", question="Narrow spread?", question_slug=slug_narrow)
    m2n = make_market(session, platform="kalshi", question="Narrow spread?", question_slug=slug_narrow)

    slug_wide = "wide spread market"
    m1w = make_market(session, platform="polymarket", question="Wide spread?", question_slug=slug_wide)
    m2w = make_market(session, platform="kalshi", question="Wide spread?", question_slug=slug_wide)
    await session.flush()

    o1n = make_outcome(session, m1n.id, name="Yes")
    o2n = make_outcome(session, m2n.id, name="Yes")
    o1w = make_outcome(session, m1w.id, name="Yes")
    o2w = make_outcome(session, m2w.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    # Narrow: 4-point spread
    make_price_snapshot(session, o1n.id, price=0.50, captured_at=now)
    make_price_snapshot(session, o2n.id, price=0.54, captured_at=now)
    # Wide: 10-point spread
    make_price_snapshot(session, o1w.id, price=0.40, captured_at=now)
    make_price_snapshot(session, o2w.id, price=0.50, captured_at=now)
    await session.flush()

    from app.signals.arbitrage import ArbitrageDetector
    detector = ArbitrageDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 2
    scores = {c.details["question_slug"]: c.signal_score for c in candidates}
    assert scores[slug_wide] > scores[slug_narrow]


@pytest.mark.asyncio
async def test_arb_disabled(session):
    """When arb_enabled is False, no signals are generated."""
    slug = "disabled test"
    m1 = make_market(session, platform="polymarket", question="Disabled test?", question_slug=slug)
    m2 = make_market(session, platform="kalshi", question="Disabled test?", question_slug=slug)
    await session.flush()

    o1 = make_outcome(session, m1.id, name="Yes")
    o2 = make_outcome(session, m2.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, o1.id, price=0.30, captured_at=now)
    make_price_snapshot(session, o2.id, price=0.60, captured_at=now)
    await session.flush()

    from app.signals.arbitrage import ArbitrageDetector
    with patch("app.signals.arbitrage.settings") as mock_settings:
        mock_settings.arb_enabled = False
        detector = ArbitrageDetector()
        candidates = await detector.detect(session)

    assert len(candidates) == 0


@pytest.mark.asyncio
async def test_inactive_market_excluded(session):
    """Inactive markets should not produce arbitrage signals."""
    slug = "inactive market test"
    m1 = make_market(session, platform="polymarket", question="Inactive?", question_slug=slug, active=True)
    m2 = make_market(session, platform="kalshi", question="Inactive?", question_slug=slug, active=False)
    await session.flush()

    o1 = make_outcome(session, m1.id, name="Yes")
    o2 = make_outcome(session, m2.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    make_price_snapshot(session, o1.id, price=0.30, captured_at=now)
    make_price_snapshot(session, o2.id, price=0.60, captured_at=now)
    await session.flush()

    from app.signals.arbitrage import ArbitrageDetector
    detector = ArbitrageDetector()
    candidates = await detector.detect(session)

    assert len(candidates) == 0
