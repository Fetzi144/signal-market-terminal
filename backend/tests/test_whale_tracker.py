"""Tests for whale tracking and SmartMoneyDetector."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from app.models.whale import WalletActivity, WalletProfile
from tests.conftest import make_market, make_outcome


def make_wallet(session, address="0xabc123def456abc123def456abc123def456abc1", **kwargs):
    defaults = dict(
        id=uuid.uuid4(),
        address=address,
        total_volume=Decimal("0"),
        trade_count=0,
        tracked=False,
    )
    defaults.update(kwargs)
    w = WalletProfile(**defaults)
    session.add(w)
    return w


def make_activity(session, wallet_id, outcome_id=None, **kwargs):
    now = datetime.now(timezone.utc)
    defaults = dict(
        id=uuid.uuid4(),
        wallet_id=wallet_id,
        outcome_id=outcome_id,
        action="buy",
        quantity=Decimal("10000"),
        notional_usd=Decimal("10000"),
        tx_hash="0x" + uuid.uuid4().hex,
        block_number=12345678,
        timestamp=now,
    )
    defaults.update(kwargs)
    a = WalletActivity(**defaults)
    session.add(a)
    return a


# --- Test 1: Large trade detected -> wallet activity persisted ---

@pytest.mark.asyncio
async def test_activity_persisted(session):
    """Creating a WalletActivity persists correctly and links to wallet."""
    wallet = make_wallet(session, address="0x1111111111111111111111111111111111111111")
    await session.flush()

    m = make_market(session)
    await session.flush()
    o = make_outcome(session, m.id)
    await session.flush()

    activity = make_activity(session, wallet.id, outcome_id=o.id, notional_usd=Decimal("15000"))
    await session.flush()

    from sqlalchemy import select
    result = await session.execute(
        select(WalletActivity).where(WalletActivity.id == activity.id)
    )
    persisted = result.scalar_one()
    assert persisted.notional_usd == Decimal("15000")
    assert persisted.wallet_id == wallet.id
    assert persisted.outcome_id == o.id


# --- Test 2: Wallet exceeds volume + win rate -> auto-tracked ---

@pytest.mark.asyncio
async def test_auto_track_wallet_on_volume_threshold(session):
    """Wallet with volume > threshold and win_rate > threshold gets auto-tracked."""
    wallet = make_wallet(
        session,
        address="0x2222222222222222222222222222222222222222",
        total_volume=Decimal("120000"),
        win_rate=Decimal("0.6000"),
        tracked=False,
    )
    await session.flush()

    from app.tracking.whale_tracker import _auto_track_wallet
    result = await _auto_track_wallet(wallet)

    assert result is True
    assert wallet.tracked is True


# --- Test 3: Wallet below threshold stays untracked ---

@pytest.mark.asyncio
async def test_wallet_below_threshold_not_tracked(session):
    """Wallet with volume below threshold stays untracked."""
    wallet = make_wallet(
        session,
        address="0x3333333333333333333333333333333333333333",
        total_volume=Decimal("50000"),
        win_rate=Decimal("0.7000"),
        tracked=False,
    )
    await session.flush()

    from app.tracking.whale_tracker import _auto_track_wallet
    result = await _auto_track_wallet(wallet)

    assert result is False
    assert wallet.tracked is False


# --- Test 4: Tracked wallet buys > $5k -> smart_money signal generated ---

@pytest.mark.asyncio
async def test_smart_money_signal_from_tracked_whale(session):
    """Tracked wallet with large buy generates smart_money signal."""
    m = make_market(session, question="Will BTC hit 100k?")
    await session.flush()
    o = make_outcome(session, m.id, name="Yes")
    await session.flush()

    wallet = make_wallet(
        session,
        address="0x4444444444444444444444444444444444444444",
        total_volume=Decimal("200000"),
        win_rate=Decimal("0.6500"),
        trade_count=50,
        tracked=True,
    )
    await session.flush()

    now = datetime.now(timezone.utc)
    make_activity(
        session,
        wallet.id,
        outcome_id=o.id,
        action="buy",
        notional_usd=Decimal("8000"),
        timestamp=now - timedelta(minutes=5),
    )
    await session.flush()

    from app.signals.smart_money import SmartMoneyDetector
    with patch("app.signals.smart_money.settings") as mock_settings:
        mock_settings.whale_tracking_enabled = True
        mock_settings.whale_signal_min_trade_usd = 5000
        detector = SmartMoneyDetector()
        candidates = await detector.detect(session)

    assert len(candidates) == 1
    c = candidates[0]
    assert c.signal_type == "smart_money"
    assert c.details["wallet_address"] == "0x4444444444444444444444444444444444444444"
    assert c.details["action"] == "buy"
    assert c.details["direction"] == "up"
    assert c.details["market_question"] == "Will BTC hit 100k?"


# --- Test 5: Untracked wallet -> no signal regardless of trade size ---

@pytest.mark.asyncio
async def test_untracked_wallet_no_signal(session):
    """Untracked wallet does not generate signal even with large trade."""
    m = make_market(session, question="Will ETH hit 10k?")
    await session.flush()
    o = make_outcome(session, m.id, name="Yes")
    await session.flush()

    wallet = make_wallet(
        session,
        address="0x5555555555555555555555555555555555555555",
        total_volume=Decimal("500000"),
        trade_count=100,
        tracked=False,  # NOT tracked
    )
    await session.flush()

    now = datetime.now(timezone.utc)
    make_activity(
        session,
        wallet.id,
        outcome_id=o.id,
        action="buy",
        notional_usd=Decimal("50000"),
        timestamp=now - timedelta(minutes=2),
    )
    await session.flush()

    from app.signals.smart_money import SmartMoneyDetector
    with patch("app.signals.smart_money.settings") as mock_settings:
        mock_settings.whale_tracking_enabled = True
        mock_settings.whale_signal_min_trade_usd = 5000
        detector = SmartMoneyDetector()
        candidates = await detector.detect(session)

    assert len(candidates) == 0


# --- Test 6: Duplicate tx_hash -> idempotent ---

@pytest.mark.asyncio
async def test_duplicate_tx_hash_rejected(session):
    """Attempting to insert duplicate tx_hash raises integrity error."""
    wallet = make_wallet(session, address="0x6666666666666666666666666666666666666666")
    await session.flush()

    tx_hash = "0xdeadbeef" + "0" * 56
    make_activity(session, wallet.id, tx_hash=tx_hash)
    await session.flush()

    # Second insert with same tx_hash should fail
    from sqlalchemy.exc import IntegrityError
    with pytest.raises(IntegrityError):
        make_activity(session, wallet.id, tx_hash=tx_hash)
        await session.flush()


# --- Test 7: Detector disabled -> no signals ---

@pytest.mark.asyncio
async def test_smart_money_disabled(session):
    """When whale_tracking_enabled is False, no signals are generated."""
    from app.signals.smart_money import SmartMoneyDetector
    with patch("app.signals.smart_money.settings") as mock_settings:
        mock_settings.whale_tracking_enabled = False
        detector = SmartMoneyDetector()
        candidates = await detector.detect(session)

    assert len(candidates) == 0


# --- Test 8: Sell action produces direction=down ---

@pytest.mark.asyncio
async def test_sell_action_direction_down(session):
    """Sell action from tracked wallet produces direction=down signal."""
    m = make_market(session, question="Will SOL hit 500?")
    await session.flush()
    o = make_outcome(session, m.id, name="Yes")
    await session.flush()

    wallet = make_wallet(
        session,
        address="0x7777777777777777777777777777777777777777",
        total_volume=Decimal("300000"),
        win_rate=Decimal("0.7000"),
        trade_count=80,
        tracked=True,
    )
    await session.flush()

    now = datetime.now(timezone.utc)
    make_activity(
        session,
        wallet.id,
        outcome_id=o.id,
        action="sell",
        notional_usd=Decimal("12000"),
        timestamp=now - timedelta(minutes=3),
    )
    await session.flush()

    from app.signals.smart_money import SmartMoneyDetector
    with patch("app.signals.smart_money.settings") as mock_settings:
        mock_settings.whale_tracking_enabled = True
        mock_settings.whale_signal_min_trade_usd = 5000
        detector = SmartMoneyDetector()
        candidates = await detector.detect(session)

    assert len(candidates) == 1
    assert candidates[0].details["direction"] == "down"
    assert candidates[0].details["action"] == "sell"
