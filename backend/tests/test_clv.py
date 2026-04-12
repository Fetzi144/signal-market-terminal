"""Tests for CLV tracking: resolution CLV computation, backfill, and performance API."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.ingestion.backfill_clv import backfill_clv
from app.ingestion.resolution import resolve_signals
from tests.conftest import make_market, make_outcome, make_price_snapshot, make_signal


# ── Resolution CLV computation ───────��──────────────────────────────────────

@pytest.mark.asyncio
async def test_resolution_sets_clv_fields_for_winner(session):
    """Winning signal gets closing_price, resolution_price=1, positive P&L."""
    market = make_market(session, platform="polymarket", platform_id="clv-win-1")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    # Create a price snapshot (closing price = 0.75)
    make_price_snapshot(session, outcome.id, price=0.75)
    await session.flush()

    # Signal fired at price 0.50, direction up
    signal = make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.500000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "clv-win-1", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)

    assert signal.resolved_correctly is True
    assert signal.resolution_price == Decimal("1.000000")
    assert signal.closing_price == Decimal("0.75")
    assert signal.clv == Decimal("0.75") - Decimal("0.500000")  # +0.25
    assert signal.profit_loss == Decimal("1.000000") - Decimal("0.500000")  # +0.50


@pytest.mark.asyncio
async def test_resolution_sets_clv_fields_for_loser(session):
    """Losing signal gets resolution_price=0, negative P&L."""
    market = make_market(session, platform="polymarket", platform_id="clv-loss-1")
    await session.flush()
    outcome_yes = make_outcome(session, market.id, name="Yes")
    outcome_no = make_outcome(session, market.id, name="No")
    await session.flush()

    # Snapshot for No outcome at price 0.30
    make_price_snapshot(session, outcome_no.id, price=0.30)
    await session.flush()

    # Signal says "up" on No, but Yes wins → loss
    signal = make_signal(
        session, market.id, outcome_no.id,
        price_at_fire=Decimal("0.300000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "No"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "clv-loss-1", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)

    assert signal.resolved_correctly is False
    assert signal.resolution_price == Decimal("0.000000")  # No outcome lost
    assert signal.closing_price == Decimal("0.30")
    # CLV: closing - fire = 0.30 - 0.30 = 0.00
    assert signal.clv == Decimal("0.30") - Decimal("0.300000")
    # P&L: resolution - fire = 0.00 - 0.30 = -0.30
    assert signal.profit_loss == Decimal("0.000000") - Decimal("0.300000")


@pytest.mark.asyncio
async def test_resolution_clv_for_down_direction(session):
    """Down direction: outcome loses → correct, P&L = price_at_fire - resolution."""
    market = make_market(session, platform="polymarket", platform_id="clv-down-1")
    await session.flush()
    outcome_yes = make_outcome(session, market.id, name="Yes")
    outcome_no = make_outcome(session, market.id, name="No")
    await session.flush()

    # Snapshot for Yes outcome at price 0.80
    make_price_snapshot(session, outcome_yes.id, price=0.80)
    await session.flush()

    # Signal says "down" on Yes outcome, and Yes wins → incorrect (price went up)
    signal = make_signal(
        session, market.id, outcome_yes.id,
        price_at_fire=Decimal("0.600000"),
        details={"direction": "down", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "clv-down-1", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)

    assert signal.resolved_correctly is False  # down on winner = wrong
    assert signal.resolution_price == Decimal("1.000000")
    assert signal.closing_price == Decimal("0.80")
    # For down direction: P&L = price_at_fire - resolution_price
    assert signal.profit_loss == Decimal("0.600000") - Decimal("1.000000")  # -0.40


@pytest.mark.asyncio
async def test_resolution_no_snapshot_leaves_closing_price_null(session):
    """If no snapshot exists, closing_price and clv stay NULL."""
    market = make_market(session, platform="polymarket", platform_id="clv-nosnap")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    # No snapshot created!
    signal = make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.500000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "clv-nosnap", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)

    assert signal.resolved_correctly is True
    assert signal.resolution_price == Decimal("1.000000")
    assert signal.closing_price is None
    assert signal.clv is None
    # profit_loss still computed (doesn't need closing_price)
    assert signal.profit_loss == Decimal("0.500000")


@pytest.mark.asyncio
async def test_resolution_uses_latest_snapshot(session):
    """If multiple snapshots exist, uses the most recent one as closing price."""
    market = make_market(session, platform="polymarket", platform_id="clv-latest")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    now = datetime.now(timezone.utc)
    # Old snapshot at 0.40
    make_price_snapshot(session, outcome.id, price=0.40, captured_at=now - timedelta(hours=2))
    # Recent snapshot at 0.85
    make_price_snapshot(session, outcome.id, price=0.85, captured_at=now - timedelta(minutes=5))
    await session.flush()

    signal = make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.500000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "clv-latest", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)

    assert signal.closing_price == Decimal("0.85")  # latest snapshot
    assert signal.clv == Decimal("0.85") - Decimal("0.500000")  # +0.35


# ── CLV Backfill ────────────────���─────────────────────────��─────────────────

@pytest.mark.asyncio
async def test_backfill_computes_clv_for_resolved_signals(session):
    """Backfill fills in CLV for already-resolved signals missing it."""
    market = make_market(session, platform="polymarket", platform_id="bf-1")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    make_price_snapshot(session, outcome.id, price=0.80)
    await session.flush()

    # Pre-resolved signal (resolved_correctly set but no CLV fields)
    signal = make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.500000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        resolved_correctly=True,
    )
    await session.commit()

    summary = await backfill_clv(session)

    assert summary["updated"] >= 1
    await session.refresh(signal)

    assert signal.closing_price == Decimal("0.80")
    assert signal.resolution_price == Decimal("1.000000")
    assert signal.clv == Decimal("0.80") - Decimal("0.500000")
    assert signal.profit_loss == Decimal("1.000000") - Decimal("0.500000")


@pytest.mark.asyncio
async def test_backfill_skips_signals_without_snapshot(session):
    """Backfill skips signals with no snapshot data."""
    market = make_market(session, platform="polymarket", platform_id="bf-nosnap")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    # No snapshot!
    signal = make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.500000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        resolved_correctly=True,
    )
    await session.commit()

    summary = await backfill_clv(session)

    assert summary["skipped_no_snapshot"] >= 1
    await session.refresh(signal)
    assert signal.clv is None


@pytest.mark.asyncio
async def test_backfill_idempotent(session):
    """Running backfill twice doesn't re-process already-backfilled signals."""
    market = make_market(session, platform="polymarket", platform_id="bf-idem")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    make_price_snapshot(session, outcome.id, price=0.70)
    await session.flush()

    signal = make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.500000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        resolved_correctly=True,
    )
    await session.commit()

    summary1 = await backfill_clv(session)
    assert summary1["updated"] >= 1

    summary2 = await backfill_clv(session)
    assert summary2["total"] == 0  # no signals need backfill


@pytest.mark.asyncio
async def test_backfill_down_direction_loser(session):
    """Backfill for down direction signal on losing outcome → correct, resolution=0."""
    market = make_market(session, platform="polymarket", platform_id="bf-down-lose")
    await session.flush()
    outcome_yes = make_outcome(session, market.id, name="Yes")
    outcome_no = make_outcome(session, market.id, name="No")
    await session.flush()

    make_price_snapshot(session, outcome_no.id, price=0.25)
    await session.flush()

    # Down on No outcome, resolved_correctly=True means outcome lost
    # direction=down + resolved_correctly=True → outcome_won=False
    signal = make_signal(
        session, market.id, outcome_no.id,
        price_at_fire=Decimal("0.300000"),
        details={"direction": "down", "market_question": "Test?", "outcome_name": "No"},
        resolved_correctly=True,
    )
    await session.commit()

    summary = await backfill_clv(session)
    assert summary["updated"] >= 1

    await session.refresh(signal)
    assert signal.resolution_price == Decimal("0.000000")  # down + correct → outcome lost
    # P&L for down: price_at_fire - resolution_price = 0.30 - 0.00 = +0.30
    assert signal.profit_loss == Decimal("0.300000")


# ── Performance API with CLV ──────────────────────────────────────────��─────

@pytest.mark.asyncio
async def test_performance_api_includes_clv_metrics(session, client):
    """Performance summary includes CLV-related fields."""
    market = make_market(session, platform="polymarket", platform_id="perf-clv")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    # Create a resolved signal with CLV data
    make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.500000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        resolved_correctly=True,
        closing_price=Decimal("0.750000"),
        resolution_price=Decimal("1.000000"),
        clv=Decimal("0.250000"),
        profit_loss=Decimal("0.500000"),
    )
    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    data = resp.json()

    # CLV fields should be present
    assert "overall_avg_clv" in data
    assert "overall_avg_profit_loss" in data
    assert "overall_total_profit_loss" in data
    assert "overall_profit_factor" in data
    assert "hypothetical_pnl_per_share" in data
    assert "signals_with_clv" in data

    assert data["signals_with_clv"] >= 1
    assert data["overall_avg_clv"] is not None
    assert data["overall_avg_clv"] > 0

    # Per-detector data should include CLV fields
    for row in data["win_rate_by_type"]:
        assert "avg_clv" in row
        assert "avg_profit_loss" in row
        assert "profit_factor" in row
        assert "signal_quality_score" in row


@pytest.mark.asyncio
async def test_performance_api_recent_calls_include_clv(session, client):
    """Recent calls in performance summary include CLV fields."""
    market = make_market(session, platform="polymarket", platform_id="perf-rc-clv")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    make_signal(
        session, market.id, outcome.id,
        price_at_fire=Decimal("0.400000"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        resolved_correctly=True,
        closing_price=Decimal("0.600000"),
        clv=Decimal("0.200000"),
        profit_loss=Decimal("0.600000"),
    )
    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    data = resp.json()

    assert len(data["recent_calls"]) >= 1
    call = data["recent_calls"][0]
    assert "clv" in call
    assert "profit_loss" in call
    assert "price_at_fire" in call
    assert "closing_price" in call


@pytest.mark.asyncio
async def test_performance_api_graceful_with_no_clv_data(session, client):
    """Performance summary works when no signals have CLV data."""
    market = make_market(session, platform="polymarket", platform_id="perf-noclv")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    # Signal without CLV fields
    make_signal(
        session, market.id, outcome.id,
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        resolved_correctly=True,
    )
    await session.commit()

    resp = await client.get("/api/v1/performance/summary")
    assert resp.status_code == 200
    data = resp.json()

    assert data["signals_with_clv"] == 0
    assert data["overall_avg_clv"] is None
