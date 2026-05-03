"""Tests for market resolution service."""

import pytest

from app.ingestion.resolution import resolve_signals
from tests.conftest import make_market, make_outcome, make_signal


@pytest.mark.asyncio
async def test_correct_direction_up_on_winner(session):
    """Signal with direction=up on winning outcome → resolved_correctly=True."""
    market = make_market(session, platform="polymarket", platform_id="pm-123")
    await session.flush()
    outcome_yes = make_outcome(session, market.id, name="Yes")
    await session.flush()

    signal = make_signal(
        session, market.id, outcome_yes.id,
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "pm-123", "winning_outcome_id": "Yes", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)
    assert signal.resolved_correctly is True


@pytest.mark.asyncio
async def test_wrong_direction_up_on_loser(session):
    """Signal with direction=up on losing outcome → resolved_correctly=False."""
    market = make_market(session, platform="polymarket", platform_id="pm-456")
    await session.flush()
    _outcome_yes = make_outcome(session, market.id, name="Yes")
    outcome_no = make_outcome(session, market.id, name="No")
    await session.flush()

    # Signal says "up" on No outcome, but Yes wins
    signal = make_signal(
        session, market.id, outcome_no.id,
        details={"direction": "up", "market_question": "Test?", "outcome_name": "No"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "pm-456", "winning_outcome_id": "Yes", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)
    assert signal.resolved_correctly is False


@pytest.mark.asyncio
async def test_direction_down_on_loser_is_correct(session):
    """Signal with direction=down on losing outcome → resolved_correctly=True."""
    market = make_market(session, platform="polymarket", platform_id="pm-789")
    await session.flush()
    _outcome_yes = make_outcome(session, market.id, name="Yes")
    outcome_no = make_outcome(session, market.id, name="No")
    await session.flush()

    # Signal says "down" on No outcome, and Yes wins (No loses) → correct
    signal = make_signal(
        session, market.id, outcome_no.id,
        details={"direction": "down", "market_question": "Test?", "outcome_name": "No"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "pm-789", "winning_outcome_id": "Yes", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 1
    await session.refresh(signal)
    assert signal.resolved_correctly is True


@pytest.mark.asyncio
async def test_non_directional_signal_stays_null(session):
    """Signal without direction in details → resolved_correctly stays NULL."""
    market = make_market(session, platform="polymarket", platform_id="pm-nd")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    signal = make_signal(
        session, market.id, outcome.id,
        signal_type="spread_change",
        details={"market_question": "Test?", "outcome_name": "Yes"},  # no direction
    )
    await session.commit()

    resolved_markets = [{"platform_id": "pm-nd", "winning_outcome_id": "Yes", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)

    assert count == 0
    await session.refresh(signal)
    assert signal.resolved_correctly is None


@pytest.mark.asyncio
async def test_market_not_yet_resolved(session):
    """Signal on unresolved market → unchanged."""
    market = make_market(session, platform="polymarket", platform_id="pm-open")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    signal = make_signal(
        session, market.id, outcome.id,
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    # Empty resolved list — market still open
    count = await resolve_signals(session, "polymarket", [])

    assert count == 0
    await session.refresh(signal)
    assert signal.resolved_correctly is None


@pytest.mark.asyncio
async def test_idempotent_resolution(session):
    """Running resolution twice doesn't double-update."""
    market = make_market(session, platform="polymarket", platform_id="pm-idem")
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes")
    await session.flush()

    signal = make_signal(
        session, market.id, outcome.id,
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "pm-idem", "winning_outcome_id": "Yes", "winner": "Yes"}]

    count1 = await resolve_signals(session, "polymarket", resolved_markets)
    assert count1 == 1

    # Second run — signal already resolved, should skip
    count2 = await resolve_signals(session, "polymarket", resolved_markets)
    assert count2 == 0

    await session.refresh(signal)
    assert signal.resolved_correctly is True


@pytest.mark.asyncio
async def test_kalshi_resolution(session):
    """Kalshi resolution: winning_outcome='yes' maps to Yes outcome."""
    market = make_market(session, platform="kalshi", platform_id="KTEST-123")
    await session.flush()
    outcome_yes = make_outcome(session, market.id, name="Yes")
    _outcome_no = make_outcome(session, market.id, name="No")
    await session.flush()

    signal = make_signal(
        session, market.id, outcome_yes.id,
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    resolved_markets = [{"platform_id": "KTEST-123", "winning_outcome": "yes"}]
    count = await resolve_signals(session, "kalshi", resolved_markets)

    assert count == 1
    await session.refresh(signal)
    assert signal.resolved_correctly is True


@pytest.mark.asyncio
async def test_kalshi_resolution_uses_platform_outcome_side(session):
    """Kalshi resolution maps sides encoded in platform_outcome_id."""
    market = make_market(session, platform="kalshi", platform_id="KTEST-PLATFORM-SIDE")
    await session.flush()
    outcome_yes = make_outcome(
        session,
        market.id,
        name="KTEST-PLATFORM-SIDE",
        platform_outcome_id="KTEST-PLATFORM-SIDE_yes",
        token_id="KTEST-PLATFORM-SIDE:yes",
    )
    _outcome_no = make_outcome(
        session,
        market.id,
        name="KTEST-PLATFORM-SIDE",
        platform_outcome_id="KTEST-PLATFORM-SIDE_no",
        token_id="KTEST-PLATFORM-SIDE:no",
    )
    await session.flush()

    signal = make_signal(
        session,
        market.id,
        outcome_yes.id,
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    count = await resolve_signals(
        session,
        "kalshi",
        [{"platform_id": "KTEST-PLATFORM-SIDE", "winning_outcome": "yes"}],
    )

    assert count == 1
    await session.refresh(signal)
    assert signal.resolved_correctly is True


@pytest.mark.asyncio
async def test_kalshi_resolution_uses_token_id_side(session):
    """Kalshi resolution maps sides encoded only in token_id."""
    market = make_market(session, platform="kalshi", platform_id="KTEST-TOKEN-SIDE")
    await session.flush()
    outcome_yes = make_outcome(
        session,
        market.id,
        name="Contract",
        platform_outcome_id="contract_yes_side",
        token_id="KTEST-TOKEN-SIDE:yes",
    )
    _outcome_no = make_outcome(
        session,
        market.id,
        name="Contract",
        platform_outcome_id="contract_no_side",
        token_id="KTEST-TOKEN-SIDE:no",
    )
    await session.flush()

    signal = make_signal(
        session,
        market.id,
        outcome_yes.id,
        details={"direction": "down", "market_question": "Test?", "outcome_name": "Yes"},
    )
    await session.commit()

    count = await resolve_signals(
        session,
        "kalshi",
        [{"platform_id": "KTEST-TOKEN-SIDE", "winning_outcome": "no"}],
    )

    assert count == 1
    await session.refresh(signal)
    assert signal.resolved_correctly is True


@pytest.mark.asyncio
async def test_unknown_market_ignored(session):
    """Resolution for a market not in our DB → skipped."""
    resolved_markets = [{"platform_id": "nonexistent-999", "winner": "Yes"}]
    count = await resolve_signals(session, "polymarket", resolved_markets)
    assert count == 0
