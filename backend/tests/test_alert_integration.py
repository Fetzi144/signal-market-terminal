"""Integration test: Alert delivery — signal → alerters called + alerted flag set."""
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.signal import Signal
from tests.conftest import make_market, make_outcome, make_signal


@pytest.mark.asyncio
async def test_alert_high_rank_signal_triggers_alerters(session: AsyncSession):
    """A high-rank signal triggers alerters and marks signal as alerted."""
    market = make_market(session, question="Will SOL hit $300?")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session, market.id, outcome.id,
        rank_score=Decimal("0.850"),
        signal_score=Decimal("0.900"),
        confidence=Decimal("0.950"),
        alerted=False,
    )
    await session.commit()

    mock_alerter = MagicMock()
    mock_alerter.send = AsyncMock()

    with patch("app.jobs.scheduler.settings") as mock_settings, \
         patch("app.jobs.scheduler._build_alerters", return_value=[mock_alerter]):
        mock_settings.alert_rank_threshold = 0.7
        mock_settings.alert_batch_limit = 20

        from app.jobs.scheduler import _alert_high_rank_signals
        await _alert_high_rank_signals(session)

    # Verify signal is now marked alerted
    refreshed = await session.get(Signal, signal.id)
    assert refreshed.alerted is True

    # Verify alerter was called with signal data
    mock_alerter.send.assert_called_once()
    call_args = mock_alerter.send.call_args
    sent_signal = call_args[0][0]
    sent_question = call_args[0][1]
    assert str(sent_signal.id) == str(signal.id)
    assert sent_question == "Will SOL hit $300?"


@pytest.mark.asyncio
async def test_alert_below_threshold_not_alerted(session: AsyncSession):
    """A signal below the rank threshold is NOT alerted."""
    market = make_market(session, question="Low rank test")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session, market.id, outcome.id,
        rank_score=Decimal("0.300"),
        alerted=False,
    )
    await session.commit()

    mock_alerter = MagicMock()
    mock_alerter.send = AsyncMock()

    with patch("app.jobs.scheduler.settings") as mock_settings, \
         patch("app.jobs.scheduler._build_alerters", return_value=[mock_alerter]):
        mock_settings.alert_rank_threshold = 0.7
        mock_settings.alert_batch_limit = 20

        from app.jobs.scheduler import _alert_high_rank_signals
        await _alert_high_rank_signals(session)

    refreshed = await session.get(Signal, signal.id)
    assert refreshed.alerted is False
    mock_alerter.send.assert_not_called()


@pytest.mark.asyncio
async def test_alert_already_alerted_not_sent_twice(session: AsyncSession):
    """A signal that was already alerted is not sent again."""
    market = make_market(session, question="Already alerted test")
    outcome = make_outcome(session, market.id, name="Yes")
    make_signal(
        session, market.id, outcome.id,
        rank_score=Decimal("0.900"),
        alerted=True,  # Already alerted
    )
    await session.commit()

    mock_alerter = MagicMock()
    mock_alerter.send = AsyncMock()

    with patch("app.jobs.scheduler.settings") as mock_settings, \
         patch("app.jobs.scheduler._build_alerters", return_value=[mock_alerter]):
        mock_settings.alert_rank_threshold = 0.7
        mock_settings.alert_batch_limit = 20

        from app.jobs.scheduler import _alert_high_rank_signals
        await _alert_high_rank_signals(session)

    # Alerter should NOT have been called
    mock_alerter.send.assert_not_called()


@pytest.mark.asyncio
async def test_alert_discord_payload_format(session: AsyncSession):
    """Discord alerter sends correctly structured webhook payload."""
    market = make_market(session, question="Discord format test")
    outcome = make_outcome(session, market.id, name="Yes")
    signal = make_signal(
        session, market.id, outcome.id,
        rank_score=Decimal("0.850"),
        signal_score=Decimal("0.700"),
        confidence=Decimal("0.900"),
        alerted=False,
    )
    await session.commit()

    from app.alerts.discord_alert import DiscordAlerter

    alerter = DiscordAlerter()

    with patch("app.alerts.discord_alert.settings") as mock_settings, \
         patch("app.alerts.discord_alert.httpx.AsyncClient") as mock_httpx:
        mock_settings.alert_discord_webhook_url = "https://discord.com/api/webhooks/test"

        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 204
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_httpx.return_value = mock_client

        await alerter.send(signal, "Discord format test")

    mock_client.post.assert_called_once()
    call_kwargs = mock_client.post.call_args
    payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "title" in embed
    assert "fields" in embed
    field_names = [f["name"] for f in embed["fields"]]
    assert "Market" in field_names
    assert "Rank Score" in field_names
    assert "Signal Score" in field_names
    assert "Confidence" in field_names


@pytest.mark.asyncio
async def test_multiple_signals_batch_alerted(session: AsyncSession):
    """Multiple high-rank signals are alerted in one batch."""
    market = make_market(session, question="Batch alert test")
    outcome = make_outcome(session, market.id, name="Yes")

    now = datetime.now(timezone.utc)
    signals = []
    for i in range(3):
        sig = make_signal(
            session, market.id, outcome.id,
            rank_score=Decimal("0.800"),
            alerted=False,
            dedupe_bucket=now.replace(minute=((now.minute // 15) * 15 - i * 15) % 60,
                                      second=0, microsecond=0),
        )
        signals.append(sig)
    await session.commit()

    mock_alerter = MagicMock()
    mock_alerter.send = AsyncMock()

    with patch("app.jobs.scheduler.settings") as mock_settings, \
         patch("app.jobs.scheduler._build_alerters", return_value=[mock_alerter]):
        mock_settings.alert_rank_threshold = 0.7
        mock_settings.alert_batch_limit = 20

        from app.jobs.scheduler import _alert_high_rank_signals
        await _alert_high_rank_signals(session)

    assert mock_alerter.send.call_count == 3

    for sig in signals:
        refreshed = await session.get(Signal, sig.id)
        assert refreshed.alerted is True
