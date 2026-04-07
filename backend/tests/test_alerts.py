"""Tests for alerter implementations."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import httpx
import pytest
import respx

from app.models.signal import Signal


def _make_signal(**kwargs) -> Signal:
    """Create a Signal instance for testing (not persisted)."""
    defaults = dict(
        id=uuid.uuid4(),
        signal_type="price_move",
        market_id=uuid.uuid4(),
        outcome_id=uuid.uuid4(),
        fired_at=datetime.now(timezone.utc),
        dedupe_bucket=datetime.now(timezone.utc),
        signal_score=Decimal("0.800"),
        confidence=Decimal("0.900"),
        rank_score=Decimal("0.750"),
        details={"direction": "up", "outcome_name": "Yes", "market_question": "Test?"},
        price_at_fire=Decimal("0.500000"),
        resolved=False,
        alerted=False,
    )
    defaults.update(kwargs)
    return Signal(**defaults)


@pytest.mark.asyncio
class TestWebhookAlerter:
    async def test_webhook_sends_payload(self):
        signal = _make_signal()
        with respx.mock:
            route = respx.post("https://example.com/webhook").mock(
                return_value=httpx.Response(200)
            )
            with patch("app.alerts.webhook_alert.settings") as mock_settings:
                mock_settings.alert_webhook_url = "https://example.com/webhook"
                from app.alerts.webhook_alert import WebhookAlerter
                alerter = WebhookAlerter()
                await alerter.send(signal, "Will BTC hit 100k?")

            assert route.called
            payload = route.calls[0].request.content
            import json
            body = json.loads(payload)
            assert body["signal_type"] == "price_move"
            assert body["market_question"] == "Will BTC hit 100k?"
            assert body["rank_score"] == 0.75

    async def test_webhook_no_url_skips(self):
        signal = _make_signal()
        with patch("app.alerts.webhook_alert.settings") as mock_settings:
            mock_settings.alert_webhook_url = None
            from app.alerts.webhook_alert import WebhookAlerter
            alerter = WebhookAlerter()
            # Should return without error
            await alerter.send(signal, "Test market")


@pytest.mark.asyncio
class TestTelegramAlerter:
    async def test_telegram_sends_message(self):
        signal = _make_signal()
        with respx.mock:
            route = respx.post("https://api.telegram.org/botTEST_TOKEN/sendMessage").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            with patch("app.alerts.telegram_alert.settings") as mock_settings:
                mock_settings.alert_telegram_bot_token = "TEST_TOKEN"
                mock_settings.alert_telegram_chat_id = "12345"
                from app.alerts.telegram_alert import TelegramAlerter
                alerter = TelegramAlerter()
                await alerter.send(signal, "Will it rain?")

            assert route.called
            import json
            body = json.loads(route.calls[0].request.content)
            assert body["chat_id"] == "12345"
            assert "price_move" in body["text"]

    async def test_telegram_no_token_skips(self):
        signal = _make_signal()
        with patch("app.alerts.telegram_alert.settings") as mock_settings:
            mock_settings.alert_telegram_bot_token = None
            mock_settings.alert_telegram_chat_id = None
            from app.alerts.telegram_alert import TelegramAlerter
            alerter = TelegramAlerter()
            await alerter.send(signal, "Test market")


@pytest.mark.asyncio
class TestLoggerAlerter:
    async def test_logger_alerter_logs(self, caplog):
        from app.alerts.logger_alert import LoggerAlerter
        signal = _make_signal()
        alerter = LoggerAlerter()
        with caplog.at_level("INFO"):
            await alerter.send(signal, "Test market")
        assert any("signal" in r.message.lower() or "alert" in r.message.lower() for r in caplog.records)
