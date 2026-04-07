"""Tests for Discord alerter."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import httpx
import pytest
import respx

from app.models.signal import Signal


def _make_signal(**kwargs) -> Signal:
    defaults = dict(
        id=uuid.uuid4(),
        signal_type="price_move",
        market_id=uuid.uuid4(),
        outcome_id=uuid.uuid4(),
        fired_at=datetime.now(timezone.utc),
        dedupe_bucket=datetime.now(timezone.utc),
        signal_score=Decimal("0.800"),
        confidence=Decimal("0.900"),
        rank_score=Decimal("0.850"),
        details={"direction": "up", "outcome_name": "Yes", "market_question": "Test?"},
        price_at_fire=Decimal("0.500000"),
        resolved=False,
        alerted=False,
    )
    defaults.update(kwargs)
    return Signal(**defaults)


@pytest.mark.asyncio
class TestDiscordAlerter:
    async def test_discord_sends_embed(self):
        signal = _make_signal()
        with respx.mock:
            route = respx.post("https://discord.com/api/webhooks/test/hook").mock(
                return_value=httpx.Response(204)
            )
            with patch("app.alerts.discord_alert.settings") as mock_settings:
                mock_settings.alert_discord_webhook_url = "https://discord.com/api/webhooks/test/hook"
                from app.alerts.discord_alert import DiscordAlerter
                alerter = DiscordAlerter()
                await alerter.send(signal, "Will BTC hit 100k?")

            assert route.called
            import json
            body = json.loads(route.calls[0].request.content)
            embed = body["embeds"][0]
            assert "Price Move" in embed["title"]
            assert embed["color"] == 0x2ECC71  # green for rank > 0.8
            field_names = [f["name"] for f in embed["fields"]]
            assert "Market" in field_names
            assert "Rank Score" in field_names
            assert "Confidence" in field_names

    async def test_discord_yellow_color_for_medium_rank(self):
        signal = _make_signal(rank_score=Decimal("0.700"))
        with respx.mock:
            route = respx.post("https://discord.com/api/webhooks/test/hook").mock(
                return_value=httpx.Response(204)
            )
            with patch("app.alerts.discord_alert.settings") as mock_settings:
                mock_settings.alert_discord_webhook_url = "https://discord.com/api/webhooks/test/hook"
                from app.alerts.discord_alert import DiscordAlerter
                alerter = DiscordAlerter()
                await alerter.send(signal, "Test market")

            import json
            body = json.loads(route.calls[0].request.content)
            assert body["embeds"][0]["color"] == 0xF1C40F  # yellow

    async def test_discord_red_color_for_low_rank(self):
        signal = _make_signal(rank_score=Decimal("0.500"))
        with respx.mock:
            route = respx.post("https://discord.com/api/webhooks/test/hook").mock(
                return_value=httpx.Response(204)
            )
            with patch("app.alerts.discord_alert.settings") as mock_settings:
                mock_settings.alert_discord_webhook_url = "https://discord.com/api/webhooks/test/hook"
                from app.alerts.discord_alert import DiscordAlerter
                alerter = DiscordAlerter()
                await alerter.send(signal, "Test market")

            import json
            body = json.loads(route.calls[0].request.content)
            assert body["embeds"][0]["color"] == 0xE74C3C  # red

    async def test_discord_empty_url_skips(self):
        signal = _make_signal()
        with patch("app.alerts.discord_alert.settings") as mock_settings:
            mock_settings.alert_discord_webhook_url = ""
            from app.alerts.discord_alert import DiscordAlerter
            alerter = DiscordAlerter()
            # Should return without error
            await alerter.send(signal, "Test market")

    async def test_discord_arbitrage_signal_includes_spread(self):
        signal = _make_signal(
            signal_type="arbitrage",
            details={
                "direction": "up",
                "outcome_name": "Yes",
                "market_question": "Test?",
                "platform_a": "polymarket",
                "platform_b": "kalshi",
                "price_a": 0.55,
                "price_b": 0.50,
                "spread": 0.05,
            },
        )
        with respx.mock:
            route = respx.post("https://discord.com/api/webhooks/test/hook").mock(
                return_value=httpx.Response(204)
            )
            with patch("app.alerts.discord_alert.settings") as mock_settings:
                mock_settings.alert_discord_webhook_url = "https://discord.com/api/webhooks/test/hook"
                from app.alerts.discord_alert import DiscordAlerter
                alerter = DiscordAlerter()
                await alerter.send(signal, "BTC price arb")

            import json
            body = json.loads(route.calls[0].request.content)
            field_names = [f["name"] for f in body["embeds"][0]["fields"]]
            assert "Arb Spread" in field_names
            arb_field = next(f for f in body["embeds"][0]["fields"] if f["name"] == "Arb Spread")
            assert "polymarket" in arb_field["value"]
            assert "kalshi" in arb_field["value"]
