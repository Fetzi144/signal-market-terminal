"""Tests for HMAC-signed webhook alerts."""
import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from app.alerts.webhook_alert import WebhookAlerter
from app.models.signal import Signal


def _make_signal():
    """Create a minimal Signal-like object for testing."""
    s = Signal(
        id=uuid.uuid4(),
        signal_type="price_move",
        market_id=uuid.uuid4(),
        outcome_id=uuid.uuid4(),
        fired_at=datetime.now(timezone.utc),
        dedupe_bucket=datetime.now(timezone.utc),
        signal_score=Decimal("0.800"),
        confidence=Decimal("0.900"),
        rank_score=Decimal("0.720"),
        details={"direction": "up", "outcome_name": "Yes"},
        price_at_fire=Decimal("0.550000"),
        resolved=False,
    )
    return s


@pytest.mark.asyncio
async def test_webhook_hmac_signature_present_when_secret_set():
    """X-SMT-Signature header is present and correct when webhook secret is configured."""
    signal = _make_signal()
    alerter = WebhookAlerter()

    captured_headers = {}

    class FakeResponse:
        status_code = 200

    async def fake_post(url, content=None, headers=None, **kwargs):
        captured_headers.update(headers or {})
        return FakeResponse()

    fake_client = AsyncMock()
    fake_client.post = fake_post
    fake_client.__aenter__ = AsyncMock(return_value=fake_client)
    fake_client.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("app.alerts.webhook_alert.settings") as mock_settings,
        patch("app.alerts.webhook_alert.httpx.AsyncClient", return_value=fake_client),
    ):
        mock_settings.alert_webhook_url = "https://example.com/hook"
        mock_settings.alert_webhook_secret = "my-test-secret"

        await alerter.send(signal, "Will it rain?")

    # Verify header exists
    assert "X-SMT-Signature" in captured_headers
    sig_header = captured_headers["X-SMT-Signature"]
    assert sig_header.startswith("sha256=")

    # Verify the HMAC is correct
    payload = {
        "signal_type": signal.signal_type,
        "rank_score": float(signal.rank_score),
        "signal_score": float(signal.signal_score),
        "confidence": float(signal.confidence),
        "market_question": "Will it rain?",
        "outcome_name": "Yes",
        "direction": "up",
        "price_at_fire": float(signal.price_at_fire),
        "fired_at": signal.fired_at.isoformat(),
        "signal_id": str(signal.id),
    }
    json_body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    expected = hmac.new(b"my-test-secret", json_body.encode(), hashlib.sha256).hexdigest()
    assert sig_header == f"sha256={expected}"


@pytest.mark.asyncio
async def test_webhook_no_signature_when_secret_empty():
    """X-SMT-Signature header is absent when no webhook secret is configured."""
    signal = _make_signal()
    alerter = WebhookAlerter()

    captured_headers = {}

    class FakeResponse:
        status_code = 200

    async def fake_post(url, content=None, headers=None, **kwargs):
        captured_headers.update(headers or {})
        return FakeResponse()

    fake_client = AsyncMock()
    fake_client.post = fake_post
    fake_client.__aenter__ = AsyncMock(return_value=fake_client)
    fake_client.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("app.alerts.webhook_alert.settings") as mock_settings,
        patch("app.alerts.webhook_alert.httpx.AsyncClient", return_value=fake_client),
    ):
        mock_settings.alert_webhook_url = "https://example.com/hook"
        mock_settings.alert_webhook_secret = ""

        await alerter.send(signal, "Will it rain?")

    assert "X-SMT-Signature" not in captured_headers
