"""Tests for push notification alerter and subscription API."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.models.signal import Signal
from tests.conftest import make_market


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
class TestPushSubscriptionAPI:
    async def test_subscribe_stores_subscription(self, client, session):
        resp = await client.post("/api/v1/push/subscribe", json={
            "endpoint": "https://push.example.com/sub/123",
            "keys": {"p256dh": "test-p256dh-key", "auth": "test-auth-key"},
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "subscribed"

        # Verify stored in DB
        from sqlalchemy import select
        from app.models.push_subscription import PushSubscription
        result = await session.execute(select(PushSubscription))
        subs = result.scalars().all()
        assert len(subs) == 1
        assert subs[0].endpoint == "https://push.example.com/sub/123"
        assert subs[0].keys["p256dh"] == "test-p256dh-key"

    async def test_subscribe_upserts_existing(self, client, session):
        # Subscribe twice with same endpoint
        await client.post("/api/v1/push/subscribe", json={
            "endpoint": "https://push.example.com/sub/456",
            "keys": {"p256dh": "key1", "auth": "auth1"},
        })
        await client.post("/api/v1/push/subscribe", json={
            "endpoint": "https://push.example.com/sub/456",
            "keys": {"p256dh": "key2", "auth": "auth2"},
        })

        from sqlalchemy import select
        from app.models.push_subscription import PushSubscription
        result = await session.execute(select(PushSubscription))
        subs = result.scalars().all()
        assert len(subs) == 1
        assert subs[0].keys["p256dh"] == "key2"

    async def test_unsubscribe_removes(self, client, session):
        await client.post("/api/v1/push/subscribe", json={
            "endpoint": "https://push.example.com/sub/789",
            "keys": {"p256dh": "key", "auth": "auth"},
        })
        resp = await client.request("DELETE", "/api/v1/push/subscribe", json={
            "endpoint": "https://push.example.com/sub/789",
            "keys": {},
        })
        assert resp.status_code == 200
        assert resp.json()["status"] == "unsubscribed"

        from sqlalchemy import select
        from app.models.push_subscription import PushSubscription
        result = await session.execute(select(PushSubscription))
        subs = result.scalars().all()
        assert len(subs) == 0

    async def test_vapid_key_endpoint(self, client):
        resp = await client.get("/api/v1/push/vapid-key")
        assert resp.status_code == 200
        assert "vapid_public_key" in resp.json()


@pytest.mark.asyncio
class TestPushAlerter:
    async def test_push_skips_when_no_vapid_keys(self):
        from unittest.mock import patch
        signal = _make_signal()
        with patch("app.alerts.push_alert.settings") as mock_settings:
            mock_settings.push_vapid_private_key = ""
            mock_settings.push_vapid_public_key = ""
            from app.alerts.push_alert import PushAlerter
            alerter = PushAlerter()
            # Should return without error
            await alerter.send(signal, "Test market")
