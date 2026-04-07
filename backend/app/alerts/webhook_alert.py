"""Webhook alerter: POST JSON payload to a configurable URL with optional HMAC signing."""
import hashlib
import hmac
import json
import logging

import httpx

from app.alerts.base import BaseAlerter
from app.config import settings
from app.models.signal import Signal

logger = logging.getLogger("smt.alerts.webhook")


class WebhookAlerter(BaseAlerter):
    async def send(self, signal: Signal, market_question: str) -> None:
        url = settings.alert_webhook_url
        if not url:
            return

        payload = {
            "signal_type": signal.signal_type,
            "rank_score": float(signal.rank_score),
            "signal_score": float(signal.signal_score),
            "confidence": float(signal.confidence),
            "market_question": market_question,
            "outcome_name": (signal.details or {}).get("outcome_name", ""),
            "direction": (signal.details or {}).get("direction", ""),
            "price_at_fire": float(signal.price_at_fire) if signal.price_at_fire else None,
            "fired_at": signal.fired_at.isoformat() if signal.fired_at else None,
            "signal_id": str(signal.id),
        }

        headers: dict[str, str] = {"Content-Type": "application/json"}

        # HMAC-SHA256 signing: receiver can verify with
        #   expected = hmac.new(secret.encode(), request.body, hashlib.sha256).hexdigest()
        #   assert hmac.compare_digest(f"sha256={expected}", request.headers["X-SMT-Signature"])
        json_body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        if settings.alert_webhook_secret:
            signature = hmac.new(
                settings.alert_webhook_secret.encode(),
                json_body.encode(),
                hashlib.sha256,
            ).hexdigest()
            headers["X-SMT-Signature"] = f"sha256={signature}"

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, content=json_body, headers=headers)
                if resp.status_code >= 400:
                    logger.warning("Webhook returned %d for signal %s", resp.status_code, signal.id)
                else:
                    logger.info("Webhook sent for signal %s (status %d)", signal.id, resp.status_code)
        except httpx.HTTPError as e:
            logger.error("Webhook failed for signal %s: %s", signal.id, e)
