"""Push notification alerter: send Web Push notifications via VAPID."""
import json
import logging

from app.alerts.base import BaseAlerter
from app.config import settings
from app.models.signal import Signal

logger = logging.getLogger("smt.alerts.push")


class PushAlerter(BaseAlerter):
    """Sends Web Push notifications to all stored subscriptions."""

    async def send(self, signal: Signal, market_question: str) -> None:
        if not settings.push_vapid_private_key or not settings.push_vapid_public_key:
            return

        try:
            from pywebpush import WebPushException, webpush
        except ImportError:
            logger.warning("pywebpush not installed — skipping push notifications")
            return

        from app.db import async_session
        from app.models.push_subscription import PushSubscription

        from sqlalchemy import select

        details = signal.details or {}
        direction = details.get("direction", "")
        direction_arrow = "\u2b06" if direction == "up" else ("\u2b07" if direction == "down" else "\u26a0")

        payload = json.dumps({
            "title": f"{direction_arrow} {signal.signal_type.replace('_', ' ').title()}",
            "body": (
                f"{market_question[:100]}\n"
                f"Rank: {float(signal.rank_score):.2f} | "
                f"Price: {float(signal.price_at_fire) if signal.price_at_fire else 'N/A'}"
            ),
            "icon": "/favicon.ico",
            "url": f"/signals/{signal.id}",
        })

        vapid_claims = {
            "sub": f"mailto:{settings.push_vapid_email}",
        }

        async with async_session() as session:
            result = await session.execute(select(PushSubscription))
            subscriptions = result.scalars().all()

        expired_endpoints = []
        sent = 0
        for sub in subscriptions:
            subscription_info = {
                "endpoint": sub.endpoint,
                "keys": sub.keys,
            }
            try:
                webpush(
                    subscription_info=subscription_info,
                    data=payload,
                    vapid_private_key=settings.push_vapid_private_key,
                    vapid_claims=vapid_claims,
                )
                sent += 1
            except WebPushException as e:
                if e.response and e.response.status_code in (404, 410):
                    expired_endpoints.append(sub.endpoint)
                    logger.info("Push subscription expired: %s", sub.endpoint[:60])
                else:
                    logger.warning("Push failed for %s: %s", sub.endpoint[:60], e)
            except Exception as e:
                logger.error("Push notification error: %s", e)

        # Clean up expired subscriptions
        if expired_endpoints:
            async with async_session() as session:
                from sqlalchemy import delete
                await session.execute(
                    delete(PushSubscription).where(
                        PushSubscription.endpoint.in_(expired_endpoints)
                    )
                )
                await session.commit()
                logger.info("Removed %d expired push subscriptions", len(expired_endpoints))

        if sent:
            logger.info("Push notifications sent for signal %s to %d subscribers", signal.id, sent)
