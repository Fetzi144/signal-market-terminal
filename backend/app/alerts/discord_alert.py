"""Discord alerter: send rich embeds via Discord webhook URL."""
import logging
from datetime import timezone

import httpx

from app.alerts.base import BaseAlerter
from app.config import settings
from app.models.signal import Signal

logger = logging.getLogger("smt.alerts.discord")

# Embed colour thresholds
COLOR_GREEN = 0x2ECC71   # rank > 0.8
COLOR_YELLOW = 0xF1C40F  # 0.6–0.8
COLOR_RED = 0xE74C3C     # < 0.6


def _rank_color(rank_score: float) -> int:
    if rank_score > 0.8:
        return COLOR_GREEN
    if rank_score >= 0.6:
        return COLOR_YELLOW
    return COLOR_RED


class DiscordAlerter(BaseAlerter):
    async def send(self, signal: Signal, market_question: str) -> None:
        url = settings.alert_discord_webhook_url
        if not url:
            return

        details = signal.details or {}
        direction = details.get("direction", "")
        outcome = details.get("outcome_name", "")
        rank = float(signal.rank_score)
        score = float(signal.signal_score)
        confidence = float(signal.confidence)
        price = float(signal.price_at_fire) if signal.price_at_fire else None
        platform = details.get("platform", "")

        direction_arrow = "\u2b06" if direction == "up" else ("\u2b07" if direction == "down" else "\u26a0")
        title = f"{direction_arrow} {signal.signal_type.replace('_', ' ').title()} — {direction.upper() or 'N/A'}"

        fields = [
            {"name": "Market", "value": market_question[:256], "inline": False},
            {"name": "Rank Score", "value": f"{rank:.2f}", "inline": True},
            {"name": "Signal Score", "value": f"{score:.2f}", "inline": True},
            {"name": "Confidence", "value": f"{confidence:.2f}", "inline": True},
            {"name": "Outcome", "value": outcome or "—", "inline": True},
        ]

        if platform:
            fields.append({"name": "Platform", "value": platform, "inline": True})

        if price is not None:
            fields.append({"name": "Price", "value": f"{price:.4f}", "inline": True})

        # Arbitrage signals: show both platform prices + spread
        if signal.signal_type == "arbitrage":
            price_a = details.get("price_a")
            price_b = details.get("price_b")
            platform_a = details.get("platform_a", "")
            platform_b = details.get("platform_b", "")
            spread = details.get("spread")
            if price_a is not None and price_b is not None:
                fields.append({
                    "name": "Arb Spread",
                    "value": (
                        f"{platform_a}: {float(price_a):.4f} vs "
                        f"{platform_b}: {float(price_b):.4f}\n"
                        f"Spread: {float(spread):.4f}" if spread is not None else ""
                    ),
                    "inline": False,
                })

        fired_at = signal.fired_at
        timestamp = fired_at.isoformat() if fired_at else ""
        if fired_at and fired_at.tzinfo is None:
            timestamp = fired_at.replace(tzinfo=timezone.utc).isoformat()

        embed = {
            "title": title,
            "color": _rank_color(rank),
            "fields": fields,
            "footer": {"text": "Signal Market Terminal"},
            "timestamp": timestamp,
        }

        payload = {"embeds": [embed]}

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code >= 400:
                    logger.warning(
                        "Discord webhook returned %d for signal %s: %s",
                        resp.status_code, signal.id, resp.text[:200],
                    )
                else:
                    logger.info("Discord alert sent for signal %s", signal.id)
        except httpx.HTTPError as e:
            logger.error("Discord alert failed for signal %s: %s", signal.id, e)
