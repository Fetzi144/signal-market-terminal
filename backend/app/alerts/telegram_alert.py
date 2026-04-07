"""Telegram alerter: send message via Telegram Bot API."""
import logging

import httpx

from app.alerts.base import BaseAlerter
from app.config import settings
from app.models.signal import Signal

logger = logging.getLogger("smt.alerts.telegram")

TELEGRAM_API = "https://api.telegram.org"


class TelegramAlerter(BaseAlerter):
    async def send(self, signal: Signal, market_question: str) -> None:
        token = settings.alert_telegram_bot_token
        chat_id = settings.alert_telegram_chat_id
        if not token or not chat_id:
            return

        details = signal.details or {}
        direction = details.get("direction", "")
        outcome = details.get("outcome_name", "")
        direction_emoji = "\u2b06" if direction == "up" else ("\u2b07" if direction == "down" else "\u26a0")

        text = (
            f"{direction_emoji} <b>Signal: {signal.signal_type}</b>\n"
            f"Rank: {signal.rank_score:.2f} | Score: {signal.signal_score:.2f}\n"
            f"Market: {market_question[:100]}\n"
            f"Outcome: {outcome} | Direction: {direction}\n"
            f"Price: {signal.price_at_fire}"
        )

        url = f"{TELEGRAM_API}/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code != 200:
                    logger.warning("Telegram API returned %d: %s", resp.status_code, resp.text[:200])
                else:
                    logger.info("Telegram alert sent for signal %s", signal.id)
        except httpx.HTTPError as e:
            logger.error("Telegram alert failed for signal %s: %s", signal.id, e)
