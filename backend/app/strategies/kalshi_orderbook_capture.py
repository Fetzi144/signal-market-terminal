from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.connectors import get_connector
from app.ingestion.snapshots import _depth_within_pct
from app.models.market import Market, Outcome
from app.models.signal import Signal
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot

logger = logging.getLogger(__name__)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _outcome_token_for_signal(session: AsyncSession, signal: Signal) -> str | None:
    if signal.source_token_id:
        return str(signal.source_token_id)
    if signal.outcome_id is None:
        return None
    token = (
        await session.execute(select(Outcome.token_id).where(Outcome.id == signal.outcome_id).limit(1))
    ).scalar_one_or_none()
    return str(token) if token else None


def signal_is_recent_enough_for_forward_orderbook(signal: Signal, now: datetime) -> bool:
    fired_at = _ensure_utc(signal.fired_at)
    if fired_at is None:
        return True
    if fired_at > now:
        return True
    max_age = max(
        1,
        int(settings.shadow_execution_max_forward_seconds),
        int(settings.paper_trading_pending_decision_max_age_seconds),
    )
    return (now - fired_at).total_seconds() <= max_age


async def capture_targeted_kalshi_orderbook_snapshot(
    session: AsyncSession,
    signal: Signal,
    *,
    captured_at: datetime,
    log_context: str,
) -> dict[str, Any]:
    if signal.outcome_id is None:
        return {"captured": False, "reason": "missing_outcome"}
    if not signal_is_recent_enough_for_forward_orderbook(signal, captured_at):
        return {"captured": False, "reason": "signal_too_old_for_forward_orderbook"}

    token_id = await _outcome_token_for_signal(session, signal)
    if not token_id:
        return {"captured": False, "reason": "missing_token_id"}

    connector = get_connector("kalshi")
    try:
        orderbook = await connector.fetch_orderbook(token_id)
        midpoint: Decimal | None = None
        try:
            midpoints = await connector.fetch_midpoints([token_id])
            midpoint = midpoints.get(token_id)
        except Exception as exc:
            logger.warning("%s midpoint capture failed for %s: %s", log_context, token_id, exc)

        session.add(
            OrderbookSnapshot(
                outcome_id=signal.outcome_id,
                bids=orderbook.bids,
                asks=orderbook.asks,
                spread=orderbook.spread,
                depth_bid_10pct=_depth_within_pct(orderbook.bids, side="bid", pct=0.10),
                depth_ask_10pct=_depth_within_pct(orderbook.asks, side="ask", pct=0.10),
                captured_at=captured_at,
            )
        )
        if midpoint is not None:
            market = await session.get(Market, signal.market_id) if signal.market_id is not None else None
            session.add(
                PriceSnapshot(
                    outcome_id=signal.outcome_id,
                    price=midpoint,
                    volume_24h=market.last_volume_24h if market is not None else None,
                    liquidity=market.last_liquidity if market is not None else None,
                    captured_at=captured_at,
                )
            )
        await session.flush()
        return {
            "captured": True,
            "token_id": token_id,
            "captured_at": captured_at.isoformat(),
            "midpoint_captured": midpoint is not None,
            "midpoint": str(midpoint) if midpoint is not None else None,
        }
    except Exception as exc:
        logger.warning("%s targeted orderbook capture failed for %s", log_context, token_id, exc_info=True)
        return {"captured": False, "reason": "targeted_orderbook_capture_failed", "error": str(exc)}
    finally:
        await connector.close()


__all__ = [
    "capture_targeted_kalshi_orderbook_snapshot",
    "signal_is_recent_enough_for_forward_orderbook",
]
