from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.market import Market, Outcome
from app.models.signal import Signal
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return _ensure_utc(value).isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(_json_safe(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def select_research_universe(
    session: AsyncSession,
    *,
    window_start: datetime,
    window_end: datetime,
    max_markets: int,
) -> dict[str, Any]:
    """Select a bounded, Kalshi-only research universe.

    The selector intentionally returns a capped sample and metadata instead of doing
    unrestricted scans. Lanes can drill into this universe later if they need IDs.
    """
    start = _ensure_utc(window_start)
    end = _ensure_utc(window_end)
    capped = max(1, min(int(max_markets), 5000))
    horizon_end = end + timedelta(days=30)
    liquidity_floor = Decimal(str(settings.paper_trading_min_market_liquidity_usd))

    primary_query = (
        select(
            Market.id,
            Market.platform_id,
            Market.question,
            Market.end_date,
            Market.last_liquidity,
            Market.last_volume_24h,
            Market.updated_at,
        )
        .where(
            Market.platform == "kalshi",
            Market.end_date.is_not(None),
            Market.end_date >= start,
            Market.end_date <= horizon_end,
            (Market.last_liquidity.is_(None)) | (Market.last_liquidity >= liquidity_floor),
        )
        .order_by(
            func.coalesce(Market.last_liquidity, 0).desc(),
            Market.end_date.asc(),
            Market.updated_at.desc(),
        )
        .limit(capped)
    )
    rows = (await session.execute(primary_query)).all()

    fallback_used = False
    if not rows:
        fallback_used = True
        rows = (
            await session.execute(
                select(
                    Market.id,
                    Market.platform_id,
                    Market.question,
                    Market.end_date,
                    Market.last_liquidity,
                    Market.last_volume_24h,
                    Market.updated_at,
                )
                .where(Market.platform == "kalshi")
                .order_by(func.coalesce(Market.last_liquidity, 0).desc(), Market.updated_at.desc())
                .limit(capped)
            )
        ).all()

    market_ids = [row.id for row in rows]
    selected = [
        {
            "market_id": str(row.id),
            "platform_id": row.platform_id,
            "question": row.question,
            "end_date": _ensure_utc(row.end_date).isoformat() if row.end_date else None,
            "last_liquidity": float(row.last_liquidity) if row.last_liquidity is not None else None,
            "last_volume_24h": float(row.last_volume_24h) if row.last_volume_24h is not None else None,
            "updated_at": _ensure_utc(row.updated_at).isoformat() if row.updated_at else None,
        }
        for row in rows
    ]

    outcome_count = 0
    price_snapshot_count = 0
    orderbook_snapshot_count = 0
    signal_count = 0
    if market_ids:
        outcome_count = int(
            (await session.execute(select(func.count(Outcome.id)).where(Outcome.market_id.in_(market_ids)))).scalar_one()
            or 0
        )
        outcome_ids = (
            await session.execute(select(Outcome.id).where(Outcome.market_id.in_(market_ids)).limit(capped * 8))
        ).scalars().all()
        if outcome_ids:
            price_snapshot_count = int(
                (
                    await session.execute(
                        select(func.count(PriceSnapshot.id)).where(
                            PriceSnapshot.outcome_id.in_(outcome_ids),
                            PriceSnapshot.captured_at >= start,
                            PriceSnapshot.captured_at <= end,
                        )
                    )
                ).scalar_one()
                or 0
            )
            orderbook_snapshot_count = int(
                (
                    await session.execute(
                        select(func.count(OrderbookSnapshot.id)).where(
                            OrderbookSnapshot.outcome_id.in_(outcome_ids),
                            OrderbookSnapshot.captured_at >= start,
                            OrderbookSnapshot.captured_at <= end,
                        )
                    )
                ).scalar_one()
                or 0
            )
        signal_count = int(
            (
                await session.execute(
                    select(func.count(Signal.id)).where(
                        Signal.market_id.in_(market_ids),
                        Signal.fired_at >= start,
                        Signal.fired_at <= end,
                    )
                )
            ).scalar_one()
            or 0
        )

    fingerprint_payload = {
        "selector": "profit_hunt_v1",
        "window_start": start,
        "window_end": end,
        "max_markets": capped,
        "market_ids": [str(value) for value in market_ids],
        "fallback_used": fallback_used,
    }
    return {
        "selector": "profit_hunt_v1",
        "platform": "kalshi",
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "horizon_end": horizon_end.isoformat(),
        "max_markets": capped,
        "market_count": len(market_ids),
        "outcome_count": outcome_count,
        "price_snapshot_count": price_snapshot_count,
        "orderbook_snapshot_count": orderbook_snapshot_count,
        "signal_count": signal_count,
        "fallback_used": fallback_used,
        "liquidity_floor": float(liquidity_floor),
        "markets": selected,
        "market_ids": [str(value) for value in market_ids],
        "fingerprint": _fingerprint(fingerprint_payload),
    }
