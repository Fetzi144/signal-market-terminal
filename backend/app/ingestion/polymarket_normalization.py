from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.polymarket_stream import PolymarketMarketEvent, PolymarketNormalizedEvent


@dataclass(slots=True)
class ParsedPolymarketEvent:
    side: str | None = None
    price: Decimal | None = None
    size: Decimal | None = None
    best_bid: Decimal | None = None
    best_bid_size: Decimal | None = None
    best_ask: Decimal | None = None
    best_ask_size: Decimal | None = None
    is_book_event: bool = False
    is_top_of_book: bool = False
    parse_status: str = "parsed"
    details_json: dict[str, Any] | None = None


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _level_price(level: Any) -> Decimal | None:
    if isinstance(level, dict):
        return _to_decimal(level.get("price"))
    if isinstance(level, (list, tuple)) and level:
        return _to_decimal(level[0])
    return None


def _level_size(level: Any) -> Decimal | None:
    if isinstance(level, dict):
        return _to_decimal(level.get("size"))
    if isinstance(level, (list, tuple)) and len(level) > 1:
        return _to_decimal(level[1])
    return None


def _extract_book_top(levels: Any) -> tuple[Decimal | None, Decimal | None]:
    if not isinstance(levels, list) or not levels:
        return None, None
    top = levels[0]
    return _level_price(top), _level_size(top)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def normalize_polymarket_payload(
    *,
    message_type: str,
    payload: dict[str, Any] | list[Any] | str,
    fallback_asset_id: str | None,
    fallback_market_id: str | None,
) -> ParsedPolymarketEvent:
    if not isinstance(payload, dict):
        return ParsedPolymarketEvent(
            parse_status="unknown",
            details_json={"reason": "payload_not_object"},
        )

    result = ParsedPolymarketEvent()
    message_type = (message_type or "unknown").lower()
    price = _to_decimal(payload.get("price")) or _to_decimal(payload.get("last_trade_price"))
    size = _to_decimal(payload.get("size"))
    side = payload.get("side")

    if isinstance(side, str):
        result.side = side.lower()

    if message_type in {"book", "rest_orderbook"}:
        result.is_book_event = True
        result.is_top_of_book = True
        result.best_bid, result.best_bid_size = _extract_book_top(payload.get("bids"))
        result.best_ask, result.best_ask_size = _extract_book_top(payload.get("asks"))
        result.price = price
        result.size = size
        result.details_json = _json_safe({
            "asset_id": payload.get("asset_id") or payload.get("assetId") or fallback_asset_id,
            "market_id": payload.get("market") or fallback_market_id,
            "has_bids": bool(payload.get("bids")),
            "has_asks": bool(payload.get("asks")),
        })
        return result

    if message_type in {"best_bid_ask", "bbo"}:
        result.is_top_of_book = True
        result.best_bid = _to_decimal(payload.get("best_bid")) or _to_decimal(payload.get("bid"))
        result.best_ask = _to_decimal(payload.get("best_ask")) or _to_decimal(payload.get("ask"))
        result.best_bid_size = _to_decimal(payload.get("best_bid_size")) or _to_decimal(payload.get("bid_size"))
        result.best_ask_size = _to_decimal(payload.get("best_ask_size")) or _to_decimal(payload.get("ask_size"))
        result.price = price
        result.size = size
        result.details_json = _json_safe({
            "spread": _to_decimal(payload.get("spread")),
            "asset_id": payload.get("asset_id") or payload.get("assetId") or fallback_asset_id,
            "market_id": payload.get("market") or fallback_market_id,
        })
        return result

    if message_type in {"last_trade_price", "trade"}:
        result.price = price
        result.size = size or _to_decimal(payload.get("trade_size"))
        result.details_json = _json_safe({
            "asset_id": payload.get("asset_id") or payload.get("assetId") or fallback_asset_id,
            "market_id": payload.get("market") or fallback_market_id,
        })
        return result

    if message_type == "price_change":
        changes = payload.get("price_changes")
        if isinstance(changes, list) and changes:
            first = changes[0]
            if isinstance(first, dict):
                result.price = _to_decimal(first.get("price")) or price
                result.size = _to_decimal(first.get("size")) or size
                change_side = first.get("side")
                if isinstance(change_side, str):
                    result.side = change_side.lower()
                result.details_json = _json_safe({
                    "change_count": len(changes),
                    "asset_id": first.get("asset_id") or first.get("assetId") or fallback_asset_id,
                    "market_id": first.get("market") or payload.get("market") or fallback_market_id,
                })
                return result
        result.parse_status = "unknown"
        result.details_json = {"reason": "price_change_missing_changes"}
        return result

    if message_type == "malformed":
        return ParsedPolymarketEvent(
            parse_status="unknown",
            details_json={"reason": "malformed_json"},
        )

    if price is not None or size is not None or result.side is not None:
        result.price = price
        result.size = size
        result.details_json = _json_safe({
            "asset_id": payload.get("asset_id") or payload.get("assetId") or fallback_asset_id,
            "market_id": payload.get("market") or fallback_market_id,
        })
        return result

    return ParsedPolymarketEvent(
        parse_status="unknown",
        details_json={"reason": "message_shape_unrecognized"},
    )


async def ensure_normalized_event(
    session: AsyncSession,
    raw_event: PolymarketMarketEvent,
) -> PolymarketNormalizedEvent:
    existing = await session.get(PolymarketNormalizedEvent, raw_event.id)
    if existing is not None:
        return existing

    parsed = normalize_polymarket_payload(
        message_type=raw_event.message_type,
        payload=raw_event.payload,
        fallback_asset_id=raw_event.asset_id,
        fallback_market_id=raw_event.market_id,
    )
    normalized = PolymarketNormalizedEvent(
        raw_event_id=raw_event.id,
        venue=raw_event.venue,
        provenance=raw_event.provenance,
        channel=raw_event.channel,
        message_type=raw_event.message_type,
        market_id=raw_event.market_id,
        asset_id=raw_event.asset_id,
        event_time=raw_event.event_time,
        received_at_local=raw_event.received_at_local,
        side=parsed.side,
        price=parsed.price,
        size=parsed.size,
        best_bid=parsed.best_bid,
        best_bid_size=parsed.best_bid_size,
        best_ask=parsed.best_ask,
        best_ask_size=parsed.best_ask_size,
        is_book_event=parsed.is_book_event,
        is_top_of_book=parsed.is_top_of_book,
        parse_status=parsed.parse_status,
        details_json=parsed.details_json,
    )
    session.add(normalized)
    await session.flush()
    return normalized


async def fetch_normalized_events_for_raw_ids(
    session: AsyncSession,
    raw_event_ids: list[int],
) -> list[PolymarketNormalizedEvent]:
    if not raw_event_ids:
        return []
    result = await session.execute(
        select(PolymarketNormalizedEvent)
        .where(PolymarketNormalizedEvent.raw_event_id.in_(raw_event_ids))
        .order_by(PolymarketNormalizedEvent.raw_event_id.asc())
    )
    return list(result.scalars().all())


def serialize_normalized_event(model: PolymarketNormalizedEvent) -> dict[str, Any]:
    return {
        "raw_event_id": model.raw_event_id,
        "venue": model.venue,
        "provenance": model.provenance,
        "channel": model.channel,
        "message_type": model.message_type,
        "market_id": model.market_id,
        "asset_id": model.asset_id,
        "event_time": model.event_time,
        "received_at_local": model.received_at_local,
        "side": model.side,
        "price": float(model.price) if model.price is not None else None,
        "size": float(model.size) if model.size is not None else None,
        "best_bid": float(model.best_bid) if model.best_bid is not None else None,
        "best_bid_size": float(model.best_bid_size) if model.best_bid_size is not None else None,
        "best_ask": float(model.best_ask) if model.best_ask is not None else None,
        "best_ask_size": float(model.best_ask_size) if model.best_ask_size is not None else None,
        "is_book_event": model.is_book_event,
        "is_top_of_book": model.is_top_of_book,
        "parse_status": model.parse_status,
        "details_json": model.details_json,
        "created_at": model.created_at,
    }
