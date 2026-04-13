from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from dateutil.parser import isoparse

STREAM_CHANNEL = "market"
REST_RESYNC_CHANNEL = "rest_orderbook"
STREAM_PROVENANCE = "stream"
RESYNC_PROVENANCE = "rest_resync"
STATUS_VENUE = "polymarket"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_polymarket_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            timestamp = int(stripped)
            if len(stripped) > 10:
                timestamp = timestamp / 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        try:
            parsed = isoparse(stripped)
        except (TypeError, ValueError):
            return None
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)

    return None


def unique_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def parse_json_if_string(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return json.loads(stripped)
        except (TypeError, ValueError):
            return value
    return value


def parse_listish(value: Any) -> list[Any]:
    parsed = parse_json_if_string(value)
    if isinstance(parsed, list):
        return parsed
    return []


def extract_asset_ids(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []

    for key in ("asset_id", "assetId", "token_id", "tokenId"):
        value = payload.get(key)
        if value is not None:
            values.append(str(value))

    for key in ("asset_ids", "assets_ids", "token_ids", "tokenIds", "clob_token_ids", "clobTokenIds"):
        value = payload.get(key)
        if isinstance(value, list):
            values.extend(str(item) for item in value if item is not None)

    price_changes = payload.get("price_changes")
    if isinstance(price_changes, list):
        for change in price_changes:
            if not isinstance(change, dict):
                continue
            for key in ("asset_id", "assetId", "token_id", "tokenId"):
                value = change.get(key)
                if value is not None:
                    values.append(str(value))

    event_message = payload.get("event_message")
    if isinstance(event_message, dict):
        values.extend(extract_asset_ids(event_message))

    return unique_preserving_order(values)


@dataclass(slots=True)
class EventMetadata:
    message_type: str
    market_id: str | None
    asset_id: str | None
    asset_ids: list[str] | None
    event_time: datetime | None
    source_message_id: str | None
    source_hash: str | None
    source_sequence: str | None
    source_cursor: str | None


def extract_event_metadata(payload: dict[str, Any]) -> EventMetadata:
    asset_ids = extract_asset_ids(payload)
    asset_id = asset_ids[0] if len(asset_ids) == 1 else None

    message_type = str(payload.get("event_type") or payload.get("type") or "unknown")
    market_id = payload.get("market") or payload.get("condition_id")
    event_time = parse_polymarket_timestamp(payload.get("timestamp"))

    source_message_id = payload.get("id")
    if source_message_id is None:
        event_message = payload.get("event_message")
        if isinstance(event_message, dict):
            source_message_id = event_message.get("id")

    source_hash = payload.get("hash")
    if source_hash is None:
        price_changes = payload.get("price_changes")
        if isinstance(price_changes, list) and len(price_changes) == 1 and isinstance(price_changes[0], dict):
            source_hash = price_changes[0].get("hash")

    source_sequence = payload.get("sequence_id") or payload.get("seq") or payload.get("offset")
    source_cursor = payload.get("cursor")

    return EventMetadata(
        message_type=message_type,
        market_id=str(market_id) if market_id is not None else None,
        asset_id=asset_id,
        asset_ids=asset_ids or None,
        event_time=event_time,
        source_message_id=str(source_message_id) if source_message_id is not None else None,
        source_hash=str(source_hash) if source_hash is not None else None,
        source_sequence=str(source_sequence) if source_sequence is not None else None,
        source_cursor=str(source_cursor) if source_cursor is not None else None,
    )
