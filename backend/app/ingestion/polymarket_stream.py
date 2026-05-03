"""Public Polymarket stream ingestion and REST resync support."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx
import websockets
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_common import (
    REST_RESYNC_CHANNEL,
    RESYNC_PROVENANCE,
    STATUS_VENUE,
    STREAM_CHANNEL,
    STREAM_PROVENANCE,
    EventMetadata,
    extract_event_metadata,
    unique_preserving_order,
    utcnow,
)
from app.ingestion.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketMarketDim,
    apply_stream_event_to_registry,
    fetch_polymarket_meta_sync_status,
    meta_sync_run_in_progress,
    seed_registry_from_book_snapshot,
)
from app.ingestion.polymarket_normalization import ensure_normalized_event
from app.metrics import (
    polymarket_gap_suspicions,
    polymarket_malformed_messages,
    polymarket_raw_events_ingested,
    polymarket_resync_runs,
    polymarket_stream_active_subscriptions,
    polymarket_stream_active_watches,
    polymarket_stream_connected,
    polymarket_stream_reconnects,
    polymarket_stream_resyncs,
)
from app.models.market import Market, Outcome
from app.models.polymarket_stream import (
    PolymarketIngestIncident,
    PolymarketMarketEvent,
    PolymarketNormalizedEvent,
    PolymarketResyncRun,
    PolymarketStreamStatus,
    PolymarketWatchAsset,
)

logger = logging.getLogger(__name__)

UNSET = object()
ASYNC_PG_BIND_PARAMETER_LIMIT = 32767
WATCH_REGISTRY_LOOKUP_BATCH_SIZE = 5000
BOOK_RESYNC_BATCH_SIZE = 250
STREAM_SUBSCRIPTION_BATCH_SIZE = 1000
AUTO_BOOTSTRAP_WATCH_REASONS = ("active_universe_bootstrap", "registry_live_bootstrap")


def _chunk_values(values: list[Any], size: int) -> list[list[Any]]:
    if size <= 0:
        raise ValueError("Chunk size must be positive")
    return [values[index:index + size] for index in range(0, len(values), size)]


def _watch_registry_insert_batch_size(
    *,
    requested_size: int,
    row_field_count: int,
    dialect_name: str,
) -> int:
    if requested_size <= 0:
        raise ValueError("Requested batch size must be positive")
    if row_field_count <= 0:
        raise ValueError("Row field count must be positive")
    if dialect_name != "postgresql":
        return requested_size
    return max(1, min(requested_size, ASYNC_PG_BIND_PARAMETER_LIMIT // row_field_count))


def build_subscription_diff(current: set[str], desired: set[str]) -> tuple[list[str], list[str]]:
    to_subscribe = sorted(desired - current)
    to_unsubscribe = sorted(current - desired)
    return to_subscribe, to_unsubscribe


async def log_ingest_incident(
    session: AsyncSession,
    *,
    incident_type: str,
    severity: str,
    asset_id: str | None = None,
    details_json: dict[str, Any] | None = None,
    connection_id: uuid.UUID | None = None,
    raw_event_id: int | None = None,
    resync_run_id: uuid.UUID | None = None,
    resolved_at: datetime | None = None,
) -> PolymarketIngestIncident:
    incident = PolymarketIngestIncident(
        incident_type=incident_type,
        severity=severity,
        asset_id=asset_id,
        connection_id=connection_id,
        raw_event_id=raw_event_id,
        resync_run_id=resync_run_id,
        details_json=details_json,
        resolved_at=resolved_at,
    )
    session.add(incident)
    await session.flush()
    return incident


async def _count_watched_assets(session: AsyncSession) -> int:
    result = await session.execute(
        select(func.count(PolymarketWatchAsset.id)).where(PolymarketWatchAsset.watch_enabled.is_(True))
    )
    count = int(result.scalar_one() or 0)
    polymarket_stream_active_watches.set(count)
    return count


async def _load_registry_watch_candidates(session: AsyncSession) -> list[tuple[uuid.UUID, str]]:
    result = await session.execute(
        select(PolymarketAssetDim.outcome_id, PolymarketAssetDim.asset_id)
        .join(PolymarketMarketDim, PolymarketAssetDim.market_dim_id == PolymarketMarketDim.id)
        .where(
            PolymarketAssetDim.outcome_id.is_not(None),
            PolymarketAssetDim.asset_id.is_not(None),
            PolymarketMarketDim.active.is_(True),
            func.coalesce(PolymarketMarketDim.closed, False).is_(False),
            func.coalesce(PolymarketMarketDim.archived, False).is_(False),
            func.coalesce(PolymarketMarketDim.resolved, False).is_(False),
        )
    )
    return [
        (outcome_id, str(asset_id))
        for outcome_id, asset_id in result.all()
        if outcome_id is not None and asset_id is not None
    ]


async def _load_generic_watch_candidates(session: AsyncSession) -> list[tuple[uuid.UUID, str]]:
    result = await session.execute(
        select(Outcome.id, Outcome.token_id)
        .join(Market, Outcome.market_id == Market.id)
        .where(
            Market.platform == STATUS_VENUE,
            Market.active.is_(True),
            Outcome.token_id.is_not(None),
        )
    )
    return [
        (outcome_id, str(token_id))
        for outcome_id, token_id in result.all()
        if outcome_id is not None and token_id is not None
    ]


async def _disable_auto_bootstrap_watches(
    session: AsyncSession,
    *,
    desired_outcome_ids: list[uuid.UUID] | None,
) -> int:
    query = (
        update(PolymarketWatchAsset)
        .where(
            PolymarketWatchAsset.watch_enabled.is_(True),
            PolymarketWatchAsset.watch_reason.in_(AUTO_BOOTSTRAP_WATCH_REASONS),
        )
        .values(watch_enabled=False, updated_at=utcnow())
    )
    if desired_outcome_ids is not None:
        if desired_outcome_ids:
            query = query.where(PolymarketWatchAsset.outcome_id.not_in(desired_outcome_ids))
        else:
            # When registry truth is not ready yet, suppress the stale auto-watch set entirely.
            pass
    result = await session.execute(query)
    return int(result.rowcount or 0)


async def ensure_watch_registry_bootstrapped(
    session: AsyncSession,
    *,
    commit: bool = False,
) -> dict[str, int]:
    if not settings.polymarket_watch_bootstrap_from_active_universe:
        await _count_watched_assets(session)
        return {"created_count": 0, "updated_count": 0, "disabled_count": 0, "source": "disabled"}

    active_rows = await _load_registry_watch_candidates(session)
    source = "registry_live_bootstrap"
    if not active_rows:
        if settings.polymarket_meta_sync_enabled:
            disabled_count = await _disable_auto_bootstrap_watches(session, desired_outcome_ids=[])
            if disabled_count:
                await session.flush()
                if commit:
                    await session.commit()
            await _count_watched_assets(session)
            return {
                "created_count": 0,
                "updated_count": 0,
                "disabled_count": disabled_count,
                "source": "registry_pending",
            }
        active_rows = await _load_generic_watch_candidates(session)
        source = "active_universe_bootstrap"
    if not active_rows:
        await _count_watched_assets(session)
        return {"created_count": 0, "updated_count": 0, "disabled_count": 0, "source": source}

    desired_by_outcome_id: dict[uuid.UUID, str] = {}
    for outcome_id, token_id in active_rows:
        desired_by_outcome_id[outcome_id] = str(token_id)

    outcome_ids = list(desired_by_outcome_id.keys())
    existing_by_outcome_id: dict[uuid.UUID, PolymarketWatchAsset] = {}
    for batch in _chunk_values(outcome_ids, WATCH_REGISTRY_LOOKUP_BATCH_SIZE):
        existing_result = await session.execute(
            select(PolymarketWatchAsset).where(PolymarketWatchAsset.outcome_id.in_(batch))
        )
        existing_by_outcome_id.update({
            row.outcome_id: row
            for row in existing_result.scalars().all()
        })

    created_count = 0
    updated_count = 0
    disabled_count = 0
    rows_to_insert: list[dict[str, Any]] = []
    now = utcnow()
    if source == "registry_live_bootstrap":
        disabled_count = await _disable_auto_bootstrap_watches(session, desired_outcome_ids=outcome_ids)
    for outcome_id, token_id in desired_by_outcome_id.items():
        watch_asset = existing_by_outcome_id.get(outcome_id)
        if watch_asset is None:
            rows_to_insert.append(
                {
                    "id": uuid.uuid4(),
                    "outcome_id": outcome_id,
                    "asset_id": str(token_id),
                    "watch_enabled": True,
                    "watch_reason": source,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            continue
        if watch_asset.asset_id != str(token_id):
            watch_asset.asset_id = str(token_id)
            updated_count += 1
        if source == "registry_live_bootstrap" and watch_asset.watch_reason in AUTO_BOOTSTRAP_WATCH_REASONS:
            if not watch_asset.watch_enabled:
                watch_asset.watch_enabled = True
                updated_count += 1
            if watch_asset.watch_reason != source:
                watch_asset.watch_reason = source
                updated_count += 1

    if rows_to_insert:
        insert_fn = postgresql_insert if session.bind.dialect.name == "postgresql" else sqlite_insert
        insert_batch_size = _watch_registry_insert_batch_size(
            requested_size=WATCH_REGISTRY_LOOKUP_BATCH_SIZE,
            row_field_count=len(rows_to_insert[0]),
            dialect_name=session.bind.dialect.name,
        )
        for batch in _chunk_values(rows_to_insert, insert_batch_size):
            result = await session.execute(
                insert_fn(PolymarketWatchAsset)
                .values(batch)
                .on_conflict_do_nothing(index_elements=["outcome_id"])
            )
            created_count += int(result.rowcount or 0)

    if created_count or updated_count or disabled_count:
        await session.flush()
        if commit:
            await session.commit()

    await _count_watched_assets(session)
    return {
        "created_count": created_count,
        "updated_count": updated_count,
        "disabled_count": disabled_count,
        "source": source,
    }


async def list_watched_polymarket_assets(
    session: AsyncSession,
    *,
    limit: int | None = None,
    prioritize: bool = False,
) -> list[str]:
    await ensure_watch_registry_bootstrapped(session, commit=True)
    query = (
        select(PolymarketWatchAsset.asset_id)
        .join(Outcome, PolymarketWatchAsset.outcome_id == Outcome.id)
        .join(Market, Outcome.market_id == Market.id)
        .where(
            PolymarketWatchAsset.watch_enabled.is_(True),
            Market.platform == STATUS_VENUE,
            Outcome.token_id.is_not(None),
        )
    )
    if prioritize:
        query = query.order_by(
            PolymarketWatchAsset.priority.desc().nullslast(),
            PolymarketWatchAsset.updated_at.desc(),
        )
    if limit is not None:
        query = query.limit(limit)
    result = await session.execute(query)
    return [str(asset_id) for asset_id in result.scalars().all()]


async def list_watch_asset_rows(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    await ensure_watch_registry_bootstrapped(session, commit=True)
    total_result = await session.execute(select(func.count(PolymarketWatchAsset.id)))
    total = int(total_result.scalar_one() or 0)

    result = await session.execute(
        select(PolymarketWatchAsset, Outcome, Market)
        .join(Outcome, PolymarketWatchAsset.outcome_id == Outcome.id)
        .join(Market, Outcome.market_id == Market.id)
        .order_by(
            PolymarketWatchAsset.watch_enabled.desc(),
            PolymarketWatchAsset.priority.desc().nullslast(),
            PolymarketWatchAsset.updated_at.desc(),
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    rows: list[dict[str, Any]] = []
    for watch_asset, outcome, market in result.all():
        rows.append(
            {
                "id": watch_asset.id,
                "outcome_id": watch_asset.outcome_id,
                "asset_id": watch_asset.asset_id,
                "watch_enabled": watch_asset.watch_enabled,
                "watch_reason": watch_asset.watch_reason,
                "priority": watch_asset.priority,
                "created_at": watch_asset.created_at,
                "updated_at": watch_asset.updated_at,
                "market_id": market.id,
                "market_platform_id": market.platform_id,
                "market_question": market.question,
                "market_active": market.active,
                "outcome_name": outcome.name,
            }
        )
    return rows, total


async def _resolve_watch_outcome(
    session: AsyncSession,
    *,
    outcome_id: uuid.UUID | None,
    asset_id: str | None,
) -> tuple[Outcome, Market]:
    query = (
        select(Outcome, Market)
        .join(Market, Outcome.market_id == Market.id)
        .where(Market.platform == STATUS_VENUE)
    )
    if outcome_id is not None:
        query = query.where(Outcome.id == outcome_id)
    elif asset_id is not None:
        query = query.where(Outcome.token_id == asset_id).order_by(Market.active.desc(), Market.updated_at.desc())
    else:
        raise ValueError("Either outcome_id or asset_id is required")

    result = await session.execute(query.limit(1))
    row = result.first()
    if row is None:
        raise ValueError("No matching Polymarket outcome found for watch registration")
    return row


async def upsert_watch_asset(
    session: AsyncSession,
    *,
    outcome_id: uuid.UUID | None,
    asset_id: str | None,
    watch_enabled: bool = True,
    watch_reason: str | None = None,
    priority: int | None = None,
) -> PolymarketWatchAsset:
    outcome, _market = await _resolve_watch_outcome(session, outcome_id=outcome_id, asset_id=asset_id)
    result = await session.execute(
        select(PolymarketWatchAsset).where(PolymarketWatchAsset.outcome_id == outcome.id)
    )
    watch_asset = result.scalar_one_or_none()
    if watch_asset is None:
        watch_asset = PolymarketWatchAsset(
            outcome_id=outcome.id,
            asset_id=str(outcome.token_id),
            watch_enabled=watch_enabled,
            watch_reason=watch_reason,
            priority=priority,
        )
        session.add(watch_asset)
    else:
        watch_asset.asset_id = str(outcome.token_id)
        watch_asset.watch_enabled = watch_enabled
        watch_asset.watch_reason = watch_reason
        watch_asset.priority = priority

    await session.flush()
    await _count_watched_assets(session)
    return watch_asset


async def upsert_stream_status(
    session: AsyncSession,
    *,
    connected: bool | None = None,
    connection_started_at: datetime | None = None,
    current_connection_id: uuid.UUID | None | object = UNSET,
    last_message_received_at: datetime | None = None,
    active_subscription_count: int | None = None,
    increment_reconnects: bool = False,
    increment_resyncs: bool = False,
    gap_suspected_count_delta: int = 0,
    malformed_message_count_delta: int = 0,
    last_resync_at: datetime | None = None,
    last_reconciliation_at: datetime | None = None,
    last_error: str | None | object = UNSET,
    last_error_at: datetime | None = None,
) -> PolymarketStreamStatus:
    status = await session.get(PolymarketStreamStatus, STATUS_VENUE)
    if status is None:
        status = PolymarketStreamStatus(venue=STATUS_VENUE)
        session.add(status)
        await session.flush()

    if connected is not None:
        status.connected = connected
        polymarket_stream_connected.set(1 if connected else 0)
    if connection_started_at is not None:
        status.connection_started_at = connection_started_at
    if current_connection_id is not UNSET:
        status.current_connection_id = current_connection_id
    if last_message_received_at is not None:
        status.last_message_received_at = last_message_received_at
    if active_subscription_count is not None:
        status.active_subscription_count = active_subscription_count
        polymarket_stream_active_subscriptions.set(active_subscription_count)
    if increment_reconnects:
        status.reconnect_count += 1
        polymarket_stream_reconnects.inc()
    if increment_resyncs:
        status.resync_count += 1
        polymarket_stream_resyncs.inc()
    if gap_suspected_count_delta:
        status.gap_suspected_count += gap_suspected_count_delta
    if malformed_message_count_delta:
        status.malformed_message_count += malformed_message_count_delta
    if last_resync_at is not None:
        status.last_resync_at = last_resync_at
    if last_reconciliation_at is not None:
        status.last_reconciliation_at = last_reconciliation_at
    if last_error is not UNSET:
        status.last_error = last_error
    if last_error_at is not None:
        status.last_error_at = last_error_at

    status.updated_at = utcnow()
    await session.flush()
    return status


async def persist_market_event(
    session: AsyncSession,
    *,
    provenance: str,
    channel: str,
    message_type: str,
    payload: dict[str, Any] | list[Any] | str,
    received_at_local: datetime,
    market_id: str | None = None,
    asset_id: str | None = None,
    asset_ids: list[str] | None = None,
    event_time: datetime | None = None,
    connection_id: uuid.UUID | None = None,
    ingest_batch_id: uuid.UUID | None = None,
    source_message_id: str | None = None,
    source_hash: str | None = None,
    source_sequence: str | None = None,
    source_cursor: str | None = None,
    resync_reason: str | None = None,
    resync_run_id: uuid.UUID | None = None,
) -> PolymarketMarketEvent:
    event = PolymarketMarketEvent(
        provenance=provenance,
        channel=channel,
        message_type=message_type,
        payload=payload,
        received_at_local=received_at_local,
        market_id=market_id,
        asset_id=asset_id,
        asset_ids=asset_ids,
        event_time=event_time,
        connection_id=connection_id,
        ingest_batch_id=ingest_batch_id,
        source_message_id=source_message_id,
        source_hash=source_hash,
        source_sequence=source_sequence,
        source_cursor=source_cursor,
        resync_reason=resync_reason,
        resync_run_id=resync_run_id,
    )
    session.add(event)
    await session.flush()
    polymarket_raw_events_ingested.labels(provenance=provenance, message_type=message_type).inc()

    if settings.polymarket_normalization_enabled:
        try:
            await ensure_normalized_event(session, event)
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.warning("Polymarket normalization failed for raw event %s", event.id, exc_info=True)
            fallback = await session.get(PolymarketNormalizedEvent, event.id)
            if fallback is None:
                session.add(
                    PolymarketNormalizedEvent(
                        raw_event_id=event.id,
                        venue=event.venue,
                        provenance=event.provenance,
                        channel=event.channel,
                        message_type=event.message_type,
                        market_id=event.market_id,
                        asset_id=event.asset_id,
                        event_time=event.event_time,
                        received_at_local=event.received_at_local,
                        parse_status="error",
                        details_json={"error": str(exc)[:500]},
                    )
                )
                await session.flush()

    return event


async def _count_events_since(session: AsyncSession, since: datetime) -> int:
    result = await session.execute(
        select(func.count(PolymarketMarketEvent.id)).where(
            PolymarketMarketEvent.venue == STATUS_VENUE,
            PolymarketMarketEvent.received_at_local >= since,
        )
    )
    return int(result.scalar_one() or 0)


def _serialize_incident(incident: PolymarketIngestIncident) -> dict[str, Any]:
    return {
        "id": incident.id,
        "created_at": incident.created_at,
        "incident_type": incident.incident_type,
        "severity": incident.severity,
        "asset_id": incident.asset_id,
        "connection_id": incident.connection_id,
        "raw_event_id": incident.raw_event_id,
        "resync_run_id": incident.resync_run_id,
        "details_json": incident.details_json,
        "resolved_at": incident.resolved_at,
    }


def _serialize_resync_run(run: PolymarketResyncRun) -> dict[str, Any]:
    return {
        "id": run.id,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "status": run.status,
        "reason": run.reason,
        "connection_id": run.connection_id,
        "requested_asset_count": run.requested_asset_count,
        "succeeded_asset_count": run.succeeded_asset_count,
        "failed_asset_count": run.failed_asset_count,
        "details_json": run.details_json,
    }


async def list_polymarket_incidents(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    total_result = await session.execute(select(func.count(PolymarketIngestIncident.id)))
    total = int(total_result.scalar_one() or 0)
    result = await session.execute(
        select(PolymarketIngestIncident)
        .order_by(PolymarketIngestIncident.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return [_serialize_incident(row) for row in result.scalars().all()], total


async def list_polymarket_resync_runs(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int]:
    total_result = await session.execute(select(func.count(PolymarketResyncRun.id)))
    total = int(total_result.scalar_one() or 0)
    result = await session.execute(
        select(PolymarketResyncRun)
        .order_by(PolymarketResyncRun.started_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return [_serialize_resync_run(row) for row in result.scalars().all()], total


async def fetch_polymarket_stream_status(
    session: AsyncSession,
    *,
    refresh_watch_registry: bool = True,
    include_details: bool = True,
) -> dict[str, Any]:
    if refresh_watch_registry:
        await ensure_watch_registry_bootstrapped(session, commit=True)
    status = await session.get(PolymarketStreamStatus, STATUS_VENUE)
    watched_count = await _count_watched_assets(session)
    metadata_sync = await fetch_polymarket_meta_sync_status(session) if include_details else {}

    last_event_result = await session.execute(
        select(func.max(PolymarketMarketEvent.received_at_local)).where(
            PolymarketMarketEvent.venue == STATUS_VENUE,
            PolymarketMarketEvent.provenance == STREAM_PROVENANCE,
        )
    )
    last_event_received_at = last_event_result.scalar_one_or_none()

    last_successful_resync_result = await session.execute(
        select(func.max(PolymarketResyncRun.completed_at)).where(
            PolymarketResyncRun.status.in_(("completed", "partial")),
        )
    )
    last_successful_resync_at = last_successful_resync_result.scalar_one_or_none()

    now = utcnow()
    recent_incidents: list[dict[str, Any]] = []
    recent_resync_runs: list[dict[str, Any]] = []
    events_1m = 0
    events_5m = 0
    events_15m = 0
    if include_details:
        recent_incidents_result = await session.execute(
            select(PolymarketIngestIncident)
            .order_by(PolymarketIngestIncident.created_at.desc())
            .limit(10)
        )
        recent_incidents = [_serialize_incident(row) for row in recent_incidents_result.scalars().all()]

        recent_runs_result = await session.execute(
            select(PolymarketResyncRun)
            .order_by(PolymarketResyncRun.started_at.desc())
            .limit(10)
        )
        recent_resync_runs = [_serialize_resync_run(row) for row in recent_runs_result.scalars().all()]

        events_1m = await _count_events_since(session, now - timedelta(minutes=1))
        events_5m = await _count_events_since(session, now - timedelta(minutes=5))
        events_15m = await _count_events_since(session, now - timedelta(minutes=15))
    heartbeat_reference = last_event_received_at or (status.last_message_received_at if status else None)
    if heartbeat_reference is not None and heartbeat_reference.tzinfo is None:
        heartbeat_reference = heartbeat_reference.replace(tzinfo=timezone.utc)
    heartbeat_freshness_seconds = (
        max(0, int((now - heartbeat_reference).total_seconds()))
        if heartbeat_reference is not None
        else None
    )
    if not settings.polymarket_stream_enabled:
        continuity_status = "disabled"
    elif status is None or not status.connected:
        continuity_status = "disconnected"
    elif heartbeat_freshness_seconds is None:
        continuity_status = "awaiting_events"
    elif heartbeat_freshness_seconds > settings.polymarket_stream_ping_interval_seconds * 3:
        continuity_status = "stale"
    else:
        continuity_status = "healthy"

    return {
        "enabled": settings.polymarket_stream_enabled,
        "connected": status.connected if status else False,
        "connection_started_at": status.connection_started_at if status else None,
        "current_connection_id": status.current_connection_id if status else None,
        "last_event_received_at": heartbeat_reference,
        "heartbeat_freshness_seconds": heartbeat_freshness_seconds,
        "continuity_status": continuity_status,
        "active_watch_count": watched_count,
        "watched_asset_count": watched_count,
        "active_subscription_count": status.active_subscription_count if status else 0,
        "subscribed_asset_count": status.active_subscription_count if status else 0,
        "events_ingested_5m": events_5m,
        "events_ingested": {"1m": events_1m, "5m": events_5m, "15m": events_15m},
        "reconnect_count": status.reconnect_count if status else 0,
        "resync_count": status.resync_count if status else 0,
        "gap_suspected_count": status.gap_suspected_count if status else 0,
        "malformed_message_count": status.malformed_message_count if status else 0,
        "last_resync_at": status.last_resync_at if status else None,
        "last_successful_resync_at": last_successful_resync_at or (status.last_resync_at if status else None),
        "last_reconciliation_at": status.last_reconciliation_at if status else None,
        "last_error": status.last_error if status else None,
        "last_error_at": status.last_error_at if status else None,
        "updated_at": status.updated_at if status else None,
        "recent_incidents": recent_incidents,
        "recent_resync_runs": recent_resync_runs,
        "metadata_sync": metadata_sync,
    }


class PolymarketResyncService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._client_factory = client_factory or self._default_client_factory
        self._client: httpx.AsyncClient | None = None

    def _default_client_factory(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=settings.connector_timeout_seconds)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = self._client_factory()
        return self._client

    def _normalize_books_response(self, data: Any, asset_ids: list[str]) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            if isinstance(data.get("data"), list):
                return [row for row in data["data"] if isinstance(row, dict)]
            rows: list[dict[str, Any]] = []
            for asset_id in asset_ids:
                row = data.get(asset_id)
                if isinstance(row, dict):
                    row.setdefault("asset_id", asset_id)
                    rows.append(row)
            return rows
        return []

    async def _fetch_books_batch(self, asset_ids: list[str]) -> list[dict[str, Any]]:
        client = await self._get_client()
        response = await client.post(
            f"{settings.polymarket_api_base}/books",
            json=[{"token_id": asset_id} for asset_id in asset_ids],
        )
        response.raise_for_status()
        return self._normalize_books_response(response.json(), asset_ids)

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def resync_assets(
        self,
        asset_ids: list[str],
        *,
        reason: str,
        connection_id: uuid.UUID | None = None,
    ) -> dict[str, Any]:
        requested_asset_ids = unique_preserving_order([str(asset_id) for asset_id in asset_ids if asset_id])
        started_at = utcnow()
        run_id = uuid.uuid4()

        async with self._session_factory() as session:
            run = PolymarketResyncRun(
                id=run_id,
                reason=reason,
                status="running",
                connection_id=connection_id,
                requested_asset_count=len(requested_asset_ids),
                details_json={"requested_asset_ids": requested_asset_ids},
            )
            session.add(run)
            await session.flush()
            await log_ingest_incident(
                session,
                incident_type="resync_started",
                severity="info",
                connection_id=connection_id,
                resync_run_id=run.id,
                details_json={"reason": reason, "requested_asset_ids": requested_asset_ids},
            )
            await session.commit()

        if not requested_asset_ids:
            completed_at = utcnow()
            async with self._session_factory() as session:
                run = await session.get(PolymarketResyncRun, run_id)
                assert run is not None
                run.status = "completed"
                run.completed_at = completed_at
                await upsert_stream_status(
                    session,
                    increment_resyncs=True,
                    last_resync_at=completed_at,
                )
                await log_ingest_incident(
                    session,
                    incident_type="resync_completed",
                    severity="info",
                    connection_id=connection_id,
                    resync_run_id=run.id,
                    details_json={"reason": reason, "requested_asset_ids": []},
                )
                await session.commit()
            polymarket_resync_runs.labels(reason=reason, status="completed").inc()
            return {
                "run_id": run_id,
                "asset_ids": [],
                "requested_asset_count": 0,
                "succeeded_asset_count": 0,
                "failed_asset_count": 0,
                "succeeded_asset_ids": [],
                "failed_asset_ids": [],
                "events_persisted": 0,
                "reason": reason,
                "status": "completed",
            }

        payload_by_asset_id: dict[str, dict[str, Any]] = {}
        failed_asset_ids: list[str] = []
        batch_errors: list[dict[str, Any]] = []
        for batch in _chunk_values(requested_asset_ids, BOOK_RESYNC_BATCH_SIZE):
            try:
                payloads = await self._fetch_books_batch(batch)
            except Exception as exc:
                logger.warning(
                    "Polymarket resync batch failed for %d asset(s): %s",
                    len(batch),
                    exc,
                )
                failed_asset_ids.extend(batch)
                batch_errors.append(
                    {
                        "asset_count": len(batch),
                        "sample_asset_ids": batch[:5],
                        "error": str(exc),
                    }
                )
                continue
            payload_by_asset_id.update({
                str(payload.get("asset_id") or payload.get("assetId")): payload
                for payload in payloads
                if isinstance(payload, dict) and (payload.get("asset_id") or payload.get("assetId"))
            })

        succeeded_asset_ids = [asset_id for asset_id in requested_asset_ids if asset_id in payload_by_asset_id]
        failed_asset_ids.extend(
            asset_id for asset_id in requested_asset_ids
            if asset_id not in payload_by_asset_id and asset_id not in set(failed_asset_ids)
        )
        ingest_batch_id = uuid.uuid4()
        events_persisted = 0
        completed_at = utcnow()

        async with self._session_factory() as session:
            run = await session.get(PolymarketResyncRun, run_id)
            assert run is not None

            for asset_id in succeeded_asset_ids:
                payload = payload_by_asset_id[asset_id]
                metadata = extract_event_metadata(payload)
                event = await persist_market_event(
                    session,
                    provenance=RESYNC_PROVENANCE,
                    channel=REST_RESYNC_CHANNEL,
                    message_type=metadata.message_type if metadata.message_type not in {"", "unknown"} else "book",
                    payload=payload,
                    received_at_local=started_at,
                    market_id=metadata.market_id,
                    asset_id=metadata.asset_id or payload.get("asset_id") or payload.get("assetId"),
                    asset_ids=metadata.asset_ids,
                    event_time=metadata.event_time,
                    connection_id=connection_id,
                    ingest_batch_id=ingest_batch_id,
                    source_message_id=metadata.source_message_id,
                    source_hash=metadata.source_hash,
                    source_sequence=metadata.source_sequence,
                    source_cursor=metadata.source_cursor,
                    resync_reason=reason,
                    resync_run_id=run.id,
                )
                if not await meta_sync_run_in_progress(session):
                    await seed_registry_from_book_snapshot(
                        session,
                        payload=payload,
                        observed_at_local=started_at,
                        sync_run_id=None,
                        raw_event_id=event.id,
                    )
                events_persisted += 1

            run.completed_at = completed_at
            run.succeeded_asset_count = len(succeeded_asset_ids)
            run.failed_asset_count = len(failed_asset_ids)
            run.status = "completed" if not failed_asset_ids else "partial"
            run.details_json = {
                "requested_asset_ids": requested_asset_ids,
                "succeeded_asset_ids": succeeded_asset_ids,
                "failed_asset_ids": failed_asset_ids,
                "batch_errors": batch_errors,
            }
            await upsert_stream_status(
                session,
                increment_resyncs=True,
                last_resync_at=completed_at,
            )
            await log_ingest_incident(
                session,
                incident_type="resync_completed" if not failed_asset_ids else "resync_failed",
                severity="info" if not failed_asset_ids else "warning",
                connection_id=connection_id,
                resync_run_id=run.id,
                details_json={
                    "reason": reason,
                    "requested_asset_ids": requested_asset_ids,
                    "succeeded_asset_ids": succeeded_asset_ids,
                    "failed_asset_ids": failed_asset_ids,
                    "batch_errors": batch_errors,
                },
            )
            await session.commit()

        run_status = "completed" if not failed_asset_ids else "partial"
        polymarket_resync_runs.labels(reason=reason, status=run_status).inc()
        return {
            "run_id": run_id,
            "asset_ids": requested_asset_ids,
            "requested_asset_count": len(requested_asset_ids),
            "succeeded_asset_count": len(succeeded_asset_ids),
            "failed_asset_count": len(failed_asset_ids),
            "succeeded_asset_ids": succeeded_asset_ids,
            "failed_asset_ids": failed_asset_ids,
            "events_persisted": events_persisted,
            "reason": reason,
            "status": run_status,
        }


class PolymarketStreamService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        connect_factory: Callable[..., Any] | None = None,
        resync_service: PolymarketResyncService | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._connect_factory = connect_factory or websockets.connect
        self._resync_service = resync_service or PolymarketResyncService(session_factory)
        self._had_successful_connection = False

    async def _list_watched_asset_ids(self) -> list[str]:
        async with self._session_factory() as session:
            return await list_watched_polymarket_assets(
                session,
                limit=max(1, int(settings.polymarket_snapshot_max_watched_assets)),
                prioritize=True,
            )

    async def _set_connected(self, connection_id: uuid.UUID, subscription_count: int) -> None:
        async with self._session_factory() as session:
            await upsert_stream_status(
                session,
                connected=True,
                connection_started_at=utcnow(),
                current_connection_id=connection_id,
                active_subscription_count=subscription_count,
                last_error=None,
            )
            await session.commit()

    async def _set_disconnected(
        self,
        *,
        active_subscription_count: int = 0,
        last_error: str | None | object = UNSET,
        increment_reconnects: bool = False,
    ) -> None:
        async with self._session_factory() as session:
            await upsert_stream_status(
                session,
                connected=False,
                current_connection_id=None,
                active_subscription_count=active_subscription_count,
                increment_reconnects=increment_reconnects,
                last_error=last_error,
                last_error_at=utcnow() if last_error not in (UNSET, None) else None,
            )
            await session.commit()

    async def _update_subscription_count(self, subscription_count: int) -> None:
        async with self._session_factory() as session:
            await upsert_stream_status(
                session,
                active_subscription_count=subscription_count,
                last_reconciliation_at=utcnow(),
            )
            await session.commit()

    async def _record_gap_suspicion(
        self,
        *,
        reason: str,
        connection_id: uuid.UUID | None,
        asset_id: str | None = None,
        raw_event_id: int | None = None,
        details_json: dict[str, Any] | None = None,
    ) -> None:
        async with self._session_factory() as session:
            await upsert_stream_status(session, gap_suspected_count_delta=1)
            await log_ingest_incident(
                session,
                incident_type="gap_suspected",
                severity="warning",
                asset_id=asset_id,
                connection_id=connection_id,
                raw_event_id=raw_event_id,
                details_json={"reason": reason, **(details_json or {})},
            )
            await session.commit()
        polymarket_gap_suspicions.labels(reason=reason).inc()

    async def persist_stream_message(
        self,
        raw_message: str | bytes,
        connection_id: uuid.UUID,
    ) -> PolymarketMarketEvent | None:
        received_at_local = utcnow()
        raw_text = raw_message.decode("utf-8", "replace") if isinstance(raw_message, bytes) else str(raw_message)

        if raw_text.strip().upper() == "PONG":
            return None

        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            async with self._session_factory() as session:
                event = await persist_market_event(
                    session,
                    provenance=STREAM_PROVENANCE,
                    channel=STREAM_CHANNEL,
                    message_type="malformed",
                    payload=raw_text,
                    received_at_local=received_at_local,
                    connection_id=connection_id,
                )
                await upsert_stream_status(
                    session,
                    last_message_received_at=received_at_local,
                    malformed_message_count_delta=1,
                )
                await session.commit()
            polymarket_malformed_messages.inc()
            return event

        if isinstance(payload, list):
            metadata = EventMetadata(
                message_type="batch",
                market_id=None,
                asset_id=None,
                asset_ids=None,
                event_time=None,
                source_message_id=None,
                source_hash=None,
                source_sequence=None,
                source_cursor=None,
            )
        elif isinstance(payload, dict):
            metadata = extract_event_metadata(payload)
        else:
            metadata = EventMetadata(
                message_type="unknown",
                market_id=None,
                asset_id=None,
                asset_ids=None,
                event_time=None,
                source_message_id=None,
                source_hash=None,
                source_sequence=None,
                source_cursor=None,
            )

        async with self._session_factory() as session:
            event = await persist_market_event(
                session,
                provenance=STREAM_PROVENANCE,
                channel=STREAM_CHANNEL,
                message_type=metadata.message_type,
                payload=payload,
                received_at_local=received_at_local,
                market_id=metadata.market_id,
                asset_id=metadata.asset_id,
                asset_ids=metadata.asset_ids,
                event_time=metadata.event_time,
                connection_id=connection_id,
                source_message_id=metadata.source_message_id,
                source_hash=metadata.source_hash,
                source_sequence=metadata.source_sequence,
                source_cursor=metadata.source_cursor,
            )
            await upsert_stream_status(session, last_message_received_at=received_at_local)
            await session.commit()
        if event.message_type in {"new_market", "tick_size_change", "market_resolved"}:
            try:
                await apply_stream_event_to_registry(self._session_factory, raw_event_id=event.id)
            except Exception:
                logger.warning(
                    "Polymarket registry update failed for raw stream event %s",
                    event.id,
                    exc_info=True,
                )
        return event

    async def reconcile_subscriptions(self, websocket: Any, subscribed_asset_ids: set[str]) -> set[str]:
        desired_asset_ids = set(await self._list_watched_asset_ids())
        to_subscribe, to_unsubscribe = build_subscription_diff(subscribed_asset_ids, desired_asset_ids)

        if to_subscribe:
            logger.info("Polymarket stream subscribing to %d assets", len(to_subscribe))
            for batch in _chunk_values(to_subscribe, STREAM_SUBSCRIPTION_BATCH_SIZE):
                await websocket.send(json.dumps({
                    "assets_ids": batch,
                    "operation": "subscribe",
                    "custom_feature_enabled": True,
                }))

        if to_unsubscribe:
            logger.info("Polymarket stream unsubscribing from %d assets", len(to_unsubscribe))
            for batch in _chunk_values(to_unsubscribe, STREAM_SUBSCRIPTION_BATCH_SIZE):
                await websocket.send(json.dumps({
                    "assets_ids": batch,
                    "operation": "unsubscribe",
                }))

        new_subscribed = (subscribed_asset_ids | set(to_subscribe)) - set(to_unsubscribe)

        async with self._session_factory() as session:
            await upsert_stream_status(
                session,
                active_subscription_count=len(new_subscribed),
                last_reconciliation_at=utcnow(),
            )
            if to_subscribe or to_unsubscribe:
                await log_ingest_incident(
                    session,
                    incident_type="subscription_reconciled",
                    severity="info",
                    details_json={
                        "subscribed_asset_ids": sorted(new_subscribed),
                        "to_subscribe": to_subscribe,
                        "to_unsubscribe": to_unsubscribe,
                    },
                )
            await session.commit()

        if to_subscribe:
            await self._resync_service.resync_assets(
                to_subscribe,
                reason="subscription_change",
            )

        return new_subscribed

    async def close(self) -> None:
        await self._resync_service.close()

    async def run(self, stop_event: asyncio.Event) -> None:
        if not settings.polymarket_stream_enabled:
            logger.info("Polymarket stream disabled; skipping worker startup")
            return

        reconnect_sleep = settings.polymarket_stream_reconnect_base_seconds
        while not stop_event.is_set():
            watched_asset_ids = await self._list_watched_asset_ids()
            if not watched_asset_ids:
                await self._set_disconnected(active_subscription_count=0)
                await asyncio.sleep(settings.polymarket_watch_reconcile_interval_seconds)
                continue

            connection_id = uuid.uuid4()
            subscribed_asset_ids: set[str] = set()
            last_message_received_at = utcnow()
            last_gap_resync_at: datetime | None = None
            last_sequence: int | None = None
            malformed_times: deque[datetime] = deque()

            try:
                async with self._connect_factory(
                    settings.polymarket_stream_url,
                    open_timeout=settings.connector_timeout_seconds,
                    max_size=(
                        settings.polymarket_stream_max_message_bytes
                        if settings.polymarket_stream_max_message_bytes > 0
                        else None
                    ),
                ) as websocket:
                    for batch in _chunk_values(watched_asset_ids, STREAM_SUBSCRIPTION_BATCH_SIZE):
                        await websocket.send(json.dumps({
                            "type": STREAM_CHANNEL,
                            "assets_ids": batch,
                            "custom_feature_enabled": True,
                        }))
                    subscribed_asset_ids = set(watched_asset_ids)
                    await self._set_connected(connection_id, len(subscribed_asset_ids))

                    if self._had_successful_connection:
                        async with self._session_factory() as session:
                            await log_ingest_incident(
                                session,
                                incident_type="reconnect",
                                severity="info",
                                connection_id=connection_id,
                                details_json={"asset_ids": sorted(subscribed_asset_ids)},
                            )
                            await session.commit()
                        await self._record_gap_suspicion(
                            reason="reconnect",
                            connection_id=connection_id,
                            details_json={"asset_ids": sorted(subscribed_asset_ids)},
                        )
                        await self._resync_service.resync_assets(
                            sorted(subscribed_asset_ids),
                            reason="reconnect",
                            connection_id=connection_id,
                        )
                    elif settings.polymarket_resync_on_startup:
                        await self._resync_service.resync_assets(
                            sorted(subscribed_asset_ids),
                            reason="startup",
                            connection_id=connection_id,
                        )

                    self._had_successful_connection = True
                    reconnect_sleep = settings.polymarket_stream_reconnect_base_seconds
                    loop = asyncio.get_running_loop()
                    next_reconcile_at = loop.time() + settings.polymarket_watch_reconcile_interval_seconds

                    while not stop_event.is_set():
                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=max(0.1, settings.polymarket_stream_ping_interval_seconds),
                            )
                            event = await self.persist_stream_message(raw_message, connection_id)
                            if event is None:
                                continue

                            last_message_received_at = event.received_at_local

                            if event.message_type == "malformed":
                                malformed_times.append(last_message_received_at)
                                while malformed_times and (
                                    last_message_received_at - malformed_times[0]
                                ).total_seconds() > settings.polymarket_malformed_burst_window_seconds:
                                    malformed_times.popleft()
                                if len(malformed_times) >= settings.polymarket_malformed_burst_threshold:
                                    if (
                                        last_gap_resync_at is None
                                        or (last_message_received_at - last_gap_resync_at).total_seconds()
                                        >= settings.polymarket_gap_suspect_after_seconds
                                    ):
                                        await self._record_gap_suspicion(
                                            reason="malformed_burst",
                                            connection_id=connection_id,
                                            raw_event_id=event.id,
                                            details_json={"count": len(malformed_times)},
                                        )
                                        await self._resync_service.resync_assets(
                                            sorted(subscribed_asset_ids),
                                            reason="gap_suspected",
                                            connection_id=connection_id,
                                        )
                                        last_gap_resync_at = last_message_received_at
                            else:
                                malformed_times.clear()

                            if event.source_sequence and event.source_sequence.isdigit():
                                current_sequence = int(event.source_sequence)
                                if last_sequence is not None and current_sequence != last_sequence + 1:
                                    if (
                                        last_gap_resync_at is None
                                        or (last_message_received_at - last_gap_resync_at).total_seconds()
                                        >= settings.polymarket_gap_suspect_after_seconds
                                    ):
                                        await self._record_gap_suspicion(
                                            reason="sequence_discontinuity",
                                            connection_id=connection_id,
                                            asset_id=event.asset_id,
                                            raw_event_id=event.id,
                                            details_json={
                                                "previous_sequence": last_sequence,
                                                "current_sequence": current_sequence,
                                            },
                                        )
                                        await self._resync_service.resync_assets(
                                            event.asset_ids or sorted(subscribed_asset_ids),
                                            reason="gap_suspected",
                                            connection_id=connection_id,
                                        )
                                        last_gap_resync_at = last_message_received_at
                                last_sequence = current_sequence

                        except asyncio.TimeoutError:
                            now = utcnow()
                            silence_seconds = (now - last_message_received_at).total_seconds()
                            if silence_seconds >= settings.polymarket_gap_suspect_after_seconds:
                                if (
                                    last_gap_resync_at is None
                                    or (now - last_gap_resync_at).total_seconds()
                                    >= settings.polymarket_gap_suspect_after_seconds
                                ):
                                    await self._record_gap_suspicion(
                                        reason="heartbeat_silence",
                                        connection_id=connection_id,
                                        details_json={"silence_seconds": silence_seconds},
                                    )
                                    await self._resync_service.resync_assets(
                                        sorted(subscribed_asset_ids),
                                        reason="gap_suspected",
                                        connection_id=connection_id,
                                    )
                                    last_gap_resync_at = now
                            await websocket.send("PING")

                        if loop.time() >= next_reconcile_at:
                            desired_asset_ids = set(await self._list_watched_asset_ids())
                            if not desired_asset_ids:
                                logger.info("Polymarket stream watch set is empty; closing live connection")
                                await websocket.close()
                                break
                            subscribed_asset_ids = await self.reconcile_subscriptions(websocket, subscribed_asset_ids)
                            next_reconcile_at = loop.time() + settings.polymarket_watch_reconcile_interval_seconds

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                async with self._session_factory() as session:
                    await log_ingest_incident(
                        session,
                        incident_type="disconnect",
                        severity="warning",
                        connection_id=connection_id,
                        details_json={"error": str(exc)},
                    )
                    await session.commit()
                await self._set_disconnected(
                    active_subscription_count=0,
                    last_error=str(exc),
                    increment_reconnects=self._had_successful_connection,
                )
                logger.warning("Polymarket stream connection failed: %s", str(exc))
                if stop_event.is_set():
                    break
                await asyncio.sleep(reconnect_sleep)
                reconnect_sleep = min(
                    settings.polymarket_stream_reconnect_max_seconds,
                    max(settings.polymarket_stream_reconnect_base_seconds, reconnect_sleep * 2),
                )
                continue

            await self._set_disconnected(active_subscription_count=0)
            reconnect_sleep = settings.polymarket_stream_reconnect_base_seconds


async def trigger_manual_polymarket_resync(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    asset_ids: list[str] | None,
    reason: str,
) -> dict[str, Any]:
    resolved_asset_ids = unique_preserving_order(asset_ids or [])
    if not resolved_asset_ids:
        async with session_factory() as session:
            resolved_asset_ids = await list_watched_polymarket_assets(session)

    service = PolymarketResyncService(session_factory)
    try:
        return await service.resync_assets(resolved_asset_ids, reason=reason)
    finally:
        await service.close()
