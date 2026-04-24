import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion import polymarket_stream as polymarket_stream_module
from app.ingestion.polymarket_stream import (
    PolymarketResyncService,
    PolymarketStreamService,
    build_subscription_diff,
    ensure_watch_registry_bootstrapped,
    persist_market_event,
)
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketDim
from app.models.polymarket_stream import (
    PolymarketIngestIncident,
    PolymarketMarketEvent,
    PolymarketNormalizedEvent,
    PolymarketResyncRun,
    PolymarketStreamStatus,
    PolymarketWatchAsset,
)
from tests.conftest import (
    make_market,
    make_outcome,
    make_polymarket_market_event,
    make_polymarket_stream_status,
)


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class FakeWebSocket:
    def __init__(self, *, messages=None, exception_on_empty=None):
        self.messages = list(messages or [])
        self.exception_on_empty = exception_on_empty
        self.sent = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send(self, payload):
        self.sent.append(payload)

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        if self.exception_on_empty is not None:
            raise self.exception_on_empty
        await asyncio.sleep(0.01)
        return "PONG"

    async def close(self):
        self.closed = True


class SequentialConnectFactory:
    def __init__(self, *websockets_):
        self.websockets = list(websockets_)

    def __call__(self, *_args, **_kwargs):
        if not self.websockets:
            raise RuntimeError("no websocket left")
        return self.websockets.pop(0)


class RecordingConnectFactory:
    def __init__(self, websocket):
        self.websocket = websocket
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append({"args": args, "kwargs": kwargs})
        return self.websocket


class RecordingResyncStub:
    def __init__(self):
        self.calls = []

    async def resync_assets(self, asset_ids, *, reason, connection_id=None):
        self.calls.append((list(asset_ids), reason, connection_id))
        return {
            "run_id": uuid.uuid4(),
            "asset_ids": list(asset_ids),
            "requested_asset_count": len(asset_ids),
            "succeeded_asset_count": len(asset_ids),
            "failed_asset_count": 0,
            "events_persisted": len(asset_ids),
            "reason": reason,
            "status": "completed",
        }

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_build_subscription_diff():
    to_subscribe, to_unsubscribe = build_subscription_diff({"a"}, {"a", "b", "c"})
    assert to_subscribe == ["b", "c"]
    assert to_unsubscribe == []

    to_subscribe, to_unsubscribe = build_subscription_diff({"a", "b"}, {"b"})
    assert to_subscribe == []
    assert to_unsubscribe == ["a"]


def test_watch_registry_insert_batch_size_respects_asyncpg_limit():
    assert polymarket_stream_module._watch_registry_insert_batch_size(
        requested_size=polymarket_stream_module.WATCH_REGISTRY_LOOKUP_BATCH_SIZE,
        row_field_count=7,
        dialect_name="postgresql",
    ) == polymarket_stream_module.ASYNC_PG_BIND_PARAMETER_LIMIT // 7
    assert polymarket_stream_module._watch_registry_insert_batch_size(
        requested_size=polymarket_stream_module.WATCH_REGISTRY_LOOKUP_BATCH_SIZE,
        row_field_count=7,
        dialect_name="sqlite",
    ) == polymarket_stream_module.WATCH_REGISTRY_LOOKUP_BATCH_SIZE


@pytest.mark.asyncio
async def test_stream_message_persists_append_only_preserves_timestamps_and_normalizes(engine):
    session_factory = _session_factory(engine)
    service = PolymarketStreamService(session_factory, resync_service=RecordingResyncStub())
    connection_id = uuid.uuid4()
    message = json.dumps(
        {
            "event_type": "best_bid_ask",
            "market": "cond-1",
            "asset_id": "token-1",
            "best_bid": "0.45",
            "best_ask": "0.47",
            "spread": "0.02",
            "timestamp": "1757908892351",
        }
    )

    event = await service.persist_stream_message(message, connection_id)
    assert event is not None

    async with session_factory() as session:
        result = await session.execute(select(PolymarketMarketEvent).order_by(PolymarketMarketEvent.id.asc()))
        events = list(result.scalars().all())
        assert len(events) == 1
        assert events[0].event_time.replace(tzinfo=timezone.utc) == datetime.fromtimestamp(1757908892.351, tz=timezone.utc)
        assert events[0].provenance == "stream"

        normalized = await session.get(PolymarketNormalizedEvent, events[0].id)
        assert normalized is not None
        assert normalized.is_top_of_book is True
        assert normalized.best_bid is not None
        assert normalized.best_ask is not None


@pytest.mark.asyncio
async def test_unknown_normalization_fails_soft_without_losing_raw_event(session):
    event = await persist_market_event(
        session,
        provenance="stream",
        channel="market",
        message_type="mystery",
        payload={"foo": "bar"},
        received_at_local=datetime.now(timezone.utc),
    )
    await session.commit()

    raw = await session.get(PolymarketMarketEvent, event.id)
    normalized = await session.get(PolymarketNormalizedEvent, event.id)
    assert raw is not None
    assert normalized is not None
    assert normalized.parse_status == "unknown"


@pytest.mark.asyncio
async def test_resync_service_persists_runs_events_and_partial_failures(engine):
    session_factory = _session_factory(engine)
    service = PolymarketResyncService(session_factory)

    with respx.mock(assert_all_called=True) as router:
        router.post("https://clob.polymarket.com/books").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "asset_id": "token-rest",
                        "market": "cond-rest",
                        "timestamp": "2026-04-13T09:30:00Z",
                        "bids": [{"price": "0.40", "size": "100"}],
                        "asks": [{"price": "0.60", "size": "110"}],
                        "hash": "0xabc",
                    }
                ],
            )
        )
        result = await service.resync_assets(["token-rest", "token-missing"], reason="startup")

    await service.close()

    assert result["requested_asset_count"] == 2
    assert result["succeeded_asset_count"] == 1
    assert result["failed_asset_count"] == 1
    assert result["status"] == "partial"

    async with session_factory() as session:
        event = (await session.execute(select(PolymarketMarketEvent))).scalar_one()
        run = (await session.execute(select(PolymarketResyncRun))).scalar_one()
        status = await session.get(PolymarketStreamStatus, "polymarket")
        assert event.provenance == "rest_resync"
        assert event.resync_reason == "startup"
        assert event.resync_run_id == run.id
        assert status is not None
        assert status.resync_count == 1


@pytest.mark.asyncio
async def test_resync_service_batches_large_book_requests(engine, monkeypatch):
    session_factory = _session_factory(engine)
    service = PolymarketResyncService(session_factory)
    batches: list[list[str]] = []

    async def fake_fetch_books_batch(asset_ids):
        batches.append(list(asset_ids))
        return [
            {
                "asset_id": asset_id,
                "market": f"cond-{asset_id}",
                "timestamp": "2026-04-13T09:30:00Z",
                "bids": [],
                "asks": [],
            }
            for asset_id in asset_ids
        ]

    monkeypatch.setattr(polymarket_stream_module, "BOOK_RESYNC_BATCH_SIZE", 2)
    monkeypatch.setattr(service, "_fetch_books_batch", fake_fetch_books_batch)

    result = await service.resync_assets(["asset-a", "asset-b", "asset-c", "asset-d", "asset-e"], reason="startup")
    await service.close()

    assert batches == [["asset-a", "asset-b"], ["asset-c", "asset-d"], ["asset-e"]]
    assert result["requested_asset_count"] == 5
    assert result["succeeded_asset_count"] == 5
    assert result["failed_asset_count"] == 0


@pytest.mark.asyncio
async def test_bootstrap_watch_registry_preserves_active_universe_coverage(session):
    market_one = make_market(session, platform="polymarket", platform_id="mkt-1", active=True)
    market_two = make_market(session, platform="polymarket", platform_id="mkt-2", active=True)
    await session.flush()
    outcome_one = make_outcome(session, market_one.id, token_id="token-a")
    outcome_two = make_outcome(session, market_two.id, token_id="token-b")
    await session.commit()

    bootstrap = await ensure_watch_registry_bootstrapped(session)
    await session.commit()

    rows = (await session.execute(select(PolymarketWatchAsset))).scalars().all()
    assert bootstrap["created_count"] == 2
    assert {row.asset_id for row in rows} == {outcome_one.token_id, outcome_two.token_id}


@pytest.mark.asyncio
async def test_bootstrap_watch_registry_chunks_existing_outcome_lookup(session, monkeypatch):
    for index in range(5):
        market = make_market(session, platform="polymarket", platform_id=f"chunked-{index}", active=True)
        await session.flush()
        outcome = make_outcome(session, market.id, token_id=f"token-{index}")
        await session.flush()
        if index == 0:
            session.add(
                PolymarketWatchAsset(
                    outcome_id=outcome.id,
                    asset_id="stale-token",
                    watch_enabled=True,
                    watch_reason="seeded",
                )
            )
    await session.commit()

    watch_lookup_count = 0
    original_execute = session.execute

    async def tracking_execute(statement, *args, **kwargs):
        nonlocal watch_lookup_count
        rendered = str(statement)
        if "FROM polymarket_watch_assets" in rendered and "IN (" in rendered:
            watch_lookup_count += 1
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(polymarket_stream_module, "WATCH_REGISTRY_LOOKUP_BATCH_SIZE", 2)
    monkeypatch.setattr(session, "execute", tracking_execute)

    bootstrap = await ensure_watch_registry_bootstrapped(session)
    await session.commit()

    rows = (await original_execute(select(PolymarketWatchAsset))).scalars().all()
    assert bootstrap["created_count"] == 4
    assert bootstrap["updated_count"] == 1
    assert len(rows) == 5
    assert watch_lookup_count == 3


@pytest.mark.asyncio
async def test_bootstrap_watch_registry_chunks_insert_batches_independently(session, monkeypatch):
    for index in range(5):
        market = make_market(session, platform="polymarket", platform_id=f"insert-{index}", active=True)
        await session.flush()
        make_outcome(session, market.id, token_id=f"insert-token-{index}")
    await session.commit()

    insert_count = 0
    original_execute = session.execute

    async def tracking_execute(statement, *args, **kwargs):
        nonlocal insert_count
        if str(statement).startswith("INSERT INTO polymarket_watch_assets"):
            insert_count += 1
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(polymarket_stream_module, "_watch_registry_insert_batch_size", lambda **_kwargs: 2)
    monkeypatch.setattr(session, "execute", tracking_execute)

    bootstrap = await ensure_watch_registry_bootstrapped(session)
    await session.commit()

    rows = (await original_execute(select(PolymarketWatchAsset))).scalars().all()
    assert bootstrap["created_count"] == 5
    assert len(rows) == 5
    assert insert_count == 3


@pytest.mark.asyncio
async def test_bootstrap_watch_registry_suppresses_stale_auto_watch_set_until_registry_ready(session, monkeypatch):
    market = make_market(session, platform="polymarket", platform_id="mkt-pending", active=True)
    await session.flush()
    outcome = make_outcome(session, market.id, token_id="token-pending")
    await session.flush()
    session.add(
        PolymarketWatchAsset(
            outcome_id=outcome.id,
            asset_id="token-pending",
            watch_enabled=True,
            watch_reason="active_universe_bootstrap",
        )
    )
    await session.commit()

    monkeypatch.setattr(settings, "polymarket_meta_sync_enabled", True)

    bootstrap = await ensure_watch_registry_bootstrapped(session)
    await session.commit()

    row = (await session.execute(select(PolymarketWatchAsset))).scalar_one()
    assert bootstrap["source"] == "registry_pending"
    assert bootstrap["disabled_count"] == 1
    assert row.watch_enabled is False


@pytest.mark.asyncio
async def test_bootstrap_watch_registry_prefers_live_registry_truth_and_disables_stale_auto_rows(session, monkeypatch):
    market_one = make_market(session, platform="polymarket", platform_id="mkt-registry-1", active=True)
    market_two = make_market(session, platform="polymarket", platform_id="mkt-registry-2", active=True)
    await session.flush()
    outcome_one = make_outcome(session, market_one.id, token_id="token-live")
    outcome_two = make_outcome(session, market_two.id, token_id="token-stale")
    await session.flush()

    market_dim = PolymarketMarketDim(
        gamma_market_id="gamma-live-1",
        condition_id="cond-live-1",
        question="Registry-backed market?",
        active=True,
        closed=False,
        archived=False,
        resolved=False,
        accepting_orders=True,
    )
    session.add(market_dim)
    await session.flush()
    session.add(
        PolymarketAssetDim(
            asset_id="token-live",
            condition_id="cond-live-1",
            market_dim_id=market_dim.id,
            outcome_id=outcome_one.id,
            outcome_name="Yes",
            outcome_index=0,
            active=True,
        )
    )
    session.add_all(
        [
            PolymarketWatchAsset(
                outcome_id=outcome_one.id,
                asset_id="token-live",
                watch_enabled=False,
                watch_reason="active_universe_bootstrap",
            ),
            PolymarketWatchAsset(
                outcome_id=outcome_two.id,
                asset_id="token-stale",
                watch_enabled=True,
                watch_reason="active_universe_bootstrap",
            ),
        ]
    )
    await session.commit()

    monkeypatch.setattr(settings, "polymarket_meta_sync_enabled", True)

    bootstrap = await ensure_watch_registry_bootstrapped(session)
    await session.commit()

    rows = (
        await session.execute(
            select(PolymarketWatchAsset).order_by(PolymarketWatchAsset.asset_id.asc())
        )
    ).scalars().all()
    row_by_asset = {row.asset_id: row for row in rows}

    assert bootstrap["source"] == "registry_live_bootstrap"
    assert bootstrap["disabled_count"] == 1
    assert row_by_asset["token-live"].watch_enabled is True
    assert row_by_asset["token-live"].watch_reason == "registry_live_bootstrap"
    assert row_by_asset["token-stale"].watch_enabled is False


@pytest.mark.asyncio
async def test_reconciliation_uses_watch_registry_and_unchanged_set_does_not_churn(engine):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market_one = make_market(session, platform="polymarket", platform_id="mkt-1", active=True)
        market_two = make_market(session, platform="polymarket", platform_id="mkt-2", active=True)
        await session.flush()
        make_outcome(session, market_one.id, token_id="token-a")
        outcome_two = make_outcome(session, market_two.id, token_id="token-b")
        await session.flush()
        await ensure_watch_registry_bootstrapped(session)
        await session.commit()
        watch_two = (
            await session.execute(select(PolymarketWatchAsset).where(PolymarketWatchAsset.outcome_id == outcome_two.id))
        ).scalar_one()
        watch_two.watch_enabled = False
        await session.commit()

    websocket = FakeWebSocket()
    service = PolymarketStreamService(session_factory, resync_service=RecordingResyncStub())
    subscribed = await service.reconcile_subscriptions(websocket, set())
    assert subscribed == {"token-a"}
    assert len(websocket.sent) == 1
    assert json.loads(websocket.sent[0])["operation"] == "subscribe"
    assert json.loads(websocket.sent[0])["custom_feature_enabled"] is True

    websocket.sent.clear()
    subscribed = await service.reconcile_subscriptions(websocket, subscribed)
    assert subscribed == {"token-a"}
    assert websocket.sent == []


@pytest.mark.asyncio
async def test_worker_loop_records_reconnect_and_gap_incidents(engine, monkeypatch):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="mkt-run", active=True)
        await session.flush()
        make_outcome(session, market.id, token_id="token-run")
        await session.commit()

    monkeypatch.setattr(settings, "polymarket_stream_enabled", True)
    monkeypatch.setattr(settings, "polymarket_stream_reconnect_base_seconds", 0.01)
    monkeypatch.setattr(settings, "polymarket_stream_reconnect_max_seconds", 0.02)
    monkeypatch.setattr(settings, "polymarket_stream_ping_interval_seconds", 1)
    monkeypatch.setattr(settings, "polymarket_watch_reconcile_interval_seconds", 1)

    ws_one = FakeWebSocket(
        messages=[
            json.dumps(
                {
                    "event_type": "book",
                    "asset_id": "token-run",
                    "market": "cond-run",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "10"}],
                    "timestamp": "1757908892351",
                    "sequence_id": "1",
                    "hash": "0xrun",
                }
            )
        ],
        exception_on_empty=RuntimeError("socket dropped"),
    )
    ws_two = FakeWebSocket(exception_on_empty=RuntimeError("socket dropped again"))
    connect_factory = SequentialConnectFactory(ws_one, ws_two)
    resync_stub = RecordingResyncStub()
    service = PolymarketStreamService(
        session_factory,
        connect_factory=connect_factory,
        resync_service=resync_stub,
    )

    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run(stop_event))
    try:
        for _ in range(50):
            if len(resync_stub.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await service.close()

    async with session_factory() as session:
        incidents = (
            await session.execute(select(PolymarketIngestIncident).order_by(PolymarketIngestIncident.created_at.asc()))
        ).scalars().all()
        status = await session.get(PolymarketStreamStatus, "polymarket")
        assert any(incident.incident_type == "disconnect" for incident in incidents)
        assert any(
            incident.incident_type == "gap_suspected"
            and (incident.details_json or {}).get("reason") == "reconnect"
            for incident in incidents
        )
        assert status is not None
        assert status.reconnect_count >= 1
        assert json.loads(ws_one.sent[0])["custom_feature_enabled"] is True
        assert any(reason == "startup" for _assets, reason, _connection_id in resync_stub.calls)
        assert any(reason == "reconnect" for _assets, reason, _connection_id in resync_stub.calls)


@pytest.mark.asyncio
async def test_worker_loop_passes_configured_max_message_size_to_websocket(engine, monkeypatch):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="mkt-max-size", active=True)
        await session.flush()
        make_outcome(session, market.id, token_id="token-max-size")
        await session.commit()

    monkeypatch.setattr(settings, "polymarket_stream_enabled", True)
    monkeypatch.setattr(settings, "polymarket_stream_reconnect_base_seconds", 0.01)
    monkeypatch.setattr(settings, "polymarket_stream_reconnect_max_seconds", 0.02)
    monkeypatch.setattr(settings, "polymarket_watch_reconcile_interval_seconds", 1)
    monkeypatch.setattr(settings, "polymarket_stream_max_message_bytes", 1234567)

    websocket = FakeWebSocket(exception_on_empty=RuntimeError("socket dropped"))
    connect_factory = RecordingConnectFactory(websocket)
    service = PolymarketStreamService(
        session_factory,
        connect_factory=connect_factory,
        resync_service=RecordingResyncStub(),
    )

    stop_event = asyncio.Event()
    task = asyncio.create_task(service.run(stop_event))
    try:
        for _ in range(50):
            if connect_factory.calls:
                break
            await asyncio.sleep(0.02)
        stop_event.set()
        await asyncio.wait_for(task, timeout=1)
    finally:
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await service.close()

    assert connect_factory.calls
    assert connect_factory.calls[0]["kwargs"]["max_size"] == 1234567


@pytest.mark.asyncio
async def test_status_and_manual_resync_and_paginated_endpoints(client, engine):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="mkt-api", active=True)
        await session.flush()
        make_outcome(session, market.id, token_id="token-api")
        await session.flush()
        make_polymarket_stream_status(
            session,
            connected=True,
            active_subscription_count=1,
            reconnect_count=2,
            resync_count=1,
            gap_suspected_count=1,
            malformed_message_count=3,
            last_error="last failure",
            last_error_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )
        stream_event = make_polymarket_market_event(
            session,
            provenance="stream",
            received_at_local=datetime.now(timezone.utc),
            asset_id="token-api",
        )
        await session.flush()
        await ensure_watch_registry_bootstrapped(session)
        await session.commit()
        assert stream_event.id is not None

    with respx.mock(assert_all_called=True) as router:
        router.post("https://clob.polymarket.com/books").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "asset_id": "token-api",
                        "market": "cond-api",
                        "timestamp": "2026-04-13T11:00:00Z",
                        "bids": [],
                        "asks": [],
                        "hash": "0xmanual",
                    }
                ],
            )
        )
        manual = await client.post("/api/v1/ingest/polymarket/resync", json={"reason": "manual"})

    assert manual.status_code == 200
    manual_data = manual.json()
    assert manual_data["requested_asset_count"] == 1
    assert manual_data["status"] == "completed"

    status_response = await client.get("/api/v1/ingest/polymarket/status")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["enabled"] is False
    assert status_data["connected"] is True
    assert status_data["continuity_status"] in {"disabled", "healthy", "stale", "awaiting_events"}
    assert status_data["heartbeat_freshness_seconds"] is not None
    assert status_data["watched_asset_count"] == 1
    assert status_data["active_subscription_count"] == 1
    assert status_data["gap_suspected_count"] >= 1
    assert status_data["malformed_message_count"] == 3
    assert status_data["last_event_received_at"] is not None
    assert "replay" in status_data
    assert status_data["replay"]["coverage_mode"] == "no_detector_activity"
    assert any(row["family"] == "default_strategy" for row in status_data["strategy_families"])
    assert len(status_data["recent_resync_runs"]) >= 1

    incidents = await client.get("/api/v1/ingest/polymarket/incidents?page=1&page_size=20")
    assert incidents.status_code == 200
    assert incidents.json()["total"] >= 1

    runs = await client.get("/api/v1/ingest/polymarket/resync-runs?page=1&page_size=20")
    assert runs.status_code == 200
    assert runs.json()["total"] >= 1

    watch_assets = await client.get("/api/v1/ingest/polymarket/watch-assets?page=1&page_size=20")
    assert watch_assets.status_code == 200
    watch_row = watch_assets.json()["watch_assets"][0]
    patch = await client.patch(
        f"/api/v1/ingest/polymarket/watch-assets/{watch_row['id']}",
        json={"watch_enabled": False, "priority": 5},
    )
    assert patch.status_code == 200
    assert patch.json()["watch_enabled"] is False
