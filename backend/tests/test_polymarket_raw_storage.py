from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_raw_storage import (
    SOURCE_KIND_REST_PERIODIC_SNAPSHOT,
    PolymarketRawStorageService,
)
from app.ingestion.polymarket_stream import ensure_watch_registry_bootstrapped
from app.models.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketMarketDim,
    PolymarketMarketParamHistory,
)
from app.models.polymarket_raw import (
    PolymarketBboEvent,
    PolymarketBookDelta,
    PolymarketBookSnapshot,
    PolymarketOpenInterestHistory,
    PolymarketRawCaptureRun,
    PolymarketTradeTape,
)
from tests.conftest import make_market, make_outcome, make_polymarket_market_event


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed_registry(
    session: AsyncSession,
    *,
    condition_id: str = "cond-raw-1",
    asset_id: str = "token-raw-1",
    outcome_name: str = "Yes",
    outcome_index: int = 0,
):
    market = make_market(
        session,
        platform="polymarket",
        platform_id=f"platform-{condition_id}",
        question=f"Question for {condition_id}",
    )
    await session.flush()
    outcome = make_outcome(session, market.id, name=outcome_name, token_id=asset_id)
    await session.flush()
    await ensure_watch_registry_bootstrapped(session)

    market_dim = PolymarketMarketDim(
        gamma_market_id=f"gamma-{condition_id}",
        condition_id=condition_id,
        market_slug=f"market-{condition_id}",
        question=market.question,
        active=True,
        closed=False,
        archived=False,
        source_payload_json={"conditionId": condition_id},
        last_gamma_sync_at=datetime.now(timezone.utc),
    )
    session.add(market_dim)
    await session.flush()

    asset_dim = PolymarketAssetDim(
        asset_id=asset_id,
        condition_id=condition_id,
        market_dim_id=market_dim.id,
        outcome_id=outcome.id,
        outcome_name=outcome.name,
        outcome_index=outcome_index,
        active=True,
        source_payload_json={"asset_id": asset_id},
        last_gamma_sync_at=datetime.now(timezone.utc),
    )
    session.add(asset_dim)
    await session.commit()

    return {
        "market": market,
        "outcome": outcome,
        "market_dim": market_dim,
        "asset_dim": asset_dim,
    }


@pytest.mark.asyncio
async def test_projector_backfills_book_price_change_bbo_and_trade_rows_idempotently(engine):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-project", asset_id="token-project")
        make_polymarket_market_event(
            session,
            provenance="rest_resync",
            channel="rest_orderbook",
            message_type="snapshot",
            market_id="cond-project",
            asset_id="token-project",
            event_time=now - timedelta(minutes=5),
            received_at_local=now - timedelta(minutes=5),
            payload={
                "market": "cond-project",
                "asset_id": "token-project",
                "timestamp": (now - timedelta(minutes=5)).isoformat(),
                "hash": "0xresync",
                "bids": [{"price": "0.41", "size": "80"}],
                "asks": [{"price": "0.59", "size": "90"}],
            },
        )
        stream_book = make_polymarket_market_event(
            session,
            message_type="book",
            market_id="cond-project",
            asset_id="token-project",
            event_time=now - timedelta(minutes=4),
            received_at_local=now - timedelta(minutes=4),
            payload={
                "event_type": "book",
                "market": "cond-project",
                "asset_id": "token-project",
                "timestamp": (now - timedelta(minutes=4)).isoformat(),
                "hash": "0xbook",
                "tick_size": "0.01",
                "min_order_size": "5",
                "bids": [{"price": "0.44", "size": "100"}],
                "asks": [{"price": "0.56", "size": "110"}],
            },
        )
        price_change = make_polymarket_market_event(
            session,
            message_type="price_change",
            market_id="cond-project",
            asset_id="token-project",
            event_time=now - timedelta(minutes=3),
            received_at_local=now - timedelta(minutes=3),
            payload={
                "event_type": "price_change",
                "market": "cond-project",
                "asset_id": "token-project",
                "price_changes": [
                    {
                        "market": "cond-project",
                        "asset_id": "token-project",
                        "price": "0.45",
                        "size": "50",
                        "side": "BUY",
                        "best_bid": "0.45",
                        "best_ask": "0.55",
                    },
                    {
                        "market": "cond-project",
                        "asset_id": "token-project",
                        "price": "0.55",
                        "size": "40",
                        "side": "SELL",
                        "best_bid": "0.45",
                        "best_ask": "0.55",
                    },
                ],
            },
        )
        bbo_event = make_polymarket_market_event(
            session,
            message_type="best_bid_ask",
            market_id="cond-project",
            asset_id="token-project",
            event_time=now - timedelta(minutes=2),
            received_at_local=now - timedelta(minutes=2),
            payload={
                "event_type": "best_bid_ask",
                "market": "cond-project",
                "asset_id": "token-project",
                "best_bid": "0.46",
                "best_ask": "0.54",
                "spread": "0.08",
            },
        )
        trade_event = make_polymarket_market_event(
            session,
            message_type="last_trade_price",
            market_id="cond-project",
            asset_id="token-project",
            event_time=now - timedelta(minutes=1),
            received_at_local=now - timedelta(minutes=1),
            payload={
                "event_type": "last_trade_price",
                "market": "cond-project",
                "asset_id": "token-project",
                "price": "0.51",
                "size": "12",
                "side": "buy",
                "transaction_hash": "0xws-project",
            },
        )
        await session.commit()

    service = PolymarketRawStorageService(session_factory)
    first_run = await service.project_pending_events(reason="manual", after_raw_event_id=0)
    second_run = await service.project_pending_events(reason="manual", after_raw_event_id=0)
    await service.close()

    assert first_run["status"] == "completed"
    assert first_run["rows_inserted_json"] == {
        "book_snapshots": 2,
        "book_deltas": 2,
        "bbo_events": 1,
        "trade_tape": 1,
    }
    assert second_run["status"] == "completed"
    assert second_run["rows_inserted_json"] == {
        "book_snapshots": 0,
        "book_deltas": 0,
        "bbo_events": 0,
        "trade_tape": 0,
    }

    async with session_factory() as session:
        snapshots = (
            await session.execute(select(PolymarketBookSnapshot).order_by(PolymarketBookSnapshot.id.asc()))
        ).scalars().all()
        deltas = (
            await session.execute(select(PolymarketBookDelta).order_by(PolymarketBookDelta.delta_index.asc()))
        ).scalars().all()
        bbo_rows = (
            await session.execute(select(PolymarketBboEvent).order_by(PolymarketBboEvent.id.asc()))
        ).scalars().all()
        trades = (
            await session.execute(select(PolymarketTradeTape).order_by(PolymarketTradeTape.id.asc()))
        ).scalars().all()

        assert [row.source_kind for row in snapshots] == ["rest_resync_snapshot", "ws_book"]
        assert snapshots[1].raw_event_id == stream_book.id
        assert snapshots[1].book_hash == "0xbook"
        assert [row.delta_index for row in deltas] == [0, 1]
        assert deltas[0].raw_event_id == price_change.id
        assert [str(row.price) for row in deltas] == ["0.45000000", "0.55000000"]
        assert len(bbo_rows) == 1
        assert bbo_rows[0].raw_event_id == bbo_event.id
        assert len(trades) == 1
        assert trades[0].raw_event_id == trade_event.id
        assert trades[0].source_kind == "ws_last_trade_price"
        assert trades[0].transaction_hash == "0xws-project"


@pytest.mark.asyncio
async def test_trade_backfill_inserts_new_rows_and_dedupes_ws_trades(engine):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-trades", asset_id="token-trades")
        make_polymarket_market_event(
            session,
            message_type="last_trade_price",
            market_id="cond-trades",
            asset_id="token-trades",
            event_time=now - timedelta(hours=1),
            received_at_local=now - timedelta(hours=1),
            payload={
                "event_type": "last_trade_price",
                "market": "cond-trades",
                "asset_id": "token-trades",
                "price": "0.62",
                "size": "7",
                "side": "sell",
                "transaction_hash": "0xshared-trade",
            },
        )
        await session.commit()

    service = PolymarketRawStorageService(session_factory)
    projected = await service.project_pending_events(reason="manual", after_raw_event_id=0)
    assert projected["rows_inserted_json"]["trade_tape"] == 1

    with respx.mock(assert_all_called=True) as router:
        router.get("https://data-api.polymarket.com/trades").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "market": "cond-trades",
                        "conditionId": "cond-trades",
                        "asset": "token-trades",
                        "timestamp": (now - timedelta(minutes=30)).isoformat(),
                        "price": "0.62",
                        "size": "7",
                        "side": "SELL",
                        "transactionHash": "0xshared-trade",
                        "eventSlug": "trade-event",
                        "outcome": "Yes",
                        "outcomeIndex": 0,
                    },
                    {
                        "market": "cond-trades",
                        "conditionId": "cond-trades",
                        "asset": "token-trades",
                        "timestamp": (now - timedelta(minutes=20)).isoformat(),
                        "price": "0.63",
                        "size": "11",
                        "side": "BUY",
                        "transactionHash": "0xnew-trade",
                        "eventSlug": "trade-event",
                        "outcome": "Yes",
                        "outcomeIndex": 0,
                    },
                ],
            )
        )
        backfill = await service.backfill_trades(
            reason="manual",
            condition_ids=["cond-trades"],
            lookback_hours=24,
        )
    await service.close()

    assert backfill["status"] == "completed"
    assert backfill["rows_inserted_json"]["trade_tape"] == 1

    async with session_factory() as session:
        trades = (
            await session.execute(select(PolymarketTradeTape).order_by(PolymarketTradeTape.id.asc()))
        ).scalars().all()

        assert len(trades) == 2
        assert [row.source_kind for row in trades] == ["ws_last_trade_price", "data_api_trades"]
        assert {row.transaction_hash for row in trades} == {"0xshared-trade", "0xnew-trade"}


@pytest.mark.asyncio
async def test_periodic_books_snapshot_capture_stores_hash_and_preserves_param_dedupe(engine):
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-books", asset_id="token-books")

    service = PolymarketRawStorageService(session_factory)
    with respx.mock(assert_all_called=True) as router:
        router.post("https://clob.polymarket.com/books").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "market": "cond-books",
                        "asset_id": "token-books",
                        "timestamp": "2026-04-13T10:15:00Z",
                        "hash": "0xperiodic-book",
                        "tick_size": "0.001",
                        "min_order_size": "10",
                        "neg_risk": True,
                        "last_trade_price": "0.48",
                        "bids": [{"price": "0.47", "size": "130"}],
                        "asks": [{"price": "0.49", "size": "140"}],
                    }
                ],
            )
        )
        first_run = await service.capture_book_snapshots(
            reason="scheduled",
            asset_ids=["token-books"],
            source_kind=SOURCE_KIND_REST_PERIODIC_SNAPSHOT,
        )

    with respx.mock(assert_all_called=True) as router:
        router.post("https://clob.polymarket.com/books").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "market": "cond-books",
                        "asset_id": "token-books",
                        "timestamp": "2026-04-13T10:15:00Z",
                        "hash": "0xperiodic-book",
                        "tick_size": "0.001",
                        "min_order_size": "10",
                        "neg_risk": True,
                        "last_trade_price": "0.48",
                        "bids": [{"price": "0.47", "size": "130"}],
                        "asks": [{"price": "0.49", "size": "140"}],
                    }
                ],
            )
        )
        second_run = await service.capture_book_snapshots(
            reason="scheduled",
            asset_ids=["token-books"],
            source_kind=SOURCE_KIND_REST_PERIODIC_SNAPSHOT,
        )
    await service.close()

    assert first_run["status"] == "completed"
    assert first_run["rows_inserted_json"]["book_snapshots"] == 1
    assert first_run["rows_inserted_json"]["param_rows_inserted"] == 1
    assert second_run["status"] == "completed"
    assert second_run["rows_inserted_json"]["book_snapshots"] == 0
    assert second_run["rows_inserted_json"]["param_rows_inserted"] == 0

    async with session_factory() as session:
        snapshots = (await session.execute(select(PolymarketBookSnapshot))).scalars().all()
        param_rows = (await session.execute(select(PolymarketMarketParamHistory))).scalars().all()

        assert len(snapshots) == 1
        assert snapshots[0].source_kind == SOURCE_KIND_REST_PERIODIC_SNAPSHOT
        assert snapshots[0].book_hash == "0xperiodic-book"
        assert str(snapshots[0].tick_size) == "0.00100000"
        assert len(param_rows) == 1
        assert str(param_rows[0].tick_size) == "0.00100000"


@pytest.mark.asyncio
async def test_oi_poll_inserts_append_only_observed_samples(engine):
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-oi", asset_id="token-oi")

    service = PolymarketRawStorageService(session_factory)
    with respx.mock(assert_all_called=True) as router:
        router.get("https://data-api.polymarket.com/oi").mock(
            return_value=httpx.Response(200, json=[{"market": "cond-oi", "value": "1200"}])
        )
        first_run = await service.poll_open_interest(reason="manual", condition_ids=["cond-oi"])

    with respx.mock(assert_all_called=True) as router:
        router.get("https://data-api.polymarket.com/oi").mock(
            return_value=httpx.Response(200, json=[{"market": "cond-oi", "value": "1400"}])
        )
        second_run = await service.poll_open_interest(reason="manual", condition_ids=["cond-oi"])
    await service.close()

    assert first_run["status"] == "completed"
    assert first_run["rows_inserted_json"]["open_interest_history"] == 1
    assert second_run["status"] == "completed"
    assert second_run["rows_inserted_json"]["open_interest_history"] == 1

    async with session_factory() as session:
        rows = (
            await session.execute(
                select(PolymarketOpenInterestHistory).order_by(PolymarketOpenInterestHistory.id.asc())
            )
        ).scalars().all()

        assert len(rows) == 2
        assert [str(row.value) for row in rows] == ["1200.00000000", "1400.00000000"]
        assert all(row.source_kind == "data_api_oi_poll" for row in rows)
        assert rows[0].observed_at_local <= rows[1].observed_at_local


@pytest.mark.asyncio
async def test_manual_raw_operator_apis_and_health_surface(client, engine, monkeypatch):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    monkeypatch.setattr(settings, "polymarket_raw_storage_enabled", True)
    monkeypatch.setattr(settings, "polymarket_trade_backfill_enabled", True)
    monkeypatch.setattr(settings, "polymarket_oi_poll_enabled", True)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-api-raw", asset_id="token-api-raw")
        make_polymarket_market_event(
            session,
            message_type="book",
            market_id="cond-api-raw",
            asset_id="token-api-raw",
            event_time=now - timedelta(minutes=4),
            received_at_local=now - timedelta(minutes=4),
            payload={
                "event_type": "book",
                "market": "cond-api-raw",
                "asset_id": "token-api-raw",
                "timestamp": (now - timedelta(minutes=4)).isoformat(),
                "hash": "0xapi-book",
                "bids": [{"price": "0.40", "size": "100"}],
                "asks": [{"price": "0.60", "size": "120"}],
            },
        )
        make_polymarket_market_event(
            session,
            message_type="price_change",
            market_id="cond-api-raw",
            asset_id="token-api-raw",
            event_time=now - timedelta(minutes=3),
            received_at_local=now - timedelta(minutes=3),
            payload={
                "event_type": "price_change",
                "market": "cond-api-raw",
                "asset_id": "token-api-raw",
                "price_changes": [
                    {
                        "market": "cond-api-raw",
                        "asset_id": "token-api-raw",
                        "price": "0.41",
                        "size": "90",
                        "side": "BUY",
                    }
                ],
            },
        )
        make_polymarket_market_event(
            session,
            message_type="best_bid_ask",
            market_id="cond-api-raw",
            asset_id="token-api-raw",
            event_time=now - timedelta(minutes=2),
            received_at_local=now - timedelta(minutes=2),
            payload={
                "event_type": "best_bid_ask",
                "market": "cond-api-raw",
                "asset_id": "token-api-raw",
                "best_bid": "0.42",
                "best_ask": "0.58",
                "spread": "0.16",
            },
        )
        make_polymarket_market_event(
            session,
            message_type="last_trade_price",
            market_id="cond-api-raw",
            asset_id="token-api-raw",
            event_time=now - timedelta(minutes=1),
            received_at_local=now - timedelta(minutes=1),
            payload={
                "event_type": "last_trade_price",
                "market": "cond-api-raw",
                "asset_id": "token-api-raw",
                "price": "0.52",
                "size": "14",
                "side": "buy",
                "transaction_hash": "0xapi-ws-trade",
            },
        )
        await session.commit()

    with respx.mock(assert_all_called=True) as router:
        router.post("https://clob.polymarket.com/books").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "market": "cond-api-raw",
                        "asset_id": "token-api-raw",
                        "timestamp": now.isoformat(),
                        "hash": "0xapi-snapshot",
                        "tick_size": "0.005",
                        "min_order_size": "20",
                        "bids": [{"price": "0.43", "size": "150"}],
                        "asks": [{"price": "0.57", "size": "160"}],
                    }
                ],
            )
        )
        router.get("https://data-api.polymarket.com/trades").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "market": "cond-api-raw",
                        "conditionId": "cond-api-raw",
                        "asset": "token-api-raw",
                        "timestamp": now.isoformat(),
                        "price": "0.52",
                        "size": "14",
                        "side": "BUY",
                        "transactionHash": "0xapi-ws-trade",
                    },
                    {
                        "market": "cond-api-raw",
                        "conditionId": "cond-api-raw",
                        "asset": "token-api-raw",
                        "timestamp": (now + timedelta(minutes=1)).isoformat(),
                        "price": "0.53",
                        "size": "9",
                        "side": "SELL",
                        "transactionHash": "0xapi-rest-trade",
                    },
                ],
            )
        )
        router.get("https://data-api.polymarket.com/oi").mock(
            return_value=httpx.Response(200, json=[{"market": "cond-api-raw", "value": "999"}])
        )

        project_response = await client.post("/api/v1/ingest/polymarket/raw/project", json={"reason": "manual"})
        snapshot_response = await client.post(
            "/api/v1/ingest/polymarket/raw/book-snapshots/trigger",
            json={"reason": "manual"},
        )
        backfill_response = await client.post(
            "/api/v1/ingest/polymarket/raw/trade-backfill/trigger",
            json={"reason": "manual", "condition_ids": ["cond-api-raw"]},
        )
        oi_response = await client.post(
            "/api/v1/ingest/polymarket/raw/oi-poll/trigger",
            json={"reason": "manual", "condition_ids": ["cond-api-raw"]},
        )

    assert project_response.status_code == 200
    assert any(
        (run.get("rows_inserted_json") or {}).get("trade_tape") == 1
        for run in project_response.json()["runs"]
    )
    assert snapshot_response.status_code == 200
    assert snapshot_response.json()["status"] == "completed"
    assert backfill_response.status_code == 200
    assert backfill_response.json()["rows_inserted_json"]["trade_tape"] == 1
    assert oi_response.status_code == 200
    assert oi_response.json()["rows_inserted_json"]["open_interest_history"] == 1

    status_response = await client.get("/api/v1/ingest/polymarket/raw/status")
    assert status_response.status_code == 200
    status_data = status_response.json()
    assert status_data["enabled"] is True
    assert status_data["projector_lag"] == 0
    assert status_data["last_successful_book_snapshot_at"] is not None
    assert status_data["last_successful_trade_backfill_at"] is not None
    assert status_data["last_successful_oi_poll_at"] is not None

    runs_response = await client.get("/api/v1/ingest/polymarket/raw/runs?page=1&page_size=20")
    assert runs_response.status_code == 200
    assert runs_response.json()["total"] >= 4

    snapshots_response = await client.get("/api/v1/ingest/polymarket/raw/book-snapshots?asset_id=token-api-raw")
    deltas_response = await client.get("/api/v1/ingest/polymarket/raw/book-deltas?asset_id=token-api-raw")
    bbo_response = await client.get("/api/v1/ingest/polymarket/raw/bbo-events?asset_id=token-api-raw")
    trades_response = await client.get("/api/v1/ingest/polymarket/raw/trade-tape?condition_id=cond-api-raw")
    oi_history_response = await client.get("/api/v1/ingest/polymarket/raw/oi-history?condition_id=cond-api-raw")

    assert snapshots_response.status_code == 200
    assert snapshots_response.json()["rows"]
    assert deltas_response.status_code == 200
    assert deltas_response.json()["rows"]
    assert bbo_response.status_code == 200
    assert bbo_response.json()["rows"]
    assert trades_response.status_code == 200
    assert len(trades_response.json()["rows"]) == 2
    assert oi_history_response.status_code == 200
    assert oi_history_response.json()["rows"][0]["value"] == 999.0

    ingest_status = await client.get("/api/v1/ingest/polymarket/status")
    assert ingest_status.status_code == 200
    assert ingest_status.json()["raw_storage"]["enabled"] is True

    health = await client.get("/api/v1/health")
    assert health.status_code == 200
    health_data = health.json()
    assert health_data["polymarket_phase3"]["enabled"] is True
    assert health_data["polymarket_phase3"]["projector_lag"] == 0
    assert health_data["polymarket_phase3"]["last_successful_book_snapshot_at"] is not None

    async with session_factory() as session:
        runs = (await session.execute(select(PolymarketRawCaptureRun))).scalars().all()
        assert len(runs) >= 4
