from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_book_reconstruction import (
    PolymarketBookReconstructionService,
)
from app.ingestion.polymarket_stream import ensure_watch_registry_bootstrapped
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketDim
from app.models.polymarket_raw import PolymarketBboEvent, PolymarketBookDelta, PolymarketBookSnapshot
from app.models.polymarket_reconstruction import PolymarketBookReconIncident, PolymarketBookReconState
from app.models.polymarket_stream import PolymarketIngestIncident
from tests.conftest import make_market, make_outcome


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class _RecordingResyncStub:
    def __init__(self):
        self.calls = []

    async def resync_assets(self, asset_ids, *, reason, connection_id=None):
        self.calls.append({"asset_ids": list(asset_ids), "reason": reason, "connection_id": connection_id})
        return {
            "run_id": "stub-run",
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


class _NoopRawStorageStub:
    async def project_until_idle(self, *, reason, max_batches=10):
        return {"run_count": 0, "last_run": None, "runs": [], "reason": reason, "max_batches": max_batches}

    async def close(self):
        return None


async def _seed_registry(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str,
    outcome_name: str = "Yes",
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
        outcome_index=0,
        active=True,
        source_payload_json={"asset_id": asset_id},
        last_gamma_sync_at=datetime.now(timezone.utc),
    )
    session.add(asset_dim)
    await session.commit()
    return market_dim, asset_dim


async def _insert_snapshot(
    session: AsyncSession,
    *,
    asset_id: str,
    condition_id: str,
    source_kind: str,
    raw_event_id: int | None,
    observed_at_local: datetime,
    bids: list[dict[str, str]],
    asks: list[dict[str, str]],
    book_hash: str,
):
    row = PolymarketBookSnapshot(
        market_dim_id=None,
        asset_dim_id=None,
        condition_id=condition_id,
        asset_id=asset_id,
        source_kind=source_kind,
        event_ts_exchange=observed_at_local,
        recv_ts_local=observed_at_local,
        ingest_ts_db=observed_at_local,
        observed_at_local=observed_at_local,
        raw_event_id=raw_event_id,
        book_hash=book_hash,
        bids_json=bids,
        asks_json=asks,
        tick_size=Decimal("0.01"),
        best_bid=Decimal(bids[0]["price"]) if bids else None,
        best_ask=Decimal(asks[0]["price"]) if asks else None,
        spread=Decimal(asks[0]["price"]) - Decimal(bids[0]["price"]) if bids and asks else None,
        source_payload_json={"asset_id": asset_id, "market": condition_id, "hash": book_hash},
    )
    session.add(row)
    await session.flush()
    return row


@pytest.mark.asyncio
async def test_reconstruction_replays_snapshot_and_deltas_idempotently(engine):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-recon-1", asset_id="token-recon-1")
        snapshot = await _insert_snapshot(
            session,
            asset_id="token-recon-1",
            condition_id="cond-recon-1",
            source_kind="ws_book",
            raw_event_id=10,
            observed_at_local=now - timedelta(minutes=5),
            bids=[{"price": "0.40", "size": "100"}, {"price": "0.39", "size": "50"}],
            asks=[{"price": "0.60", "size": "90"}],
            book_hash="0xseed",
        )
        session.add_all(
            [
                PolymarketBookDelta(
                    condition_id="cond-recon-1",
                    asset_id="token-recon-1",
                    raw_event_id=11,
                    delta_index=0,
                    price=Decimal("0.41"),
                    size=Decimal("80"),
                    side="BUY",
                    event_ts_exchange=now - timedelta(minutes=4),
                    recv_ts_local=now - timedelta(minutes=4),
                    ingest_ts_db=now - timedelta(minutes=4),
                ),
                PolymarketBookDelta(
                    condition_id="cond-recon-1",
                    asset_id="token-recon-1",
                    raw_event_id=12,
                    delta_index=0,
                    price=Decimal("0.60"),
                    size=Decimal("0"),
                    side="SELL",
                    event_ts_exchange=now - timedelta(minutes=3),
                    recv_ts_local=now - timedelta(minutes=3),
                    ingest_ts_db=now - timedelta(minutes=3),
                ),
                PolymarketBookDelta(
                    condition_id="cond-recon-1",
                    asset_id="token-recon-1",
                    raw_event_id=12,
                    delta_index=1,
                    price=Decimal("0.59"),
                    size=Decimal("70"),
                    side="SELL",
                    event_ts_exchange=now - timedelta(minutes=3),
                    recv_ts_local=now - timedelta(minutes=3),
                    ingest_ts_db=now - timedelta(minutes=3),
                ),
                PolymarketBboEvent(
                    condition_id="cond-recon-1",
                    asset_id="token-recon-1",
                    raw_event_id=13,
                    best_bid=Decimal("0.41"),
                    best_ask=Decimal("0.59"),
                    spread=Decimal("0.18"),
                    event_ts_exchange=now - timedelta(minutes=2),
                    recv_ts_local=now - timedelta(minutes=2),
                    ingest_ts_db=now - timedelta(minutes=2),
                ),
            ]
        )
        await session.commit()
        assert snapshot.id is not None

    service = PolymarketBookReconstructionService(session_factory)
    first = await service.sync_asset("token-recon-1", reason="manual", allow_auto_resync=False)
    second = await service.sync_asset("token-recon-1", reason="manual", allow_auto_resync=False)
    await service.close()

    assert first["status"] == "live"
    assert second["status"] == "live"

    async with session_factory() as session:
        state = (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == "token-recon-1")
            )
        ).scalar_one()
        incidents = (await session.execute(select(PolymarketBookReconIncident))).scalars().all()
        assert state.last_snapshot_source_kind == "ws_book"
        assert str(state.best_bid) == "0.41000000"
        assert str(state.best_ask) == "0.59000000"
        assert str(state.spread) == "0.18000000"
        assert state.depth_levels_bid == 3
        assert state.depth_levels_ask == 1
        assert state.last_applied_delta_raw_event_id == 12
        assert state.last_applied_delta_index == 1
        assert state.last_bbo_raw_event_id == 13
        assert incidents == []


@pytest.mark.asyncio
async def test_bbo_mismatch_marks_drift_and_logs_incident(engine):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-recon-2", asset_id="token-recon-2")
        await _insert_snapshot(
            session,
            asset_id="token-recon-2",
            condition_id="cond-recon-2",
            source_kind="ws_book",
            raw_event_id=20,
            observed_at_local=now - timedelta(minutes=5),
            bids=[{"price": "0.40", "size": "100"}],
            asks=[{"price": "0.60", "size": "100"}],
            book_hash="0xmismatch",
        )
        session.add(
            PolymarketBboEvent(
                condition_id="cond-recon-2",
                asset_id="token-recon-2",
                raw_event_id=21,
                best_bid=Decimal("0.45"),
                best_ask=Decimal("0.55"),
                spread=Decimal("0.10"),
                event_ts_exchange=now - timedelta(minutes=4),
                recv_ts_local=now - timedelta(minutes=4),
                ingest_ts_db=now - timedelta(minutes=4),
            )
        )
        await session.commit()

    service = PolymarketBookReconstructionService(session_factory)
    result = await service.sync_asset("token-recon-2", reason="manual", allow_auto_resync=False)
    await service.close()

    assert result["status"] == "drifted"

    async with session_factory() as session:
        state = (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == "token-recon-2")
            )
        ).scalar_one()
        incident = (
            await session.execute(
                select(PolymarketBookReconIncident)
                .where(PolymarketBookReconIncident.asset_id == "token-recon-2")
                .order_by(PolymarketBookReconIncident.observed_at_local.desc())
                .limit(1)
            )
        ).scalar_one()
        assert state.status == "drifted"
        assert state.drift_count == 1
        assert incident.incident_type == "bbo_mismatch"


@pytest.mark.asyncio
async def test_gap_suspected_signal_triggers_resync(engine):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-recon-3", asset_id="token-recon-3")
        await _insert_snapshot(
            session,
            asset_id="token-recon-3",
            condition_id="cond-recon-3",
            source_kind="ws_book",
            raw_event_id=30,
            observed_at_local=now - timedelta(minutes=10),
            bids=[{"price": "0.42", "size": "60"}],
            asks=[{"price": "0.58", "size": "60"}],
            book_hash="0xgap",
        )
        session.add(
            PolymarketIngestIncident(
                incident_type="gap_suspected",
                severity="warning",
                asset_id="token-recon-3",
                created_at=now - timedelta(minutes=1),
                details_json={"reason": "heartbeat_silence"},
            )
        )
        await session.commit()

    resync_stub = _RecordingResyncStub()
    raw_stub = _NoopRawStorageStub()
    service = PolymarketBookReconstructionService(
        session_factory,
        resync_service=resync_stub,
        raw_storage_service=raw_stub,
    )
    await service.sync_asset("token-recon-3", reason="scheduled", allow_auto_resync=True)
    await service.close()

    assert resync_stub.calls
    assert resync_stub.calls[0]["reason"] == "gap_suspected"


@pytest.mark.asyncio
async def test_startup_bootstrap_prefers_recent_snapshot_and_only_replays_later_deltas(engine):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-recon-4", asset_id="token-recon-4")
        await _insert_snapshot(
            session,
            asset_id="token-recon-4",
            condition_id="cond-recon-4",
            source_kind="ws_book",
            raw_event_id=40,
            observed_at_local=now - timedelta(minutes=10),
            bids=[{"price": "0.40", "size": "100"}],
            asks=[{"price": "0.60", "size": "100"}],
            book_hash="0xold",
        )
        await _insert_snapshot(
            session,
            asset_id="token-recon-4",
            condition_id="cond-recon-4",
            source_kind="rest_resync_snapshot",
            raw_event_id=50,
            observed_at_local=now - timedelta(minutes=5),
            bids=[{"price": "0.45", "size": "100"}],
            asks=[{"price": "0.55", "size": "100"}],
            book_hash="0xnew",
        )
        session.add_all(
            [
                PolymarketBookDelta(
                    condition_id="cond-recon-4",
                    asset_id="token-recon-4",
                    raw_event_id=45,
                    delta_index=0,
                    price=Decimal("0.41"),
                    size=Decimal("80"),
                    side="BUY",
                    event_ts_exchange=now - timedelta(minutes=7),
                    recv_ts_local=now - timedelta(minutes=7),
                    ingest_ts_db=now - timedelta(minutes=7),
                ),
                PolymarketBookDelta(
                    condition_id="cond-recon-4",
                    asset_id="token-recon-4",
                    raw_event_id=55,
                    delta_index=0,
                    price=Decimal("0.46"),
                    size=Decimal("90"),
                    side="BUY",
                    event_ts_exchange=now - timedelta(minutes=4),
                    recv_ts_local=now - timedelta(minutes=4),
                    ingest_ts_db=now - timedelta(minutes=4),
                ),
            ]
        )
        await session.commit()

    service = PolymarketBookReconstructionService(session_factory)
    result = await service.sync_asset("token-recon-4", reason="startup", allow_auto_resync=False)
    await service.close()

    assert result["status"] == "live"
    async with session_factory() as session:
        state = (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == "token-recon-4")
            )
        ).scalar_one()
        assert state.last_snapshot_source_kind == "rest_resync_snapshot"
        assert str(state.best_bid) == "0.46000000"


@pytest.mark.asyncio
async def test_manual_resync_persists_rest_resync_snapshot_and_restores_live_state(engine):
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-recon-5", asset_id="token-recon-5")

    service = PolymarketBookReconstructionService(session_factory)
    with respx.mock(assert_all_called=True) as router:
        router.post("https://clob.polymarket.com/books").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "asset_id": "token-recon-5",
                        "market": "cond-recon-5",
                        "timestamp": "2026-04-13T11:00:00Z",
                        "hash": "0xmanual-resync",
                        "bids": [{"price": "0.48", "size": "75"}],
                        "asks": [{"price": "0.52", "size": "80"}],
                    }
                ],
            )
        )
        result = await service.manual_resync(asset_ids=["token-recon-5"], reason="manual")
    await service.close()

    assert result["status"] == "completed"
    async with session_factory() as session:
        snapshot = (
            await session.execute(
                select(PolymarketBookSnapshot)
                .where(PolymarketBookSnapshot.asset_id == "token-recon-5")
                .order_by(PolymarketBookSnapshot.id.desc())
                .limit(1)
            )
        ).scalar_one()
        state = (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == "token-recon-5")
            )
        ).scalar_one()
        assert snapshot.source_kind == "rest_resync_snapshot"
        assert snapshot.book_hash == "0xmanual-resync"
        assert state.status == "live"
        assert str(state.best_bid) == "0.48000000"
        assert str(state.best_ask) == "0.52000000"


@pytest.mark.asyncio
async def test_reconstruction_api_and_health_surface(client, engine, monkeypatch):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(settings, "polymarket_book_recon_enabled", True)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-recon-6", asset_id="token-recon-6")
        await _insert_snapshot(
            session,
            asset_id="token-recon-6",
            condition_id="cond-recon-6",
            source_kind="ws_book",
            raw_event_id=60,
            observed_at_local=now - timedelta(minutes=5),
            bids=[{"price": "0.44", "size": "50"}],
            asks=[{"price": "0.56", "size": "50"}],
            book_hash="0xapi",
        )
        session.add(
            PolymarketBboEvent(
                condition_id="cond-recon-6",
                asset_id="token-recon-6",
                raw_event_id=61,
                best_bid=Decimal("0.44"),
                best_ask=Decimal("0.56"),
                spread=Decimal("0.12"),
                event_ts_exchange=now - timedelta(minutes=4),
                recv_ts_local=now - timedelta(minutes=4),
                ingest_ts_db=now - timedelta(minutes=4),
            )
        )
        await session.commit()

    service = PolymarketBookReconstructionService(session_factory)
    await service.sync_asset("token-recon-6", reason="manual", allow_auto_resync=False)
    await service.close()

    status_response = await client.get("/api/v1/ingest/polymarket/reconstruction/status")
    assert status_response.status_code == 200
    assert status_response.json()["live_book_count"] >= 1

    state_response = await client.get("/api/v1/ingest/polymarket/reconstruction/state?asset_id=token-recon-6")
    assert state_response.status_code == 200
    assert state_response.json()["rows"][0]["asset_id"] == "token-recon-6"

    top_response = await client.get("/api/v1/ingest/polymarket/reconstruction/top-of-book?asset_id=token-recon-6")
    assert top_response.status_code == 200
    assert top_response.json()["best_bid"] == 0.44

    health_response = await client.get("/api/v1/health")
    assert health_response.status_code == 200
    assert health_response.json()["polymarket_phase4"]["live_book_count"] >= 1


@pytest.mark.asyncio
async def test_reconstruction_survives_restart(engine):
    session_factory = _session_factory(engine)
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        await _seed_registry(session, condition_id="cond-recon-7", asset_id="token-recon-7")
        await _insert_snapshot(
            session,
            asset_id="token-recon-7",
            condition_id="cond-recon-7",
            source_kind="ws_book",
            raw_event_id=70,
            observed_at_local=now - timedelta(minutes=5),
            bids=[{"price": "0.47", "size": "60"}],
            asks=[{"price": "0.53", "size": "60"}],
            book_hash="0xrestart",
        )
        session.add(
            PolymarketBookDelta(
                condition_id="cond-recon-7",
                asset_id="token-recon-7",
                raw_event_id=71,
                delta_index=0,
                price=Decimal("0.48"),
                size=Decimal("40"),
                side="BUY",
                event_ts_exchange=now - timedelta(minutes=4),
                recv_ts_local=now - timedelta(minutes=4),
                ingest_ts_db=now - timedelta(minutes=4),
            )
        )
        await session.commit()

    service_one = PolymarketBookReconstructionService(session_factory)
    first = await service_one.sync_asset("token-recon-7", reason="startup", allow_auto_resync=False)
    await service_one.close()

    service_two = PolymarketBookReconstructionService(session_factory)
    second = await service_two.sync_asset("token-recon-7", reason="startup", allow_auto_resync=False)
    await service_two.close()

    assert first["status"] == "live"
    assert second["status"] == "live"

    async with session_factory() as session:
        state = (
            await session.execute(
                select(PolymarketBookReconState).where(PolymarketBookReconState.asset_id == "token-recon-7")
            )
        ).scalar_one()
        incidents = (
            await session.execute(
                select(PolymarketBookReconIncident).where(PolymarketBookReconIncident.asset_id == "token-recon-7")
            )
        ).scalars().all()
        assert str(state.best_bid) == "0.48000000"
        assert incidents == []
