from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_microstructure import PolymarketMicrostructureService
from app.ingestion.polymarket_stream import ensure_watch_registry_bootstrapped
from app.models.polymarket_metadata import PolymarketAssetDim, PolymarketMarketDim
from app.models.polymarket_microstructure import (
    PolymarketAlphaLabel,
    PolymarketBookStateTopN,
    PolymarketMicrostructureFeature1s,
    PolymarketPassiveFillLabel,
)
from app.models.polymarket_raw import PolymarketBboEvent, PolymarketBookDelta, PolymarketBookSnapshot, PolymarketTradeTape
from app.models.polymarket_reconstruction import PolymarketBookReconIncident
from tests.conftest import make_market, make_outcome


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def _seed_registry(
    session: AsyncSession,
    *,
    condition_id: str,
    asset_id: str,
):
    market = make_market(
        session,
        platform="polymarket",
        platform_id=f"platform-{condition_id}",
        question=f"Question for {condition_id}",
    )
    await session.flush()
    outcome = make_outcome(session, market.id, name="Yes", token_id=asset_id)
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


async def _seed_phase5_fixture(session: AsyncSession, *, base: datetime, condition_id: str, asset_id: str) -> None:
    await _seed_registry(session, condition_id=condition_id, asset_id=asset_id)

    session.add(
        PolymarketBookSnapshot(
            condition_id=condition_id,
            asset_id=asset_id,
            source_kind="ws_book",
            event_ts_exchange=base,
            recv_ts_local=base,
            ingest_ts_db=base,
            observed_at_local=base,
            raw_event_id=100,
            book_hash="0xseed",
            bids_json=[{"price": "0.40", "size": "100"}, {"price": "0.39", "size": "50"}],
            asks_json=[{"price": "0.60", "size": "80"}, {"price": "0.61", "size": "60"}],
            tick_size=Decimal("0.01"),
            best_bid=Decimal("0.40"),
            best_ask=Decimal("0.60"),
            spread=Decimal("0.20"),
            source_payload_json={"asset_id": asset_id, "market": condition_id},
        )
    )
    session.add_all(
        [
            PolymarketBookDelta(
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=101,
                delta_index=0,
                price=Decimal("0.40"),
                size=Decimal("120"),
                side="BUY",
                event_ts_exchange=base + timedelta(milliseconds=100),
                recv_ts_local=base + timedelta(milliseconds=100),
                ingest_ts_db=base + timedelta(milliseconds=100),
            ),
            PolymarketBookDelta(
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=102,
                delta_index=0,
                price=Decimal("0.60"),
                size=Decimal("70"),
                side="SELL",
                event_ts_exchange=base + timedelta(milliseconds=200),
                recv_ts_local=base + timedelta(milliseconds=200),
                ingest_ts_db=base + timedelta(milliseconds=200),
            ),
            PolymarketBookDelta(
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=103,
                delta_index=0,
                price=Decimal("0.60"),
                size=Decimal("0"),
                side="SELL",
                event_ts_exchange=base + timedelta(milliseconds=350),
                recv_ts_local=base + timedelta(milliseconds=350),
                ingest_ts_db=base + timedelta(milliseconds=350),
            ),
            PolymarketBookDelta(
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=104,
                delta_index=0,
                price=Decimal("0.58"),
                size=Decimal("70"),
                side="SELL",
                event_ts_exchange=base + timedelta(milliseconds=351),
                recv_ts_local=base + timedelta(milliseconds=351),
                ingest_ts_db=base + timedelta(milliseconds=351),
            ),
            PolymarketBookDelta(
                condition_id=condition_id,
                asset_id=asset_id,
                raw_event_id=105,
                delta_index=0,
                price=Decimal("0.41"),
                size=Decimal("60"),
                side="BUY",
                event_ts_exchange=base + timedelta(milliseconds=500),
                recv_ts_local=base + timedelta(milliseconds=500),
                ingest_ts_db=base + timedelta(milliseconds=500),
            ),
        ]
    )
    session.add_all(
        [
            PolymarketTradeTape(
                condition_id=condition_id,
                asset_id=asset_id,
                source_kind="ws_last_trade_price",
                event_ts_exchange=base + timedelta(milliseconds=250),
                recv_ts_local=base + timedelta(milliseconds=250),
                ingest_ts_db=base + timedelta(milliseconds=250),
                observed_at_local=base + timedelta(milliseconds=250),
                raw_event_id=201,
                price=Decimal("0.60"),
                size=Decimal("5"),
                side="BUY",
                fingerprint="trade-1",
            ),
            PolymarketTradeTape(
                condition_id=condition_id,
                asset_id=asset_id,
                source_kind="ws_last_trade_price",
                event_ts_exchange=base + timedelta(milliseconds=600),
                recv_ts_local=base + timedelta(milliseconds=600),
                ingest_ts_db=base + timedelta(milliseconds=600),
                observed_at_local=base + timedelta(milliseconds=600),
                raw_event_id=202,
                price=Decimal("0.61"),
                size=Decimal("2"),
                side="BUY",
                fingerprint="trade-2",
            ),
        ]
    )
    session.add(
        PolymarketBboEvent(
            condition_id=condition_id,
            asset_id=asset_id,
            raw_event_id=301,
            best_bid=Decimal("0.40"),
            best_ask=Decimal("0.60"),
            spread=Decimal("0.20"),
            event_ts_exchange=base + timedelta(milliseconds=260),
            recv_ts_local=base + timedelta(milliseconds=260),
            ingest_ts_db=base + timedelta(milliseconds=260),
        )
    )
    session.add(
        PolymarketBookReconIncident(
            condition_id=condition_id,
            asset_id=asset_id,
            incident_type="bbo_mismatch",
            severity="warning",
            exchange_ts=base + timedelta(milliseconds=700),
            observed_at_local=base + timedelta(milliseconds=700),
            details_json={"reason": "test"},
        )
    )
    session.add(
        PolymarketBboEvent(
            condition_id=condition_id,
            asset_id=asset_id,
            raw_event_id=302,
            best_bid=Decimal("0.41"),
            best_ask=Decimal("0.58"),
            spread=Decimal("0.17"),
            event_ts_exchange=base + timedelta(milliseconds=1100),
            recv_ts_local=base + timedelta(milliseconds=1100),
            ingest_ts_db=base + timedelta(milliseconds=1100),
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_microstructure_materialization_is_deterministic(engine, monkeypatch):
    session_factory = _session_factory(engine)
    base = datetime(2026, 4, 13, 10, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(settings, "polymarket_features_enabled", True)
    monkeypatch.setattr(settings, "polymarket_feature_buckets_ms", "100,1000")
    monkeypatch.setattr(settings, "polymarket_label_horizons_ms", "250,1000")

    async with session_factory() as session:
        await _seed_phase5_fixture(session, base=base, condition_id="cond-phase5-1", asset_id="token-phase5-1")

    service_one = PolymarketMicrostructureService(session_factory)
    first = await service_one.materialize_scope(
        reason="manual",
        asset_ids=["token-phase5-1"],
        start=base,
        end=base + timedelta(seconds=2),
    )
    await service_one.close()

    service_two = PolymarketMicrostructureService(session_factory)
    second = await service_two.materialize_scope(
        reason="manual",
        asset_ids=["token-phase5-1"],
        start=base,
        end=base + timedelta(seconds=2),
    )
    await service_two.close()

    assert first["status"] == "completed"
    assert second["status"] == "completed"
    assert second["feature_run"]["rows_inserted_json"]["polymarket_microstructure_features_100ms"] == 0
    assert second["feature_run"]["rows_inserted_json"]["polymarket_microstructure_features_1s"] == 0

    async with session_factory() as session:
        book_state = (
            await session.execute(
                select(PolymarketBookStateTopN).where(
                    PolymarketBookStateTopN.asset_id == "token-phase5-1",
                    PolymarketBookStateTopN.bucket_start_exchange == base,
                    PolymarketBookStateTopN.bucket_width_ms == 1000,
                )
            )
        ).scalar_one()
        feature = (
            await session.execute(
                select(PolymarketMicrostructureFeature1s).where(
                    PolymarketMicrostructureFeature1s.asset_id == "token-phase5-1",
                    PolymarketMicrostructureFeature1s.bucket_start_exchange == base,
                )
            )
        ).scalar_one()
        alpha_label = (
            await session.execute(
                select(PolymarketAlphaLabel).where(
                    PolymarketAlphaLabel.asset_id == "token-phase5-1",
                    PolymarketAlphaLabel.anchor_bucket_start_exchange == base,
                    PolymarketAlphaLabel.horizon_ms == 1000,
                    PolymarketAlphaLabel.source_feature_table == "polymarket_microstructure_features_1s",
                )
            )
        ).scalar_one()
        passive_labels = (
            await session.execute(
                select(PolymarketPassiveFillLabel).where(
                    PolymarketPassiveFillLabel.asset_id == "token-phase5-1",
                    PolymarketPassiveFillLabel.anchor_bucket_start_exchange == base,
                    PolymarketPassiveFillLabel.horizon_ms == 1000,
                    PolymarketPassiveFillLabel.source_feature_table == "polymarket_microstructure_features_1s",
                )
            )
        ).scalars().all()

    assert book_state.best_bid == Decimal("0.40000000")
    assert book_state.best_ask == Decimal("0.60000000")
    assert round(float(book_state.microprice), 6) == pytest.approx(0.511111, rel=1e-6)
    assert round(float(book_state.imbalance_top1), 6) == pytest.approx(0.111111, rel=1e-6)

    assert feature.bid_add_volume == Decimal("80.00000000")
    assert feature.ask_add_volume == Decimal("70.00000000")
    assert feature.bid_remove_volume == Decimal("0E-8")
    assert feature.ask_remove_volume == Decimal("80.00000000")
    assert feature.buy_trade_volume == Decimal("7.00000000")
    assert feature.buy_trade_count == 2
    assert feature.sell_trade_count == 0
    assert feature.book_update_count == 5
    assert feature.bbo_update_count == 1
    assert feature.last_trade_price == Decimal("0.61000000")
    assert feature.last_trade_side == "BUY"
    assert feature.completeness_flags_json["affected_by_drift"] is True
    assert feature.completeness_flags_json["source_coverage_partial"] is True

    assert alpha_label.start_mid == Decimal("0.50000000")
    assert alpha_label.end_mid == Decimal("0.49500000")
    assert alpha_label.down_move is True
    assert round(float(alpha_label.mid_return_bps), 2) == pytest.approx(-100.0, rel=1e-6)
    assert round(float(alpha_label.mid_move_ticks), 2) == pytest.approx(-0.5, rel=1e-6)

    labels_by_side = {row.side: row for row in passive_labels}
    assert labels_by_side["buy_post_best_bid"].touch_observed is False
    assert labels_by_side["buy_post_best_bid"].best_price_improved_against_order is True
    assert labels_by_side["sell_post_best_ask"].touch_observed is True
    assert labels_by_side["sell_post_best_ask"].trade_through_observed is True


@pytest.mark.asyncio
async def test_microstructure_operator_apis_and_health_surface(client, engine, monkeypatch):
    session_factory = _session_factory(engine)
    base = datetime(2026, 4, 13, 11, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(settings, "polymarket_features_enabled", True)
    monkeypatch.setattr(settings, "polymarket_feature_buckets_ms", "100,1000")
    monkeypatch.setattr(settings, "polymarket_label_horizons_ms", "250,1000")

    async with session_factory() as session:
        await _seed_phase5_fixture(session, base=base, condition_id="cond-phase5-api", asset_id="token-phase5-api")

    response = await client.post(
        "/api/v1/ingest/polymarket/features/materialize",
        json={
            "reason": "manual",
            "asset_ids": ["token-phase5-api"],
            "start": base.isoformat(),
            "end": (base + timedelta(seconds=2)).isoformat(),
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "completed"

    status_response = await client.get("/api/v1/ingest/polymarket/features/status")
    assert status_response.status_code == 200
    assert status_response.json()["enabled"] is True
    assert status_response.json()["recent_feature_rows_24h"] > 0

    runs_response = await client.get("/api/v1/ingest/polymarket/features/runs?page=1&page_size=20")
    assert runs_response.status_code == 200
    assert runs_response.json()["total"] >= 3

    book_state_response = await client.get("/api/v1/ingest/polymarket/features/book-state?asset_id=token-phase5-api&bucket_width_ms=1000")
    feature_rows_response = await client.get("/api/v1/ingest/polymarket/features/rows?asset_id=token-phase5-api&bucket_width_ms=1000")
    alpha_labels_response = await client.get("/api/v1/ingest/polymarket/features/alpha-labels?asset_id=token-phase5-api&horizon_ms=1000")
    passive_labels_response = await client.get("/api/v1/ingest/polymarket/features/passive-fill-labels?asset_id=token-phase5-api&horizon_ms=1000")

    assert book_state_response.status_code == 200
    assert book_state_response.json()["rows"]
    assert feature_rows_response.status_code == 200
    assert feature_rows_response.json()["rows"]
    assert alpha_labels_response.status_code == 200
    assert alpha_labels_response.json()["rows"]
    assert passive_labels_response.status_code == 200
    assert passive_labels_response.json()["rows"]

    ingest_status = await client.get("/api/v1/ingest/polymarket/status")
    assert ingest_status.status_code == 200
    assert ingest_status.json()["features"]["enabled"] is True

    health = await client.get("/api/v1/health")
    assert health.status_code == 200
    assert health.json()["polymarket_phase5"]["enabled"] is True
    assert health.json()["polymarket_phase5"]["recent_feature_rows_24h"] > 0
