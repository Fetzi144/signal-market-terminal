import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.ingestion.polymarket_maker_economics import (
    insert_reward_history_if_changed,
    insert_token_fee_history_if_changed,
    lookup_current_reward_state,
    lookup_current_token_fee_state,
    lookup_reward_history,
    lookup_token_fee_history,
)
from app.ingestion.polymarket_metadata import PolymarketMetaSyncService, apply_stream_event_to_registry
from app.ingestion.polymarket_stream import PolymarketStreamService, ensure_watch_registry_bootstrapped
from app.models.polymarket_metadata import (
    PolymarketAssetDim,
    PolymarketEventDim,
    PolymarketMarketDim,
    PolymarketMarketParamHistory,
    PolymarketMetaSyncRun,
)
from tests.conftest import make_market, make_outcome, make_polymarket_market_event


def _session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class _ResyncStub:
    async def resync_assets(self, asset_ids, *, reason, connection_id=None):
        return {
            "run_id": uuid.uuid4(),
            "asset_ids": list(asset_ids),
            "requested_asset_count": len(asset_ids),
            "succeeded_asset_count": len(asset_ids),
            "failed_asset_count": 0,
            "events_persisted": 0,
            "reason": reason,
            "status": "completed",
        }

    async def close(self):
        return None


def _gamma_event_payload():
    return {
        "id": "evt-1",
        "ticker": "candidate-a-2026",
        "slug": "candidate-a-2026",
        "title": "Will Candidate A win in 2026?",
        "subtitle": "National race",
        "category": "Politics",
        "subcategory": "US",
        "active": True,
        "closed": False,
        "archived": False,
        "negRisk": True,
        "negRiskMarketID": "neg-1",
        "negRiskFeeBips": 25,
        "startDate": "2026-04-10T00:00:00Z",
        "endDate": "2026-11-03T00:00:00Z",
        "createdAt": "2026-04-01T00:00:00Z",
        "updatedAt": "2026-04-13T10:00:00Z",
    }


def _gamma_market_payload(*, include_params=True):
    payload = {
        "id": "mkt-1",
        "question": "Will Candidate A win in 2026?",
        "conditionId": "cond-1",
        "slug": "candidate-a-win-2026",
        "description": "Resolves to yes if Candidate A wins.",
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps(["token-yes", "token-no"]),
        "events": [_gamma_event_payload()],
        "tags": [{"id": "7", "label": "Politics", "slug": "politics"}],
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "acceptingOrders": True,
        "makerBaseFee": "0",
        "takerBaseFee": "6",
        "feesEnabled": True,
        "feeSchedule": {"exponent": 2, "rate": "0.02", "takerOnly": True, "rebateRate": "0"},
        "updatedAt": "2026-04-13T10:00:00Z",
    }
    if include_params:
        payload["orderPriceMinTickSize"] = "0.01"
        payload["orderMinSize"] = "5"
        payload["negRisk"] = True
    return payload


def _reward_payload(*, status="active", reward_daily_rate="1.25"):
    return {
        "condition_id": "cond-1",
        "status": status,
        "reward_program_id": "program-1",
        "reward_daily_rate": reward_daily_rate,
        "min_incentive_size": "1",
        "max_incentive_spread": "0.05",
        "start_date": "2026-04-13T09:00:00Z",
        "end_date": "2026-04-14T09:00:00Z",
        "rewards_config": [{"rate": reward_daily_rate, "pool": "base"}],
    }


@pytest.mark.asyncio
async def test_gamma_sync_bootstrap_creates_registry_and_is_idempotent(engine):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="generic-mkt")
        await session.flush()
        make_outcome(session, market.id, name="Yes", token_id="token-yes")
        make_outcome(session, market.id, name="No", token_id="token-no")
        await session.commit()

    service = PolymarketMetaSyncService(session_factory)
    with respx.mock(assert_all_called=True) as router:
        router.get("https://gamma-api.polymarket.com/events/keyset").mock(
            return_value=httpx.Response(200, json={"events": [_gamma_event_payload()]})
        )
        router.get("https://gamma-api.polymarket.com/markets/keyset").mock(
            return_value=httpx.Response(200, json={"markets": [_gamma_market_payload()]})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-yes").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-no").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/rewards/markets/current").mock(
            return_value=httpx.Response(200, json={"markets": [_reward_payload()], "next_cursor": None})
        )
        first_run = await service.sync_metadata(reason="manual")

    with respx.mock(assert_all_called=True) as router:
        router.get("https://gamma-api.polymarket.com/events/keyset").mock(
            return_value=httpx.Response(200, json={"events": [_gamma_event_payload()]})
        )
        router.get("https://gamma-api.polymarket.com/markets/keyset").mock(
            return_value=httpx.Response(200, json={"markets": [_gamma_market_payload()]})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-yes").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-no").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/rewards/markets/current").mock(
            return_value=httpx.Response(200, json={"markets": [_reward_payload()], "next_cursor": None})
        )
        second_run = await service.sync_metadata(reason="manual")
    await service.close()

    assert first_run["status"] == "completed"
    assert first_run["param_rows_inserted"] == 2
    assert second_run["status"] == "completed"
    assert second_run["param_rows_inserted"] == 0

    async with session_factory() as session:
        event_dim = (await session.execute(select(PolymarketEventDim))).scalar_one()
        market_dim = (await session.execute(select(PolymarketMarketDim))).scalar_one()
        asset_dims = (await session.execute(select(PolymarketAssetDim).order_by(PolymarketAssetDim.asset_id.asc()))).scalars().all()
        param_rows = (await session.execute(select(PolymarketMarketParamHistory))).scalars().all()
        sync_runs = (await session.execute(select(PolymarketMetaSyncRun))).scalars().all()

        assert event_dim.event_slug == "candidate-a-2026"
        assert market_dim.condition_id == "cond-1"
        assert [asset.asset_id for asset in asset_dims] == ["token-no", "token-yes"]
        assert all(asset.outcome_id is not None for asset in asset_dims)
        assert len(param_rows) == 2
        assert len(sync_runs) == 2


@pytest.mark.asyncio
async def test_gamma_sync_tolerates_reward_cursor_400_and_keeps_first_page(engine):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="generic-mkt")
        await session.flush()
        make_outcome(session, market.id, name="Yes", token_id="token-yes")
        make_outcome(session, market.id, name="No", token_id="token-no")
        await session.commit()

    service = PolymarketMetaSyncService(session_factory)

    def reward_handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("next_cursor"):
            return httpx.Response(400, json={"error": "bad cursor"})
        return httpx.Response(200, json={"markets": [_reward_payload()], "next_cursor": "LTE="})

    with respx.mock(assert_all_called=True) as router:
        router.get("https://gamma-api.polymarket.com/events/keyset").mock(
            return_value=httpx.Response(200, json={"events": [_gamma_event_payload()]})
        )
        router.get("https://gamma-api.polymarket.com/markets/keyset").mock(
            return_value=httpx.Response(200, json={"markets": [_gamma_market_payload()]})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-yes").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-no").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/rewards/markets/current").mock(side_effect=reward_handler)
        result = await service.sync_metadata(reason="manual")
    await service.close()

    assert result["status"] == "completed"

    async with session_factory() as session:
        current_reward = await lookup_current_reward_state(
            session,
            condition_id="cond-1",
            as_of=None,
            limit=10,
        )

    assert current_reward
    assert current_reward[0]["reward_status"] == "active"


@pytest.mark.asyncio
async def test_gamma_sync_tolerates_reward_cursor_500_and_keeps_first_page(engine):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="generic-mkt")
        await session.flush()
        make_outcome(session, market.id, name="Yes", token_id="token-yes")
        make_outcome(session, market.id, name="No", token_id="token-no")
        await session.commit()

    service = PolymarketMetaSyncService(session_factory)

    def reward_handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("next_cursor"):
            return httpx.Response(500, json={"error": "server error"})
        return httpx.Response(200, json={"markets": [_reward_payload()], "next_cursor": "MjUwMA=="})

    with respx.mock(assert_all_called=True) as router:
        router.get("https://gamma-api.polymarket.com/events/keyset").mock(
            return_value=httpx.Response(200, json={"events": [_gamma_event_payload()]})
        )
        router.get("https://gamma-api.polymarket.com/markets/keyset").mock(
            return_value=httpx.Response(200, json={"markets": [_gamma_market_payload()]})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-yes").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-no").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/rewards/markets/current").mock(side_effect=reward_handler)
        result = await service.sync_metadata(reason="manual")
    await service.close()

    assert result["status"] == "completed"

    async with session_factory() as session:
        current_reward = await lookup_current_reward_state(
            session,
            condition_id="cond-1",
            as_of=None,
            limit=10,
        )

    assert current_reward
    assert current_reward[0]["reward_status"] == "active"


@pytest.mark.asyncio
async def test_stream_lifecycle_events_update_registry_and_append_only_history(engine):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="generic-stream")
        await session.flush()
        make_outcome(session, market.id, name="Yes", token_id="token-stream-yes")
        make_outcome(session, market.id, name="No", token_id="token-stream-no")
        await session.commit()

    service = PolymarketStreamService(session_factory, resync_service=_ResyncStub())
    connection_id = uuid.uuid4()

    new_market_payload = {
        "id": "mkt-stream-1",
        "question": "Will Stream Market resolve yes?",
        "market": "cond-stream-1",
        "slug": "stream-market-1",
        "description": "Lifecycle test market",
        "assets_ids": ["token-stream-yes", "token-stream-no"],
        "outcomes": ["Yes", "No"],
        "event_message": {
            "id": "evt-stream-1",
            "ticker": "stream-evt-1",
            "slug": "stream-evt-1",
            "title": "Stream Event 1",
        },
        "timestamp": "1766790415550",
        "event_type": "new_market",
        "tags": ["test"],
        "condition_id": "cond-stream-1",
        "active": True,
        "clob_token_ids": ["token-stream-yes", "token-stream-no"],
        "order_price_min_tick_size": "0.01",
        "fees_enabled": True,
        "fee_schedule": {"exponent": "2", "rate": "0.02", "taker_only": True, "rebate_rate": "0"},
    }
    tick_size_change_payload = {
        "event_type": "tick_size_change",
        "asset_id": "token-stream-yes",
        "market": "cond-stream-1",
        "old_tick_size": "0.01",
        "new_tick_size": "0.001",
        "timestamp": "1766790515550",
    }
    market_resolved_payload = {
        "id": "mkt-stream-1",
        "question": "Will Stream Market resolve yes?",
        "market": "cond-stream-1",
        "slug": "stream-market-1",
        "description": "Lifecycle test market",
        "assets_ids": ["token-stream-yes", "token-stream-no"],
        "outcomes": ["Yes", "No"],
        "winning_asset_id": "token-stream-yes",
        "winning_outcome": "Yes",
        "event_message": {
            "id": "evt-stream-1",
            "ticker": "stream-evt-1",
            "slug": "stream-evt-1",
            "title": "Stream Event 1",
        },
        "timestamp": "1766790615550",
        "event_type": "market_resolved",
    }

    await service.persist_stream_message(json.dumps(new_market_payload), connection_id)
    await service.persist_stream_message(json.dumps(tick_size_change_payload), connection_id)
    await service.persist_stream_message(json.dumps(tick_size_change_payload), connection_id)
    await service.persist_stream_message(json.dumps(market_resolved_payload), connection_id)
    await service.close()

    async with session_factory() as session:
        market_dim = (await session.execute(select(PolymarketMarketDim))).scalar_one()
        asset_yes = (await session.execute(select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == "token-stream-yes"))).scalar_one()
        asset_no = (await session.execute(select(PolymarketAssetDim).where(PolymarketAssetDim.asset_id == "token-stream-no"))).scalar_one()
        param_rows = (await session.execute(select(PolymarketMarketParamHistory).order_by(PolymarketMarketParamHistory.id.asc()))).scalars().all()

        assert market_dim.resolved is True
        assert market_dim.winning_asset_id == "token-stream-yes"
        assert asset_yes.winner is True
        assert asset_no.winner is False
        assert len(param_rows) == 5
        yes_rows = [row for row in param_rows if row.asset_id == "token-stream-yes"]
        assert len(yes_rows) == 3
        assert str(yes_rows[1].tick_size) == "0.00100000"
        assert yes_rows[-1].resolution_state == "resolved"
        assert yes_rows[-1].winning_asset_id == "token-stream-yes"


@pytest.mark.asyncio
async def test_missing_watched_asset_metadata_is_seeded_from_books(engine):
    session_factory = _session_factory(engine)
    async with session_factory() as session:
        market = make_market(session, platform="polymarket", platform_id="watch-mkt")
        await session.flush()
        make_outcome(session, market.id, name="Yes", token_id="token-book")
        await session.flush()
        await ensure_watch_registry_bootstrapped(session)
        await session.commit()

    service = PolymarketMetaSyncService(session_factory)
    with respx.mock(assert_all_called=True) as router:
        router.get("https://gamma-api.polymarket.com/events/keyset").mock(
            return_value=httpx.Response(200, json={"events": [_gamma_event_payload()]})
        )
        router.get("https://gamma-api.polymarket.com/markets/keyset").mock(
            return_value=httpx.Response(200, json={"markets": [_gamma_market_payload(include_params=False)]})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-yes").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-no").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/rewards/markets/current").mock(
            return_value=httpx.Response(200, json={"markets": [_reward_payload()], "next_cursor": None})
        )
        router.post("https://clob.polymarket.com/books").mock(
            return_value=httpx.Response(
                200,
                json=[{
                    "asset_id": "token-book",
                    "market": "cond-1",
                    "timestamp": "2026-04-13T10:15:00Z",
                    "tick_size": "0.001",
                    "min_order_size": "10",
                    "neg_risk": True,
                    "bids": [],
                    "asks": [],
                }],
            )
        )
        result = await service.sync_metadata(reason="repair")
    await service.close()

    assert result["status"] in {"completed", "partial"}
    assert result["param_rows_inserted"] >= 1

    async with session_factory() as session:
        latest = (
            await session.execute(
                select(PolymarketMarketParamHistory)
                .where(PolymarketMarketParamHistory.asset_id == "token-book")
                .order_by(PolymarketMarketParamHistory.id.desc())
                .limit(1)
            )
        ).scalar_one()
        assert latest.source_kind == "rest_book_seed"
        assert str(latest.tick_size) == "0.00100000"
        assert str(latest.min_order_size) == "10.00000000"
        assert latest.neg_risk is True


@pytest.mark.asyncio
async def test_metadata_sync_marks_run_cancelled_when_task_is_cancelled(engine, monkeypatch):
    session_factory = _session_factory(engine)
    service = PolymarketMetaSyncService(session_factory)

    async def raise_cancelled(*_args, **_kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(settings, "polymarket_reward_history_enabled", False)
    monkeypatch.setattr(service, "_sync_gamma_events", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "_sync_gamma_markets", raise_cancelled)

    with pytest.raises(asyncio.CancelledError):
        await service.sync_metadata(reason="manual")
    await service.close()

    async with session_factory() as session:
        run = (await session.execute(select(PolymarketMetaSyncRun))).scalar_one()
        assert run.status == "cancelled"
        assert run.completed_at is not None
        assert run.error_count == 1


@pytest.mark.asyncio
async def test_apply_stream_event_to_registry_yields_while_meta_sync_is_running(engine):
    session_factory = _session_factory(engine)

    async with session_factory() as session:
        event = make_polymarket_market_event(
            session,
            message_type="new_market",
            market_id="cond-stream-skip",
            asset_id="token-stream-skip",
            payload={
                "id": "mkt-stream-skip",
                "market": "cond-stream-skip",
                "question": "Skip registry update while sync runs?",
                "slug": "skip-registry-update",
                "description": "skip while metadata sync is running",
                "assets_ids": ["token-stream-skip"],
                "clob_token_ids": ["token-stream-skip"],
                "outcomes": ["Yes"],
                "event_message": {
                    "id": "evt-stream-skip",
                    "slug": "evt-stream-skip",
                    "title": "Skip event",
                },
            },
        )
        session.add(
            PolymarketMetaSyncRun(
                reason="startup",
                status="running",
                include_closed=False,
            )
        )
        await session.commit()
        raw_event_id = event.id

    await apply_stream_event_to_registry(session_factory, raw_event_id=raw_event_id)

    async with session_factory() as session:
        market_dims = (await session.execute(select(PolymarketMarketDim))).scalars().all()
        assert market_dims == []


@pytest.mark.asyncio
async def test_maker_history_lookup_supports_effective_time_queries(engine):
    session_factory = _session_factory(engine)
    observed_at = datetime(2026, 4, 13, 10, 0, tzinfo=timezone.utc)
    effective_old = observed_at - timedelta(minutes=30)
    effective_new = observed_at + timedelta(minutes=30)

    async with session_factory() as session:
        event_dim = PolymarketEventDim(
            gamma_event_id="evt-history",
            event_slug="maker-history",
            title="Maker History",
            active=True,
            closed=False,
            archived=False,
            neg_risk=False,
            last_gamma_sync_at=observed_at,
            source_payload_json={},
        )
        session.add(event_dim)
        await session.flush()

        market_dim = PolymarketMarketDim(
            gamma_market_id="mkt-history",
            condition_id="cond-history",
            market_slug="maker-history",
            question="History market?",
            description="History market",
            event_dim_id=event_dim.id,
            enable_order_book=True,
            active=True,
            closed=False,
            archived=False,
            accepting_orders=True,
            resolved=False,
            resolution_state="open",
            fees_enabled=True,
            fee_schedule_json={"rate": "0.02"},
            maker_base_fee=Decimal("0.0000"),
            taker_base_fee=Decimal("0.0200"),
            last_gamma_sync_at=observed_at,
            source_payload_json={},
        )
        session.add(market_dim)
        await session.flush()

        asset_dim = PolymarketAssetDim(
            asset_id="token-history",
            condition_id=market_dim.condition_id,
            market_dim_id=market_dim.id,
            outcome_name="Yes",
            outcome_index=0,
            active=True,
            last_gamma_sync_at=observed_at,
            source_payload_json={},
        )
        session.add(asset_dim)
        await session.flush()

        assert await insert_token_fee_history_if_changed(
            session,
            market_dim=market_dim,
            asset_dim=asset_dim,
            condition_id=market_dim.condition_id,
            asset_id=asset_dim.asset_id,
            source_kind="test_fee",
            effective_at_exchange=effective_old,
            observed_at_local=observed_at,
            sync_run_id=None,
            fees_enabled=True,
            maker_fee_rate=Decimal("0.0000"),
            taker_fee_rate=Decimal("0.0200"),
            token_base_fee_rate=Decimal("6.0000"),
            fee_schedule_json={"rate": "0.02"},
        ) is True
        assert await insert_token_fee_history_if_changed(
            session,
            market_dim=market_dim,
            asset_dim=asset_dim,
            condition_id=market_dim.condition_id,
            asset_id=asset_dim.asset_id,
            source_kind="test_fee",
            effective_at_exchange=effective_new,
            observed_at_local=observed_at + timedelta(minutes=1),
            sync_run_id=None,
            fees_enabled=True,
            maker_fee_rate=Decimal("0.0005"),
            taker_fee_rate=Decimal("0.0180"),
            token_base_fee_rate=Decimal("5.5000"),
            fee_schedule_json={"rate": "0.018"},
        ) is True
        assert await insert_reward_history_if_changed(
            session,
            market_dim=market_dim,
            condition_id=market_dim.condition_id,
            source_kind="test_reward",
            effective_at_exchange=effective_old,
            observed_at_local=observed_at,
            sync_run_id=None,
            reward_status="active",
            reward_program_id="program-old",
            reward_daily_rate=Decimal("1.2500"),
            min_incentive_size=Decimal("1.0000"),
            max_incentive_spread=Decimal("0.0500"),
            start_at_exchange=effective_old,
            end_at_exchange=effective_new,
            rewards_config_json=[{"rate": "1.25"}],
        ) is True
        assert await insert_reward_history_if_changed(
            session,
            market_dim=market_dim,
            condition_id=market_dim.condition_id,
            source_kind="test_reward",
            effective_at_exchange=effective_new,
            observed_at_local=observed_at + timedelta(minutes=1),
            sync_run_id=None,
            reward_status="expired",
            reward_program_id="program-new",
            reward_daily_rate=Decimal("0.5000"),
            min_incentive_size=Decimal("2.0000"),
            max_incentive_spread=Decimal("0.0200"),
            start_at_exchange=effective_new,
            end_at_exchange=effective_new + timedelta(hours=1),
            rewards_config_json=[{"rate": "0.50"}],
        ) is True
        await session.commit()

        fee_history = await lookup_token_fee_history(
            session,
            asset_id="token-history",
            condition_id="cond-history",
            start=None,
            end=None,
            limit=10,
        )
        current_fee_old = await lookup_current_token_fee_state(
            session,
            asset_id="token-history",
            condition_id="cond-history",
            as_of=effective_old + timedelta(minutes=1),
            limit=10,
        )
        current_fee_new = await lookup_current_token_fee_state(
            session,
            asset_id="token-history",
            condition_id="cond-history",
            as_of=effective_new + timedelta(minutes=1),
            limit=10,
        )
        reward_history = await lookup_reward_history(
            session,
            condition_id="cond-history",
            start=None,
            end=None,
            limit=10,
        )
        current_reward_old = await lookup_current_reward_state(
            session,
            condition_id="cond-history",
            as_of=effective_old + timedelta(minutes=1),
            limit=10,
        )
        current_reward_new = await lookup_current_reward_state(
            session,
            condition_id="cond-history",
            as_of=effective_new + timedelta(minutes=1),
            limit=10,
        )

    assert len(fee_history) == 2
    assert current_fee_old[0]["taker_fee_rate"] == "0.02000000"
    assert current_fee_new[0]["taker_fee_rate"] == "0.01800000"
    assert current_fee_new[0]["token_base_fee_rate"] == "5.50000000"
    assert len(reward_history) == 2
    assert current_reward_old[0]["reward_status"] == "active"
    assert current_reward_new[0]["reward_status"] == "expired"


@pytest.mark.asyncio
async def test_metadata_sync_api_lookup_and_health_serialization(client):
    with respx.mock(assert_all_called=True) as router:
        router.get("https://gamma-api.polymarket.com/events/keyset").mock(
            return_value=httpx.Response(200, json={"events": [_gamma_event_payload()]})
        )
        router.get("https://gamma-api.polymarket.com/markets/keyset").mock(
            return_value=httpx.Response(200, json={"markets": [_gamma_market_payload()]})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-yes").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/fee-rate/token-no").mock(
            return_value=httpx.Response(200, json={"base_fee": "6"})
        )
        router.get("https://clob.polymarket.com/rewards/markets/current").mock(
            return_value=httpx.Response(200, json={"markets": [_reward_payload()], "next_cursor": None})
        )
        response = await client.post("/api/v1/ingest/polymarket/meta-sync", json={"reason": "manual"})

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["details_json"]["fee_rows_inserted"] == 2
    assert response.json()["details_json"]["reward_rows_inserted"] == 1

    status_response = await client.get("/api/v1/ingest/polymarket/meta-sync/status")
    assert status_response.status_code == 200
    assert status_response.json()["last_run_status"] == "completed"

    runs_response = await client.get("/api/v1/ingest/polymarket/meta-sync/runs?page=1&page_size=20")
    assert runs_response.status_code == 200
    assert runs_response.json()["total"] >= 1

    market_lookup = await client.get("/api/v1/ingest/polymarket/registry/markets?condition_id=cond-1")
    assert market_lookup.status_code == 200
    assert market_lookup.json()["rows"][0]["condition_id"] == "cond-1"

    asset_lookup = await client.get("/api/v1/ingest/polymarket/registry/assets?asset_id=token-yes")
    assert asset_lookup.status_code == 200
    assert asset_lookup.json()["rows"][0]["asset_id"] == "token-yes"

    history_lookup = await client.get("/api/v1/ingest/polymarket/registry/param-history?asset_id=token-yes&limit=20")
    assert history_lookup.status_code == 200
    assert history_lookup.json()["rows"]

    fee_status = await client.get("/api/v1/ingest/polymarket/maker-economics/status")
    assert fee_status.status_code == 200
    assert fee_status.json()["fee_history_rows"] == 2
    assert fee_status.json()["reward_history_rows"] == 1

    current_fee = await client.get("/api/v1/ingest/polymarket/maker-economics/fees/current?asset_id=token-yes")
    assert current_fee.status_code == 200
    assert current_fee.json()["rows"][0]["asset_id"] == "token-yes"
    assert current_fee.json()["rows"][0]["token_base_fee_rate"] == "6.00000000"

    fee_history = await client.get("/api/v1/ingest/polymarket/maker-economics/fees/history?asset_id=token-yes")
    assert fee_history.status_code == 200
    assert fee_history.json()["rows"]

    current_reward = await client.get("/api/v1/ingest/polymarket/maker-economics/rewards/current?condition_id=cond-1")
    assert current_reward.status_code == 200
    assert current_reward.json()["rows"][0]["reward_status"] == "active"

    reward_history = await client.get("/api/v1/ingest/polymarket/maker-economics/rewards/history?condition_id=cond-1")
    assert reward_history.status_code == 200
    assert reward_history.json()["rows"]

    ingest_status = await client.get("/api/v1/ingest/polymarket/status")
    assert ingest_status.status_code == 200
    assert ingest_status.json()["metadata_sync"]["last_run_status"] == "completed"
    assert ingest_status.json()["maker_economics"]["fee_history_rows"] == 2
    assert ingest_status.json()["maker_economics"]["reward_history_rows"] == 1

    health = await client.get("/api/v1/health")
    assert health.status_code == 200
    assert health.json()["polymarket_phase2"]["last_run_status"] == "completed"
    assert health.json()["polymarket_phase9"]["fee_history_rows"] == 2
    assert health.json()["polymarket_phase9"]["reward_state_counts"]["active"] == 1
