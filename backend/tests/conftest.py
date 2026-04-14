"""Test fixtures: async SQLite engine + session + FastAPI test client."""
import asyncio
import sqlite3
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import JSON, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.db import Base, get_db, get_session_factory
from app.models import *  # noqa: F401,F403

# Register UUID adapter for sqlite3 so it stores as text
sqlite3.register_adapter(uuid.UUID, lambda u: str(u))


_patched = False


def _patch_metadata_for_sqlite():
    """Patch PostgreSQL-specific types to SQLite-compatible equivalents (once)."""
    global _patched
    if _patched:
        return
    _patched = True
    from sqlalchemy import Uuid
    for table in Base.metadata.tables.values():
        for col in table.columns:
            if isinstance(col.type, JSONB):
                col.type = JSON()
            elif isinstance(col.type, PG_UUID):
                col.type = Uuid(native_uuid=False)


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def reset_default_strategy_window(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "default_strategy_start_at", None)
    monkeypatch.setattr(settings, "scheduler_enabled", False)


@pytest_asyncio.fixture
async def engine(tmp_path):
    _patch_metadata_for_sqlite()
    database_path = tmp_path / "test.db"
    eng = create_async_engine(
        f"sqlite+aiosqlite:///{database_path.as_posix()}",
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(eng.sync_engine, "connect")
    def _enable_sqlite_foreign_keys(dbapi_connection, connection_record):  # noqa: ARG001
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine):
    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_sess() as sess:
        yield sess


@pytest_asyncio.fixture
async def client(engine):
    from httpx import ASGITransport, AsyncClient

    from app.main import app

    async_sess = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_db():
        async with async_sess() as sess:
            yield sess

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_session_factory] = lambda: async_sess
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


# -- Helpers for building test data --

def make_market(session, **kwargs):
    from app.models.market import Market
    defaults = dict(
        id=uuid.uuid4(),
        platform="polymarket",
        platform_id=str(uuid.uuid4()),
        question="Will it rain tomorrow?",
        active=True,
    )
    defaults.update(kwargs)
    m = Market(**defaults)
    session.add(m)
    return m


def make_outcome(session, market_id, **kwargs):
    from app.models.market import Outcome
    defaults = dict(
        id=uuid.uuid4(),
        market_id=market_id,
        platform_outcome_id=str(uuid.uuid4()),
        name="Yes",
        token_id=str(uuid.uuid4()),
    )
    defaults.update(kwargs)
    o = Outcome(**defaults)
    session.add(o)
    return o


def make_price_snapshot(session, outcome_id, price, captured_at=None, **kwargs):
    from app.models.snapshot import PriceSnapshot
    from app.models.market import Outcome
    if captured_at is None:
        captured_at = datetime.now(timezone.utc)
    sync_session = getattr(session, "sync_session", session)
    outcome = next(
        (
            obj
            for obj in sync_session.identity_map.values()
            if isinstance(obj, Outcome) and obj.id == outcome_id
        ),
        None,
    )
    s = PriceSnapshot(
        outcome_id=outcome_id,
        price=Decimal(str(price)),
        captured_at=captured_at,
        **kwargs,
    )
    if outcome is not None:
        s.outcome = outcome
    session.add(s)
    return s


def make_orderbook_snapshot(session, outcome_id, spread, depth_bid=None, depth_ask=None, captured_at=None, **kwargs):
    from app.models.snapshot import OrderbookSnapshot
    from app.models.market import Outcome
    if captured_at is None:
        captured_at = datetime.now(timezone.utc)
    sync_session = getattr(session, "sync_session", session)
    outcome = next(
        (
            obj
            for obj in sync_session.identity_map.values()
            if isinstance(obj, Outcome) and obj.id == outcome_id
        ),
        None,
    )
    s = OrderbookSnapshot(
        outcome_id=outcome_id,
        bids=kwargs.pop("bids", []),
        asks=kwargs.pop("asks", []),
        spread=Decimal(str(spread)) if spread is not None else None,
        depth_bid_10pct=Decimal(str(depth_bid)) if depth_bid is not None else None,
        depth_ask_10pct=Decimal(str(depth_ask)) if depth_ask is not None else None,
        captured_at=captured_at,
        **kwargs,
    )
    if outcome is not None:
        s.outcome = outcome
    session.add(s)
    return s


def make_signal(session, market_id, outcome_id, **kwargs):
    from app.models.signal import Signal
    from app.models.market import Outcome
    now = datetime.now(timezone.utc)
    sync_session = getattr(session, "sync_session", session)
    outcome = next(
        (
            obj
            for obj in sync_session.identity_map.values()
            if isinstance(obj, Outcome) and obj.id == outcome_id
        ),
        None,
    )
    defaults = dict(
        id=uuid.uuid4(),
        signal_type="price_move",
        market_id=market_id,
        outcome_id=outcome_id,
        fired_at=now,
        dedupe_bucket=now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0),
        signal_score=Decimal("0.500"),
        confidence=Decimal("0.800"),
        rank_score=Decimal("0.400"),
        details={"direction": "up", "market_question": "Test?", "outcome_name": "Yes"},
        price_at_fire=Decimal("0.500000"),
        resolved=False,
    )
    defaults.update(kwargs)
    s = Signal(**defaults)
    if outcome is not None:
        s.outcome = outcome
    session.add(s)
    return s


def make_polymarket_market_event(session, **kwargs):
    from app.models.polymarket_stream import PolymarketMarketEvent

    defaults = dict(
        venue="polymarket",
        provenance="stream",
        channel="market",
        message_type="book",
        market_id="cond-test",
        asset_id="token-test",
        asset_ids=["token-test"],
        event_time=datetime.now(timezone.utc),
        received_at_local=datetime.now(timezone.utc),
        payload={"event_type": "book", "asset_id": "token-test"},
    )
    defaults.update(kwargs)
    event = PolymarketMarketEvent(**defaults)
    session.add(event)
    return event


def make_polymarket_stream_status(session, **kwargs):
    from app.models.polymarket_stream import PolymarketStreamStatus

    defaults = dict(
        venue="polymarket",
        connected=False,
        active_subscription_count=0,
        reconnect_count=0,
        resync_count=0,
        gap_suspected_count=0,
        malformed_message_count=0,
        updated_at=datetime.now(timezone.utc),
    )
    defaults.update(kwargs)
    status = PolymarketStreamStatus(**defaults)
    session.add(status)
    return status


def make_polymarket_watch_asset(session, outcome_id, asset_id, **kwargs):
    from app.models.polymarket_stream import PolymarketWatchAsset

    defaults = dict(
        outcome_id=outcome_id,
        asset_id=asset_id,
        watch_enabled=True,
    )
    defaults.update(kwargs)
    watch_asset = PolymarketWatchAsset(**defaults)
    session.add(watch_asset)
    return watch_asset
