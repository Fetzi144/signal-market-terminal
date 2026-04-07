"""Test fixtures: async SQLite engine + session + FastAPI test client."""
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, AsyncSession

from app.db import Base, get_db
from app.models import *  # noqa: F401,F403


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine("sqlite+aiosqlite:///:memory:")
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
    if captured_at is None:
        captured_at = datetime.now(timezone.utc)
    s = PriceSnapshot(
        outcome_id=outcome_id,
        price=Decimal(str(price)),
        captured_at=captured_at,
        **kwargs,
    )
    session.add(s)
    return s


def make_signal(session, market_id, outcome_id, **kwargs):
    from app.models.signal import Signal
    now = datetime.now(timezone.utc)
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
    session.add(s)
    return s
