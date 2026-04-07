"""Market discovery: fetch active markets from all enabled platforms and upsert into DB."""
import logging
import uuid
from datetime import datetime, timezone

from dateutil.parser import isoparse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.connectors import get_connector, get_enabled_platforms
from app.connectors.base import BaseConnector, RawMarket
from app.models.ingestion import IngestionRun
from app.models.market import Market, Outcome

logger = logging.getLogger(__name__)


async def discover_markets(session: AsyncSession) -> int:
    """Fetch and upsert active markets from all enabled platforms."""
    total = 0
    for platform in get_enabled_platforms():
        try:
            count = await _discover_platform(session, platform)
            total += count
        except Exception:
            logger.error("Market discovery failed for %s", platform, exc_info=True)
    return total


async def _discover_platform(session: AsyncSession, platform: str) -> int:
    """Fetch and upsert markets for a single platform. Returns count processed."""
    run = IngestionRun(
        id=uuid.uuid4(),
        run_type="market_discovery",
        platform=platform,
        started_at=datetime.now(timezone.utc),
        status="running",
    )
    session.add(run)
    await session.flush()

    connector = get_connector(platform)
    total = 0

    try:
        if platform == "kalshi":
            total = await _paginate_kalshi(connector, session)
        else:
            total = await _paginate_offset(connector, session)

        run.status = "success"
        run.markets_processed = total
        run.finished_at = datetime.now(timezone.utc)
        logger.info("Market discovery complete for %s: %d markets", platform, total)

    except Exception as e:
        run.status = "failed"
        run.error = str(e)[:2000]
        run.finished_at = datetime.now(timezone.utc)
        logger.error("Market discovery failed for %s", platform, exc_info=True)
        raise
    finally:
        await connector.close()
        await session.commit()

    return total


async def _paginate_offset(connector: BaseConnector, session: AsyncSession) -> int:
    """Offset-based pagination (Polymarket)."""
    total = 0
    offset = 0
    page_size = 100
    while True:
        raw_markets = await connector.fetch_markets(limit=page_size, offset=offset)
        if not raw_markets:
            break
        for rm in raw_markets:
            if rm.volume_24h is not None and rm.volume_24h < settings.min_volume_24h:
                continue
            await _upsert_market(session, rm)
            total += 1
        offset += page_size
        if offset >= settings.market_pagination_cap:
            break
    return total


async def _paginate_kalshi(connector, session: AsyncSession) -> int:
    """Cursor-based pagination for Kalshi."""
    total = 0
    cursor = None
    pages = 0
    while True:
        raw_markets, next_cursor = await connector.fetch_markets_cursor(limit=200, cursor=cursor)
        if not raw_markets:
            break
        for rm in raw_markets:
            if rm.volume_24h is not None and rm.volume_24h < settings.min_volume_24h:
                continue
            await _upsert_market(session, rm)
            total += 1
        pages += 1
        if not next_cursor or pages >= (settings.market_pagination_cap // 200):  # safety cap
            break
        cursor = next_cursor
    return total


async def _upsert_market(session: AsyncSession, rm: RawMarket):
    """Insert or update a market and its outcomes."""
    result = await session.execute(
        select(Market).where(Market.platform == rm.platform, Market.platform_id == rm.platform_id)
    )
    market = result.scalar_one_or_none()

    end_date = None
    if rm.end_date:
        try:
            end_date = isoparse(rm.end_date)
        except (ValueError, TypeError):
            pass

    if market is None:
        market = Market(
            id=uuid.uuid4(),
            platform=rm.platform,
            platform_id=rm.platform_id,
            slug=rm.slug,
            question=rm.question,
            category=rm.category,
            end_date=end_date,
            active=rm.active,
            last_volume_24h=rm.volume_24h,
            last_liquidity=rm.liquidity,
            metadata_=rm.metadata,
        )
        session.add(market)
        await session.flush()
    else:
        market.question = rm.question
        market.active = rm.active
        market.end_date = end_date
        market.last_volume_24h = rm.volume_24h
        market.last_liquidity = rm.liquidity
        market.metadata_ = rm.metadata
        market.updated_at = datetime.now(timezone.utc)

    # Upsert outcomes
    for ro in rm.outcomes:
        result = await session.execute(
            select(Outcome).where(
                Outcome.market_id == market.id,
                Outcome.platform_outcome_id == ro.platform_outcome_id,
            )
        )
        outcome = result.scalar_one_or_none()
        if outcome is None:
            outcome = Outcome(
                id=uuid.uuid4(),
                market_id=market.id,
                platform_outcome_id=ro.platform_outcome_id,
                name=ro.name,
                token_id=ro.token_id,
            )
            session.add(outcome)
        else:
            outcome.name = ro.name
            outcome.token_id = ro.token_id
