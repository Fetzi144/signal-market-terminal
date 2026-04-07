"""Market discovery: fetch active markets from Polymarket and upsert into DB."""
import logging
import uuid
from datetime import datetime, timezone

from dateutil.parser import isoparse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.base import RawMarket
from app.connectors.polymarket import PolymarketConnector
from app.config import settings
from app.models.market import Market, Outcome
from app.models.ingestion import IngestionRun

logger = logging.getLogger(__name__)


async def discover_markets(session: AsyncSession) -> int:
    """Fetch and upsert active markets. Returns count of markets processed."""
    run = IngestionRun(
        id=uuid.uuid4(),
        run_type="market_discovery",
        platform="polymarket",
        started_at=datetime.now(timezone.utc),
        status="running",
    )
    session.add(run)
    await session.flush()

    connector = PolymarketConnector()
    total = 0

    try:
        offset = 0
        page_size = 100
        while True:
            raw_markets = await connector.fetch_markets(limit=page_size, offset=offset)
            if not raw_markets:
                break

            for rm in raw_markets:
                # Filter out low-volume markets
                if rm.volume_24h is not None and rm.volume_24h < settings.min_volume_24h:
                    continue
                await _upsert_market(session, rm)
                total += 1

            offset += page_size
            # Safety cap: don't paginate forever
            if offset >= 1000:
                break

        run.status = "success"
        run.markets_processed = total
        run.finished_at = datetime.now(timezone.utc)
        logger.info("Market discovery complete: %d markets", total)

    except Exception as e:
        run.status = "failed"
        run.error = str(e)[:2000]
        run.finished_at = datetime.now(timezone.utc)
        logger.error("Market discovery failed", exc_info=True)
        raise
    finally:
        await connector.close()
        await session.commit()

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
