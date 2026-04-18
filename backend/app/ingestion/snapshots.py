"""Snapshot capture: fetch prices and orderbooks from all platforms, persist as append-only rows."""
import logging
import random
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.connectors import get_connector, get_enabled_platforms
from app.connectors.polymarket import PolymarketTokenNotFoundError
from app.models.ingestion import IngestionRun
from app.models.market import Market, Outcome
from app.models.polymarket_stream import PolymarketWatchAsset
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot

logger = logging.getLogger(__name__)


async def capture_snapshots(session: AsyncSession) -> int:
    """Fetch current prices + orderbooks for all active outcomes across platforms."""
    total = 0
    for platform in get_enabled_platforms():
        try:
            count = await _capture_platform(session, platform)
            total += count
        except Exception:
            logger.error("Snapshot capture failed for %s", platform, exc_info=True)
    return total


async def _capture_platform(session: AsyncSession, platform: str) -> int:
    """Capture snapshots for a single platform. Returns price snapshot count."""
    run = IngestionRun(
        id=uuid.uuid4(),
        run_type="snapshot",
        platform=platform,
        started_at=datetime.now(timezone.utc),
        status="running",
    )
    session.add(run)
    await session.flush()

    connector = get_connector(platform)
    total = 0

    try:
        # Get all active outcomes with token IDs for this platform
        statement = (
            select(Outcome, Market)
            .join(Market, Outcome.market_id == Market.id)
            .where(Market.active.is_(True), Market.platform == platform)
            .where(Outcome.token_id.isnot(None))
        )
        if platform == "polymarket":
            # Full-capture Polymarket hosts maintain a much larger historical market table
            # than the live watch universe. Limit legacy snapshot jobs to the watch set so
            # scheduler-driven scanning tracks the active capture scope instead of millions
            # of stale midpoint requests.
            statement = (
                statement.join(PolymarketWatchAsset, PolymarketWatchAsset.outcome_id == Outcome.id)
                .where(PolymarketWatchAsset.watch_enabled.is_(True))
                .order_by(
                    PolymarketWatchAsset.priority.desc().nullslast(),
                    Market.last_volume_24h.desc().nullslast(),
                    Market.last_liquidity.desc().nullslast(),
                    Outcome.id.asc(),
                )
                .limit(settings.polymarket_snapshot_max_watched_assets)
            )

        result = await session.execute(statement)
        rows = result.all()

        if not rows:
            logger.info("No active %s outcomes to snapshot", platform)
            run.status = "success"
            run.finished_at = datetime.now(timezone.utc)
            await session.commit()
            return 0

        # Build token_id -> (outcome, market) mapping
        token_to_outcome: dict[str, Outcome] = {}
        token_to_market: dict[str, Market] = {}
        for outcome, market in rows:
            if outcome.token_id:
                token_to_outcome[outcome.token_id] = outcome
                token_to_market[outcome.token_id] = market

        token_ids = list(token_to_outcome.keys())
        now = datetime.now(timezone.utc)

        # Batch fetch midpoints
        midpoints = await connector.fetch_midpoints(token_ids)

        # Persist price snapshots with volume/liquidity from parent market
        for tid, outcome in token_to_outcome.items():
            mid = midpoints.get(tid)
            if mid is None:
                continue
            market = token_to_market.get(tid)
            snap = PriceSnapshot(
                outcome_id=outcome.id,
                price=mid,
                volume_24h=market.last_volume_24h if market else None,
                liquidity=market.last_liquidity if market else None,
                captured_at=now,
            )
            session.add(snap)
            total += 1

        # Fetch orderbooks (rate-limited, sample random subset)
        from app.config import settings as _settings
        ob_sample = random.sample(token_ids, min(_settings.orderbook_sample_size, len(token_ids)))
        for tid in ob_sample:
            try:
                ob = await connector.fetch_orderbook(tid)
                outcome = token_to_outcome[tid]

                depth_bid = _depth_within_pct(ob.bids, side="bid", pct=0.10)
                depth_ask = _depth_within_pct(ob.asks, side="ask", pct=0.10)

                session.add(OrderbookSnapshot(
                    outcome_id=outcome.id,
                    bids=ob.bids,
                    asks=ob.asks,
                    spread=ob.spread,
                    depth_bid_10pct=depth_bid,
                    depth_ask_10pct=depth_ask,
                    captured_at=now,
                ))
            except PolymarketTokenNotFoundError:
                logger.info("Skipping stale Polymarket orderbook token %s", tid)
            except Exception:
                logger.warning("Failed to fetch orderbook for %s", tid, exc_info=True)

        run.status = "success"
        run.markets_processed = total
        run.finished_at = datetime.now(timezone.utc)
        logger.info("Snapshot capture complete for %s: %d price snapshots", platform, total)

    except Exception as e:
        run.status = "failed"
        run.error = str(e)[:2000]
        run.finished_at = datetime.now(timezone.utc)
        logger.error("Snapshot capture failed for %s", platform, exc_info=True)
        raise
    finally:
        await connector.close()
        await session.commit()

    return total


def _depth_within_pct(levels: list[list[str]], side: str, pct: float) -> Decimal | None:
    """Sum size of orders within pct of the best level."""
    if not levels:
        return None
    try:
        best = Decimal(levels[0][0])
        if best == 0:
            return None
        total = Decimal("0")
        for price_str, size_str in levels:
            price = Decimal(price_str)
            if side == "bid" and price >= best * (1 - Decimal(str(pct))):
                total += Decimal(size_str)
            elif side == "ask" and price <= best * (1 + Decimal(str(pct))):
                total += Decimal(size_str)
            else:
                break  # levels are sorted, no point continuing
        return total
    except (InvalidOperation, IndexError):
        return None
