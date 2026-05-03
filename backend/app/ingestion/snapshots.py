"""Snapshot capture: fetch prices and orderbooks from all platforms, persist as append-only rows."""
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.connectors import get_connector, get_enabled_platforms
from app.connectors.polymarket import PolymarketTokenNotFoundError
from app.models.execution_decision import ExecutionDecision
from app.models.ingestion import IngestionRun
from app.models.market import Market, Outcome
from app.models.paper_trade import PaperTrade
from app.models.polymarket_stream import PolymarketWatchAsset
from app.models.signal import Signal
from app.models.snapshot import OrderbookSnapshot, PriceSnapshot

logger = logging.getLogger(__name__)


async def capture_snapshots(session: AsyncSession) -> int:
    """Fetch current prices + orderbooks for all active outcomes across platforms."""
    total = 0
    enabled_platforms = get_enabled_platforms()
    platforms = list(enabled_platforms)
    for platform in await _open_paper_trade_platforms(session):
        if platform not in platforms:
            platforms.append(platform)

    for platform in platforms:
        try:
            count = await _capture_platform(
                session,
                platform,
                held_positions_only=platform not in enabled_platforms,
            )
            total += count
        except Exception:
            logger.error("Snapshot capture failed for %s", platform, exc_info=True)
    return total


async def _capture_platform(
    session: AsyncSession,
    platform: str,
    *,
    held_positions_only: bool = False,
) -> int:
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
        if held_positions_only:
            rows = await _load_open_paper_trade_snapshot_rows(session, platform=platform)
        elif platform == "polymarket":
            rows = await _load_polymarket_snapshot_rows(session)
        elif platform == "kalshi":
            rows = await _load_kalshi_snapshot_rows(session)
        else:
            statement = (
                select(Outcome, Market)
                .join(Market, Outcome.market_id == Market.id)
                .where(Market.active.is_(True), Market.platform == platform)
                .where(Outcome.token_id.isnot(None))
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
        latest_by_outcome_id = await _load_latest_price_snapshots(
            session,
            outcome_ids=[outcome.id for outcome in token_to_outcome.values()],
        )

        # Persist price snapshots with volume/liquidity from parent market
        for tid, outcome in token_to_outcome.items():
            mid = midpoints.get(tid)
            if mid is None:
                continue
            market = token_to_market.get(tid)
            latest_snapshot = latest_by_outcome_id.get(outcome.id)
            if not _should_persist_price_snapshot(
                latest_snapshot,
                price=mid,
                volume_24h=market.last_volume_24h if market else None,
                liquidity=market.last_liquidity if market else None,
                now=now,
            ):
                continue
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


async def _open_paper_trade_platforms(session: AsyncSession) -> list[str]:
    result = await session.execute(
        select(Market.platform)
        .join(PaperTrade, PaperTrade.market_id == Market.id)
        .where(PaperTrade.status == "open")
        .where(Market.platform.isnot(None))
        .distinct()
    )
    return [str(platform) for platform in result.scalars().all() if platform]


async def _load_open_paper_trade_snapshot_rows(
    session: AsyncSession,
    *,
    platform: str,
) -> list[tuple[Outcome, Market]]:
    statement = (
        select(Outcome, Market)
        .join(Market, Outcome.market_id == Market.id)
        .join(PaperTrade, PaperTrade.outcome_id == Outcome.id)
        .where(Market.active.is_(True), Market.platform == platform)
        .where(Outcome.token_id.isnot(None))
        .where(PaperTrade.status == "open")
        .order_by(PaperTrade.opened_at.desc(), Outcome.id.asc())
    )
    rows: list[tuple[Outcome, Market]] = []
    seen_outcome_ids = set()
    for outcome, market in (await session.execute(statement)).all():
        if outcome.id in seen_outcome_ids:
            continue
        seen_outcome_ids.add(outcome.id)
        rows.append((outcome, market))
    return rows


async def _load_polymarket_snapshot_rows(session: AsyncSession) -> list[tuple[Outcome, Market]]:
    # Full-capture Polymarket hosts maintain a much larger historical market table
    # than the live watch universe. Limit legacy snapshot jobs to the watch set, then
    # append open paper positions so mark-to-market evidence never goes stale because
    # a held outcome fell out of the current discovery/watch scope.
    watched_statement = (
        select(Outcome, Market)
        .join(Market, Outcome.market_id == Market.id)
        .join(PolymarketWatchAsset, PolymarketWatchAsset.outcome_id == Outcome.id)
        .where(Market.active.is_(True), Market.platform == "polymarket")
        .where(Outcome.token_id.isnot(None))
        .where(PolymarketWatchAsset.watch_enabled.is_(True))
        .order_by(
            PolymarketWatchAsset.priority.desc().nullslast(),
            Market.last_volume_24h.desc().nullslast(),
            Market.last_liquidity.desc().nullslast(),
            Outcome.id.asc(),
        )
        .limit(settings.polymarket_snapshot_max_watched_assets)
    )
    watched_rows = (await session.execute(watched_statement)).all()

    open_trade_rows = await _load_open_paper_trade_snapshot_rows(session, platform="polymarket")

    rows: list[tuple[Outcome, Market]] = []
    seen_outcome_ids = set()
    for outcome, market in [*watched_rows, *open_trade_rows]:
        if outcome.id in seen_outcome_ids:
            continue
        seen_outcome_ids.add(outcome.id)
        rows.append((outcome, market))
    return rows


async def _load_kalshi_snapshot_rows(session: AsyncSession) -> list[tuple[Outcome, Market]]:
    open_trade_rows = await _load_open_paper_trade_snapshot_rows(session, platform="kalshi")
    pending_decision_rows = await _load_pending_execution_decision_snapshot_rows(session, platform="kalshi")

    if settings.kalshi_snapshot_full_universe_enabled:
        active_statement = (
            select(Outcome, Market)
            .join(Market, Outcome.market_id == Market.id)
            .where(Market.active.is_(True), Market.platform == "kalshi")
            .where(Outcome.token_id.isnot(None))
            .order_by(
                Market.last_liquidity.desc().nullslast(),
                Market.last_volume_24h.desc().nullslast(),
                Market.end_date.asc().nullslast(),
                Outcome.id.asc(),
            )
        )
    else:
        horizon_cutoff = datetime.now(timezone.utc) + timedelta(days=settings.kalshi_snapshot_max_market_horizon_days)
        active_statement = (
            select(Outcome, Market)
            .join(Market, Outcome.market_id == Market.id)
            .where(Market.active.is_(True), Market.platform == "kalshi")
            .where(Outcome.token_id.isnot(None))
            .where(Market.end_date.isnot(None), Market.end_date <= horizon_cutoff)
            .order_by(
                Market.last_liquidity.desc().nullslast(),
                Market.last_volume_24h.desc().nullslast(),
                Market.end_date.asc(),
                Outcome.id.asc(),
            )
            .limit(settings.kalshi_snapshot_max_active_outcomes)
        )
    active_rows = (await session.execute(active_statement)).all()
    return _dedupe_snapshot_rows([*open_trade_rows, *pending_decision_rows, *active_rows])


async def _load_pending_execution_decision_snapshot_rows(
    session: AsyncSession,
    *,
    platform: str,
) -> list[tuple[Outcome, Market]]:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.paper_trading_pending_decision_max_age_seconds)
    statement = (
        select(Outcome, Market)
        .join(Signal, Signal.outcome_id == Outcome.id)
        .join(ExecutionDecision, ExecutionDecision.signal_id == Signal.id)
        .join(Market, Outcome.market_id == Market.id)
        .where(Market.active.is_(True), Market.platform == platform)
        .where(Outcome.token_id.isnot(None))
        .where(ExecutionDecision.decision_status == "pending_decision")
        .where(ExecutionDecision.decision_at >= cutoff)
        .order_by(ExecutionDecision.decision_at.desc(), Outcome.id.asc())
    )
    return (await session.execute(statement)).all()


def _dedupe_snapshot_rows(rows: list[tuple[Outcome, Market]]) -> list[tuple[Outcome, Market]]:
    deduped: list[tuple[Outcome, Market]] = []
    seen_outcome_ids = set()
    for outcome, market in rows:
        if outcome.id in seen_outcome_ids:
            continue
        seen_outcome_ids.add(outcome.id)
        deduped.append((outcome, market))
    return deduped


async def _load_latest_price_snapshots(
    session: AsyncSession,
    *,
    outcome_ids: list[uuid.UUID],
) -> dict[uuid.UUID, PriceSnapshot]:
    if not outcome_ids:
        return {}
    latest_subquery = (
        select(
            PriceSnapshot.outcome_id,
            func.max(PriceSnapshot.captured_at).label("latest_captured_at"),
        )
        .where(PriceSnapshot.outcome_id.in_(outcome_ids))
        .group_by(PriceSnapshot.outcome_id)
        .subquery()
    )
    result = await session.execute(
        select(PriceSnapshot).join(
            latest_subquery,
            (PriceSnapshot.outcome_id == latest_subquery.c.outcome_id)
            & (PriceSnapshot.captured_at == latest_subquery.c.latest_captured_at),
        )
    )
    return {snapshot.outcome_id: snapshot for snapshot in result.scalars().all()}


def _should_persist_price_snapshot(
    latest: PriceSnapshot | None,
    *,
    price: Decimal,
    volume_24h: Decimal | None,
    liquidity: Decimal | None,
    now: datetime,
) -> bool:
    if latest is None:
        return True

    if abs(Decimal(price) - latest.price) >= Decimal(str(settings.snapshot_price_change_epsilon)):
        return True

    if _relative_change(latest.volume_24h, volume_24h) >= Decimal(str(settings.snapshot_volume_liquidity_change_ratio)):
        return True
    if _relative_change(latest.liquidity, liquidity) >= Decimal(str(settings.snapshot_volume_liquidity_change_ratio)):
        return True

    latest_captured_at = _ensure_aware_utc(latest.captured_at)
    return (now - latest_captured_at).total_seconds() >= settings.snapshot_price_heartbeat_seconds


def _relative_change(old: Decimal | None, new: Decimal | None) -> Decimal:
    if old is None or new is None:
        return Decimal("0")
    if old == 0:
        return Decimal("1") if new != 0 else Decimal("0")
    return abs(Decimal(new) - old) / abs(old)


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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
