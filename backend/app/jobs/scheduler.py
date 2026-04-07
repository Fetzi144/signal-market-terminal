"""Scheduled jobs: market discovery, snapshot capture, signal detection, evaluation."""
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.config import settings
from app.db import async_session

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def _run_market_discovery():
    from app.ingestion.markets import discover_markets

    logger.info("Job: market_discovery starting")
    async with async_session() as session:
        try:
            count = await discover_markets(session)
            logger.info("Job: market_discovery done, %d markets", count)
        except Exception:
            logger.error("Job: market_discovery failed", exc_info=True)


async def _run_snapshot_capture():
    from app.ingestion.snapshots import capture_snapshots

    logger.info("Job: snapshot_capture starting")
    async with async_session() as session:
        try:
            count = await capture_snapshots(session)
            logger.info("Job: snapshot_capture done, %d snapshots", count)
        except Exception:
            logger.error("Job: snapshot_capture failed", exc_info=True)


async def _run_signal_detection():
    from app.ranking.scorer import persist_signals
    from app.signals.deadline_near import DeadlineNearDetector
    from app.signals.liquidity_vacuum import LiquidityVacuumDetector
    from app.signals.price_move import PriceMoveDetector
    from app.signals.spread_change import SpreadChangeDetector
    from app.signals.volume_spike import VolumeSpikeDetector

    logger.info("Job: signal_detection starting")
    async with async_session() as session:
        try:
            detectors = [
                PriceMoveDetector(),
                VolumeSpikeDetector(),
                SpreadChangeDetector(),
                LiquidityVacuumDetector(),
                DeadlineNearDetector(),
            ]
            all_candidates = []
            for detector in detectors:
                candidates = await detector.detect(session)
                all_candidates.extend(candidates)

            if all_candidates:
                created = await persist_signals(session, all_candidates)
                logger.info("Job: signal_detection done, %d new signals", created)

                # Alert on high-ranking new signals
                if created > 0:
                    await _alert_high_rank_signals(session)
            else:
                logger.info("Job: signal_detection done, no candidates")
        except Exception:
            logger.error("Job: signal_detection failed", exc_info=True)


async def _alert_high_rank_signals(session):
    """Send alerts for new signals above the rank threshold (only once per signal)."""
    from decimal import Decimal

    from sqlalchemy import select

    from app.models.market import Market
    from app.models.signal import Signal

    threshold = Decimal(str(settings.alert_rank_threshold))
    alerters = _build_alerters()

    if not alerters:
        return

    # Only alert signals that haven't been alerted yet
    result = await session.execute(
        select(Signal, Market.question)
        .join(Market, Signal.market_id == Market.id)
        .where(
            Signal.resolved.is_(False),
            Signal.alerted.is_(False),
            Signal.rank_score >= threshold,
        )
        .order_by(Signal.fired_at.desc())
        .limit(settings.alert_batch_limit)
    )
    rows = result.all()
    for signal, question in rows:
        for alerter in alerters:
            try:
                await alerter.send(signal, question)
            except Exception:
                logger.warning("Failed to send alert via %s for signal %s",
                               type(alerter).__name__, signal.id, exc_info=True)
        signal.alerted = True

    if rows:
        await session.commit()
        logger.info("Alerted %d signals", len(rows))


def _build_alerters():
    """Build list of active alerters based on config."""
    from app.alerts.logger_alert import LoggerAlerter
    alerters = [LoggerAlerter()]

    if settings.alert_webhook_url:
        from app.alerts.webhook_alert import WebhookAlerter
        alerters.append(WebhookAlerter())

    if settings.alert_telegram_bot_token and settings.alert_telegram_chat_id:
        from app.alerts.telegram_alert import TelegramAlerter
        alerters.append(TelegramAlerter())

    return alerters


async def _run_evaluation():
    from app.evaluation.evaluator import evaluate_signals

    logger.info("Job: evaluation starting")
    async with async_session() as session:
        try:
            count = await evaluate_signals(session)
            logger.info("Job: evaluation done, %d evaluations", count)
        except Exception:
            logger.error("Job: evaluation failed", exc_info=True)


async def _run_cleanup():
    from app.jobs.cleanup import cleanup_old_data

    logger.info("Job: cleanup starting")
    async with async_session() as session:
        try:
            counts = await cleanup_old_data(session)
            logger.info("Job: cleanup done, %s", counts)
        except Exception:
            logger.error("Job: cleanup failed", exc_info=True)


def start_scheduler():
    scheduler.add_job(
        _run_market_discovery,
        "interval",
        seconds=settings.market_discovery_interval_seconds,
        id="market_discovery",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_snapshot_capture,
        "interval",
        seconds=settings.snapshot_interval_seconds,
        id="snapshot_capture",
        replace_existing=True,
    )
    # Run detection shortly after snapshots
    scheduler.add_job(
        _run_signal_detection,
        "interval",
        seconds=settings.snapshot_interval_seconds + 10,
        id="signal_detection",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_evaluation,
        "interval",
        seconds=settings.evaluation_interval_seconds,
        id="evaluation",
        replace_existing=True,
    )
    scheduler.add_job(
        _run_cleanup,
        "interval",
        hours=settings.cleanup_interval_hours,
        id="cleanup",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))


def stop_scheduler():
    scheduler.shutdown(wait=False)
