"""Scheduled jobs: market discovery, snapshot capture, signal detection, evaluation."""
import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
import logging
import os
import socket
import uuid

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import delete, or_
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.config import settings
from app.db import async_session
from app.models.scheduler_lease import SchedulerLease

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
SCHEDULER_LEASE_NAME = "default"

_scheduler_owner_token: str | None = None
_scheduler_lease_task: asyncio.Task | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _scheduler_owner_label() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _build_scheduler_owner_token() -> str:
    return f"{_scheduler_owner_label()}:{uuid.uuid4()}"


async def _upsert_scheduler_lease(
    owner_token: str,
    *,
    allow_takeover: bool,
) -> bool:
    now = _utcnow()
    expires_at = now + timedelta(seconds=settings.scheduler_lease_seconds)

    async with async_session() as session:
        bind = session.sync_session.get_bind()
        values = {
            "scheduler_name": SCHEDULER_LEASE_NAME,
            "owner_token": owner_token,
            "acquired_at": now,
            "heartbeat_at": now,
            "expires_at": expires_at,
        }
        if bind.dialect.name == "postgresql":
            stmt = postgresql_insert(SchedulerLease).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerLease.scheduler_name],
                set_=values,
                where=(
                    SchedulerLease.owner_token == owner_token
                    if not allow_takeover
                    else or_(
                        SchedulerLease.owner_token == owner_token,
                        SchedulerLease.expires_at < now,
                    )
                ),
            )
            await session.execute(stmt)
        elif bind.dialect.name == "sqlite":
            stmt = sqlite_insert(SchedulerLease).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[SchedulerLease.scheduler_name],
                set_=values,
                where=(
                    SchedulerLease.owner_token == owner_token
                    if not allow_takeover
                    else or_(
                        SchedulerLease.owner_token == owner_token,
                        SchedulerLease.expires_at < now,
                    )
                ),
            )
            await session.execute(stmt)
        else:
            lease = await session.get(SchedulerLease, SCHEDULER_LEASE_NAME)
            if lease is None:
                session.add(SchedulerLease(**values))
            elif lease.owner_token == owner_token or (allow_takeover and lease.expires_at < now):
                lease.owner_token = owner_token
                lease.acquired_at = now
                lease.heartbeat_at = now
                lease.expires_at = expires_at
            else:
                await session.rollback()
                return False

        await session.commit()
        lease = await session.get(SchedulerLease, SCHEDULER_LEASE_NAME)
        return lease is not None and lease.owner_token == owner_token


async def _acquire_scheduler_ownership(owner_token: str) -> bool:
    return await _upsert_scheduler_lease(owner_token, allow_takeover=True)


async def _renew_scheduler_ownership(owner_token: str) -> bool:
    return await _upsert_scheduler_lease(owner_token, allow_takeover=False)


async def _release_scheduler_ownership(owner_token: str) -> bool:
    async with async_session() as session:
        result = await session.execute(
            delete(SchedulerLease).where(
                SchedulerLease.scheduler_name == SCHEDULER_LEASE_NAME,
                SchedulerLease.owner_token == owner_token,
            )
        )
        await session.commit()
        return bool(result.rowcount)


async def _stop_scheduler_after_ownership_loss(owner_token: str | None) -> None:
    global _scheduler_owner_token, _scheduler_lease_task

    if owner_token is None or _scheduler_owner_token != owner_token:
        return

    logger.error(
        "Scheduler ownership lost for %s; stopping local scheduler to avoid duplicate job execution",
        _scheduler_owner_label(),
    )
    _scheduler_owner_token = None
    current_task = asyncio.current_task()
    lease_task = _scheduler_lease_task
    if lease_task is not None and lease_task is not current_task:
        lease_task.cancel()
    _scheduler_lease_task = None
    if scheduler.running:
        scheduler.shutdown(wait=False)


async def _scheduler_lease_heartbeat(owner_token: str) -> None:
    global _scheduler_lease_task

    try:
        while scheduler.running and _scheduler_owner_token == owner_token:
            await asyncio.sleep(settings.scheduler_lease_renew_interval_seconds)
            if not scheduler.running or _scheduler_owner_token != owner_token:
                return
            renewed = await _renew_scheduler_ownership(owner_token)
            if not renewed:
                await _stop_scheduler_after_ownership_loss(owner_token)
                return
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.error("Scheduler lease heartbeat failed", exc_info=True)
        await _stop_scheduler_after_ownership_loss(owner_token)
    finally:
        if _scheduler_lease_task is asyncio.current_task():
            _scheduler_lease_task = None


async def _run_owned_job(job_name: str, job_func) -> None:
    owner_token = _scheduler_owner_token
    if owner_token is None:
        logger.warning("Skipping scheduler job %s because no scheduler owner is registered", job_name)
        return
    if not await _renew_scheduler_ownership(owner_token):
        logger.warning("Skipping scheduler job %s because ownership renewal failed", job_name)
        await _stop_scheduler_after_ownership_loss(owner_token)
        return
    await job_func()


def _add_owned_job(job_name: str, job_func, trigger: str, **trigger_kwargs) -> None:
    scheduler.add_job(
        _run_owned_job,
        trigger,
        id=job_name,
        replace_existing=True,
        args=[job_name, job_func],
        **trigger_kwargs,
    )


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
    from app.signals.arbitrage import ArbitrageDetector
    from app.signals.deadline_near import DeadlineNearDetector
    from app.signals.liquidity_vacuum import LiquidityVacuumDetector
    from app.signals.order_flow import OrderFlowImbalanceDetector
    from app.signals.price_move import PriceMoveDetector
    from app.signals.smart_money import SmartMoneyDetector
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
                ArbitrageDetector(),
                OrderFlowImbalanceDetector(),
                SmartMoneyDetector(),
            ]
            all_candidates = []
            for detector in detectors:
                candidates = await detector.detect(session)
                all_candidates.extend(candidates)

            if all_candidates:
                created, new_signals = await persist_signals(session, all_candidates)
                logger.info("Job: signal_detection done, %d new signals", created)

                # Run Bayesian confluence engine on recent signals
                confluence_signals = []
                if created > 0:
                    confluence_count, confluence_signals = await _run_confluence(session, all_candidates)
                    if confluence_count > 0:
                        logger.info("Job: confluence engine created %d fused signals", confluence_count)

                # Auto-open paper trades for EV-positive signals
                if created > 0:
                    await _run_paper_trading(session, [*new_signals, *confluence_signals])

                # Broadcast new signals via SSE
                if created > 0:
                    await _broadcast_new_signals(session, new_signals)
                    await _alert_high_rank_signals(session)
            else:
                logger.info("Job: signal_detection done, no candidates")
        except Exception:
            logger.error("Job: signal_detection failed", exc_info=True)


async def _run_confluence(session, candidates):
    """Run Bayesian confluence on candidates grouped by outcome_id."""
    from collections import defaultdict
    from decimal import Decimal

    from app.ranking.scorer import persist_signals
    from app.signals.confluence import fuse_signals

    # Group candidates by outcome_id
    by_outcome: dict[str, list] = defaultdict(list)
    for c in candidates:
        if c.outcome_id:
            by_outcome[c.outcome_id].append(c)

    confluence_candidates = []
    for outcome_id, group in by_outcome.items():
        if len(group) < 2:
            continue
        # Use the first candidate's price_at_fire as market price
        market_price = group[0].price_at_fire
        if market_price is None:
            continue
        fused = fuse_signals(group, market_price)
        if fused is not None:
            confluence_candidates.append(fused)

    if confluence_candidates:
        created, new_signals = await persist_signals(session, confluence_candidates)
        return created, new_signals
    return 0, []


async def _run_paper_trading(session, signals):
    """Auto-open paper trades for EV-positive signals."""
    from app.default_strategy import evaluate_default_strategy_signal
    from app.paper_trading.engine import attempt_open_trade
    from app.strategy_runs.service import ensure_active_default_strategy_run

    count = 0
    candidate_count = 0
    skip_counts: dict[str, int] = {}
    bootstrap_candidates = []
    for signal in signals:
        if signal.fired_at is None:
            continue
        if signal.fired_at.tzinfo is None:
            bootstrap_candidates.append(signal.fired_at.replace(tzinfo=timezone.utc))
        else:
            bootstrap_candidates.append(signal.fired_at.astimezone(timezone.utc))
    bootstrap_started_at = min(bootstrap_candidates, default=None)
    strategy_run = await ensure_active_default_strategy_run(
        session,
        bootstrap_started_at=bootstrap_started_at,
    )
    baseline_start_at = strategy_run.started_at.isoformat() if strategy_run.started_at else None

    for signal in signals:
        evaluation = evaluate_default_strategy_signal(signal, started_at=strategy_run.started_at)
        if not evaluation.signal_type_match or not evaluation.in_window:
            continue

        candidate_count += 1
        market_question = (signal.details or {}).get("market_question", "")
        attempted_at = datetime.now(timezone.utc).isoformat()

        if evaluation.eligible:
            result = await attempt_open_trade(
                session=session,
                signal_id=signal.id,
                outcome_id=signal.outcome_id,
                market_id=signal.market_id,
                estimated_probability=signal.estimated_probability,
                market_price=signal.price_at_fire,
                market_question=market_question,
                fired_at=signal.fired_at,
                strategy_run_id=strategy_run.id,
            )
        else:
            result = None

        details = dict(signal.details or {})
        strategy_details = dict(details.get("default_strategy") or {})
        strategy_details.update({
            "strategy_name": settings.default_strategy_name,
            "strategy_run_id": str(strategy_run.id),
            "baseline_start_at": baseline_start_at,
            "evaluated_at": attempted_at,
            "eligible": evaluation.eligible,
            "decision": (result.decision if result is not None else "skipped"),
            "reason_code": (result.reason_code if result is not None else evaluation.reason_code),
            "reason_label": (result.reason_label if result is not None else evaluation.reason_label),
            "detail": (result.detail if result is not None else None),
            "trade_id": str(result.trade.id) if result is not None and result.trade is not None else None,
        })
        if result is not None and result.diagnostics:
            strategy_details["diagnostics"] = result.diagnostics
        details["default_strategy"] = strategy_details
        signal.details = details

        if result is not None and result.trade is not None:
            count += 1
        else:
            reason_code = strategy_details.get("reason_code") or "unknown"
            skip_counts[reason_code] = skip_counts.get(reason_code, 0) + 1

    if candidate_count > 0:
        await session.commit()
        logger.info(
            "Paper trading: opened %d trades from %d in-window default-strategy signal(s)",
            count,
            candidate_count,
        )
        if skip_counts:
            logger.info("Paper trading skips by reason: %s", skip_counts)


async def _broadcast_new_signals(session, signals):
    """Publish new signal events to SSE subscribers."""
    try:
        from app.api.sse import broadcaster
        if broadcaster.subscriber_count == 0:
            return
        for s in signals:
            await broadcaster.publish("new_signal", {
                "signal_type": s.signal_type,
                "market_question": (s.details or {}).get("market_question", ""),
                "rank_score": float(s.rank_score),
                "outcome_name": (s.details or {}).get("outcome_name", ""),
                "direction": (s.details or {}).get("direction", ""),
            })
    except Exception:
        logger.warning("Failed to broadcast SSE events", exc_info=True)


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
        # Broadcast alert events via SSE
        try:
            from app.api.sse import broadcaster
            for signal, question in rows:
                await broadcaster.publish("new_alert", {
                    "signal_type": signal.signal_type,
                    "market_question": question,
                    "rank_score": float(signal.rank_score),
                })
        except Exception:
            logger.warning("Failed to broadcast alert SSE events", exc_info=True)


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

    if settings.alert_discord_webhook_url:
        from app.alerts.discord_alert import DiscordAlerter
        alerters.append(DiscordAlerter())

    if settings.push_vapid_private_key and settings.push_vapid_public_key:
        from app.alerts.push_alert import PushAlerter
        alerters.append(PushAlerter())

    return alerters


async def _run_resolution():
    from datetime import datetime, timezone

    from app.connectors import get_connector, get_enabled_platforms
    from app.ingestion.resolution import resolve_signals
    from app.models.ingestion import IngestionRun

    logger.info("Job: resolution starting")
    async with async_session() as session:
        total = 0
        for platform in get_enabled_platforms():
            run = IngestionRun(
                run_type="resolution",
                platform=platform,
                status="running",
            )
            session.add(run)
            await session.flush()
            try:
                connector = get_connector(platform)
                resolved_markets = await connector.fetch_resolved_markets(since_hours=24)
                count = 0
                if resolved_markets:
                    count = await resolve_signals(session, platform, resolved_markets)
                    total += count
                    # Resolve paper trades for settled markets
                    await _resolve_paper_trades(session, resolved_markets)
                await connector.close()
                run.status = "success"
                run.markets_processed = count
            except Exception:
                logger.error("Job: resolution failed for %s", platform, exc_info=True)
                run.status = "error"
                import traceback
                run.error = traceback.format_exc()[-500:]
            run.finished_at = datetime.now(timezone.utc)
            await session.commit()
        logger.info("Job: resolution done, %d signals resolved", total)


async def _resolve_paper_trades(session, resolved_markets):
    """Resolve paper trades when markets settle."""
    import uuid

    from app.paper_trading.engine import resolve_trades

    total = 0
    for market_data in resolved_markets:
        outcomes = market_data.get("outcomes", [])
        for outcome in outcomes:
            outcome_id = outcome.get("id") or outcome.get("outcome_id")
            won = outcome.get("won", False)
            if outcome_id:
                try:
                    oid = uuid.UUID(str(outcome_id))
                    count = await resolve_trades(session, oid, won)
                    total += count
                except (ValueError, Exception):
                    continue

    if total > 0:
        await session.commit()
        logger.info("Paper trading: resolved %d trades", total)


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


async def _run_portfolio_price_refresh():
    from app.portfolio.service import resolve_positions, update_current_prices

    logger.info("Job: portfolio_price_refresh starting")
    async with async_session() as session:
        try:
            updated = await update_current_prices(session)
            resolved = await resolve_positions(session)
            logger.info(
                "Job: portfolio_price_refresh done, %d prices updated, %d positions resolved",
                updated, resolved,
            )
        except Exception:
            logger.error("Job: portfolio_price_refresh failed", exc_info=True)


async def _run_whale_scan():
    from app.tracking.whale_tracker import scan_recent_activity

    logger.info("Job: whale_scan starting")
    async with async_session() as session:
        try:
            activities = await scan_recent_activity(session, hours=1)
            logger.info("Job: whale_scan done, %d new activities", len(activities))
        except Exception:
            logger.error("Job: whale_scan failed", exc_info=True)


async def start_scheduler() -> bool:
    global _scheduler_owner_token, _scheduler_lease_task

    if scheduler.running:
        logger.info("Scheduler already running; skipping duplicate start")
        return True

    owner_token = _build_scheduler_owner_token()
    acquired = await _acquire_scheduler_ownership(owner_token)
    if not acquired:
        logger.warning(
            "Scheduler ownership already held by another process; %s will not start scheduler jobs",
            _scheduler_owner_label(),
        )
        return False

    _scheduler_owner_token = owner_token
    scheduler.remove_all_jobs()
    _add_owned_job(
        "market_discovery",
        _run_market_discovery,
        "interval",
        seconds=settings.market_discovery_interval_seconds,
    )
    _add_owned_job(
        "snapshot_capture",
        _run_snapshot_capture,
        "interval",
        seconds=settings.snapshot_interval_seconds,
    )
    # Run detection shortly after snapshots
    _add_owned_job(
        "signal_detection",
        _run_signal_detection,
        "interval",
        seconds=settings.snapshot_interval_seconds + 10,
    )
    _add_owned_job(
        "evaluation",
        _run_evaluation,
        "interval",
        seconds=settings.evaluation_interval_seconds,
    )
    _add_owned_job(
        "resolution",
        _run_resolution,
        "interval",
        minutes=15,
    )
    _add_owned_job(
        "cleanup",
        _run_cleanup,
        "interval",
        hours=settings.cleanup_interval_hours,
    )
    _add_owned_job(
        "portfolio_price_refresh",
        _run_portfolio_price_refresh,
        "interval",
        minutes=5,
    )
    if settings.whale_tracking_enabled:
        _add_owned_job(
            "whale_scan",
            _run_whale_scan,
            "interval",
            seconds=settings.whale_scan_interval_seconds,
        )
    scheduler.start()
    _scheduler_lease_task = asyncio.create_task(_scheduler_lease_heartbeat(owner_token))
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))
    return True


async def stop_scheduler() -> None:
    global _scheduler_owner_token, _scheduler_lease_task

    owner_token = _scheduler_owner_token
    lease_task = _scheduler_lease_task

    if lease_task is not None:
        lease_task.cancel()
        _scheduler_lease_task = None
        with suppress(asyncio.CancelledError):
            await lease_task

    if scheduler.running:
        scheduler.shutdown(wait=False)
    if owner_token is not None:
        _scheduler_owner_token = None
        try:
            await _release_scheduler_ownership(owner_token)
        except Exception:
            logger.warning("Failed to release scheduler ownership", exc_info=True)
