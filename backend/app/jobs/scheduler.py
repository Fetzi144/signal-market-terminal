"""Scheduled jobs: market discovery, snapshot capture, signal detection, evaluation."""
import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import or_, select

from app.config import settings
from app.db import async_session
from app.jobs.lease import (
    acquire_named_lease,
    build_lease_owner_token,
    lease_owner_label,
    release_named_lease,
    renew_named_lease,
)
from app.metrics import default_strategy_scheduler_no_active_run

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
SCHEDULER_LEASE_NAME = "default"
PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE = 1000
PAPER_TRADING_PENDING_RETRY_BATCH_SIZE = 100
PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE = 100

_scheduler_owner_token: str | None = None
_scheduler_lease_task: asyncio.Task | None = None
_alpha_factory_no_new_candidate_runs = 0
_alpha_factory_autopilot_paused = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _scheduler_owner_label() -> str:
    return lease_owner_label()


def _build_scheduler_owner_token() -> str:
    return build_lease_owner_token(lease_name=SCHEDULER_LEASE_NAME)


async def _acquire_scheduler_ownership(owner_token: str) -> bool:
    return await acquire_named_lease(
        async_session,
        lease_name=SCHEDULER_LEASE_NAME,
        owner_token=owner_token,
        lease_seconds=settings.scheduler_lease_seconds,
    )


async def _renew_scheduler_ownership(owner_token: str) -> bool:
    return await renew_named_lease(
        async_session,
        lease_name=SCHEDULER_LEASE_NAME,
        owner_token=owner_token,
        lease_seconds=settings.scheduler_lease_seconds,
    )


async def _release_scheduler_ownership(owner_token: str) -> bool:
    return await release_named_lease(
        async_session,
        lease_name=SCHEDULER_LEASE_NAME,
        owner_token=owner_token,
    )


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

                # Default-strategy processing runs every detection pass; without an active run it records a no-op metric.
                await _run_paper_trading(session, [*new_signals, *confluence_signals])

                # Broadcast new signals via SSE
                if created > 0:
                    await _broadcast_new_signals(session, new_signals)
                    await _alert_high_rank_signals(session)
            else:
                await _run_paper_trading(session, [])
                logger.info("Job: signal_detection done, no candidates")
        except Exception:
            logger.error("Job: signal_detection failed", exc_info=True)


async def _run_confluence(session, candidates):
    """Run Bayesian confluence on candidates grouped by outcome_id."""
    from collections import defaultdict

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
    from app.models.execution_decision import ExecutionDecision
    from app.models.signal import Signal
    from app.paper_trading.engine import attempt_open_trade, ensure_pending_execution_decision
    from app.paper_trading.reconciliation import (
        backfill_execution_decisions_from_strategy_metadata,
        expire_stale_pending_execution_decisions,
        finalize_unrecoverable_orderbook_context_decisions,
        hydrate_strategy_run_state,
        load_missing_qualified_signals,
    )
    from app.strategies.kalshi_cheap_yes_follow import run_kalshi_cheap_yes_follow_paper_lane
    from app.strategies.kalshi_down_yes_fade import run_kalshi_down_yes_fade_paper_lane
    from app.strategies.kalshi_low_yes_fade import run_kalshi_low_yes_fade_paper_lane
    from app.strategies.kalshi_very_low_yes_fade import run_kalshi_very_low_yes_fade_paper_lane
    from app.strategy_runs.service import get_active_strategy_run

    count = 0
    candidate_count = 0
    retry_candidates = 0
    backlog_candidates = 0
    expired_pending_decisions = 0
    finalized_orderbook_context_decisions = 0
    skip_counts: dict[str, int] = {}
    strategy_run = await get_active_strategy_run(session, settings.default_strategy_name)
    if strategy_run is None:
        default_strategy_scheduler_no_active_run.inc()
        logger.info(
            "Paper trading skipped for %d signal(s): no active default-strategy run is bootstrapped",
            len(signals),
        )
        await run_kalshi_down_yes_fade_paper_lane(
            session,
            signals,
            pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
            backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
            pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
        )
        await run_kalshi_low_yes_fade_paper_lane(
            session,
            signals,
            pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
            backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
            pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
        )
        await run_kalshi_very_low_yes_fade_paper_lane(
            session,
            signals,
            pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
            backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
            pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
        )
        await run_kalshi_cheap_yes_follow_paper_lane(
            session,
            signals,
            pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
            backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
            pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
        )
        return
    state_rehydrated = await hydrate_strategy_run_state(session, strategy_run)
    baseline_start_at = strategy_run.started_at.isoformat() if strategy_run.started_at else None
    fresh_signal_ids = {signal.id for signal in signals}
    missing_backlog_signals = await load_missing_qualified_signals(
        session,
        strategy_run,
        exclude_signal_ids=fresh_signal_ids,
        limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
    )
    backfilled_signal_ids = await backfill_execution_decisions_from_strategy_metadata(
        session,
        strategy_run,
        signals=missing_backlog_signals,
    )
    expired_pending_decisions = await expire_stale_pending_execution_decisions(
        session,
        strategy_run,
        limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
    )
    backlog_retry_signals = [
        signal
        for signal in missing_backlog_signals
        if signal.id not in backfilled_signal_ids
    ]
    pending_signal_query = (
        select(Signal)
        .join(ExecutionDecision, ExecutionDecision.signal_id == Signal.id)
        .where(
            ExecutionDecision.strategy_run_id == strategy_run.id,
            ExecutionDecision.decision_status == "pending_decision",
        )
        .order_by(ExecutionDecision.decision_at.asc(), ExecutionDecision.id.asc())
        .limit(PAPER_TRADING_PENDING_RETRY_BATCH_SIZE)
    )
    if fresh_signal_ids:
        pending_signal_query = pending_signal_query.where(Signal.id.not_in(fresh_signal_ids))
    pending_signal_result = await session.execute(pending_signal_query)
    pending_retry_signals = pending_signal_result.scalars().all()
    work_items = (
        [(signal, "fresh_signal") for signal in signals]
        + [(signal, "retry") for signal in pending_retry_signals]
        + [(signal, "backlog_repair") for signal in backlog_retry_signals]
    )

    for signal, attempt_kind in work_items:
        evaluation = evaluate_default_strategy_signal(signal, started_at=strategy_run.started_at)
        if not evaluation.signal_type_match or not evaluation.in_window:
            continue

        candidate_count += 1
        if attempt_kind == "retry":
            retry_candidates += 1
        elif attempt_kind == "backlog_repair":
            backlog_candidates += 1
        market_question = (signal.details or {}).get("market_question", "")
        attempted_at = datetime.now(timezone.utc).isoformat()
        if evaluation.eligible and attempt_kind != "retry":
            await ensure_pending_execution_decision(
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
            precheck_reason_code=None if evaluation.eligible else evaluation.reason_code,
            precheck_reason_label=evaluation.reason_label,
        )

        details = dict(signal.details or {})
        strategy_details = dict(details.get("default_strategy") or {})
        strategy_details.update({
            "strategy_name": settings.default_strategy_name,
            "strategy_run_id": str(strategy_run.id),
            "baseline_start_at": baseline_start_at,
            "evaluated_at": attempted_at,
            "attempt_kind": attempt_kind,
            "eligible": evaluation.eligible,
            "decision": result.decision,
            "reason_code": result.reason_code,
            "reason_label": result.reason_label,
            "detail": result.detail,
            "trade_id": str(result.trade.id) if result.trade is not None else None,
        })
        if result.diagnostics:
            strategy_details["diagnostics"] = result.diagnostics
        details["default_strategy"] = strategy_details
        signal.details = details

        if result.trade is not None:
            count += 1
        else:
            reason_code = strategy_details.get("reason_code") or "unknown"
            skip_counts[reason_code] = skip_counts.get(reason_code, 0) + 1

    post_attempt_expired_pending_decisions = await expire_stale_pending_execution_decisions(
        session,
        strategy_run,
        limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
    )
    expired_pending_decisions += post_attempt_expired_pending_decisions
    finalized_orderbook_context_decisions = await finalize_unrecoverable_orderbook_context_decisions(
        session,
        strategy_run,
        limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
    )

    if (
        candidate_count > 0
        or state_rehydrated
        or backfilled_signal_ids
        or expired_pending_decisions
        or finalized_orderbook_context_decisions
    ):
        await session.commit()
        if state_rehydrated:
            logger.warning(
                "Paper trading rehydrated incomplete risk state for strategy run %s",
                strategy_run.id,
            )
        if backfilled_signal_ids:
            logger.warning(
                "Paper trading backfilled %d historical execution decision(s) from stored signal metadata",
                len(backfilled_signal_ids),
            )
        if expired_pending_decisions:
            logger.warning(
                "Paper trading expired %d stale pending execution decision(s) older than %d seconds",
                expired_pending_decisions,
                settings.paper_trading_pending_decision_max_age_seconds,
            )
        if finalized_orderbook_context_decisions:
            logger.warning(
                "Paper trading finalized %d orderbook-context pending decision(s) after the event-time recovery window",
                finalized_orderbook_context_decisions,
            )
        if expired_pending_decisions == PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE:
            logger.info(
                "Paper trading pending-expiry backlog capped at %d execution decision(s) for strategy run %s",
                PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
                strategy_run.id,
            )
        if len(pending_retry_signals) == PAPER_TRADING_PENDING_RETRY_BATCH_SIZE:
            logger.info(
                "Paper trading retry backlog capped at %d pending execution decision(s) for strategy run %s",
                PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
                strategy_run.id,
            )
        if len(backlog_retry_signals) == PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE:
            logger.info(
                "Paper trading repair backlog capped at %d qualified signal(s) for strategy run %s",
                PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
                strategy_run.id,
            )
        logger.info(
            "Paper trading: opened %d trades from %d in-window default-strategy signal(s)",
            count,
            candidate_count,
        )
        if retry_candidates:
            logger.info(
                "Paper trading: retried %d pending execution decision(s)",
                retry_candidates,
            )
        if backlog_candidates:
            logger.info(
                "Paper trading: repaired %d qualified signal(s) that were missing execution decisions",
                backlog_candidates,
            )
        if skip_counts:
            logger.info("Paper trading skips by reason: %s", skip_counts)

    await run_kalshi_down_yes_fade_paper_lane(
        session,
        signals,
        pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
        backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
        pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
    )
    await run_kalshi_low_yes_fade_paper_lane(
        session,
        signals,
        pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
        backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
        pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
    )
    await run_kalshi_very_low_yes_fade_paper_lane(
        session,
        signals,
        pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
        backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
        pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
    )
    await run_kalshi_cheap_yes_follow_paper_lane(
        session,
        signals,
        pending_retry_limit=PAPER_TRADING_PENDING_RETRY_BATCH_SIZE,
        backlog_limit=PAPER_TRADING_BACKLOG_REPAIR_BATCH_SIZE,
        pending_expiry_limit=PAPER_TRADING_PENDING_EXPIRY_BATCH_SIZE,
    )


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

    from app.connectors import get_connector
    from app.ingestion.resolution import resolve_signals
    from app.models.ingestion import IngestionRun

    logger.info("Job: resolution starting")
    async with async_session() as session:
        total = 0
        for platform in await _get_resolution_platforms(session):
            run = IngestionRun(
                run_type="resolution",
                platform=platform,
                status="running",
            )
            session.add(run)
            await session.flush()
            connector = None
            backfill_run = None
            try:
                connector = get_connector(platform)
                resolved_markets = await connector.fetch_resolved_markets(since_hours=24)
                overdue_open_trade_resolutions = await _fetch_overdue_open_trade_resolutions(
                    session,
                    connector,
                    platform=platform,
                )
                if platform == "kalshi":
                    backfill_run = IngestionRun(
                        run_type="resolution_backfill",
                        platform=platform,
                        status="success",
                        markets_processed=len(overdue_open_trade_resolutions),
                        finished_at=datetime.now(timezone.utc),
                    )
                    session.add(backfill_run)
                if overdue_open_trade_resolutions:
                    resolved_markets.extend(overdue_open_trade_resolutions)
                count = 0
                if resolved_markets:
                    count = await resolve_signals(session, platform, resolved_markets)
                    total += count
                    # Resolve paper trades for settled markets
                    await _resolve_paper_trades(session, resolved_markets, platform=platform)
                run.status = "success"
                run.markets_processed = count
            except Exception:
                logger.error("Job: resolution failed for %s", platform, exc_info=True)
                run.status = "error"
                if platform == "kalshi" and backfill_run is None:
                    backfill_run = IngestionRun(
                        run_type="resolution_backfill",
                        platform=platform,
                        status="error",
                    )
                    session.add(backfill_run)
                if backfill_run is not None and backfill_run.status != "success":
                    backfill_run.status = "error"
                    backfill_run.finished_at = datetime.now(timezone.utc)
                import traceback
                error_summary = traceback.format_exc()[-500:]
                run.error = error_summary
                if backfill_run is not None and backfill_run.error is None:
                    backfill_run.error = error_summary
            finally:
                if connector is not None:
                    try:
                        await connector.close()
                    except Exception:
                        logger.warning("Failed to close %s connector after resolution job", platform, exc_info=True)
            run.finished_at = datetime.now(timezone.utc)
            await session.commit()
        logger.info("Job: resolution done, %d signals resolved", total)


async def _has_overdue_open_trade_markets(session, *, platform: str) -> bool:
    from app.models.market import Market
    from app.models.paper_trade import PaperTrade

    now = datetime.now(timezone.utc)
    result = await session.execute(
        select(Market.id)
        .join(PaperTrade, PaperTrade.market_id == Market.id)
        .where(
            Market.platform == platform,
            PaperTrade.status == "open",
            Market.platform_id.is_not(None),
            or_(
                Market.active.is_(False),
                Market.end_date < now,
            ),
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


async def _get_resolution_platforms(session) -> list[str]:
    from app.connectors import get_enabled_platforms

    platforms = list(get_enabled_platforms())
    if (
        "kalshi" not in platforms
        and settings.kalshi_resolution_backfill_enabled
        and await _has_overdue_open_trade_markets(session, platform="kalshi")
    ):
        platforms.append("kalshi")
        logger.info(
            "Kalshi resolution backfill enabled despite KALSHI_ENABLED=false because overdue open paper trades exist"
        )
    return platforms


async def _resolved_trade_outcomes_for_market_data(session, market_data, *, platform: str | None = None):
    import uuid

    from app.models.market import Market, Outcome

    resolved_outcomes: list[tuple[uuid.UUID, bool]] = []
    for outcome in market_data.get("outcomes", []):
        outcome_id = outcome.get("id") or outcome.get("outcome_id")
        won = outcome.get("won", False)
        if not outcome_id:
            continue
        try:
            resolved_outcomes.append((uuid.UUID(str(outcome_id)), bool(won)))
        except ValueError:
            continue

    if resolved_outcomes or platform is None:
        return resolved_outcomes

    platform_id = market_data.get("platform_id")
    winner = market_data.get("winning_outcome")
    if winner is None:
        winner = market_data.get("winner")
    if not platform_id or winner is None:
        return []

    market_result = await session.execute(
        select(Market).where(
            Market.platform == platform,
            Market.platform_id == str(platform_id),
        )
    )
    market = market_result.scalar_one_or_none()
    if market is None:
        return []

    outcome_result = await session.execute(select(Outcome).where(Outcome.market_id == market.id))
    outcomes = outcome_result.scalars().all()
    if not outcomes:
        return []

    winner_text = str(winner).strip().lower()
    if platform == "kalshi" and winner_text in {"yes", "no"}:
        inferred_outcomes = [
            (outcome.id, side == winner_text)
            for outcome in outcomes
            if (side := _kalshi_outcome_side(outcome)) is not None
        ]
        if inferred_outcomes:
            return inferred_outcomes

    winning_outcome_ids = {
        outcome.id
        for outcome in outcomes
        if (outcome.name or "").strip().lower() == winner_text
        or (outcome.platform_outcome_id or "").strip().lower() == winner_text
        or (platform == "kalshi" and (outcome.platform_outcome_id or "").strip().lower().endswith(f"_{winner_text}"))
    }
    if not winning_outcome_ids:
        logger.warning(
            "Resolved market payload did not map to local outcomes for platform=%s platform_id=%s winner=%s",
            platform,
            platform_id,
            winner,
        )
        return []

    return [(outcome.id, outcome.id in winning_outcome_ids) for outcome in outcomes]


def _kalshi_outcome_side(outcome) -> str | None:
    for raw_value in (outcome.name, outcome.platform_outcome_id, outcome.token_id):
        text = str(raw_value or "").strip().lower()
        if not text:
            continue
        if text in {"yes", "no"}:
            return text
        normalized_parts = text.replace(":", "_").replace("-", "_").split("_")
        if normalized_parts and normalized_parts[-1] in {"yes", "no"}:
            return normalized_parts[-1]
    return None


async def _fetch_overdue_open_trade_resolutions(session, connector, *, platform: str) -> list[dict]:
    if platform != "kalshi":
        return []

    from app.models.market import Market
    from app.models.paper_trade import PaperTrade

    now = datetime.now(timezone.utc)
    ticker_result = await session.execute(
        select(Market.platform_id)
        .join(PaperTrade, PaperTrade.market_id == Market.id)
        .where(
            Market.platform == platform,
            PaperTrade.status == "open",
            Market.platform_id.is_not(None),
            or_(
                Market.active.is_(False),
                Market.end_date < now,
            ),
        )
        .distinct()
    )
    tickers = [ticker for ticker in ticker_result.scalars().all() if ticker]
    if not tickers:
        return []

    resolved_markets: list[dict] = []
    for start in range(0, len(tickers), 200):
        batch = tickers[start : start + 200]
        try:
            response = await connector._request_with_retry(
                "get",
                f"{connector.api_base}/markets",
                params={"tickers": ",".join(batch)},
            )
        except Exception:
            logger.warning(
                "Kalshi overdue open-trade resolution fetch failed for tickers %s",
                batch,
                exc_info=True,
            )
            continue

        for market_data in response.json().get("markets") or []:
            status = str(market_data.get("status") or "").strip().lower()
            result = market_data.get("result")
            if status not in {"settled", "finalized"} or result is None:
                continue
            resolved_markets.append(
                {
                    "platform_id": market_data.get("ticker", ""),
                    "winning_outcome": result,
                }
            )

    if resolved_markets:
        logger.info(
            "Kalshi overdue open-trade backfill found %d resolved markets",
            len(resolved_markets),
        )

    return resolved_markets


async def _resolve_paper_trades(session, resolved_markets, *, platform: str | None = None):
    """Resolve paper trades when markets settle."""

    from app.paper_trading.engine import resolve_trades

    total = 0
    for market_data in resolved_markets:
        resolved_outcomes = await _resolved_trade_outcomes_for_market_data(
            session,
            market_data,
            platform=platform,
        )
        for outcome_id, won in resolved_outcomes:
            try:
                count = await resolve_trades(session, outcome_id, won)
                total += count
            except Exception:
                logger.warning(
                    "Paper trading failed to resolve trade(s) for platform=%s platform_id=%s outcome_id=%s won=%s",
                    platform,
                    market_data.get("platform_id"),
                    outcome_id,
                    won,
                    exc_info=True,
                )
                continue

    if total > 0:
        await session.commit()
        logger.info("Paper trading: resolved %d trades", total)


def _should_generate_default_strategy_review(health: dict) -> bool:
    from app.reports.strategy_review import should_generate_default_strategy_review

    return should_generate_default_strategy_review(health)


async def _run_default_strategy_review_generation():
    logger.info("Job: default_strategy_review_generation starting")
    async with async_session() as session:
        try:
            from app.paper_trading.analysis import get_strategy_health
            from app.reports.profitability_snapshot import generate_profitability_snapshot_artifact
            from app.reports.strategy_review import generate_default_strategy_review

            health = await get_strategy_health(session, use_cache=False)
            review_result = None
            if not _should_generate_default_strategy_review(health):
                freshness = health.get("evidence_freshness") or {}
                latest_artifact = health.get("latest_review_artifact") or {}
                logger.info(
                    "Job: default_strategy_review_generation skipped; status=%s generation_status=%s review_outdated=%s identity=%s",
                    freshness.get("status"),
                    latest_artifact.get("generation_status"),
                    freshness.get("review_outdated"),
                    freshness.get("artifact_identity_status"),
                )
            else:
                review_result = await generate_default_strategy_review(session, health=health)
                verdict = review_result.get("review_verdict") or {}
                logger.info(
                    "Job: default_strategy_review_generation wrote %s and %s with verdict=%s",
                    review_result.get("review_path"),
                    review_result.get("review_json_path"),
                    verdict.get("verdict"),
                )

            snapshot_result = await generate_profitability_snapshot_artifact(
                session,
                ensure_review_current=False,
            )
            logger.info(
                "Job: default_strategy_review_generation wrote profitability snapshot %s",
                snapshot_result.get("snapshot_json_path"),
            )
        except Exception:
            logger.error("Job: default_strategy_review_generation failed", exc_info=True)


def _alpha_factory_new_candidate_count(snapshot: dict) -> int:
    candidates = snapshot.get("top_candidates") or []
    return sum(
        1
        for candidate in candidates
        if candidate.get("ready_for_paper_lane")
        and candidate.get("trade_direction")
        and not candidate.get("blockers")
        and not candidate.get("existing_lane")
        and candidate.get("dedupe_status", "new_candidate") == "new_candidate"
    )


def _alpha_factory_existing_lane_candidate_count(snapshot: dict) -> int:
    candidates = snapshot.get("top_candidates") or []
    return sum(
        1
        for candidate in candidates
        if candidate.get("ready_for_paper_lane") and candidate.get("existing_lane")
    )


async def _run_alpha_factory_autopilot():
    global _alpha_factory_autopilot_paused, _alpha_factory_no_new_candidate_runs

    if _alpha_factory_autopilot_paused:
        logger.info("Job: alpha_factory_autopilot skipped because the no-new-candidate stop condition was reached")
        return

    logger.info("Job: alpha_factory_autopilot starting")
    async with async_session() as session:
        try:
            from app.reports.alpha_factory import generate_alpha_factory_artifact

            result = await generate_alpha_factory_artifact(
                session,
                window_days=settings.alpha_factory_auto_run_window_days,
                max_signals=settings.alpha_factory_auto_run_max_signals,
                platform="kalshi",
                max_candidates=settings.alpha_factory_auto_run_max_candidates,
                min_train_sample=settings.alpha_factory_auto_run_min_train_sample,
                min_validation_sample=settings.alpha_factory_auto_run_min_validation_sample,
                min_test_sample=settings.alpha_factory_auto_run_min_test_sample,
            )
            snapshot = result.get("snapshot") or {}
            new_candidate_count = _alpha_factory_new_candidate_count(snapshot)
            existing_lane_candidate_count = _alpha_factory_existing_lane_candidate_count(snapshot)
            candidate_count = int(snapshot.get("candidate_count") or 0)
            ready_candidate_count = int(snapshot.get("ready_candidate_count") or 0)

            if new_candidate_count:
                _alpha_factory_no_new_candidate_runs = 0
                logger.warning(
                    "Job: alpha_factory_autopilot found %d new paper-lane candidate(s); artifact=%s",
                    new_candidate_count,
                    result.get("alpha_factory_json_path"),
                )
                return

            _alpha_factory_no_new_candidate_runs += 1
            logger.info(
                (
                    "Job: alpha_factory_autopilot done with no new lane; artifact=%s "
                    "candidates=%d ready=%d existing_ready=%d no_new_runs=%d/%d"
                ),
                result.get("alpha_factory_json_path"),
                candidate_count,
                ready_candidate_count,
                existing_lane_candidate_count,
                _alpha_factory_no_new_candidate_runs,
                settings.alpha_factory_auto_run_stop_after_no_new_candidate_runs,
            )

            if (
                _alpha_factory_no_new_candidate_runs
                >= settings.alpha_factory_auto_run_stop_after_no_new_candidate_runs
            ):
                _alpha_factory_autopilot_paused = True
                job = scheduler.get_job("alpha_factory_autopilot")
                if job is not None:
                    job.pause()
                logger.warning(
                    (
                        "Job: alpha_factory_autopilot paused after %d consecutive run(s) "
                        "without a new paper-lane candidate"
                    ),
                    _alpha_factory_no_new_candidate_runs,
                )
        except Exception:
            logger.error("Job: alpha_factory_autopilot failed", exc_info=True)


async def _run_evaluation():
    from app.evaluation.evaluator import evaluate_signals
    from app.models.ingestion import IngestionRun

    logger.info("Job: evaluation starting")
    async with async_session() as session:
        run = IngestionRun(
            run_type="evaluation",
            platform="system",
            status="running",
        )
        session.add(run)
        await session.flush()
        try:
            count = await evaluate_signals(session)
            stats = session.sync_session.info.pop("signal_evaluation_stats", {})
            failed = int(stats.get("failed", 0) or 0)
            run.markets_processed = count
            if failed > 0:
                run.status = "error"
                run.error = f"{failed} signal evaluation horizon(s) failed"
                logger.warning(
                    "Job: evaluation completed with %d failed signal horizon(s)",
                    failed,
                )
            else:
                run.status = "success"
            logger.info("Job: evaluation done, %d evaluations", count)
        except Exception:
            logger.error("Job: evaluation failed", exc_info=True)
            run.status = "error"
            import traceback
            run.error = traceback.format_exc()[-500:]
        run.finished_at = _utcnow()
        await session.commit()


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
    if settings.default_strategy_review_auto_generate_enabled:
        _add_owned_job(
            "default_strategy_review_generation",
            _run_default_strategy_review_generation,
            "interval",
            seconds=settings.default_strategy_review_auto_generate_interval_seconds,
        )
    if settings.alpha_factory_auto_run_enabled:
        _add_owned_job(
            "alpha_factory_autopilot",
            _run_alpha_factory_autopilot,
            "interval",
            seconds=settings.alpha_factory_auto_run_interval_seconds,
            next_run_time=_utcnow(),
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
