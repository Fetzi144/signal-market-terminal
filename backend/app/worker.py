"""Dedicated scheduler worker entrypoint."""
import asyncio
import inspect
import logging
import signal

from prometheus_client import start_http_server

from app.config import settings
from app.db import async_session
from app.execution.polymarket_live_reconciler import PolymarketLiveReconciler
from app.execution.polymarket_pilot_supervisor import PolymarketPilotSupervisor
from app.execution.polymarket_user_stream import PolymarketUserStreamService
from app.ingestion.polymarket_book_reconstruction import PolymarketBookReconstructionService
from app.ingestion.polymarket_metadata import PolymarketMetaSyncService
from app.ingestion.polymarket_microstructure import PolymarketMicrostructureService
from app.ingestion.polymarket_raw_storage import PolymarketRawStorageService
from app.ingestion.polymarket_replay_simulator import PolymarketReplaySimulatorService
from app.ingestion.polymarket_risk_graph import PolymarketRiskGraphService
from app.ingestion.polymarket_stream import PolymarketStreamService
from app.ingestion.structure_engine import PolymarketStructureEngineService
from app.jobs.scheduler import scheduler as scheduler_runtime
from app.jobs.scheduler import start_scheduler, stop_scheduler
from app.strategies.registry import sync_strategy_registry

logger = logging.getLogger(__name__)
_worker_metrics_started = False


def _start_worker_metrics_server() -> None:
    global _worker_metrics_started
    if _worker_metrics_started or not settings.worker_metrics_enabled:
        return
    start_http_server(settings.worker_metrics_port)
    _worker_metrics_started = True
    logger.info("Worker metrics server listening on port %s", settings.worker_metrics_port)


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


async def _sync_strategy_registry_on_startup() -> None:
    try:
        async with async_session() as session:
            await sync_strategy_registry(session)
            await session.commit()
        logger.info("Strategy-family registry synced on worker startup")
    except Exception:
        logger.warning("Strategy-family registry sync failed on worker startup", exc_info=True)


def _scheduler_supervisor_retry_seconds() -> float:
    return max(
        0.1,
        min(
            float(settings.scheduler_lease_renew_interval_seconds),
            float(settings.scheduler_lease_seconds),
        ),
    )


async def _wait_for_stop_or_timeout(stop_event: asyncio.Event, timeout_seconds: float) -> bool:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout_seconds)
        return True
    except asyncio.TimeoutError:
        return stop_event.is_set()


async def _run_scheduler_supervisor(stop_event: asyncio.Event) -> None:
    retry_seconds = _scheduler_supervisor_retry_seconds()
    waiting_for_lease = False

    while not stop_event.is_set():
        if scheduler_runtime.running:
            waiting_for_lease = False
            if await _wait_for_stop_or_timeout(stop_event, retry_seconds):
                return
            continue

        try:
            started = await _maybe_await(start_scheduler())
        except Exception:
            logger.error("Scheduler startup failed; retrying", exc_info=True)
            started = False

        if started:
            waiting_for_lease = False
        elif not waiting_for_lease:
            logger.warning(
                "Scheduler worker did not acquire ownership; retrying every %.1fs until the lease becomes available",
                retry_seconds,
            )
            waiting_for_lease = True

        if await _wait_for_stop_or_timeout(stop_event, retry_seconds):
            return


async def _run_worker() -> None:
    if (
        not settings.scheduler_enabled
        and not settings.polymarket_stream_enabled
        and not settings.polymarket_meta_sync_enabled
        and not settings.polymarket_raw_storage_enabled
        and not settings.polymarket_book_recon_enabled
        and not settings.polymarket_features_enabled
        and not settings.polymarket_structure_engine_enabled
        and not settings.polymarket_risk_graph_enabled
        and not settings.polymarket_portfolio_optimizer_enabled
        and not settings.polymarket_replay_enabled
        and not settings.polymarket_user_stream_enabled
        and not settings.polymarket_live_trading_enabled
    ):
        logger.warning("Worker started with all worker features disabled; exiting")
        return

    _start_worker_metrics_server()
    await _sync_strategy_registry_on_startup()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    scheduler_supervisor_task: asyncio.Task | None = None
    stream_service: PolymarketStreamService | None = None
    stream_task: asyncio.Task | None = None
    meta_sync_service: PolymarketMetaSyncService | None = None
    meta_sync_task: asyncio.Task | None = None
    raw_storage_service: PolymarketRawStorageService | None = None
    raw_storage_task: asyncio.Task | None = None
    book_recon_service: PolymarketBookReconstructionService | None = None
    book_recon_task: asyncio.Task | None = None
    feature_service: PolymarketMicrostructureService | None = None
    feature_task: asyncio.Task | None = None
    structure_service: PolymarketStructureEngineService | None = None
    structure_task: asyncio.Task | None = None
    risk_service: PolymarketRiskGraphService | None = None
    risk_task: asyncio.Task | None = None
    replay_service: PolymarketReplaySimulatorService | None = None
    replay_task: asyncio.Task | None = None
    user_stream_service: PolymarketUserStreamService | None = None
    user_stream_task: asyncio.Task | None = None
    reconcile_service: PolymarketLiveReconciler | None = None
    reconcile_task: asyncio.Task | None = None
    pilot_supervisor_service: PolymarketPilotSupervisor | None = None
    pilot_supervisor_task: asyncio.Task | None = None

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    if settings.scheduler_enabled:
        scheduler_supervisor_task = asyncio.create_task(_run_scheduler_supervisor(stop_event))

    if settings.polymarket_stream_enabled:
        stream_service = PolymarketStreamService(async_session)
        stream_task = asyncio.create_task(stream_service.run(stop_event))

    if settings.polymarket_meta_sync_enabled:
        meta_sync_service = PolymarketMetaSyncService(async_session)
        meta_sync_task = asyncio.create_task(meta_sync_service.run(stop_event))

    if settings.polymarket_raw_storage_enabled:
        raw_storage_service = PolymarketRawStorageService(async_session)
        raw_storage_task = asyncio.create_task(raw_storage_service.run(stop_event))

    if settings.polymarket_book_recon_enabled:
        book_recon_service = PolymarketBookReconstructionService(async_session)
        book_recon_task = asyncio.create_task(book_recon_service.run(stop_event))

    if settings.polymarket_features_enabled:
        feature_service = PolymarketMicrostructureService(async_session)
        feature_task = asyncio.create_task(feature_service.run(stop_event))

    if settings.polymarket_structure_engine_enabled:
        structure_service = PolymarketStructureEngineService(async_session)
        structure_task = asyncio.create_task(structure_service.run(stop_event))

    if settings.polymarket_risk_graph_enabled or settings.polymarket_portfolio_optimizer_enabled:
        risk_service = PolymarketRiskGraphService(async_session)
        risk_task = asyncio.create_task(risk_service.run(stop_event))

    if settings.polymarket_replay_enabled:
        replay_service = PolymarketReplaySimulatorService(async_session)
        replay_task = asyncio.create_task(replay_service.run(stop_event))

    if settings.polymarket_user_stream_enabled:
        user_stream_service = PolymarketUserStreamService(async_session)
        user_stream_task = asyncio.create_task(user_stream_service.run(stop_event))

    if settings.polymarket_user_stream_enabled or settings.polymarket_live_trading_enabled:
        reconcile_service = PolymarketLiveReconciler(async_session)
        reconcile_task = asyncio.create_task(reconcile_service.run(stop_event))

    if settings.polymarket_pilot_enabled or settings.polymarket_live_trading_enabled or settings.polymarket_user_stream_enabled:
        pilot_supervisor_service = PolymarketPilotSupervisor(async_session)
        pilot_supervisor_task = asyncio.create_task(pilot_supervisor_service.run(stop_event))

    if (
        scheduler_supervisor_task is None
        and stream_task is None
        and meta_sync_task is None
        and raw_storage_task is None
        and book_recon_task is None
        and feature_task is None
        and structure_task is None
        and risk_task is None
        and replay_task is None
        and user_stream_task is None
        and reconcile_task is None
        and pilot_supervisor_task is None
    ):
        logger.warning("No worker responsibilities started; exiting")
        return

    logger.info("Worker is running")
    try:
        await stop_event.wait()
    finally:
        logger.info("Worker stopping")
        if scheduler_supervisor_task is not None:
            scheduler_supervisor_task.cancel()
            try:
                await scheduler_supervisor_task
            except asyncio.CancelledError:
                pass
        if stream_task is not None:
            stream_task.cancel()
            try:
                await stream_task
            except asyncio.CancelledError:
                pass
        if stream_service is not None:
            await stream_service.close()
        if meta_sync_task is not None:
            meta_sync_task.cancel()
            try:
                await meta_sync_task
            except asyncio.CancelledError:
                pass
        if meta_sync_service is not None:
            await meta_sync_service.close()
        if raw_storage_task is not None:
            raw_storage_task.cancel()
            try:
                await raw_storage_task
            except asyncio.CancelledError:
                pass
        if raw_storage_service is not None:
            await raw_storage_service.close()
        if book_recon_task is not None:
            book_recon_task.cancel()
            try:
                await book_recon_task
            except asyncio.CancelledError:
                pass
        if book_recon_service is not None:
            await book_recon_service.close()
        if feature_task is not None:
            feature_task.cancel()
            try:
                await feature_task
            except asyncio.CancelledError:
                pass
        if feature_service is not None:
            await feature_service.close()
        if structure_task is not None:
            structure_task.cancel()
            try:
                await structure_task
            except asyncio.CancelledError:
                pass
        if structure_service is not None:
            await structure_service.close()
        if risk_task is not None:
            risk_task.cancel()
            try:
                await risk_task
            except asyncio.CancelledError:
                pass
        if risk_service is not None:
            await risk_service.close()
        if replay_task is not None:
            replay_task.cancel()
            try:
                await replay_task
            except asyncio.CancelledError:
                pass
        if replay_service is not None:
            await replay_service.close()
        if user_stream_task is not None:
            user_stream_task.cancel()
            try:
                await user_stream_task
            except asyncio.CancelledError:
                pass
        if user_stream_service is not None:
            await user_stream_service.close()
        if reconcile_task is not None:
            reconcile_task.cancel()
            try:
                await reconcile_task
            except asyncio.CancelledError:
                pass
        if pilot_supervisor_task is not None:
            pilot_supervisor_task.cancel()
            try:
                await pilot_supervisor_task
            except asyncio.CancelledError:
                pass
        if pilot_supervisor_service is not None:
            await pilot_supervisor_service.close()
        if settings.scheduler_enabled:
            await _maybe_await(stop_scheduler())


def main() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
