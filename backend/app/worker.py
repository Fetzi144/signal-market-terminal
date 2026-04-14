"""Dedicated scheduler worker entrypoint."""
import asyncio
import inspect
import logging
import signal

from app.db import async_session
from app.config import settings
from app.execution.polymarket_live_reconciler import PolymarketLiveReconciler
from app.execution.polymarket_user_stream import PolymarketUserStreamService
from app.ingestion.polymarket_book_reconstruction import PolymarketBookReconstructionService
from app.ingestion.polymarket_metadata import PolymarketMetaSyncService
from app.ingestion.polymarket_microstructure import PolymarketMicrostructureService
from app.ingestion.polymarket_raw_storage import PolymarketRawStorageService
from app.ingestion.polymarket_stream import PolymarketStreamService
from app.jobs.scheduler import start_scheduler, stop_scheduler

logger = logging.getLogger(__name__)


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


async def _run_worker() -> None:
    if (
        not settings.scheduler_enabled
        and not settings.polymarket_stream_enabled
        and not settings.polymarket_meta_sync_enabled
        and not settings.polymarket_raw_storage_enabled
        and not settings.polymarket_book_recon_enabled
        and not settings.polymarket_features_enabled
        and not settings.polymarket_user_stream_enabled
        and not settings.polymarket_live_trading_enabled
    ):
        logger.warning("Worker started with all worker features disabled; exiting")
        return

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    scheduler_started = False
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
    user_stream_service: PolymarketUserStreamService | None = None
    user_stream_task: asyncio.Task | None = None
    reconcile_service: PolymarketLiveReconciler | None = None
    reconcile_task: asyncio.Task | None = None

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    if settings.scheduler_enabled:
        started = await _maybe_await(start_scheduler())
        if started is False:
            logger.warning("Scheduler worker did not acquire ownership; continuing without scheduler jobs")
        else:
            scheduler_started = True

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

    if settings.polymarket_user_stream_enabled:
        user_stream_service = PolymarketUserStreamService(async_session)
        user_stream_task = asyncio.create_task(user_stream_service.run(stop_event))

    if settings.polymarket_user_stream_enabled or settings.polymarket_live_trading_enabled:
        reconcile_service = PolymarketLiveReconciler(async_session)
        reconcile_task = asyncio.create_task(reconcile_service.run(stop_event))

    if (
        not scheduler_started
        and stream_task is None
        and meta_sync_task is None
        and raw_storage_task is None
        and book_recon_task is None
        and feature_task is None
        and user_stream_task is None
        and reconcile_task is None
    ):
        logger.warning("No worker responsibilities started; exiting")
        return

    logger.info("Worker is running")
    try:
        await stop_event.wait()
    finally:
        logger.info("Worker stopping")
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
        if scheduler_started:
            await _maybe_await(stop_scheduler())


def main() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
