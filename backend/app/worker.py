"""Dedicated scheduler worker entrypoint."""
import asyncio
import inspect
import logging
import signal

from app.db import async_session
from app.config import settings
from app.ingestion.polymarket_metadata import PolymarketMetaSyncService
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

    if not scheduler_started and stream_task is None and meta_sync_task is None:
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
        if scheduler_started:
            await _maybe_await(stop_scheduler())


def main() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
