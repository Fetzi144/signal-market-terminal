"""Dedicated scheduler worker entrypoint."""
import asyncio
import logging
import signal

from app.config import settings
from app.jobs.scheduler import start_scheduler, stop_scheduler

logger = logging.getLogger(__name__)


async def _run_worker() -> None:
    if not settings.scheduler_enabled:
        logger.warning("Scheduler worker started with SCHEDULER_ENABLED=false; exiting")
        return

    start_scheduler()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    logger.info("Scheduler worker is running")
    await stop_event.wait()
    logger.info("Scheduler worker stopping")
    stop_scheduler()


def main() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
