"""Server-Sent Events endpoint for real-time signal and alert streaming."""
import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import Counter

from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/events", tags=["events"])

sse_queue_drops_total = Counter(
    "sse_queue_drops_total",
    "Total number of SSE subscriber drops due to queue overflow",
)


class SignalBroadcaster:
    """Pub/sub broadcaster for SSE clients. Each subscriber gets its own asyncio.Queue."""

    def __init__(self):
        self._subscribers: dict[int, asyncio.Queue] = {}
        self._counter = 0

    def subscribe(self) -> tuple[int, asyncio.Queue]:
        self._counter += 1
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._subscribers[self._counter] = q
        logger.info("SSE client subscribed (id=%d, total=%d)", self._counter, len(self._subscribers))
        return self._counter, q

    def unsubscribe(self, sub_id: int):
        self._subscribers.pop(sub_id, None)
        logger.info("SSE client unsubscribed (id=%d, total=%d)", sub_id, len(self._subscribers))

    async def publish(self, event_type: str, data: dict):
        """Broadcast an event to all subscribers. Force-close subscribers with full queues."""
        message = {"event": event_type, "data": data, "timestamp": datetime.now(timezone.utc).isoformat()}
        dead = []
        for sub_id, q in self._subscribers.items():
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                dead.append(sub_id)
                sse_queue_drops_total.inc()
                logger.warning("SSE queue full for subscriber %d, forcing reconnect", sub_id)
                # Put a close sentinel so the event generator knows to stop
                try:
                    # Drain one item to make room for the sentinel
                    q.get_nowait()
                    q.put_nowait({"event": "__close__", "data": {}, "timestamp": datetime.now(timezone.utc).isoformat()})
                except (asyncio.QueueFull, asyncio.QueueEmpty):
                    pass
        for sub_id in dead:
            self._subscribers.pop(sub_id, None)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


# Singleton broadcaster
broadcaster = SignalBroadcaster()


@router.get("/signals")
async def signal_events(request: Request):
    """SSE endpoint streaming new signals and alerts in real time."""
    # Enforce connection limit
    if broadcaster.subscriber_count >= settings.sse_max_connections:
        return JSONResponse(
            status_code=503,
            content={"detail": "SSE connection limit reached. Try again later."},
        )

    sub_id, queue = broadcaster.subscribe()

    async def event_generator():
        try:
            # Send initial connection event
            yield f"event: connected\ndata: {json.dumps({'subscriber_id': sub_id})}\n\n"

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    # Check for close sentinel (queue overflow → force reconnect)
                    if message.get("event") == "__close__":
                        yield "event: reconnect\ndata: {}\n\n"
                        break
                    event_type = message.get("event", "message")
                    payload = json.dumps(message.get("data", {}))
                    yield f"event: {event_type}\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    # Send keepalive comment
                    yield ": keepalive\n\n"
        finally:
            broadcaster.unsubscribe(sub_id)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
