"""Tests for SSE SignalBroadcaster."""
import asyncio

import pytest

from app.api.sse import SignalBroadcaster


@pytest.mark.asyncio
async def test_subscribe_receives_published_events():
    """Subscriber receives events published after subscription."""
    b = SignalBroadcaster()
    sub_id, queue = b.subscribe()

    await b.publish("new_signal", {"signal_type": "price_move"})

    msg = queue.get_nowait()
    assert msg["event"] == "new_signal"
    assert msg["data"]["signal_type"] == "price_move"
    assert "timestamp" in msg

    b.unsubscribe(sub_id)


@pytest.mark.asyncio
async def test_unsubscribe_no_longer_receives():
    """After unsubscribe, no events are received."""
    b = SignalBroadcaster()
    sub_id, queue = b.subscribe()
    b.unsubscribe(sub_id)

    await b.publish("new_signal", {"test": True})

    assert queue.empty()


@pytest.mark.asyncio
async def test_multiple_subscribers_all_receive():
    """Multiple subscribers all receive the same event."""
    b = SignalBroadcaster()
    id1, q1 = b.subscribe()
    id2, q2 = b.subscribe()
    id3, q3 = b.subscribe()

    await b.publish("new_alert", {"rank": 0.9})

    for q in (q1, q2, q3):
        msg = q.get_nowait()
        assert msg["event"] == "new_alert"
        assert msg["data"]["rank"] == 0.9

    b.unsubscribe(id1)
    b.unsubscribe(id2)
    b.unsubscribe(id3)


@pytest.mark.asyncio
async def test_queue_full_subscriber_dropped():
    """When queue is full (maxsize=100), subscriber is removed (dropped)."""
    b = SignalBroadcaster()
    sub_id, queue = b.subscribe()
    assert b.subscriber_count == 1

    # Fill the queue to max
    for i in range(100):
        await b.publish("event", {"i": i})

    # Queue should be full now — next publish drops the subscriber
    await b.publish("overflow", {"dropped": True})

    assert b.subscriber_count == 0


@pytest.mark.asyncio
async def test_subscriber_count_tracks_correctly():
    """subscriber_count reflects current number of subscribers."""
    b = SignalBroadcaster()
    assert b.subscriber_count == 0

    id1, _ = b.subscribe()
    assert b.subscriber_count == 1

    id2, _ = b.subscribe()
    assert b.subscriber_count == 2

    b.unsubscribe(id1)
    assert b.subscriber_count == 1

    b.unsubscribe(id2)
    assert b.subscriber_count == 0


@pytest.mark.asyncio
async def test_unsubscribe_nonexistent_id_no_error():
    """Unsubscribing a non-existent ID does not raise."""
    b = SignalBroadcaster()
    b.unsubscribe(9999)  # should not raise


@pytest.mark.asyncio
async def test_publish_with_no_subscribers():
    """Publishing with zero subscribers does not raise."""
    b = SignalBroadcaster()
    await b.publish("event", {"data": "test"})
    # No error = pass
