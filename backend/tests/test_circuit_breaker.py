"""Tests for circuit breaker state transitions."""
import time

import pytest

from app.connectors.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, State


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker("test")
        assert cb.state == State.CLOSED

    def test_stays_closed_under_threshold(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == State.CLOSED

    def test_opens_at_threshold(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.state == State.OPEN

    def test_open_raises_on_check(self):
        cb = CircuitBreaker("test", failure_threshold=1)
        cb.record_failure()
        with pytest.raises(CircuitBreakerOpen):
            cb.check()

    def test_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker("test", failure_threshold=1, reset_timeout=0.1)
        cb.record_failure()
        assert cb.state == State.OPEN

        # Advance past reset_timeout
        cb._last_failure_time = time.monotonic() - 1.0  # 1s ago, well past 0.1s timeout

        assert cb.state == State.HALF_OPEN

    def test_half_open_success_closes(self):
        cb = CircuitBreaker("test", failure_threshold=1, reset_timeout=0.1)
        cb.record_failure()
        cb._last_failure_time = time.monotonic() - 1.0
        assert cb.state == State.HALF_OPEN

        cb.record_success()
        assert cb.state == State.CLOSED

    def test_half_open_failure_reopens(self):
        cb = CircuitBreaker("test", failure_threshold=1, reset_timeout=0.1)
        cb.record_failure()
        cb._last_failure_time = time.monotonic() - 1.0
        assert cb.state == State.HALF_OPEN

        cb.record_failure()
        assert cb.state == State.OPEN

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker("test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        assert cb.state == State.CLOSED  # count reset to 1, below threshold
