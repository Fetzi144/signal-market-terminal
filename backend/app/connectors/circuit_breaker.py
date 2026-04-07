"""Circuit breaker pattern for connector resilience.

States:
  CLOSED   -> request flows normally; failures increment counter
  OPEN     -> requests immediately fail; after reset_timeout, transitions to HALF_OPEN
  HALF_OPEN-> one request allowed; success -> CLOSED, failure -> OPEN
"""
import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpen(Exception):
    """Raised when the circuit is open and the request is rejected."""
    pass


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, reset_timeout: float = 300.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._state = State.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0

    @property
    def state(self) -> State:
        if self._state == State.OPEN:
            if time.monotonic() - self._last_failure_time >= self.reset_timeout:
                self._state = State.HALF_OPEN
                logger.info("Circuit breaker '%s' transitioning to HALF_OPEN", self.name)
        return self._state

    def record_success(self):
        if self._state in (State.HALF_OPEN, State.OPEN):
            logger.info("Circuit breaker '%s' closing after success", self.name)
        self._failure_count = 0
        self._state = State.CLOSED

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._failure_count >= self.failure_threshold:
            self._state = State.OPEN
            logger.warning(
                "Circuit breaker '%s' OPEN after %d failures (reset in %.0fs)",
                self.name, self._failure_count, self.reset_timeout,
            )

    def check(self):
        """Raise CircuitBreakerOpen if the circuit is open."""
        state = self.state  # triggers timeout check
        if state == State.OPEN:
            raise CircuitBreakerOpen(f"Circuit breaker '{self.name}' is open")
