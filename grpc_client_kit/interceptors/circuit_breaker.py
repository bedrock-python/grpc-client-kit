"""Circuit breaker interceptor: stops dialing a method that keeps failing.

Reading the outcome is the whole job of this layer, and in `grpc.aio` that is not free. A
continuation resolves to a `Call` the moment the RPC is *created* — identically for a call that will
succeed and one that will fail — and it never raises; the status only surfaces later, when the
caller awaits that Call or iterates it. A breaker that took the continuation's result for the
response therefore recorded a **success for every failed call**, and the circuit could not open no
matter how badly the server behaved.

The fix is structural rather than local: this interceptor is an `AsyncAroundClientInterceptor`, so
its ``yield`` spans the awaited call and the whole of a response stream, and a failure arrives as
the `grpc.aio.AioRpcError` it is — mid-stream failures included. Being a logical interceptor also
means the channel registers it for all four RPC kinds instead of unary-unary alone, which is what
used to leave every streaming call unguarded.
"""

from __future__ import annotations

import enum
import logging
import time
from collections import OrderedDict
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TypedDict

import grpc.aio

from ..errors import GrpcClientKitError
from ..protocols import CircuitBreakerMetricsProtocol, GrpcClientMetricsProtocol
from .base import AsyncAroundClientInterceptor, ClientCall

logger = logging.getLogger(__name__)


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"


class CircuitBreakerStatus(TypedDict):
    """Read-only snapshot of one method's circuit, as returned by ``get_states()``."""

    state: str
    failure_count: int
    last_failure_time: float
    half_open_calls: int


@dataclass(slots=True)
class MethodCircuitState:
    """Mutable circuit state of a single gRPC method.

    No lock, on purpose: every section that mutates this state is await-free, which makes it
    atomic with respect to the event loop — and an ``asyncio.Lock`` offers no cross-thread
    protection anyway, so all it bought was two acquire/release cycles per call. Adding an
    ``await`` inside any of those sections requires bringing a lock back.

    Attributes:
        state: Current circuit state.
        failure_count: Consecutive failures recorded while CLOSED.
        last_failure_time: Unix timestamp of the last recorded failure.
        half_open_calls: Trial calls currently in flight while HALF-OPEN.
    """

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0.0
    half_open_calls: int = 0


class CircuitBreakerOpenError(grpc.aio.AioRpcError, GrpcClientKitError):
    """Raised when the circuit is open."""

    def __init__(self, method: str) -> None:
        # Explicit rather than super(): with GrpcClientKitError also in the bases, super() would
        # resolve the keyword arguments against Exception, which takes none of them.
        grpc.aio.AioRpcError.__init__(
            self,
            code=grpc.StatusCode.UNAVAILABLE,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details=f"Circuit breaker for {method} is open",
        )


def _error_code(error: BaseException) -> grpc.StatusCode | None:
    """Extract the gRPC status code of an error, if it carries one.

    Args:
        error: The exception raised by the call.

    Returns:
        The status code, or None when the error exposes no usable code.
    """
    code_func = getattr(error, "code", None)
    if not callable(code_func):
        return None

    code = code_func()
    return code if isinstance(code, grpc.StatusCode) else None


class AsyncCircuitBreakerInterceptor(AsyncAroundClientInterceptor):
    """Async circuit breaker interceptor for gRPC clients.

    This interceptor implements the Circuit Breaker pattern on a per-method basis.
    It prevents cascading failures by "opening" the circuit for methods that fail
    repeatedly, immediately rejecting subsequent calls until a recovery period
    has passed.

    States:
    - CLOSED: Normal operation. Successes and failures are tracked.
    - OPEN: Service is failing. Calls are immediately rejected with CircuitBreakerOpenError.
    - HALF-OPEN: Limited trial calls are allowed to see if the service has recovered.

    Scope:
        An interceptor is bound to one channel, hence to one target. Give each target its own
        instance (``GrpcClientFactory`` does) and state is effectively per (target, method), so one
        failing backend cannot trip the breaker for its healthy peers.

    What counts as a failure:
        Only the statuses in `_should_record_failure` — the ones that describe the server rather
        than the request. A malformed request or a rejected permission is the caller's problem and
        must not be able to trip a circuit. A cancellation is nobody's failure: `asyncio.CancelledError`
        derives from `BaseException`, so it passes this interceptor's handlers untouched and only
        the trial slot it was holding is given back.

    Features:
    - Thread-safe and async-safe implementation.
    - Per-method state isolation.
    - LRU cache for method states to prevent memory leaks in long-running processes.
    - Observability via optional metrics registry.
    """

    def __init__(
        self,
        fail_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 1,
        max_methods: int = 1000,
        metrics: GrpcClientMetricsProtocol | None = None,
    ) -> None:
        """Initialize the circuit breaker interceptor.

        Args:
            fail_threshold: Number of failures before transitioning from CLOSED to OPEN.
            recovery_timeout: Seconds to wait in OPEN state before transitioning to HALF-OPEN.
            half_open_max_calls: Maximum number of concurrent trial calls allowed in HALF-OPEN.
            max_methods: Maximum number of distinct methods to track in LRU cache.
            metrics: Optional metrics registry for recording state transitions.

        Raises:
            ValueError: If any threshold or timeout is out of range.
        """
        if fail_threshold <= 0:
            raise ValueError("fail_threshold must be positive")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be non-negative")
        if half_open_max_calls <= 0:
            raise ValueError("half_open_max_calls must be positive")
        if max_methods <= 0:
            raise ValueError("max_methods must be positive")

        self._fail_threshold = fail_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._max_methods = max_methods
        self._metrics = metrics
        # State transitions and rejections are an optional protocol extension of the registry,
        # checked once here rather than with hasattr magic on every transition.
        self._state_metrics: CircuitBreakerMetricsProtocol | None = (
            metrics if isinstance(metrics, CircuitBreakerMetricsProtocol) else None
        )

        # Map: method_name -> state data (using OrderedDict for LRU). No lock: every mutation of
        # the map happens in an await-free section, atomic with respect to the event loop.
        self._states: OrderedDict[str, MethodCircuitState] = OrderedDict()

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Refuse the call while the circuit is open, otherwise run it and record how it ended.

        Args:
            call: The call being guarded.

        Yields:
            Once, with the RPC in flight. For a streaming response the yield spans the whole stream,
            so a server that fails halfway through it still counts as a failure.

        Raises:
            CircuitBreakerOpenError: If the circuit for this method is open, or every half-open
                trial slot is taken. Raised before the ``yield``, so the RPC is never created and
                the rejected call never touches the network.
        """
        state = await self._get_method_state(call.method)
        trial = await self._admit(call.method, state)

        try:
            yield
        except grpc.aio.AioRpcError as error:
            if self._should_record_failure(_error_code(error)):
                await self._record_failure(call.method, state)
            raise
        # `asyncio.CancelledError` is a BaseException and so escapes both handlers on purpose: a
        # client that walked away has told us nothing about the health of the server.
        except Exception:
            await self._record_failure(call.method, state)
            raise
        else:
            await self._record_success(call.method, state)
        finally:
            if trial:
                await self._release_trial(state)

    async def get_states(self) -> dict[str, CircuitBreakerStatus]:
        """Get the current state of all monitored methods.

        Returns:
            A dictionary mapping method names to their status (state, failure_count, etc.).
        """
        return {
            method: CircuitBreakerStatus(
                state=state.state.value,
                failure_count=state.failure_count,
                last_failure_time=state.last_failure_time,
                half_open_calls=state.half_open_calls,
            )
            for method, state in self._states.items()
        }

    async def _get_method_state(self, method: str) -> MethodCircuitState:
        """Get or create state for a specific method, implementing LRU eviction.

        Args:
            method: Full gRPC method path.

        Returns:
            The state object for the method.
        """
        if method in self._states:
            # Move to end (mark as most recently used)
            self._states.move_to_end(method)
            return self._states[method]

        if len(self._states) >= self._max_methods:
            self._evict_one_state()

        state = MethodCircuitState()
        self._states[method] = state

        # Initial state metric
        self._update_state_metrics(method, CircuitState.CLOSED)

        return state

    def _evict_one_state(self) -> None:
        """Drop one state to stay under the limit, never a state that is still protecting.

        Evicting an OPEN circuit silently re-closes it: the next call to that method goes out to a
        backend the breaker had declared down, and it takes another `fail_threshold` real network
        failures to open it again — under method churn the breaker degrades into "let threshold
        calls through per LRU cycle". So the victim is the least recently used state that carries
        no signal: CLOSED with a clean failure count first, any CLOSED second, and when every state
        is protecting something the map grows past the limit instead — with a warning, since that
        is a memory bound consciously traded for correctness.
        """
        for candidate, state in self._states.items():
            if state.state is CircuitState.CLOSED and state.failure_count == 0:
                del self._states[candidate]
                return

        for candidate, state in self._states.items():
            if state.state is CircuitState.CLOSED:
                del self._states[candidate]
                return

        logger.warning(
            "Circuit breaker holds %d OPEN/HALF_OPEN states, exceeding max_methods=%d; "
            "growing past the limit instead of evicting a protecting circuit",
            len(self._states),
            self._max_methods,
        )

    def _update_state_metrics(self, method: str, state: CircuitState) -> None:
        """Report a state transition to a registry that opted into breaker metrics."""
        if self._state_metrics is None:
            return

        try:
            self._state_metrics.record_circuit_state(method, state.value)
        except Exception:
            logger.exception("Failed to record circuit breaker state for %s", method)

    def _record_rejection(self, method: str) -> None:
        """Report one locally refused call to a registry that opted into breaker metrics."""
        if self._state_metrics is None:
            return

        try:
            self._state_metrics.record_circuit_rejection(method)
        except Exception:
            logger.exception("Failed to record circuit breaker rejection for %s", method)

    async def _admit(self, method: str, state: MethodCircuitState) -> bool:
        """Decide whether the call may go out, claiming a half-open trial slot when it does.

        Args:
            method: Full gRPC method path, for the error and the logs.
            state: Circuit state of that method.

        Returns:
            True if the call was admitted as a half-open trial and owes the slot back.

        Raises:
            CircuitBreakerOpenError: If the circuit is open, or its trial slots are all taken.
        """
        # Await-free on purpose: atomic with respect to the event loop, no lock needed or useful.
        if state.state == CircuitState.OPEN:
            if time.time() - state.last_failure_time > self._recovery_timeout:
                logger.info("Circuit breaker for %s entering HALF-OPEN state", method)
                state.state = CircuitState.HALF_OPEN
                state.half_open_calls = 0
                self._update_state_metrics(method, CircuitState.HALF_OPEN)
            else:
                self._record_rejection(method)
                raise CircuitBreakerOpenError(method)

        if state.state != CircuitState.HALF_OPEN:
            return False

        if state.half_open_calls >= self._half_open_max_calls:
            logger.debug(
                "Circuit breaker for %s: half-open calls limit reached (%d >= %d)",
                method,
                state.half_open_calls,
                self._half_open_max_calls,
            )
            self._record_rejection(method)
            raise CircuitBreakerOpenError(method)

        state.half_open_calls += 1
        return True

    async def _release_trial(self, state: MethodCircuitState) -> None:
        """Give back the half-open slot a trial call was holding.

        A recorded outcome has already reset the counter on its way to CLOSED or OPEN, so the
        decrement only applies while the circuit is still half-open — which is the case when the
        trial ended in something that was not recorded at all, a cancellation above all.
        """
        if state.state == CircuitState.HALF_OPEN:
            state.half_open_calls = max(0, state.half_open_calls - 1)

    async def _record_success(self, method: str, state: MethodCircuitState) -> None:
        """Record a successful call and potentially transition to CLOSED."""
        if state.state == CircuitState.HALF_OPEN:
            logger.info("Circuit breaker for %s CLOSED after successful half-open test", method)
            state.state = CircuitState.CLOSED
            state.failure_count = 0
            state.half_open_calls = 0
            self._update_state_metrics(method, CircuitState.CLOSED)
        elif state.state == CircuitState.CLOSED:
            # Reset failure count to prevent gradual accumulation of old failures
            state.failure_count = 0

    async def _record_failure(self, method: str, state: MethodCircuitState) -> None:
        """Record a failed call and potentially transition to OPEN."""
        state.failure_count += 1
        state.last_failure_time = time.time()

        if state.state == CircuitState.HALF_OPEN:
            logger.warning("Circuit breaker for %s OPEN after failure in half-open state", method)
            state.state = CircuitState.OPEN
            state.half_open_calls = 0
            self._update_state_metrics(method, CircuitState.OPEN)
        elif state.state == CircuitState.CLOSED and state.failure_count >= self._fail_threshold:
            logger.warning("Circuit breaker for %s OPEN after %d failures", method, state.failure_count)
            state.state = CircuitState.OPEN
            self._update_state_metrics(method, CircuitState.OPEN)

    def _should_record_failure(self, code: grpc.StatusCode | None) -> bool:
        """Determine if a gRPC error code should count as a failure.

        Args:
            code: The status code of the failed call, or None when it carries none.

        Returns:
            True if the failure says something about the server's health.
        """
        return code in (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.ABORTED,
            grpc.StatusCode.UNKNOWN,
            grpc.StatusCode.DATA_LOSS,
        )


# `MethodCircuitState` is deliberately absent: it is the breaker's mutable bookkeeping, never a
# type a caller receives — `get_states()` hands out `CircuitBreakerStatus` snapshots — and
# declaring it public would freeze this implementation into the compatibility contract.
__all__ = [
    "AsyncCircuitBreakerInterceptor",
    "CircuitBreakerOpenError",
    "CircuitBreakerStatus",
    "CircuitState",
]
