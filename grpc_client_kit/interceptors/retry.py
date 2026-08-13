"""Retry interceptor: re-issues failed calls without ever extending the call deadline."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import (
    Any,
)

import grpc.aio

from ..protocols import RetryMetricsProtocol
from .base import AsyncClientInterceptor, ClientCall
from .circuit_breaker import CircuitBreakerOpenError

logger = logging.getLogger(__name__)

# The default retryable codes are a compromise, not a guarantee. Both codes *usually* mean the
# request never reached the handler — UNAVAILABLE from connection failures and draining servers,
# RESOURCE_EXHAUSTED from quota checks ahead of it — but neither is proof: a server that dies
# mid-handler surfaces as UNAVAILABLE too (the retry then re-executes the same logical request on
# the restarted server), and a handler is free to abort with RESOURCE_EXHAUSTED *after* a write.
# Measured against a live server, both duplications are real. Where duplicates are unaffordable,
# `idempotent_methods` is the tool: with the whitelist set, nothing outside it is ever retried.
# INTERNAL is deliberately absent — it is raised by the handler itself, so retrying it duplicates
# writes with certainty rather than in the corner cases.
DEFAULT_RETRYABLE_CODES: frozenset[grpc.StatusCode] = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }
)


def _status_code(error: BaseException) -> grpc.StatusCode | None:
    """Extract the gRPC status code from an error, or None if it does not carry one."""
    code_func = getattr(error, "code", None)
    if not callable(code_func):
        return None

    code: grpc.StatusCode | None = code_func()
    return code


class AsyncRetryInterceptor(AsyncClientInterceptor):
    """Async retry interceptor for gRPC clients.

    Retries failed calls with exponential backoff and jitter, without ever extending the deadline
    of the call it is retrying.

    Reading the outcome:
        In grpc.aio a continuation resolves to a `Call` the moment the RPC is *created* and never
        raises, so a retry layer that treats that object as the response sees every call succeed.
        This one goes through `ClientCall.invoke_unary`, which awaits the Call and therefore fails
        with the real status — that is the only reason a retry ever fires on a live connection.

    Call budget:
        The relative timeout carried by the call details is the budget for the whole call, so it is
        converted into a deadline **once**, on entry. Every subsequent attempt is issued with the
        budget that is left (gRPC only understands relative timeouts, so the remainder is recomputed
        right before each attempt), and a retry is abandoned when the backoff alone would outlive the
        budget. Without this, N attempts of T seconds would silently stretch a T-second call to N * T.

    Safety:
        Retrying an RPC that the server already executed duplicates its side effects, so what gets
        retried is deliberately narrow — but the default is a compromise, not a guarantee:

        - `DEFAULT_RETRYABLE_CODES` covers the statuses that *usually* mean the request never
          reached the handler. Usually is not always: a connection that dies mid-handler surfaces
          as `UNAVAILABLE`, and an application may abort with `RESOURCE_EXHAUSTED` after a write —
          in both cases a retry duplicates the request. Where a duplicate write is unaffordable,
          set `idempotent_methods`; with the whitelist in place, nothing outside it is retried.
        - Codes like `INTERNAL`, `UNKNOWN`, `ABORTED` or `DEADLINE_EXCEEDED` are worse still — the
          write has very likely been applied — and are never retried unless a caller opts in
          through `retryable_codes`.
        - `idempotent_methods`, when given, is a whitelist that applies to **every** call type: a
          unary-unary method outside of it is not retried even on a retryable code.
        - Streaming responses need that whitelist. Restarting a stream replays items the consumer
          has already seen, so `retry_streaming` alone is not enough to enable it.
        - Calls with a streaming *request* are never retried: the request iterator is consumed by the
          first attempt and cannot be replayed without buffering it whole.
        - Native gRPC retries (`retryPolicy` in a service config) run *below* this layer and
          multiply with it: kit attempts times native attempts reach the server. Configure one
          source of retries, not both — the client warns when it sees both.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_backoff: float = 0.1,
        max_backoff: float = 10.0,
        backoff_multiplier: float = 2.0,
        jitter: float = 0.1,
        retryable_codes: set[grpc.StatusCode] | None = None,
        retry_streaming: bool = False,
        idempotent_methods: set[str] | None = None,
        on_retry: Callable[[str, int, grpc.StatusCode | None, float], Awaitable[None]] | None = None,
        metrics: RetryMetricsProtocol | None = None,
    ) -> None:
        """Initialize the retry interceptor.

        Args:
            max_attempts: Total number of attempts (including the first one).
            initial_backoff: Initial seconds to wait before first retry.
            max_backoff: Maximum seconds to wait between retries.
            backoff_multiplier: Factor by which backoff increases each attempt.
            jitter: Random variation factor (0.0 to 1.0) applied to backoff.
            retryable_codes: gRPC status codes that trigger a retry. Defaults to
                             `DEFAULT_RETRYABLE_CODES`; an empty set disables retries.
            retry_streaming: Whether to enable retries for Unary-Stream calls.
            idempotent_methods: Full method names that may be retried. Required for streaming
                                retries, and a whitelist for unary calls when given.
            on_retry: Optional async callback called on every retry attempt.
            metrics: Optional registry told about every scheduled retry. The request metrics sit
                above this layer and see one entry per *logical* call, so without this a retry
                storm — N wire attempts collapsing into one success — is invisible on a dashboard.

        Raises:
            ValueError: If any of the backoff parameters is out of range.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if initial_backoff < 0:
            raise ValueError("initial_backoff must be non-negative")
        if max_backoff < 0:
            raise ValueError("max_backoff must be non-negative")
        if backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be at least 1")
        if not (0.0 <= jitter <= 1.0):
            raise ValueError("jitter must be between 0.0 and 1.0")

        self._max_attempts = max_attempts
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._backoff_multiplier = backoff_multiplier
        self._jitter = jitter
        self._retry_streaming = retry_streaming
        self._on_retry = on_retry
        self._metrics = metrics
        self._idempotent_methods = idempotent_methods or set()
        # ``is None`` rather than a falsy check: an empty set is a valid "never retry" configuration.
        self._retryable_codes: frozenset[grpc.StatusCode] = (
            DEFAULT_RETRYABLE_CODES if retryable_codes is None else frozenset(retryable_codes)
        )

    async def intercept(self, call: ClientCall) -> Any:
        """Issue the call, retrying it as far as the configuration and the budget allow."""
        if call.response_streaming:
            return await self._start_stream(call)
        if call.request_streaming:
            # The request iterator is consumed by the first attempt and cannot be replayed, so a
            # streaming-request call is never retried. `invoke_unary` hands the Call back promptly
            # for these — awaiting the response here would deadlock the write()-style API.
            return await call.invoke_unary()

        return await self._unary_with_retries(call)

    def _deadline_from(self, client_call_details: Any) -> float | None:
        """Convert the relative timeout of the call into an absolute monotonic deadline.

        Args:
            client_call_details: Details of the call being intercepted.

        Returns:
            The monotonic deadline of the whole call, or None if the call has no budget.
        """
        timeout = getattr(client_call_details, "timeout", None)
        # ClientCallDetails is a structural type: anything that is not a real number (None, or a
        # placeholder from a custom call-details shim) means "no budget to divide".
        if not isinstance(timeout, (int, float)):
            return None

        return time.monotonic() + float(timeout)

    def _budget_left(self, deadline: float | None) -> float | None:
        """Return the seconds left before `deadline`, or None if the call has no budget."""
        if deadline is None:
            return None

        return deadline - time.monotonic()

    def _with_budget_left(self, client_call_details: Any, deadline: float | None) -> Any:
        """Re-express the remaining budget as the relative timeout that gRPC expects."""
        if deadline is None:
            return client_call_details

        return client_call_details._replace(timeout=max(deadline - time.monotonic(), 0.0))

    def _may_retry_unary(self, method: str) -> bool:
        """Check whether a unary response may be retried at all."""
        return not self._idempotent_methods or method in self._idempotent_methods

    async def _prepare_retry(
        self,
        method: str,
        attempt: int,
        code: grpc.StatusCode | None,
        deadline: float | None,
    ) -> bool:
        """Wait out the backoff for the upcoming attempt.

        Args:
            method: Full method name, for logging and the `on_retry` callback.
            attempt: Number of the upcoming attempt (1 is the first retry).
            code: Status code that caused the retry.
            deadline: Monotonic deadline of the whole call, if it has a budget.

        Returns:
            True if the retry may proceed, False if the remaining budget cannot cover it.
        """
        backoff = self._calculate_backoff(attempt)

        budget_left = self._budget_left(deadline)
        if budget_left is not None and budget_left <= backoff:
            logger.debug(
                "Not retrying %s: %.3fs of call budget left, backoff alone needs %.3fs",
                method,
                budget_left,
                backoff,
            )
            return False

        logger.info("Retry attempt %d for %s after error %s. Waiting %.2fs", attempt, method, code, backoff)

        if self._metrics is not None:
            try:
                service = method.strip("/").split("/")[0].split(".")[-1]
                self._metrics.record_retry(service, method, attempt, code.name if code else "UNKNOWN")
            except Exception:
                logger.exception("Failed to record retry metrics for %s", method)

        if self._on_retry:
            try:
                await self._on_retry(method, attempt, code, backoff)
            except Exception:
                logger.exception("on_retry callback failed for %s", method)

        await asyncio.sleep(backoff)
        return True

    def _should_retry(self, call: ClientCall, error: grpc.aio.AioRpcError, attempt: int) -> bool:
        """Decide whether a failed unary attempt may be repeated.

        Args:
            call: The call that failed.
            error: The error the attempt failed with.
            attempt: Number of retries already spent.

        Returns:
            True if another attempt is allowed by the codes, the whitelist and `max_attempts`.
        """
        # A rejection by the circuit breaker never touched the network, so repeating it can only
        # burn the budget: the breaker would refuse the retry for exactly the same reason.
        if isinstance(error, CircuitBreakerOpenError) or _status_code(error) not in self._retryable_codes:
            return False

        if not self._may_retry_unary(call.method):
            logger.debug("Retry for %s skipped (not in idempotent_methods whitelist)", call.method)
            return False

        if attempt + 1 >= self._max_attempts:
            logger.debug("Maximum retry attempts (%d) reached for %s", self._max_attempts, call.method)
            return False

        return True

    async def _unary_with_retries(self, call: ClientCall) -> Any:
        """Issue a unary-response call, repeating it while the failure and the budget allow.

        Args:
            call: The call to issue.

        Returns:
            The response of the first attempt that succeeded.

        Raises:
            grpc.aio.AioRpcError: The error of the last attempt, once no further one is allowed.
        """
        budget = self._deadline_from(call.details)
        original_details = call.details
        attempt = 0

        while True:
            try:
                return await call.invoke_unary()
            except grpc.aio.AioRpcError as error:
                if not self._should_retry(call, error, attempt):
                    raise

                attempt += 1
                if not await self._prepare_retry(call.method, attempt, _status_code(error), budget):
                    raise

            call.details = self._with_budget_left(original_details, budget)

    async def _start_stream(self, call: ClientCall) -> AsyncIterator[Any]:
        """Issue a streaming-response call and, where retries are allowed, make it restartable.

        The call is issued here rather than inside the returned generator: grpc.aio binds the `Call`
        an interceptor created to the iterator it hands back, and a lazily started stream leaves
        that binding empty.

        Args:
            call: The call to issue.

        Returns:
            The response iterator, restarting on retryable failures where that is permitted.
        """
        budget = self._deadline_from(call.details)
        original_details = call.details
        stream = await call.invoke_stream()

        if call.request_streaming or not self._retry_streaming:
            return stream
        if call.method not in self._idempotent_methods:
            logger.debug("Streaming retry for %s skipped (not in idempotent_methods whitelist)", call.method)
            return stream

        # A bare generator would leave the caller's status surface — code(), details(), cancel() —
        # wired to the first, possibly failed attempt: grpc binds it once, to whatever object this
        # interceptor returns. The wrapper keeps it wired to the attempt currently on the wire.
        restartable = _RestartingStreamCall(call)
        restartable.attach(self._restarting_stream(call, stream, original_details, budget, restartable))
        return restartable

    async def _restarting_stream(
        self,
        call: ClientCall,
        stream: AsyncIterator[Any],
        original_details: Any,
        budget: float | None,
        surface: _RestartingStreamCall | None = None,
    ) -> AsyncIterator[Any]:
        """Yield a response stream, restarting the whole stream on retryable failures.

        Args:
            call: The call being iterated, used to issue the replacement streams.
            stream: The response stream of the attempt already in flight.
            original_details: Call details of the first attempt, the base for every trimmed retry.
            budget: Monotonic deadline of the whole call, if it has one.
            surface: The caller-visible call wrapper, consulted so an explicit ``cancel()`` stops
                the restarting instead of being retried around.

        Yields:
            Items of the response stream. Items yielded before a restart are seen again.
        """
        attempt = 0
        current = stream

        while True:
            try:
                async for item in current:
                    yield item
            except grpc.aio.AioRpcError as error:
                code = _status_code(error)
                cancelled = surface is not None and surface.cancelled()
                if cancelled or code not in self._retryable_codes or attempt + 1 >= self._max_attempts:
                    raise

                attempt += 1
                if not await self._prepare_retry(call.method, attempt, code, budget):
                    raise

                logger.warning(
                    "Restarting stream %s from the beginning (attempt %d after %s): already yielded items repeat",
                    call.method,
                    attempt,
                    code,
                )
            else:
                return

            call.details = self._with_budget_left(original_details, budget)
            current = await call.invoke_stream()

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter."""
        backoff = self._initial_backoff * (self._backoff_multiplier ** (attempt - 1))

        # Apply jitter: (1 ± jitter) * backoff
        if self._jitter > 0:
            factor = 1.0 + random.uniform(-self._jitter, self._jitter)  # noqa: S311
            backoff *= factor

        return max(0.0, min(backoff, self._max_backoff))


class _RestartingStreamCall(grpc.aio.UnaryStreamCall):  # type: ignore[type-arg]
    """The caller-visible call of a stream the retry layer may restart.

    grpc.aio wires ``code()``, ``details()``, ``cancel()`` and the metadata of the call it hands
    the application to whatever object the interceptor returned — once. Status therefore has to be
    delegated to the attempt *currently* on the wire, which `ClientCall.underlying_call` tracks
    across restarts; a fully drained stream then reports the status of the attempt that drained,
    not of the first attempt that failed.
    """

    def __init__(self, call: ClientCall) -> None:
        """Bind the surface to the call whose attempts it reports on."""
        self._call = call
        self._iterator: AsyncIterator[Any] | None = None
        self._cancelled = False
        self._finished = False
        self._done_callbacks: list[Callable[[Any], None]] = []

    def attach(self, iterator: AsyncIterator[Any]) -> None:
        """Hand over the restarting iterator (built after the wrapper, which it consults)."""
        self._iterator = iterator

    @property
    def _current(self) -> Any:
        """The grpc Call of the attempt currently on the wire."""
        return self._call.underlying_call

    def __aiter__(self) -> AsyncIterator[Any]:
        assert self._iterator is not None  # attach() runs before grpc ever sees the object
        return self._drained(self._iterator)

    async def _drained(self, iterator: AsyncIterator[Any]) -> AsyncIterator[Any]:
        """Iterate the restarting stream and mark the surface finished at its true end."""
        try:
            async for item in iterator:
                yield item
        finally:
            self._finished = True
            for callback in self._done_callbacks:
                try:
                    callback(self)
                except Exception:
                    logger.exception("Done callback failed for %s", self._call.method)

    async def read(self) -> Any:
        """Read one response, EOF once the stream — restarts included — is over."""
        if self._iterator is not None and not self._finished:
            try:
                return await anext(self.__aiter__())
            except StopAsyncIteration:
                return grpc.aio.EOF
        return grpc.aio.EOF

    def cancel(self) -> bool:
        """Cancel the attempt on the wire and stop any further restarting."""
        self._cancelled = True
        current = self._current
        return bool(current.cancel()) if current is not None else False

    def cancelled(self) -> bool:
        """Whether the caller cancelled this call."""
        if self._cancelled:
            return True
        # Duck-typed: an interceptor further in may hand back an object without the Call surface.
        cancelled = getattr(self._current, "cancelled", None)
        return bool(cancelled()) if callable(cancelled) else False

    def done(self) -> bool:
        """Whether the call — restarts included — has delivered its final outcome."""
        return self._finished

    def add_done_callback(self, callback: Callable[[Any], None]) -> None:
        """Register a callback for the true end of the call, not of one attempt."""
        if self._finished:
            callback(self)
            return
        self._done_callbacks.append(callback)

    def time_remaining(self) -> float | None:
        """Time remaining of the attempt currently on the wire."""
        current = self._current
        return current.time_remaining() if current is not None else None

    async def initial_metadata(self) -> Any:
        """Initial metadata of the attempt currently on the wire."""
        return await self._current.initial_metadata()

    async def trailing_metadata(self) -> Any:
        """Trailing metadata of the attempt currently on the wire."""
        return await self._current.trailing_metadata()

    async def code(self) -> grpc.StatusCode:
        """Status code of the attempt currently on the wire."""
        code: grpc.StatusCode = await self._current.code()
        return code

    async def details(self) -> str:
        """Status details of the attempt currently on the wire."""
        details: str = await self._current.details()
        return details

    async def wait_for_connection(self) -> None:
        """Wait until the attempt currently on the wire is connected."""
        await self._current.wait_for_connection()


__all__ = [
    "DEFAULT_RETRYABLE_CODES",
    "AsyncRetryInterceptor",
]
