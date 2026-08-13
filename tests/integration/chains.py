"""The interceptor chains the tests put under load, and the probe that watches from below.

Two properties of `grpc.aio` decide what a chain can be tested for at all, and the kit answers both.
A continuation resolves to a `Call` the moment the RPC is *created*, identically for a call that will
succeed and one that will fail, and it never raises; a chain that treats that object as the response
sees every call succeed. And a channel files an interceptor into one of its four lists by class, so a
single object claiming all four kinds is registered for unary-unary only and streaming calls run past
it. The kit answers the first with `ClientCall.invoke_unary` / `invoke_stream`, which await the call,
and the second with four adapters per layer — which is why the tests below can assert on retries,
breaker verdicts and metrics for streaming calls at all.

`InnermostProbe` is not a mock. It is a plain `grpc.aio` interceptor in the documented
``extra_inner_interceptors`` slot, below the resilience layers, and everything it reports comes off a
real wire. It deliberately hands its `Call` back unawaited — the grpc-native shape — so the tests
also show that the layers above no longer depend on anyone below them awaiting anything.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import grpc
import grpc.aio

from grpc_client_kit import (
    AsyncCircuitBreakerInterceptor,
    CircuitBreakerConfig,
    CircuitBreakerStatus,
    DeadlineBudgetConfig,
    ObservabilityConfig,
    RetryConfig,
    TimeoutConfig,
    WaitForReadyConfig,
    build_interceptors,
)
from grpc_client_kit.interceptors import AsyncClientContextInterceptor, logical_interceptor

from .echo_bench import ECHO
from .recording import RecordingMetrics

CLIENT_SERVICE_NAME = "echo-client"


def canonical_chain(
    metrics: RecordingMetrics | None = None,
    *,
    service_name: str = CLIENT_SERVICE_NAME,
    timeout: float | None = 5.0,
    max_attempts: int = 3,
    initial_backoff: float = 0.01,
    retryable_codes: set[grpc.StatusCode] | None = None,
    fail_threshold: int = 3,
    recovery_timeout: float = 0.05,
    metadata: dict[str, str | bytes] | None = None,
) -> list[grpc.aio.ClientInterceptor]:
    """Build the production chain: context, logging, tracing, metrics, timeout, retry, breaker.

    Every call returns a fresh chain of fresh interceptor instances, so two chains built here are
    two distinct channel-pool identities.

    Args:
        metrics: Collector the metrics interceptor records into; without one it is left out.
        service_name: Name used for log/span attributes and for the logger name.
        timeout: Budget of a whole call in seconds; None disables the deadline.
        max_attempts: Total attempts the retry interceptor may spend.
        initial_backoff: Seconds before the first retry (jitter is off, so this is exact).
        retryable_codes: Status codes that trigger a retry; None keeps the library default.
        fail_threshold: Failures before the circuit breaker opens.
        recovery_timeout: Seconds the open circuit waits before a trial call.
        metadata: Metadata injected by the outermost interceptor, or None to inject none.

    Returns:
        The interceptor chain, outermost first.
    """
    extra_outer: list[grpc.aio.ClientInterceptor] | None = None
    if metadata is not None:
        snapshot = dict(metadata)
        extra_outer = [AsyncClientContextInterceptor(lambda: dict(snapshot))]

    return build_interceptors(
        timeout=TimeoutConfig(default=timeout),
        retry=RetryConfig(
            max_attempts=max_attempts,
            initial_backoff=initial_backoff,
            jitter=0.0,
            retryable_codes=retryable_codes,
        ),
        circuit_breaker=CircuitBreakerConfig(fail_threshold=fail_threshold, recovery_timeout=recovery_timeout),
        observability=ObservabilityConfig(
            tracing=True,
            metrics=metrics is not None,
            logging=True,
            service_name=service_name,
            metrics_registry=metrics,
        ),
        extra_interceptors=extra_outer,
    )


class InnermostProbe(grpc.aio.UnaryUnaryClientInterceptor):
    """Innermost interceptor: records the budget every attempt was issued with.

    Written the plain `grpc.aio` way and passed through the ``extra_inner_interceptors`` slot
    untouched, so it is also the proof that a chain built by the kit still accepts an interceptor
    that was not written against `ClientCall`. It returns its `Call` unawaited, the shape gRPC hands
    out, which the resilience layers above must not depend on: they await their own calls.

    Attributes:
        budgets: The relative timeout of each attempt, in the order the attempts were issued. Read
            below the retry layer, so it is the slice of the call budget that attempt was given.
    """

    def __init__(self) -> None:
        """Start with no attempts recorded."""
        self.budgets: list[float | None] = []

    async def intercept_unary_unary(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        self.budgets.append(getattr(client_call_details, "timeout", None))
        return await continuation(client_call_details, request)


@dataclass(frozen=True, slots=True)
class ResilienceChain:
    """A built interceptor chain together with the two objects the tests need to question it."""

    interceptors: list[grpc.aio.ClientInterceptor]
    breaker: AsyncCircuitBreakerInterceptor
    probe: InnermostProbe

    @property
    def budgets(self) -> list[float]:
        """The budget each attempt was issued with, dropping the attempts made without one."""
        return [budget for budget in self.probe.budgets if budget is not None]

    @property
    def attempts(self) -> int:
        """How many attempts the retry layer actually issued."""
        return len(self.probe.budgets)

    async def circuit(self, method: str = ECHO) -> CircuitBreakerStatus | None:
        """The breaker's own view of one method, or None if it has never seen that method."""
        return (await self.breaker.get_states()).get(method)


def resilience_chain(
    *,
    timeout: float | None = 5.0,
    max_attempts: int = 3,
    initial_backoff: float = 0.01,
    retryable_codes: set[grpc.StatusCode] | None = None,
    retry_streaming: bool = False,
    idempotent_methods: set[str] | None = None,
    fail_threshold: int = 10,
    recovery_timeout: float = 60.0,
    half_open_max_calls: int = 1,
    deadline_budget: DeadlineBudgetConfig | None = None,
    wait_for_ready: WaitForReadyConfig | None = None,
) -> ResilienceChain:
    """Build the production chain — logging, tracing, timeout, retry, breaker — plus the probe.

    Defaults keep whichever layer a test is not about out of its way: the breaker's threshold is
    high enough not to trip during the retry tests, and its recovery window long enough that an open
    circuit stays open unless the test asks for recovery.

    Args:
        timeout: Budget of a whole call in seconds; None leaves the call without a deadline.
        max_attempts: Total attempts the retry layer may spend, the first one included.
        initial_backoff: Seconds before the first retry. Jitter is off, so this is exact.
        retryable_codes: Codes that trigger a retry; None keeps the library default.
        retry_streaming: Whether a streaming response may be restarted.
        idempotent_methods: Methods a retry is allowed on; required for streaming retries.
        fail_threshold: Failures before the circuit opens.
        recovery_timeout: Seconds an open circuit waits before admitting a trial call.
        half_open_max_calls: Trial calls a half-open circuit admits at once.
        deadline_budget: Propagation config; None leaves the layer out, as an unconfigured chain has
            it, so a test only pays for the budget when it is what the test is about.
        wait_for_ready: Wait-for-ready config; None leaves the layer out, which is the fail-fast
            behaviour every other test in this suite is written against.

    Returns:
        The chain and the handles onto its breaker and probe.
    """
    probe = InnermostProbe()
    interceptors = build_interceptors(
        timeout=TimeoutConfig(default=timeout),
        retry=RetryConfig(
            max_attempts=max_attempts,
            initial_backoff=initial_backoff,
            jitter=0.0,
            retryable_codes=retryable_codes,
            retry_streaming=retry_streaming,
            idempotent_methods=idempotent_methods,
        ),
        circuit_breaker=CircuitBreakerConfig(
            fail_threshold=fail_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls,
        ),
        observability=ObservabilityConfig(tracing=True, logging=True, service_name=CLIENT_SERVICE_NAME),
        extra_inner_interceptors=[probe],
        deadline_budget=deadline_budget,
        wait_for_ready=wait_for_ready,
    )
    # Each layer reaches the channel as four adapters, one per RPC kind, so the breaker is found
    # through the interceptor an entry stands for rather than through the entry itself.
    breaker = next(
        item for item in map(logical_interceptor, interceptors) if isinstance(item, AsyncCircuitBreakerInterceptor)
    )
    return ResilienceChain(interceptors=interceptors, breaker=breaker, probe=probe)
