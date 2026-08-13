"""Client interceptors and the builder that assembles them into a chain.

gRPC applies a client interceptor list from the outside in: the first entry wraps the second, and
the last one sits closest to the wire. `InterceptorChainBuilder` fixes that order, because the
position of a layer changes what it can observe and what it repeats:

1. **Extra outer** — context/metadata injection (`AsyncClientContextInterceptor`). It has to run
   before the observability layers: the logging interceptor correlates records by reading
   ``request-id`` off the call metadata, so metadata injected below it would be invisible to logs
   and traces. Running outermost also means one logical call keeps one identity across retries.
2. **Logging** — one record per logical call, with the correlation metadata already attached and
   the outcome the caller actually observes (retries are reported separately by the retry layer).
3. **Tracing** — one CLIENT span per logical call, covering the retries nested below it.
4. **Metrics** — latency measured as the caller experiences it, i.e. including retry backoff.
5. **Timeout** — installs the budget for the whole call. Outermost of the resilience layers so the
   budget covers every attempt rather than being handed out fresh to each one.
6. **Deadline budget** — trims that budget to what the caller's *request* has left. It has to see
   the deadline the layers above installed, and it has to run before anything divides that deadline
   further, which puts it between the timeout layer and the retry layer.
7. **Wait-for-ready** — decides whether a call waits for its connection or fails fast. Last of the
   layers that shape a call's deadline handling, because that decision reads the deadline the two
   above it settled on: waiting is only allowed for a call something will eventually end.
8. **Retry** — divides the resulting budget between attempts.
9. **Circuit breaker** — innermost, so it sees individual attempts instead of one aggregated
   verdict, and rejects tripped methods without touching the network.
10. **Extra inner** — custom interceptors that must run per attempt, closest to the wire (for
    example a credential refresh that has to be redone for every retry).

Custom interceptors passed to `build_interceptors` or `InterceptorChainBuilder.with_custom` land in
the outer slot, because injecting metadata is by far their most common job and that only works
above the observability layers.

Every layer the kit builds is one `AsyncClientInterceptor` covering all four RPC kinds, and the
chain a builder emits is **flat**: each of them contributes four adapters, one per kind, because a
channel files an interceptor into a single one of its four lists by class (see `base`). That
expansion is precisely what makes the order above hold for streaming calls and not only for unary
ones. Order survives it, `build` returns exactly what a channel accepts, and
`base.logical_interceptor` maps an entry back to the interceptor it came from.

The two extra slots are the one place where something else may arrive: an interceptor a caller
wrote straight against grpc's ``intercept_*`` methods already is what a channel registers, so it
travels through the chain as the single entry it is.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

import grpc.aio

from ..protocols import GrpcClientMetricsProtocol, RetryMetricsProtocol
from .base import (
    AsyncAroundClientInterceptor,
    AsyncClientInterceptor,
    ClientCall,
    ClientInterceptorLike,
    flatten_interceptors,
    logical_interceptor,
)
from .circuit_breaker import AsyncCircuitBreakerInterceptor, CircuitBreakerOpenError
from .client_logging import AsyncLoggingInterceptor
from .context import AsyncClientContextInterceptor
from .retry import DEFAULT_RETRYABLE_CODES, AsyncRetryInterceptor
from .timeout import AsyncTimeoutInterceptor
from .wait_for_ready import AsyncWaitForReadyInterceptor

logger = logging.getLogger(__name__)

# Referenced from inside dataclass bodies, where a field named `logging` shadows the module.
_DEFAULT_SUCCESS_LOG_LEVEL = logging.INFO


@dataclass(slots=True)
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    fail_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 1
    max_methods: int = 1000
    metrics: GrpcClientMetricsProtocol | None = None


@dataclass(slots=True)
class RetryConfig:
    """Configuration for retries.

    Attributes:
        max_attempts: Total number of attempts, the first one included.
        initial_backoff: Seconds to wait before the first retry.
        max_backoff: Upper bound for the wait between retries.
        backoff_multiplier: Factor by which the backoff grows each attempt.
        jitter: Random variation factor (0.0 to 1.0) applied to the backoff.
        retryable_codes: Status codes that trigger a retry. Defaults to
            `retry.DEFAULT_RETRYABLE_CODES`, which only covers requests the server never started
            processing; widening it makes duplicate side effects possible on non-idempotent methods.
        retry_streaming: Whether Unary-Stream calls may be retried at all.
        idempotent_methods: Full method names that may be retried. Required for streaming retries,
            and a whitelist for unary retries when given.
        on_retry: Optional async callback ``(method, attempt, code, backoff)`` invoked for every
            scheduled retry.
        metrics: Optional registry told about every scheduled retry (`RetryMetricsProtocol`).
            The request metrics see one entry per logical call, so without this a retry storm is
            invisible on a dashboard.
    """

    max_attempts: int = 3
    initial_backoff: float = 0.1
    max_backoff: float = 10.0
    backoff_multiplier: float = 2.0
    jitter: float = 0.1
    retryable_codes: set[grpc.StatusCode] | None = None
    retry_streaming: bool = False
    idempotent_methods: set[str] | None = None
    on_retry: Callable[[str, int, grpc.StatusCode | None, float], Awaitable[None]] | None = None
    metrics: RetryMetricsProtocol | None = None


@dataclass(slots=True)
class TimeoutConfig:
    """Configuration for timeouts.

    A timeout is the budget of a whole call, retries included, not of a single attempt.

    Attributes:
        default: Budget in seconds for methods without an entry in `per_method`. ``None`` or ``0``
            means no deadline; when neither a default nor per-method budget is configured, no
            timeout interceptor is added to the chain at all.
        per_method: Per-method budgets, keyed by full method name (``/package.Service/Method``).
            ``None`` or ``0`` disables the deadline for that method only.
    """

    default: float | None = 10.0
    per_method: dict[str, float | None] = field(default_factory=dict)


@dataclass(slots=True)
class WaitForReadyConfig:
    """Configuration for waiting on a connection instead of failing fast on a cold channel.

    A `grpc.aio` channel connects lazily, so calls made in the first moments of a client's life —
    or right after a backend restart — fail with ``UNAVAILABLE`` before anything is even attempted.
    Setting this makes them wait for the connection instead, bounded by the call's deadline.

    Attributes:
        default: Value for methods without an entry in `per_method`. ``None`` leaves calls untouched.
        per_method: Values for individual methods, keyed by full method name
            (``/package.Service/Method``). ``None`` exempts that method from `default`.
        require_deadline: Whether waiting applies only to calls that carry a deadline. On by
            default: an unbounded wait replaces a fast failure with a hang.
    """

    default: bool | None = True
    per_method: dict[str, bool | None] = field(default_factory=dict)
    require_deadline: bool = True


@dataclass(slots=True)
class DeadlineBudgetConfig:
    """Configuration for propagating the caller's deadline budget into outgoing calls.

    Adding this to a chain installs `deadline.AsyncDeadlineBudgetInterceptor`, which trims each
    call's deadline to what the request budget in the current context still allows. It needs the
    ``deadline`` extra (``grpc-client-kit[deadline]``); without it the layer is skipped and the
    chain behaves as if it had never been configured.

    The budget itself is installed by the caller with `grpc_client_kit.use_budget`. A chain
    configured here but never given a budget changes nothing about any call.

    Attributes:
        reserve_for_next: Seconds every call keeps back for the work that follows it.
    """

    reserve_for_next: float = 0.0


@dataclass(slots=True)
class ObservabilityConfig:
    """Configuration for observability.

    Attributes:
        tracing: Whether to add the OpenTelemetry tracing layer (needs the ``tracing`` extra).
        metrics: Whether to add the metrics layer.
        logging: Whether to add the structured logging layer.
        service_name: Client service name used in log categorization and span attributes.
        metrics_registry: The collector RPC metrics are recorded into.
        sensitive_methods: Full method names whose payloads and errors are suppressed in logs.
        sensitive_patterns: Regex patterns marking methods as sensitive.
        sensitive_headers: Header names (case-insensitive) redacted in logged metadata.
        log_request_payload: Whether to log request payloads (truncated).
        log_response_payload: Whether to log response payloads (truncated).
        enable_method_label: Whether metrics carry the method name as a label. Turn off for
            clients of services with thousands of methods, where per-method labels blow up the
            cardinality of every counter and histogram.
        success_log_level: Level of the record a successful call emits; one INFO line per RPC is
            a flood at high QPS, so high-volume clients drop this to ``logging.DEBUG``.
    """

    tracing: bool = False
    metrics: bool = False
    logging: bool = True
    service_name: str = "unknown"
    metrics_registry: GrpcClientMetricsProtocol | None = None
    sensitive_methods: set[str] | None = None
    sensitive_patterns: list[str] | None = None
    sensitive_headers: set[str] | None = None
    log_request_payload: bool = False
    log_response_payload: bool = False
    enable_method_label: bool = True
    success_log_level: int = _DEFAULT_SUCCESS_LOG_LEVEL


def _build_observability_interceptors(
    config: ObservabilityConfig | None = None,
) -> list[AsyncClientInterceptor]:
    """Build observability interceptors based on configuration.

    Tracing and metrics live behind optional extras. Their modules import cleanly without those
    extras and fall back to a no-op, so availability is decided by their ``HAS_*`` flags rather than
    by catching `ImportError` here: a layer that cannot do its job is left out of the chain and
    reported, instead of silently costing a hop per call.

    Args:
        config: Observability configuration (Tracing, Metrics, Logging).

    Returns:
        List of configured observability interceptors, outermost first.
    """
    interceptors: list[AsyncClientInterceptor] = []
    if not config:
        return interceptors

    # 1. Logging
    if config.logging:
        interceptors.append(
            AsyncLoggingInterceptor(
                service_name=config.service_name,
                sensitive_methods=config.sensitive_methods,
                sensitive_patterns=config.sensitive_patterns,
                sensitive_headers=config.sensitive_headers,
                log_request_payload=config.log_request_payload,
                log_response_payload=config.log_response_payload,
                success_log_level=config.success_log_level,
            )
        )

    # 2. Tracing
    if config.tracing:
        from .tracing import HAS_TRACING, AsyncClientTracingInterceptor  # noqa: PLC0415

        if HAS_TRACING:
            interceptors.append(AsyncClientTracingInterceptor(service_name=config.service_name))
        else:
            logger.warning("Tracing requested but grpc-client-kit[tracing] is not installed; tracing is disabled")

    # 3. Metrics
    if config.metrics or config.metrics_registry:
        from .metrics import HAS_METRICS, AsyncClientMetricsInterceptor  # noqa: PLC0415

        # A caller-supplied registry is a working backend on its own; without one the interceptor
        # can only record through the optional prometheus extra.
        if config.metrics_registry is not None or HAS_METRICS:
            interceptors.append(
                AsyncClientMetricsInterceptor(
                    service_name=config.service_name,
                    metrics=config.metrics_registry,
                    enable_method_label=config.enable_method_label,
                )
            )
        else:
            logger.warning("Metrics requested but grpc-client-kit[metrics] is not installed; metrics are disabled")

    return interceptors


def _build_resilience_interceptors(
    timeout: TimeoutConfig | None = None,
    retry: RetryConfig | None = None,
    circuit_breaker: CircuitBreakerConfig | None = None,
    deadline_budget: DeadlineBudgetConfig | None = None,
    wait_for_ready: WaitForReadyConfig | None = None,
) -> list[AsyncClientInterceptor]:
    """Build resilience interceptors in the correct order.

    Order (outermost to innermost):
    1. Timeout — sets the budget of the whole call, retries included.
    2. Deadline budget — cuts that budget down to what the caller's request has left.
    3. Wait-for-ready — decides whether a call waits for its connection, given that deadline.
    4. Retry — divides the result between attempts.
    5. Circuit Breaker (innermost, sees every attempt and protects the server).

    Args:
        timeout: Timeout configuration.
        retry: Retry configuration.
        circuit_breaker: Circuit breaker configuration.
        deadline_budget: Deadline budget propagation configuration.
        wait_for_ready: Wait-for-ready configuration.

    Returns:
        List of configured resilience interceptors, outermost first.
    """
    interceptors: list[AsyncClientInterceptor] = []

    # 1. Timeout (outermost)
    if timeout is not None:
        per_method_timeouts: dict[str, float | None] = dict(timeout.per_method)

        if timeout.default is None and not per_method_timeouts:
            # Deadlines were switched off deliberately. A pass-through interceptor would still cost a
            # hop per call and, since the chain is part of the channel-pool identity, a separate channel.
            logger.debug("Timeout configured with no budget; timeout interceptor omitted")
        else:
            interceptors.append(
                AsyncTimeoutInterceptor(
                    default_timeout=timeout.default,
                    per_method_timeouts=per_method_timeouts,
                )
            )

    # 2. Deadline budget.
    #
    # Above the retry layer, and that part is forced: retry reads the call's timeout once, turns it
    # into a deadline and hands out slices of it, so a budget applied below would arrive after the
    # division had already been made from the untrimmed value — N attempts of a deadline the request
    # could not afford even once.
    #
    # Below the timeout layer, which is a choice rather than a constraint: both layers narrow the
    # deadline and both take the smaller of what they find and what they know, so either order ends
    # at the same number. Here it reads in the order the two things are decided — the configured
    # budget for this method, then how much of the request is left to spend on it — and what leaves
    # this layer is already the final deadline, rather than one more thing for retry's caller to
    # reason about.
    if deadline_budget is not None:
        from .deadline import HAS_DEADLINE_BUDGET, AsyncDeadlineBudgetInterceptor  # noqa: PLC0415

        if HAS_DEADLINE_BUDGET:
            interceptors.append(AsyncDeadlineBudgetInterceptor(reserve_for_next=deadline_budget.reserve_for_next))
        else:
            logger.warning(
                "Deadline budget propagation requested but grpc-client-kit[deadline] is not installed; "
                "call deadlines are not trimmed to the request budget"
            )

    # 3. Wait-for-ready.
    #
    # Below the two layers that settle the deadline, because whether a call may wait for its
    # connection is decided from that deadline: an unbounded call is left fail-fast. Above retry,
    # where one pass suffices — the retry layer rebuilds each attempt's details from the ones it was
    # given, so the flag set here travels with every attempt.
    if wait_for_ready is not None:
        interceptors.append(
            AsyncWaitForReadyInterceptor(
                default=wait_for_ready.default,
                per_method=dict(wait_for_ready.per_method),
                require_deadline=wait_for_ready.require_deadline,
            )
        )

    # 4. Retry
    if retry is not None:
        interceptors.append(
            AsyncRetryInterceptor(
                max_attempts=retry.max_attempts,
                initial_backoff=retry.initial_backoff,
                max_backoff=retry.max_backoff,
                backoff_multiplier=retry.backoff_multiplier,
                jitter=retry.jitter,
                retryable_codes=retry.retryable_codes,
                retry_streaming=retry.retry_streaming,
                idempotent_methods=retry.idempotent_methods,
                on_retry=retry.on_retry,
                metrics=retry.metrics,
            )
        )

    # 5. Circuit Breaker (innermost)
    if circuit_breaker is not None:
        interceptors.append(
            AsyncCircuitBreakerInterceptor(
                fail_threshold=circuit_breaker.fail_threshold,
                recovery_timeout=circuit_breaker.recovery_timeout,
                half_open_max_calls=circuit_breaker.half_open_max_calls,
                max_methods=circuit_breaker.max_methods,
                metrics=circuit_breaker.metrics,
            )
        )

    return interceptors


def build_interceptors(
    timeout: TimeoutConfig | None = None,
    retry: RetryConfig | None = None,
    circuit_breaker: CircuitBreakerConfig | None = None,
    observability: ObservabilityConfig | None = None,
    extra_interceptors: Sequence[ClientInterceptorLike] | None = None,
    extra_inner_interceptors: Sequence[ClientInterceptorLike] | None = None,
    deadline_budget: DeadlineBudgetConfig | None = None,
    wait_for_ready: WaitForReadyConfig | None = None,
) -> list[grpc.aio.ClientInterceptor]:
    """Build a standard interceptor chain in the order documented for this module.

    Args:
        timeout: Timeout configuration.
        retry: Retry configuration.
        circuit_breaker: Circuit breaker configuration.
        observability: Observability configuration.
        extra_interceptors: Custom interceptors for the outer slot, ahead of logging, tracing and
            metrics. This is where metadata injection belongs so that logs and spans can correlate
            on it.
        extra_inner_interceptors: Custom interceptors for the inner slot, below the resilience
            layers. These run once per attempt, closest to the wire.
        deadline_budget: Deadline budget propagation configuration. Needs the ``deadline`` extra.
        wait_for_ready: Wait-for-ready configuration, for calls made on a channel that may still be
            connecting.

    Returns:
        A flat chain, outermost first, ready to be bound to a channel — by GrpcClient or by
        `grpc.aio` directly. Each kit layer appears as its four channel adapters; see the module
        docstring.
    """
    return (
        InterceptorChainBuilder()
        .with_extra_outer(extra_interceptors)
        .with_observability(observability)
        .with_resilience(timeout, retry, circuit_breaker, deadline_budget, wait_for_ready)
        .with_extra_inner(extra_inner_interceptors)
        .build()
    )


class InterceptorChainBuilder:
    """Builder for a gRPC client interceptor chain with strict ordering.

    The order is a property of the builder, not of the call sequence: `build` always emits
    ``extra outer -> observability -> resilience -> extra inner`` regardless of which ``with_*``
    method was called first. See the module docstring for why each layer sits where it does.
    """

    def __init__(self) -> None:
        # The kit's own layers are logical interceptors, full stop; only what a caller brings to the
        # two extra slots may already be a grpc-shaped one.
        self._extra_outer: list[ClientInterceptorLike] = []
        self._observability: list[AsyncClientInterceptor] = []
        self._resilience: list[AsyncClientInterceptor] = []
        self._extra_inner: list[ClientInterceptorLike] = []

    def with_observability(self, config: ObservabilityConfig | None) -> InterceptorChainBuilder:
        """Add observability interceptors (Logging, Tracing, Metrics)."""
        self._observability = _build_observability_interceptors(config)
        return self

    def with_resilience(
        self,
        timeout: TimeoutConfig | None = None,
        retry: RetryConfig | None = None,
        circuit_breaker: CircuitBreakerConfig | None = None,
        deadline_budget: DeadlineBudgetConfig | None = None,
        wait_for_ready: WaitForReadyConfig | None = None,
    ) -> InterceptorChainBuilder:
        """Add resilience interceptors (Timeout, Deadline budget, Wait-for-ready, Retry, Breaker)."""
        self._resilience = _build_resilience_interceptors(
            timeout, retry, circuit_breaker, deadline_budget, wait_for_ready
        )
        return self

    def with_extra_outer(self, interceptors: Sequence[ClientInterceptorLike] | None) -> InterceptorChainBuilder:
        """Add custom interceptors that wrap the whole chain.

        Metadata injection belongs here: the logging interceptor correlates on ``request-id`` taken
        from the call metadata, so anything injected further in never reaches the logs or the spans.
        """
        if interceptors:
            self._extra_outer.extend(interceptors)
        return self

    def with_extra_inner(self, interceptors: Sequence[ClientInterceptorLike] | None) -> InterceptorChainBuilder:
        """Add custom interceptors below the resilience layers, run once per attempt."""
        if interceptors:
            self._extra_inner.extend(interceptors)
        return self

    def with_custom(self, interceptors: Sequence[ClientInterceptorLike] | None) -> InterceptorChainBuilder:
        """Add custom interceptors to the outer slot; an alias of `with_extra_outer`."""
        return self.with_extra_outer(interceptors)

    def build(self) -> list[grpc.aio.ClientInterceptor]:
        """Build the final chain, outermost first, expanded into what a channel can register."""
        chain: list[ClientInterceptorLike] = [
            *self._extra_outer,
            *self._observability,
            *self._resilience,
            *self._extra_inner,
        ]
        return flatten_interceptors(chain)


__all__ = [
    "DEFAULT_RETRYABLE_CODES",
    "AsyncAroundClientInterceptor",
    "AsyncCircuitBreakerInterceptor",
    "AsyncClientContextInterceptor",
    "AsyncClientInterceptor",
    "AsyncLoggingInterceptor",
    "AsyncRetryInterceptor",
    "AsyncTimeoutInterceptor",
    "AsyncWaitForReadyInterceptor",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
    "ClientCall",
    "ClientInterceptorLike",
    "DeadlineBudgetConfig",
    "InterceptorChainBuilder",
    "ObservabilityConfig",
    "RetryConfig",
    "TimeoutConfig",
    "WaitForReadyConfig",
    "build_interceptors",
    "flatten_interceptors",
    "logical_interceptor",
]
