"""Structural protocols (duck-typing seams) for gRPC client settings and collaborators.

Settings fields are declared as read-only properties, not plain attributes: protocol
attributes are invariant under type checking, while read-only properties are covariant,
so an implementation may narrow a field's type (e.g. a pydantic model typing `strategy`
as a `Literal`) and still satisfy the protocol.

Settings protocols describe only the fields real settings objects are required to carry.
Optional blocks live in their own protocols (`GrpcChannelExtrasProtocol`,
`GrpcObservabilityExtrasProtocol`) so that `isinstance` against a settings protocol stays
a meaningful check instead of failing on fields nobody defines.
"""

from __future__ import annotations

from typing import (
    Any,
    Protocol,
    runtime_checkable,
)

import grpc
import grpc.aio


@runtime_checkable
class HealthCheckerProtocol(Protocol):
    """Protocol for target health monitoring."""

    async def is_healthy(self, target: str) -> bool:
        """Check if target is healthy (from cache).

        Implementations report health from evidence only: a target that has never been
        checked is not healthy. An implementation that cannot produce evidence at all
        (its check loop is not running) may raise instead of answering.

        Args:
            target: The target address (host:port)

        Returns:
            True if the last check reported healthy, False otherwise
        """
        ...

    async def check_health(self, target: str) -> bool:
        """Perform active health check for target.

        Args:
            target: The target address (host:port)

        Returns:
            True if healthy, False otherwise
        """
        ...


@runtime_checkable
class ChannelProviderProtocol(Protocol):
    """Protocol for gRPC channel management."""

    async def get_channel(
        self,
        target: str,
        insecure: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        options: list[tuple[str, Any]] | None = None,
        compression: grpc.Compression | None = None,
        interceptors: list[grpc.aio.ClientInterceptor] | None = None,
    ) -> grpc.aio.Channel:
        """Get or create a gRPC channel for the target.

        Args:
            target: The target address (host:port)
            insecure: Whether to use insecure channel
            credentials: Optional channel credentials for secure channel
            options: Optional gRPC channel options
            compression: Optional gRPC compression
            interceptors: Optional list of interceptors

        Returns:
            An async gRPC channel
        """
        ...

    async def close_all(self, grace: float | None = None) -> None:
        """Close all pooled channels and release resources."""
        ...

    async def update_channel_health(self, target: str, is_healthy: bool) -> None:
        """Update health status for a specific target.

        Args:
            target: The target address (host:port)
            is_healthy: Whether the target is healthy
        """
        ...


@runtime_checkable
class HealthStatusCallbackProtocol(Protocol):
    """Protocol for health status change callbacks."""

    async def __call__(self, target: str, is_healthy: bool) -> None:
        """Called when health status of a target changes.

        Args:
            target: The target address (host:port)
            is_healthy: True if target is healthy, False otherwise
        """
        ...


@runtime_checkable
class GrpcClientMetricsProtocol(Protocol):
    """Protocol for gRPC client metrics collection."""

    def record_request(
        self,
        service: str,
        method: str,
        rpc_type: str,
        status: str,
        grpc_code: str,
        duration: float,
    ) -> None:
        """Record a completed gRPC request.

        Args:
            service: Name of the service.
            method: Name of the method.
            rpc_type: Type of RPC (unary_unary, unary_stream, etc.).
            status: Status of the request (success, error, cancelled).
            grpc_code: gRPC status code name.
            duration: Request duration in seconds.
        """
        ...

    def record_inflight_delta(
        self,
        service: str,
        method: str,
        rpc_type: str,
        delta: int,
    ) -> None:
        """Record a change in in-flight requests.

        Args:
            service: Name of the service.
            method: Name of the method.
            rpc_type: Type of RPC.
            delta: Change in in-flight requests (e.g., +1 or -1).
        """
        ...

    def record_pool_stats(
        self,
        active_channels: int,
        idle_targets: int,
    ) -> None:
        """Record channel pool statistics.

        Args:
            active_channels: Total number of active channels in the pool.
            idle_targets: Number of targets currently in the pool.
        """
        ...


@runtime_checkable
class RetryMetricsProtocol(Protocol):
    """Optional extension: retry attempts, invisible to `record_request` by design.

    The metrics layer sits above the retry layer and records one entry per *logical* call, so a
    retry storm — N wire attempts collapsing into one success — cannot be seen through
    `GrpcClientMetricsProtocol` alone. A registry that also implements this protocol gets told
    about every retry the moment it is scheduled.
    """

    def record_retry(self, service: str, method: str, attempt: int, grpc_code: str) -> None:
        """Record one scheduled retry.

        Args:
            service: Name of the service.
            method: Name of the method.
            attempt: Number of the upcoming attempt (1 is the first retry).
            grpc_code: Status code name of the failure that caused the retry.
        """
        ...


@runtime_checkable
class CircuitBreakerMetricsProtocol(Protocol):
    """Optional extension: circuit breaker state transitions and rejections.

    A rejection by an open breaker never touches the network, yet through `record_request` alone
    it is indistinguishable from a backend that really failed. A registry that also implements
    this protocol can chart the breaker itself: its state per method, and how many calls it
    refused locally.
    """

    def record_circuit_state(self, method: str, state: str) -> None:
        """Record a circuit state transition.

        Args:
            method: Full gRPC method path.
            state: The new state (``closed``, ``open`` or ``half-open``).
        """
        ...

    def record_circuit_rejection(self, method: str) -> None:
        """Record one call refused locally by an open circuit.

        Args:
            method: Full gRPC method path.
        """
        ...


@runtime_checkable
class ChannelPoolSettingsProtocol(Protocol):
    """Protocol for gRPC channel pool settings."""

    @property
    def max_channels_per_target(self) -> int:
        """Maximum number of channels to keep per target."""
        ...

    @property
    def idle_timeout(self) -> float:
        """Time in seconds after which an idle channel is closed."""
        ...


@runtime_checkable
class CircuitBreakerSettingsProtocol(Protocol):
    """Protocol for gRPC circuit breaker settings."""

    @property
    def fail_threshold(self) -> int:
        """Number of failures before opening the circuit."""
        ...

    @property
    def recovery_timeout(self) -> float:
        """Time in seconds to wait before attempting recovery."""
        ...

    @property
    def half_open_max_calls(self) -> int:
        """Maximum number of calls allowed in half-open state."""
        ...


@runtime_checkable
class RetrySettingsProtocol(Protocol):
    """Protocol for gRPC retry settings."""

    @property
    def max_attempts(self) -> int:
        """Maximum number of attempts (including the first one)."""
        ...

    @property
    def initial_backoff(self) -> float:
        """Initial backoff time in seconds."""
        ...

    @property
    def max_backoff(self) -> float:
        """Maximum backoff time in seconds."""
        ...

    @property
    def backoff_multiplier(self) -> float:
        """Multiplier for exponential backoff."""
        ...


@runtime_checkable
class TimeoutSettingsProtocol(Protocol):
    """Protocol for gRPC timeout settings."""

    @property
    def default(self) -> float:
        """Default timeout in seconds."""
        ...


@runtime_checkable
class LoadBalancerSettingsProtocol(Protocol):
    """Protocol for gRPC load balancer settings."""

    @property
    def strategy(self) -> str:
        """Load balancing strategy (round_robin, random, weighted)."""
        ...

    @property
    def weights(self) -> dict[str, float] | None:
        """Weights for weighted strategy (target -> weight)."""
        ...


@runtime_checkable
class HealthCheckerSettingsProtocol(Protocol):
    """Protocol for gRPC health checker settings."""

    @property
    def check_interval(self) -> float:
        """Interval between health checks in seconds."""
        ...

    @property
    def timeout(self) -> float:
        """Timeout for each health check in seconds."""
        ...


@runtime_checkable
class GrpcClientSettingsProtocol(Protocol):
    """Protocol for base gRPC client settings.

    Every field here is one a settings object must carry. Anything a client may or may
    not configure (credentials, channel options, redaction, metrics registry) belongs to
    the extras protocols below, so that a plain settings model satisfies this one.
    """

    @property
    def target(self) -> str | None:
        """Single target address (host:port)."""
        ...

    @property
    def targets(self) -> list[str] | None:
        """List of target addresses for load balancing."""
        ...

    @property
    def insecure(self) -> bool:
        """Whether to use insecure connection."""
        ...

    @property
    def tracing_enabled(self) -> bool:
        """Whether tracing is enabled."""
        ...

    @property
    def metrics_enabled(self) -> bool:
        """Whether metrics are enabled."""
        ...

    @property
    def logging_enabled(self) -> bool:
        """Whether logging is enabled."""
        ...

    @property
    def pool(self) -> ChannelPoolSettingsProtocol | None:
        """Channel pool settings."""
        ...

    @property
    def circuit_breaker(self) -> CircuitBreakerSettingsProtocol | None:
        """Circuit breaker settings."""
        ...

    @property
    def retry(self) -> RetrySettingsProtocol | None:
        """Retry settings."""
        ...

    @property
    def timeout(self) -> TimeoutSettingsProtocol | None:
        """Timeout settings."""
        ...

    @property
    def balancer(self) -> LoadBalancerSettingsProtocol | None:
        """Load balancer settings."""
        ...

    @property
    def health_checker(self) -> HealthCheckerSettingsProtocol | None:
        """Health checker settings."""
        ...


@runtime_checkable
class GrpcChannelExtrasProtocol(Protocol):
    """Protocol for optional channel-construction settings."""

    @property
    def credentials(self) -> grpc.ChannelCredentials | None:
        """Channel credentials for secure connection."""
        ...

    @property
    def options(self) -> list[tuple[str, Any]] | None:
        """Optional gRPC channel options."""
        ...

    @property
    def compression(self) -> grpc.Compression | None:
        """Optional gRPC compression."""
        ...


@runtime_checkable
class GrpcObservabilityExtrasProtocol(Protocol):
    """Protocol for optional observability settings."""

    @property
    def sensitive_headers(self) -> set[str] | None:
        """Set of header names to redact during logging."""
        ...

    @property
    def metrics_registry(self) -> GrpcClientMetricsProtocol | None:
        """Optional metrics registry for pool and client metrics."""
        ...


@runtime_checkable
class FullGrpcClientSettingsProtocol(
    GrpcClientSettingsProtocol,
    GrpcChannelExtrasProtocol,
    GrpcObservabilityExtrasProtocol,
    Protocol,
):
    """Protocol for settings that carry the required fields and every optional block."""


__all__ = [
    "ChannelPoolSettingsProtocol",
    "ChannelProviderProtocol",
    "CircuitBreakerMetricsProtocol",
    "CircuitBreakerSettingsProtocol",
    "FullGrpcClientSettingsProtocol",
    "GrpcChannelExtrasProtocol",
    "GrpcClientMetricsProtocol",
    "GrpcClientSettingsProtocol",
    "GrpcObservabilityExtrasProtocol",
    "HealthCheckerProtocol",
    "HealthCheckerSettingsProtocol",
    "HealthStatusCallbackProtocol",
    "LoadBalancerSettingsProtocol",
    "RetryMetricsProtocol",
    "RetrySettingsProtocol",
    "TimeoutSettingsProtocol",
]
