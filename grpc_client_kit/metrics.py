"""The Prometheus collector for the client metrics protocols (``[metrics]`` extra).

`GrpcClientMetrics` is the collector the kit's own layers record into. It satisfies
`protocols.GrpcClientMetricsProtocol` and the two extension protocols the retry layer and the
circuit breaker look for, `protocols.RetryMetricsProtocol` and
`protocols.CircuitBreakerMetricsProtocol`, so a factory given one instance measures RPCs, retries,
breaker state and the pool through it — the extensions are what make a retry storm visible, which the
base protocol alone cannot show.

It is shaped after ``grpc_server_kit.observability.metrics.GrpcServerMetrics`` on purpose: the same
histogram buckets, and the server's label names in the server's order with ``rpc_type`` added after
``method``. A dashboard therefore joins the caller's ``grpc_client_requests_total`` with the callee's
``grpc_requests_total`` on ``service``, ``method``, ``status`` and ``grpc_code``, and compares the two
latency histograms bucket for bucket.

Prometheus registers a metric name once per registry, so a second ``GrpcClientMetrics()`` on the
default registry raises ``ValueError`` — what a test suite runs into when it rebuilds a container.
`get_grpc_client_metrics` is the answer: it caches one instance per prefix, and it is what the Dishka
provider calls. A test that wants isolation passes a fresh ``registry=CollectorRegistry()`` instead.

This module needs the ``metrics`` extra (``grpc-client-kit[metrics]``), which pulls in
prometheus-client; importing it without raises an ``ImportError`` naming the extra.
"""

from __future__ import annotations

import re

try:
    from prometheus_client import REGISTRY, CollectorRegistry, Counter, Enum, Gauge, Histogram
except ImportError as exc:
    raise ImportError("Install grpc-client-kit[metrics] (prometheus-client) to use the Prometheus collector") from exc

from .interceptors.circuit_breaker import CircuitState

# The server kit's buckets, so the two duration histograms compare bucket for bucket (seconds).
DEFAULT_GRPC_BUCKETS: tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

_PREFIX_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _make_metric_name(name: str, prefix: str | None) -> str:
    """Prefix a metric name, refusing a prefix that is not a valid metric name fragment.

    Args:
        name: The metric name without a prefix.
        prefix: The prefix to put in front of it, or None for none.

    Returns:
        The name Prometheus is asked to register.

    Raises:
        ValueError: If the prefix is not a valid metric name fragment.
    """
    if not prefix:
        return name
    if not _PREFIX_PATTERN.match(prefix):
        raise ValueError(
            f"Invalid metric prefix: {prefix!r}. Prefixes must start with a letter or underscore and "
            "contain only letters, numbers, or underscores."
        )
    return f"{prefix}_{name}"


class GrpcClientMetrics:
    """gRPC client metrics as Prometheus collectors, one instance per registry.

    Metrics:
        - ``grpc_client_requests_total`` (Counter) — labels: service, method, rpc_type, status, grpc_code
        - ``grpc_client_request_duration_seconds`` (Histogram) — labels: service, method, rpc_type
        - ``grpc_client_requests_in_flight`` (Gauge) — labels: service, method, rpc_type
        - ``grpc_client_pool_channels`` (Gauge) — pooled channels
        - ``grpc_client_pool_targets`` (Gauge) — distinct addresses the pooled channels lead to
        - ``grpc_client_retries_total`` (Counter) — labels: service, method, grpc_code
        - ``grpc_client_circuit_breaker_state`` (Enum: closed, open, half-open) — labels: method
        - ``grpc_client_circuit_breaker_rejections_total`` (Counter) — labels: method

    The label values are recorded as the kit's layers hand them over: ``service`` and ``method``
    are the short names parsed from the method path for requests, and the full method path for
    the breaker; ``status`` is ``success``, ``error``, ``rejected`` or ``cancelled``.
    """

    def __init__(
        self,
        prefix: str | None = None,
        buckets: tuple[float, ...] = DEFAULT_GRPC_BUCKETS,
        registry: CollectorRegistry | None = None,
    ) -> None:
        """Register the collectors.

        Args:
            prefix: Optional metric name prefix (``myapp`` gives ``myapp_grpc_client_requests_total``).
            buckets: Histogram buckets for the request duration, in seconds.
            registry: The registry to register with; the process-wide default when None.

        Raises:
            ValueError: If the prefix is invalid, or a metric of the same name is already
                registered there — see `get_grpc_client_metrics` for the second instance.
        """
        self.buckets: tuple[float, ...] = tuple(buckets)
        reg = registry if registry is not None else REGISTRY
        self.requests_total = Counter(
            _make_metric_name("grpc_client_requests_total", prefix),
            "Total number of gRPC client requests",
            ["service", "method", "rpc_type", "status", "grpc_code"],
            registry=reg,
        )
        self.request_duration = Histogram(
            _make_metric_name("grpc_client_request_duration_seconds", prefix),
            "gRPC client request duration in seconds, retries and their backoff included",
            ["service", "method", "rpc_type"],
            buckets=list(buckets),
            registry=reg,
        )
        self.requests_in_flight = Gauge(
            _make_metric_name("grpc_client_requests_in_flight", prefix),
            "gRPC client requests currently in progress",
            ["service", "method", "rpc_type"],
            registry=reg,
        )
        self.pool_channels = Gauge(
            _make_metric_name("grpc_client_pool_channels", prefix),
            "Channels currently held by the channel pool",
            registry=reg,
        )
        self.pool_targets = Gauge(
            _make_metric_name("grpc_client_pool_targets", prefix),
            "Distinct target addresses the pooled channels lead to",
            registry=reg,
        )
        self.retries_total = Counter(
            _make_metric_name("grpc_client_retries_total", prefix),
            "Total number of scheduled gRPC client retries",
            ["service", "method", "grpc_code"],
            registry=reg,
        )
        self.circuit_breaker_state = Enum(
            _make_metric_name("grpc_client_circuit_breaker_state", prefix),
            "State of the circuit breaker of a method",
            ["method"],
            states=[state.value for state in CircuitState],
            registry=reg,
        )
        self.circuit_breaker_rejections_total = Counter(
            _make_metric_name("grpc_client_circuit_breaker_rejections_total", prefix),
            "Total number of gRPC client calls refused locally by an open circuit breaker",
            ["method"],
            registry=reg,
        )

    def record_request(
        self,
        service: str,
        method: str,
        rpc_type: str,
        status: str,
        grpc_code: str,
        duration: float,
    ) -> None:
        """Record a completed gRPC request."""
        self.requests_total.labels(
            service=service, method=method, rpc_type=rpc_type, status=status, grpc_code=grpc_code
        ).inc()
        self.request_duration.labels(service=service, method=method, rpc_type=rpc_type).observe(duration)

    def record_inflight_delta(self, service: str, method: str, rpc_type: str, delta: int) -> None:
        """Move the in-flight gauge by ``delta``."""
        self.requests_in_flight.labels(service=service, method=method, rpc_type=rpc_type).inc(delta)

    def record_pool_stats(self, active_channels: int, idle_targets: int) -> None:
        """Record the channel pool's size."""
        self.pool_channels.set(active_channels)
        self.pool_targets.set(idle_targets)

    def record_retry(self, service: str, method: str, attempt: int, grpc_code: str) -> None:
        """Count one scheduled retry; the attempt number is not a label, it would multiply the series."""
        self.retries_total.labels(service=service, method=method, grpc_code=grpc_code).inc()

    def record_circuit_state(self, method: str, state: str) -> None:
        """Record a circuit state transition."""
        self.circuit_breaker_state.labels(method=method).state(state)

    def record_circuit_rejection(self, method: str) -> None:
        """Count one call refused locally by an open circuit."""
        self.circuit_breaker_rejections_total.labels(method=method).inc()


_CLIENT_METRICS_CACHE: dict[str | None, GrpcClientMetrics] = {}


def get_grpc_client_metrics(
    prefix: str | None = None,
    buckets: tuple[float, ...] | None = None,
) -> GrpcClientMetrics:
    """Get (or lazily create) the cached ``GrpcClientMetrics`` for a prefix, on the default registry.

    Caching by prefix is what lets a container be rebuilt — a test suite does it per test —
    without Prometheus refusing the second registration of the same series.

    Args:
        prefix: The metric name prefix the instance was, or is, created with.
        buckets: Histogram buckets for a new instance; None takes the default.

    Returns:
        The one instance for that prefix.

    Raises:
        ValueError: If the prefix is already cached with DIFFERENT buckets — silently returning
            the old instance would record latencies into the wrong histogram bounds with no error.
    """
    requested = tuple(buckets) if buckets is not None else DEFAULT_GRPC_BUCKETS
    cached = _CLIENT_METRICS_CACHE.get(prefix)
    if cached is not None:
        if buckets is not None and requested != cached.buckets:
            raise ValueError(
                f"GrpcClientMetrics for prefix {prefix!r} already exists with buckets "
                f"{cached.buckets}; cannot re-create it with {requested}"
            )
        return cached
    metrics = GrpcClientMetrics(prefix=prefix, buckets=requested)
    _CLIENT_METRICS_CACHE[prefix] = metrics
    return metrics


__all__ = [
    "DEFAULT_GRPC_BUCKETS",
    "GrpcClientMetrics",
    "get_grpc_client_metrics",
]
