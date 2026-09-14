"""Unit tests for the shipped Prometheus collector: the protocols it satisfies, and the series it joins.

Regression cover for issue #28: the ``metrics`` extra installed prometheus-client and nothing
implemented the protocol, so every consumer wrote the same collector with its own bucket boundaries
and label order — the part that makes client and server series unjoinable across services. These pin
the collector to the server kit's shape and to the getter that survives a rebuilt container.
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

import pytest
from prometheus_client import CollectorRegistry

from grpc_client_kit.interceptors.metrics import AsyncClientMetricsInterceptor
from grpc_client_kit.metrics import DEFAULT_GRPC_BUCKETS, GrpcClientMetrics, get_grpc_client_metrics
from grpc_client_kit.protocols import (
    CircuitBreakerMetricsProtocol,
    GrpcClientMetricsProtocol,
    RetryMetricsProtocol,
)
from tests.helpers import RPC_KINDS, FakeUnaryCall, Wire, adapter_for, make_call_details

pytestmark = pytest.mark.unit

# What grpc-server-kit's GrpcServerMetrics registers, pinned here because this repository cannot
# import the server kit: ``grpc_requests_total`` carries these labels in this order, the duration
# histogram the first two, and both use these buckets. The client's series must join on them.
SERVER_REQUEST_LABELS = ("service", "method", "status", "grpc_code")
SERVER_DURATION_LABELS = ("service", "method")
SERVER_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

REQUEST = {"service": "Users", "method": "GetUser", "rpc_type": "unary_unary", "status": "success", "grpc_code": "OK"}
CALL = {"service": "Users", "method": "GetUser", "rpc_type": "unary_unary"}


@pytest.fixture
def registry() -> CollectorRegistry:
    """A fresh registry, so a test never meets the series another one registered."""
    return CollectorRegistry()


@pytest.fixture
def metrics(registry: CollectorRegistry) -> GrpcClientMetrics:
    """A collector on the fresh registry."""
    return GrpcClientMetrics(registry=registry)


# --------------------------------------------------------------------------------------------
# The shape: the server kit's, plus rpc_type.
# --------------------------------------------------------------------------------------------


def test__collector__built__satisfies_the_base_protocol_and_both_extensions(metrics: GrpcClientMetrics) -> None:
    """The factory wires the extensions by isinstance; a collector missing one leaves retries invisible."""
    # Act & Assert
    assert isinstance(metrics, GrpcClientMetricsProtocol)
    assert isinstance(metrics, RetryMetricsProtocol)
    assert isinstance(metrics, CircuitBreakerMetricsProtocol)


def test__collector__request_labels__are_the_servers_with_rpc_type_after_method(metrics: GrpcClientMetrics) -> None:
    """Joining across services needs the same label names in the same order; rpc_type is the one addition."""
    # Act
    labels = tuple(metrics.requests_total._labelnames)
    duration_labels = tuple(metrics.request_duration._labelnames)

    # Assert
    assert labels == ("service", "method", "rpc_type", "status", "grpc_code")
    assert tuple(label for label in labels if label != "rpc_type") == SERVER_REQUEST_LABELS
    assert tuple(label for label in duration_labels if label != "rpc_type") == SERVER_DURATION_LABELS


def test__collector__histogram_buckets__are_the_server_kits(metrics: GrpcClientMetrics) -> None:
    """Different bucket boundaries on the two ends of a call make the latencies incomparable."""
    # Act & Assert
    assert DEFAULT_GRPC_BUCKETS == SERVER_BUCKETS
    assert metrics.buckets == SERVER_BUCKETS
    assert metrics.request_duration._upper_bounds == [*SERVER_BUCKETS, float("inf")]


def test__collector__record_request__counts_the_call_under_its_six_arguments(
    metrics: GrpcClientMetrics, registry: CollectorRegistry
) -> None:
    """One call: one increment carrying every label, one observation of its duration."""
    # Act
    metrics.record_request(**REQUEST, duration=0.3)

    # Assert
    assert registry.get_sample_value("grpc_client_requests_total", REQUEST) == 1.0
    assert registry.get_sample_value("grpc_client_request_duration_seconds_count", CALL) == 1.0
    assert registry.get_sample_value("grpc_client_request_duration_seconds_sum", CALL) == pytest.approx(0.3)


def test__collector__inflight_deltas__move_the_gauge_both_ways(
    metrics: GrpcClientMetrics, registry: CollectorRegistry
) -> None:
    """The interceptor balances +1 and -1 per call; the gauge must end where it began."""
    # Act
    metrics.record_inflight_delta(**CALL, delta=1)
    metrics.record_inflight_delta(**CALL, delta=1)
    after_two = registry.get_sample_value("grpc_client_requests_in_flight", CALL)
    metrics.record_inflight_delta(**CALL, delta=-1)
    metrics.record_inflight_delta(**CALL, delta=-1)

    # Assert
    assert after_two == 2.0
    assert registry.get_sample_value("grpc_client_requests_in_flight", CALL) == 0.0


def test__collector__pool_stats__set_the_two_pool_gauges(
    metrics: GrpcClientMetrics, registry: CollectorRegistry
) -> None:
    # Act
    metrics.record_pool_stats(active_channels=3, idle_targets=2)

    # Assert
    assert registry.get_sample_value("grpc_client_pool_channels") == 3.0
    assert registry.get_sample_value("grpc_client_pool_targets") == 2.0


def test__collector__record_retry__counts_by_code_and_not_by_attempt(
    metrics: GrpcClientMetrics, registry: CollectorRegistry
) -> None:
    """An attempt label would multiply every series by max_attempts; the count is what a dashboard needs."""
    # Act
    metrics.record_retry("Users", "/users.v1.Users/GetUser", attempt=1, grpc_code="UNAVAILABLE")
    metrics.record_retry("Users", "/users.v1.Users/GetUser", attempt=2, grpc_code="UNAVAILABLE")

    # Assert
    labels = {"service": "Users", "method": "/users.v1.Users/GetUser", "grpc_code": "UNAVAILABLE"}
    assert registry.get_sample_value("grpc_client_retries_total", labels) == 2.0
    assert "attempt" not in metrics.retries_total._labelnames


def test__collector__circuit_state__is_an_enum_over_the_breakers_three_states(
    metrics: GrpcClientMetrics, registry: CollectorRegistry
) -> None:
    """The state the breaker reports by name lands as the one state set to 1, the others to 0."""
    # Arrange
    method = "/users.v1.Users/GetUser"

    # Act
    metrics.record_circuit_state(method, "open")

    # Assert
    state_label = "grpc_client_circuit_breaker_state"
    assert registry.get_sample_value(state_label, {"method": method, state_label: "open"}) == 1.0
    assert registry.get_sample_value(state_label, {"method": method, state_label: "closed"}) == 0.0
    assert registry.get_sample_value(state_label, {"method": method, state_label: "half-open"}) == 0.0


def test__collector__circuit_rejection__counts_the_refused_call(
    metrics: GrpcClientMetrics, registry: CollectorRegistry
) -> None:
    # Act
    metrics.record_circuit_rejection("/users.v1.Users/GetUser")

    # Assert
    labels = {"method": "/users.v1.Users/GetUser"}
    assert registry.get_sample_value("grpc_client_circuit_breaker_rejections_total", labels) == 1.0


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        (None, "grpc_client_requests_total"),
        ("myapp", "myapp_grpc_client_requests_total"),
    ],
)
def test__collector__prefix__is_put_in_front_of_every_name(
    registry: CollectorRegistry, prefix: str | None, expected: str
) -> None:
    # Arrange
    metrics = GrpcClientMetrics(prefix=prefix, registry=registry)

    # Act
    metrics.record_request(**REQUEST, duration=0.1)

    # Assert
    assert registry.get_sample_value(expected, REQUEST) == 1.0


def test__collector__invalid_prefix__is_refused(registry: CollectorRegistry) -> None:
    """A prefix Prometheus would reject is refused here, by name, instead of deep inside the client."""
    # Act & Assert
    with pytest.raises(ValueError, match="Invalid metric prefix"):
        GrpcClientMetrics(prefix="bad-prefix", registry=registry)


# --------------------------------------------------------------------------------------------
# Through the interceptor: what a real call lands as.
# --------------------------------------------------------------------------------------------


async def test__collector__behind_the_metrics_interceptor__records_one_finished_call(
    metrics: GrpcClientMetrics, registry: CollectorRegistry
) -> None:
    """End to end: the interceptor's labels and the collector's label names agree."""
    # Arrange
    interceptor = AsyncClientMetricsInterceptor(service_name="users", metrics=metrics)
    adapter = adapter_for(interceptor, RPC_KINDS["unary_unary"])
    wire = Wire(FakeUnaryCall("response"))

    # Act
    call = await adapter.intercept_unary_unary(wire, make_call_details("/users.v1.Users/GetUser"), "req")
    response = await call

    # Assert
    assert response == "response"
    assert registry.get_sample_value("grpc_client_requests_total", REQUEST) == 1.0
    assert registry.get_sample_value("grpc_client_requests_in_flight", CALL) == 0.0


# --------------------------------------------------------------------------------------------
# One registration per process: the reporter's rebuilt container.
# --------------------------------------------------------------------------------------------


def test__collector__registered_twice_on_one_registry__is_refused_by_prometheus(registry: CollectorRegistry) -> None:
    """The negative control for the getter below: this is what a rebuilt container ran into."""
    # Arrange
    GrpcClientMetrics(registry=registry)

    # Act & Assert
    with pytest.raises(ValueError, match="Duplicated timeseries"):
        GrpcClientMetrics(registry=registry)


def test__get_grpc_client_metrics__same_prefix_twice__returns_the_cached_instance() -> None:
    """A second container asking for the same prefix gets the instance the first one registered."""
    # Act
    first = get_grpc_client_metrics(prefix="cache_test")
    second = get_grpc_client_metrics(prefix="cache_test")

    # Assert
    assert first is second


def test__get_grpc_client_metrics__same_buckets_twice__returns_the_cached_instance() -> None:
    # Act
    first = get_grpc_client_metrics(prefix="bucket_same_test", buckets=DEFAULT_GRPC_BUCKETS)
    second = get_grpc_client_metrics(prefix="bucket_same_test", buckets=DEFAULT_GRPC_BUCKETS)

    # Assert
    assert first is second


def test__get_grpc_client_metrics__conflicting_buckets__raises() -> None:
    """Silently keeping the old buckets would record latencies into the wrong bounds with no error."""
    # Arrange
    get_grpc_client_metrics(prefix="bucket_conflict_test")

    # Act & Assert
    with pytest.raises(ValueError, match="already exists with buckets"):
        get_grpc_client_metrics(prefix="bucket_conflict_test", buckets=(0.1, 1.0))


# --------------------------------------------------------------------------------------------
# The extra.
# --------------------------------------------------------------------------------------------


def test__metrics_module__extra_missing__raises_the_install_hint() -> None:
    """Without prometheus-client the import fails naming the extra, not with a bare ModuleNotFoundError."""
    # Arrange
    with patch.dict("sys.modules", {"prometheus_client": None}):
        sys.modules.pop("grpc_client_kit.metrics", None)

        # Act & Assert
        with pytest.raises(ImportError, match=r"grpc-client-kit\[metrics\]"):
            importlib.import_module("grpc_client_kit.metrics")
