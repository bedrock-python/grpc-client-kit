"""Unit tests for the interceptor chain builder exposed by the package."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from grpc_client_kit.interceptors import (
    CircuitBreakerConfig,
    DeadlineBudgetConfig,
    ObservabilityConfig,
    RetryConfig,
    TimeoutConfig,
    WaitForReadyConfig,
    build_interceptors,
    logical_interceptor,
)
from grpc_client_kit.interceptors.circuit_breaker import AsyncCircuitBreakerInterceptor
from grpc_client_kit.interceptors.client_logging import AsyncLoggingInterceptor
from grpc_client_kit.interceptors.deadline import AsyncDeadlineBudgetInterceptor
from grpc_client_kit.interceptors.metrics import AsyncClientMetricsInterceptor
from grpc_client_kit.interceptors.retry import AsyncRetryInterceptor
from grpc_client_kit.interceptors.timeout import AsyncTimeoutInterceptor
from grpc_client_kit.interceptors.tracing import AsyncClientTracingInterceptor
from grpc_client_kit.interceptors.wait_for_ready import AsyncWaitForReadyInterceptor

pytestmark = pytest.mark.unit


def test__build_interceptors__every_resilience_config__orders_timeout_retry_then_breaker() -> None:
    """The timeout bounds the whole call, so it has to sit outside the retries dividing its budget."""
    # Arrange
    timeout = TimeoutConfig(default=1.0)
    retry = RetryConfig(max_attempts=2)
    circuit_breaker = CircuitBreakerConfig(fail_threshold=3)
    observability = ObservabilityConfig(tracing=False, metrics=False, logging=False)

    # Act
    interceptors = build_interceptors(
        timeout=timeout, retry=retry, circuit_breaker=circuit_breaker, observability=observability
    )

    # Assert
    # Every layer reaches the channel as four adapters, one per RPC kind, in chain order.
    layers = list(dict.fromkeys(logical_interceptor(entry) for entry in interceptors))
    assert len(interceptors) == 4 * len(layers)
    assert isinstance(layers[0], AsyncTimeoutInterceptor)
    assert isinstance(layers[1], AsyncRetryInterceptor)
    assert isinstance(layers[2], AsyncCircuitBreakerInterceptor)


def test__build_interceptors__deadline_budget_configured__sits_between_timeout_and_retry() -> None:
    """It trims the deadline the timeout layer installs, and retry divides what it leaves behind."""
    # Arrange
    observability = ObservabilityConfig(tracing=False, metrics=False, logging=False)

    # Act
    interceptors = build_interceptors(
        timeout=TimeoutConfig(default=1.0),
        retry=RetryConfig(max_attempts=2),
        circuit_breaker=CircuitBreakerConfig(fail_threshold=3),
        observability=observability,
        deadline_budget=DeadlineBudgetConfig(),
    )

    # Assert
    layers = list(dict.fromkeys(logical_interceptor(entry) for entry in interceptors))
    assert isinstance(layers[0], AsyncTimeoutInterceptor)
    assert isinstance(layers[1], AsyncDeadlineBudgetInterceptor)
    assert isinstance(layers[2], AsyncRetryInterceptor)
    assert isinstance(layers[3], AsyncCircuitBreakerInterceptor)


def test__build_interceptors__wait_for_ready_configured__sits_below_the_deadline_layers() -> None:
    """Whether a call may wait is decided from the deadline the two layers above it settled on."""
    # Act
    interceptors = build_interceptors(
        timeout=TimeoutConfig(default=1.0),
        retry=RetryConfig(max_attempts=2),
        deadline_budget=DeadlineBudgetConfig(),
        wait_for_ready=WaitForReadyConfig(),
    )

    # Assert
    layers = list(dict.fromkeys(logical_interceptor(entry) for entry in interceptors))
    assert isinstance(layers[0], AsyncTimeoutInterceptor)
    assert isinstance(layers[1], AsyncDeadlineBudgetInterceptor)
    assert isinstance(layers[2], AsyncWaitForReadyInterceptor)
    assert isinstance(layers[3], AsyncRetryInterceptor)


def test__build_interceptors__no_wait_for_ready_config__leaves_the_layer_out() -> None:
    """Waiting changes how failures behave, so it is opt-in and absent until it is asked for."""
    # Act
    interceptors = build_interceptors(timeout=TimeoutConfig(default=1.0))

    # Assert
    layers = [logical_interceptor(entry) for entry in interceptors]
    assert not any(isinstance(layer, AsyncWaitForReadyInterceptor) for layer in layers)


def test__build_interceptors__deadline_budget_without_the_extra__omits_the_layer() -> None:
    """A layer that cannot do its job is left out and reported, not run as a silent pass-through."""
    # Arrange
    timeout = TimeoutConfig(default=1.0)

    # Act
    with patch("grpc_client_kit.interceptors.deadline.HAS_DEADLINE_BUDGET", False):
        interceptors = build_interceptors(timeout=timeout, deadline_budget=DeadlineBudgetConfig())

    # Assert
    layers = [logical_interceptor(entry) for entry in interceptors]
    assert any(isinstance(layer, AsyncTimeoutInterceptor) for layer in layers)
    assert not any(isinstance(layer, AsyncDeadlineBudgetInterceptor) for layer in layers)


def test__build_interceptors__no_deadline_budget_config__leaves_the_layer_out() -> None:
    """Propagation is opt-in: an unconfigured chain must cost neither a hop nor a channel identity."""
    # Act
    interceptors = build_interceptors(timeout=TimeoutConfig(default=1.0))

    # Assert
    layers = [logical_interceptor(entry) for entry in interceptors]
    assert not any(isinstance(layer, AsyncDeadlineBudgetInterceptor) for layer in layers)


def test__build_interceptors__observability_enabled__adds_logging_tracing_and_metrics() -> None:
    """The three observability layers are independent, and all of them are installable at once."""
    # Arrange
    observability = ObservabilityConfig(tracing=True, metrics=True, logging=True)

    # Act
    with (
        patch("grpc_client_kit.interceptors.tracing.HAS_TRACING", True),
        patch("grpc_client_kit.interceptors.metrics.HAS_METRICS", True),
    ):
        interceptors = build_interceptors(observability=observability)

    # Assert
    layers = [logical_interceptor(entry) for entry in interceptors]
    assert len(interceptors) >= 3
    assert any(isinstance(layer, AsyncLoggingInterceptor) for layer in layers)
    assert any(isinstance(layer, AsyncClientTracingInterceptor) for layer in layers)
    assert any(isinstance(layer, AsyncClientMetricsInterceptor) for layer in layers)
