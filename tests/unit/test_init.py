"""Unit tests for the package's public API surface."""

from __future__ import annotations

import grpc
import grpc.aio
import pytest

import grpc_client_kit
from grpc_client_kit import (
    CircuitBreakerOpenError,
    DeadlineBudgetExhaustedError,
    GrpcClientKitError,
    HealthCheckerNotRunningError,
    NoHealthyTargetsError,
    protocols,
)
from grpc_client_kit.interceptors import circuit_breaker

pytestmark = pytest.mark.unit


def test__package_version__read_from_the_package__is_a_non_empty_string() -> None:
    """The version is what a user reports in a bug, so it must resolve on a bare import."""
    # Act
    version = grpc_client_kit.__version__

    # Assert
    assert isinstance(version, str)
    assert version


def test__public_api__every_declared_export__resolves() -> None:
    """Every declared export must resolve, including the extras-gated lazy ones."""
    # Act
    missing = [name for name in grpc_client_kit.__all__ if not hasattr(grpc_client_kit, name)]

    # Assert
    assert not missing, f"missing exports: {missing}"


def test__public_api__declared_exports__are_sorted_and_unique() -> None:
    """``__all__`` is read by people as well as by star imports, and a duplicate hides a mistake."""
    # Act & Assert
    assert grpc_client_kit.__all__ == sorted(set(grpc_client_kit.__all__))


def test__public_api__names_users_import_from_submodules__are_re_exported() -> None:
    """Interceptors, the chain builder and the concrete balancers are part of the public API."""
    # Arrange
    expected = {
        "AsyncCircuitBreakerInterceptor",
        "AsyncClientContextInterceptor",
        "AsyncLoggingInterceptor",
        "AsyncRetryInterceptor",
        "AsyncTimeoutInterceptor",
        "InterceptorChainBuilder",
        "RandomLoadBalancer",
        "RoundRobinLoadBalancer",
        "WeightedLoadBalancer",
    }

    # Act & Assert
    assert expected <= set(grpc_client_kit.__all__)


def test__package_attribute__name_that_does_not_exist__raises_attribute_error() -> None:
    """The lazy ``__getattr__`` must not turn a typo into something other than an AttributeError."""
    # Act & Assert
    with pytest.raises(AttributeError, match="has no attribute 'NotAThing'"):
        grpc_client_kit.NotAThing  # noqa: B018


def test__public_api__pool_internals__are_not_part_of_the_contract() -> None:
    """Exporting the pool's wrappers and tokens would freeze its implementation for good."""
    # Act & Assert
    assert "ChannelWrapper" not in grpc_client_kit.__all__
    assert "chain_token" not in grpc_client_kit.__all__


def test__public_api__every_declared_protocol__is_re_exported_at_the_top_level() -> None:
    """A settings object is written against these, so importing them must not need the submodule."""
    # Act
    missing = sorted(set(protocols.__all__) - set(grpc_client_kit.__all__))

    # Assert
    assert not missing, f"declared in grpc_client_kit.protocols but not exported: {missing}"


def test__public_api__protocols_module__declares_every_protocol_the_package_exports() -> None:
    """A protocol the package exports but the module does not declare is missed by a star import."""
    # Act
    exported = {name for name in grpc_client_kit.__all__ if hasattr(protocols, name)}
    undeclared = sorted(exported - set(protocols.__all__))

    # Assert
    assert not undeclared, f"exported by the package but absent from protocols.__all__: {undeclared}"


def test__public_api__circuit_breaker_internals__are_not_part_of_the_contract() -> None:
    """``MethodCircuitState`` is the breaker's mutable bookkeeping; callers get status snapshots."""
    # Act & Assert
    assert "MethodCircuitState" not in circuit_breaker.__all__
    assert "MethodCircuitState" not in grpc_client_kit.__all__
    assert "CircuitBreakerStatus" in circuit_breaker.__all__


def test__kit_errors__every_local_failure__is_catchable_as_one_family() -> None:
    """``except GrpcClientKitError`` is the one handler for "the kit said no, not the server"."""
    # Act & Assert
    assert issubclass(NoHealthyTargetsError, GrpcClientKitError)
    assert issubclass(HealthCheckerNotRunningError, GrpcClientKitError)
    assert issubclass(HealthCheckerNotRunningError, RuntimeError)
    assert issubclass(CircuitBreakerOpenError, GrpcClientKitError)
    assert issubclass(DeadlineBudgetExhaustedError, GrpcClientKitError)
    # The two that stand in for an RPC outcome stay AioRpcErrors, so existing handlers,
    # logging, metrics and tracing keep seeing them as the call failures they are.
    assert issubclass(CircuitBreakerOpenError, grpc.aio.AioRpcError)
    assert issubclass(DeadlineBudgetExhaustedError, grpc.aio.AioRpcError)


def test__kit_errors__constructed__carry_their_grpc_status() -> None:
    """The multiple inheritance must not break how the status errors are built."""
    # Act
    error = CircuitBreakerOpenError("/pkg.Svc/M")

    # Assert
    assert error.code() is grpc.StatusCode.UNAVAILABLE
    assert "/pkg.Svc/M" in (error.details() or "")
