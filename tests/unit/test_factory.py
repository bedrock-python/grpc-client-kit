"""Unit tests for the client factory: which client, chain and pool a set of settings produces."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import grpc
import pytest

from grpc_client_kit.balancers import RoundRobinLoadBalancer
from grpc_client_kit.client import GrpcClient
from grpc_client_kit.factory import GrpcClientFactory, _load_health_checker
from grpc_client_kit.health import HealthChecker
from grpc_client_kit.interceptors.circuit_breaker import AsyncCircuitBreakerInterceptor
from grpc_client_kit.interceptors.metrics import AsyncClientMetricsInterceptor
from tests.helpers import make_interceptor, make_pool

from .conftest import (
    BARE_INSTALL_PROBE,
    StubCircuitBreakerSettings,
    StubClientSettings,
    chain_layers,
    layers_of,
    make_health_checker,
    make_protocol_settings,
    make_stub_class,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# Which client the settings produce.
# --------------------------------------------------------------------------------------------


def test__factory__single_target__creates_a_client_bound_to_it() -> None:
    """The everyday case: one address, taken from the settings as it was written."""
    # Arrange
    factory = GrpcClientFactory(settings=StubClientSettings())

    # Act
    client: GrpcClient = factory.create_client(make_stub_class())

    # Assert
    assert isinstance(client, GrpcClient)
    assert client._config.target == "localhost:50051"
    assert client._config.insecure is True


def test__factory__several_targets__creates_a_client_with_a_balancer() -> None:
    """More than one address means the client cannot pick one itself, so it is given a balancer."""
    # Arrange
    settings = StubClientSettings(target=None, targets=["host1:50051", "host2:50051"])
    factory = GrpcClientFactory(settings=settings)

    # Act
    client: GrpcClient = factory.create_client(make_stub_class())

    # Assert
    assert client._balancer is not None
    assert isinstance(client._balancer, RoundRobinLoadBalancer)


def test__factory__no_target_at_all__is_refused() -> None:
    """A client with neither a target nor a balancer cannot be built."""
    # Act & Assert
    with pytest.raises(ValueError, match="No target configured"):
        GrpcClientFactory().create_client(make_stub_class("Stub"))


def test__factory__unknown_balancer_strategy__is_refused() -> None:
    """An unknown strategy fails loudly instead of silently falling back."""
    # Arrange
    settings = StubClientSettings(
        target=None,
        targets=["host1:50051"],
        balancer=MagicMock(strategy="nonsense", weights=None),
    )
    factory = GrpcClientFactory(settings=settings)

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid load balancing strategy"):
        factory.create_client(make_stub_class("Stub"))


def test__factory__no_settings__builds_a_client_with_no_chain_at_all() -> None:
    """Without settings there is no chain at all, so such clients can share pooled channels."""
    # Act
    client: GrpcClient = GrpcClientFactory().create_client(make_stub_class("Stub"), target="localhost:50051")

    # Assert
    assert client._config.target == "localhost:50051"
    assert client._config.insecure is False
    assert client.interceptors_for("localhost:50051") == []


def test__factory__credentials_and_channel_options_in_settings__reach_the_client_config() -> None:
    """Everything the pool needs to open the channel is carried over unchanged."""
    # Arrange
    settings = StubClientSettings(
        target="secure:443",
        insecure=False,
        metrics_enabled=False,
        logging_enabled=False,
        tracing_enabled=False,
        credentials=MagicMock(spec=grpc.ChannelCredentials),
        options=[("grpc.max_receive_message_length", 1024)],
        compression=grpc.Compression.Gzip,
    )
    factory = GrpcClientFactory(settings=settings)

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"))

    # Assert
    assert client._config.target == "secure:443"
    assert client._config.insecure is False
    assert client._config.credentials == settings.credentials
    assert client._config.options == settings.options
    assert client._config.compression == settings.compression


# --------------------------------------------------------------------------------------------
# Which interceptor chain the settings produce.
# --------------------------------------------------------------------------------------------


def test__factory__custom_interceptors__are_added_to_the_chain() -> None:
    """A caller may extend the chain the settings describe with layers of its own."""
    # Arrange
    factory = GrpcClientFactory(settings=StubClientSettings())
    interceptor = make_interceptor()

    # Act
    client: GrpcClient = factory.create_client(make_stub_class(), interceptors=[interceptor])

    # Assert
    assert interceptor in client.interceptors_for("localhost:50051")


def test__factory__several_targets__gives_each_of_them_its_own_circuit_breaker() -> None:
    """One failing backend must not open the breaker of its healthy peers."""
    # Arrange
    targets = ["host1:50051", "host2:50051"]
    settings = StubClientSettings(target=None, targets=targets, circuit_breaker=StubCircuitBreakerSettings())
    factory = GrpcClientFactory(settings=settings)

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"))

    # Assert
    breakers = [layers_of(client, target, AsyncCircuitBreakerInterceptor) for target in targets]
    assert all(len(found) == 1 for found in breakers)
    assert breakers[0][0] is not breakers[1][0]


def test__factory__custom_interceptors_with_several_targets__are_shared_as_they_are() -> None:
    """Interceptors the caller owns are passed through as-is, not cloned per target."""
    # Arrange
    settings = StubClientSettings(target=None, targets=["host1:50051", "host2:50051"])
    factory = GrpcClientFactory(settings=settings)
    interceptor = make_interceptor()

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"), interceptors=[interceptor])

    # Assert
    assert interceptor in client.interceptors_for("host1:50051")
    assert interceptor in client.interceptors_for("host2:50051")


def test__factory__metrics_registry_in_settings__reaches_the_rpc_metrics_layer() -> None:
    """metrics_registry from settings must reach RPC metrics, not just pool stats."""
    # Arrange
    settings = StubClientSettings(metrics_registry=MagicMock())
    factory = GrpcClientFactory(settings=settings)

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"))

    # Assert
    metrics_layers = layers_of(client, "localhost:50051", AsyncClientMetricsInterceptor)
    assert len(metrics_layers) == 1
    assert metrics_layers[0]._metrics is settings.metrics_registry


def test__factory__registry_passed_to_create_client__wins_over_the_settings_one() -> None:
    """The registry passed to create_client overrides the one from settings."""
    # Arrange
    settings = StubClientSettings(metrics_registry=MagicMock())
    explicit = MagicMock()
    factory = GrpcClientFactory(settings=settings)

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"), metrics=explicit)

    # Assert
    metrics_layer = layers_of(client, "localhost:50051", AsyncClientMetricsInterceptor)[0]
    assert metrics_layer._metrics is explicit


def test__factory__metrics_enabled_without_a_registry__skips_the_layer_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without a registry the metrics interceptor would record nothing: say so, don't add it."""
    # Arrange
    factory = GrpcClientFactory(settings=StubClientSettings())

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"))

    # Assert
    assert layers_of(client, "localhost:50051", AsyncClientMetricsInterceptor) == []
    assert "metrics_enabled is set but no metrics registry" in caplog.text


def test__factory__metrics_registry_in_settings__also_reaches_the_circuit_breaker() -> None:
    """The circuit breaker reports into the same registry as the RPC metrics."""
    # Arrange
    settings = StubClientSettings(metrics_registry=MagicMock(), circuit_breaker=StubCircuitBreakerSettings())
    factory = GrpcClientFactory(settings=settings)

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"))

    # Assert
    breaker = layers_of(client, "localhost:50051", AsyncCircuitBreakerInterceptor)[0]
    assert breaker._metrics is settings.metrics_registry


def test__factory__explicit_service_name__names_the_observability_layers_after_it() -> None:
    """The service name is what the logs and spans of this client will be filed under."""
    # Arrange
    settings = StubClientSettings(target="h:50051", metrics_registry=MagicMock())
    factory = GrpcClientFactory(settings=settings)

    # Act
    client: GrpcClient = factory.create_client(make_stub_class("Stub"), service_name="CustomSrv")

    # Assert
    chain = chain_layers(client, "h:50051")
    # Logging and metrics are both installed by these settings.
    assert len(chain) >= 2
    assert chain[0]._service_name == "CustomSrv"


# --------------------------------------------------------------------------------------------
# Pool ownership and the health checker.
# --------------------------------------------------------------------------------------------


async def test__factory__pool_it_created_itself__closes_it() -> None:
    """A pool the factory opened is a pool the factory has to close."""
    # Arrange
    pool = make_pool()

    # Act
    with patch("grpc_client_kit.factory.ChannelPool", return_value=pool):
        factory = GrpcClientFactory()
        owns_pool = factory._owns_pool
        await factory.close()

    # Assert
    assert owns_pool is True
    assert factory._pool == pool
    pool.close_all.assert_called_once()


async def test__factory__used_as_a_context_manager__closes_only_the_pool_it_owns() -> None:
    """A shared pool outlives the factory that borrowed it; one the factory opened does not."""
    # Arrange
    borrowed = make_pool()
    owned = make_pool()

    # Act
    async with GrpcClientFactory(pool=borrowed) as borrowing:
        borrows = borrowing._owns_pool

    with patch("grpc_client_kit.factory.ChannelPool", return_value=owned):
        async with GrpcClientFactory() as owning:
            owns = owning._owns_pool

    # Assert
    assert borrows is False
    assert borrowing._pool == borrowed
    borrowed.close_all.assert_not_called()
    assert owns is True
    owned.close_all.assert_called_once()


def test__factory__settings_missing_required_fields__are_refused_at_construction() -> None:
    """Chains are built lazily, so without this check a missing field would surface as a bare
    AttributeError on the first RPC — in production, far from the mistake."""
    # Arrange
    incomplete = SimpleNamespace(target="h:1", insecure=True)

    # Act & Assert
    with pytest.raises(TypeError, match="does not satisfy GrpcClientSettingsProtocol"):
        GrpcClientFactory(settings=incomplete)  # type: ignore[arg-type]


async def test__factory__exiting_the_context__closes_with_the_configured_grace() -> None:
    """A k8s SIGTERM lands on __aexit__; in-flight dependency calls must get their grace."""
    # Arrange
    pool = make_pool()
    factory = GrpcClientFactory(pool=pool, shutdown_grace=7.5)
    factory._owns_pool = True

    # Act
    async with factory:
        pass

    # Assert
    pool.close_all.assert_awaited_once_with(grace=7.5)


async def test__factory__entering_the_context__awaits_the_first_health_pass() -> None:
    """Until the first pass every target reads unhealthy; returning earlier makes the first call
    of every freshly started pod fail deterministically."""
    # Arrange
    settings = StubClientSettings(
        target=None,
        targets=["h1:1"],
        metrics_enabled=False,
        logging_enabled=False,
        tracing_enabled=False,
        health_checker=MagicMock(check_interval=10.0, timeout=1.0),
    )
    checker = make_health_checker()

    # Act
    with patch("grpc_client_kit.factory._load_health_checker", return_value=MagicMock(return_value=checker)):
        async with GrpcClientFactory(settings=settings, ready_timeout=3.0):
            pass

    # Assert
    checker.start.assert_awaited_once()
    checker.wait_until_ready.assert_awaited_once_with(timeout=3.0)


async def test__factory__health_checker_configured__is_wired_to_the_pool_and_run() -> None:
    """The checker needs the pool to report into, and the factory's lifecycle to run in."""
    # Arrange
    settings = StubClientSettings(
        target=None,
        targets=["h1:1", "h2:2"],
        metrics_enabled=False,
        logging_enabled=False,
        tracing_enabled=False,
        balancer=MagicMock(strategy="round_robin", weights=None),
        health_checker=MagicMock(check_interval=10.0, timeout=1.0),
    )
    checker = make_health_checker()
    checker_class = MagicMock(return_value=checker)

    # Act
    with patch("grpc_client_kit.factory._load_health_checker", return_value=checker_class):
        async with GrpcClientFactory(settings=settings) as factory:
            checker_class.assert_called_once()
            kwargs = checker_class.call_args.kwargs

            # Assert
            assert kwargs["pool"] == factory._pool
            assert kwargs["check_interval"] == 10.0
            assert kwargs["timeout"] == 1.0
            checker.start.assert_called_once_with(settings.targets)

    checker.stop.assert_called_once()


async def test__factory__protocol_settings__closes_cleanly_and_builds_its_balancer() -> None:
    """The factory is driven through the settings protocol, not through a concrete settings class."""
    # Arrange
    factory = GrpcClientFactory()

    # Act
    await factory.close()
    async with factory as reentered:
        pass
    balancer = GrpcClientFactory(settings=make_protocol_settings())._build_balancer()

    # Assert
    assert reentered == factory
    assert balancer is not None


# --------------------------------------------------------------------------------------------
# The health extra.
# --------------------------------------------------------------------------------------------


def test__load_health_checker__extra_installed__resolves_the_real_class() -> None:
    """With the [health] extra installed the loader resolves the real class."""
    # Act & Assert
    assert _load_health_checker() is HealthChecker


def test__load_health_checker__extra_missing__names_the_install_command() -> None:
    """Without the extra the failure names the install command instead of grpc_health."""
    # Act & Assert
    with (
        patch.dict(sys.modules, {"grpc_client_kit.health": None, "grpc_health.v1": None}),
        pytest.raises(ImportError, match=r"grpc-client-kit\[health\]"),
    ):
        _load_health_checker()


def test__package__bare_install_without_the_health_extra__still_imports() -> None:
    """`import grpc_client_kit` must work on a bare install, with health failing only on use."""
    # Act & Assert
    subprocess.run(  # noqa: S603
        [sys.executable, "-c", BARE_INSTALL_PROBE],
        cwd=Path(__file__).parents[2],
        check=True,
        capture_output=True,
    )
