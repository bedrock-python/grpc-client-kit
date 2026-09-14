"""Unit tests for the Dishka providers: the lifecycle they own, and what a container hands out.

Regression cover for issue #28: every consumer wrote the pool lifecycle themselves — build it from
the settings, park it on an exit stack, close it with the container — and the collector next to it
registered twice when a test suite rebuilt the container. These pin that a container with the bundle
and a ``BaseGrpcClientSettings`` builds without a server, that closing the container closes the pool,
that a second container does not raise, and that two upstreams are two components.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from unittest.mock import patch

import pytest
from dishka import AsyncContainer, Provider, make_async_container
from dishka.exceptions import GraphMissingFactoryError

from grpc_client_kit import GrpcClientFactory
from grpc_client_kit.dishka import (
    AsyncGrpcClientProvider,
    GrpcClientSettingsProvider,
    PrometheusGrpcClientMetricsProvider,
    grpc_client_providers,
)
from grpc_client_kit.interceptors.metrics import AsyncClientMetricsInterceptor
from grpc_client_kit.metrics import GrpcClientMetrics
from grpc_client_kit.protocols import GrpcClientMetricsProtocol, GrpcClientSettingsProtocol
from grpc_client_kit.settings import BaseGrpcClientSettings

from .conftest import layers_of, make_stub_class

pytestmark = pytest.mark.unit

TARGET = "localhost:50051"
METRICS_KEY = GrpcClientMetricsProtocol | None

ContainerFactory = Callable[..., Awaitable[AsyncContainer]]


@pytest.fixture
async def container_factory() -> AsyncIterator[ContainerFactory]:
    """Build async containers from providers, closing every one on teardown."""
    containers: list[AsyncContainer] = []

    async def _make(*providers: Provider) -> AsyncContainer:
        container = make_async_container(*providers)
        containers.append(container)
        return container

    yield _make

    for container in containers:
        await container.close()


def make_settings(**overrides: object) -> BaseGrpcClientSettings:
    """One insecure upstream on the shipped settings model, no health checker, so nothing needs a server."""
    return BaseGrpcClientSettings(**{"target": TARGET, "insecure": True, **overrides})  # type: ignore[arg-type]


# --------------------------------------------------------------------------------------------
# The bundle: a container builds, hands out a factory, and closes what the factory owns.
# --------------------------------------------------------------------------------------------


async def test__grpc_client_providers__bundle_with_base_settings__resolves_a_factory_without_a_server(
    container_factory: ContainerFactory,
) -> None:
    """The reporter's case: the shipped settings model and the bundle, and nothing listening."""
    # Arrange
    container = await container_factory(*grpc_client_providers(make_settings()))

    # Act
    factory = await container.get(GrpcClientFactory)
    client = factory.create_client(make_stub_class("Stub"))

    # Assert
    assert isinstance(factory, GrpcClientFactory)
    assert await container.get(GrpcClientSettingsProtocol) is factory._settings
    assert client._config.target == TARGET


async def test__async_grpc_client_provider__container_closed__closes_the_pool_the_factory_owns() -> None:
    """The lifecycle is the point: closing the container is what closes the channels."""
    # Arrange
    container = make_async_container(*grpc_client_providers(make_settings(), shutdown_grace=7.5))
    factory = await container.get(GrpcClientFactory)
    await factory.create_client(make_stub_class("Stub")).connect()
    pooled_before_close = len(factory._pool._entries)

    # Act
    with patch.object(factory._pool, "close_all", wraps=factory._pool.close_all) as close_all:
        await container.close()

    # Assert
    assert pooled_before_close == 1
    close_all.assert_awaited_once_with(grace=7.5)
    assert factory._pool._entries == {}


async def test__async_grpc_client_provider__resolved_twice__is_one_factory(
    container_factory: ContainerFactory,
) -> None:
    """APP scope: one pool per process, not one per resolution."""
    # Arrange
    container = await container_factory(*grpc_client_providers(make_settings()))

    # Act
    first = await container.get(GrpcClientFactory)
    second = await container.get(GrpcClientFactory)

    # Assert
    assert first is second


async def test__async_grpc_client_provider__without_the_metrics_seam__is_refused_at_container_build() -> None:
    """The registry is requested, never defaulted: a missing seam fails at build, not on the first call."""
    # Act & Assert
    with pytest.raises(GraphMissingFactoryError):
        make_async_container(GrpcClientSettingsProvider(make_settings()), AsyncGrpcClientProvider())


async def test__async_grpc_client_provider__registered_by_hand__resolves_like_the_bundle(
    container_factory: ContainerFactory,
) -> None:
    # Arrange
    container = await container_factory(
        GrpcClientSettingsProvider(make_settings()),
        PrometheusGrpcClientMetricsProvider(),
        AsyncGrpcClientProvider(),
    )

    # Act
    factory = await container.get(GrpcClientFactory)

    # Assert
    assert isinstance(factory, GrpcClientFactory)


# --------------------------------------------------------------------------------------------
# The collector: handed out when enabled, once per process.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("enabled", [True, False])
async def test__prometheus_grpc_client_metrics_provider__metrics_enabled_flag__toggles_the_collector(
    container_factory: ContainerFactory, enabled: bool
) -> None:
    """settings.metrics_enabled decides; None is the kit's own spelling of "no metrics"."""
    # Arrange
    container = await container_factory(
        *grpc_client_providers(make_settings(metrics_enabled=enabled), metrics_prefix="dishka_toggle_test")
    )

    # Act
    metrics = await container.get(METRICS_KEY)

    # Assert
    assert isinstance(metrics, GrpcClientMetrics) is enabled


async def test__prometheus_grpc_client_metrics_provider__extra_missing__provides_none_and_says_so(
    container_factory: ContainerFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """Without prometheus-client the provider degrades like the chain builder does, never raises."""
    # Arrange
    container = await container_factory(*grpc_client_providers(make_settings(metrics_enabled=True)))

    # Act
    with patch("grpc_client_kit.dishka.HAS_METRICS", False):
        metrics = await container.get(METRICS_KEY)

    # Assert
    assert metrics is None
    assert "grpc-client-kit[metrics] is not installed" in caplog.text


async def test__grpc_client_providers__container_rebuilt__hands_out_the_collector_it_registered_before() -> None:
    """The reporter's pain: a test suite rebuilding the container must not register the series twice."""

    # Arrange
    def providers() -> tuple[Provider, ...]:
        return grpc_client_providers(make_settings(metrics_enabled=True), metrics_prefix="dishka_rebuild")

    # Act
    first_container = make_async_container(*providers())
    first = await first_container.get(METRICS_KEY)
    await first_container.close()
    second_container = make_async_container(*providers())
    second = await second_container.get(METRICS_KEY)
    await second_container.close()

    # Assert
    assert isinstance(first, GrpcClientMetrics)
    assert second is first


async def test__async_grpc_client_provider__metrics_enabled__wires_the_collector_into_every_client(
    container_factory: ContainerFactory,
) -> None:
    """The provided registry is the one the RPC metrics layer records into, not just the pool."""
    # Arrange
    container = await container_factory(
        *grpc_client_providers(make_settings(metrics_enabled=True), metrics_prefix="dishka_wiring_test")
    )
    factory = await container.get(GrpcClientFactory)

    # Act
    client = factory.create_client(make_stub_class("Stub"))

    # Assert
    metrics_layer = layers_of(client, TARGET, AsyncClientMetricsInterceptor)[0]
    assert metrics_layer._metrics is await container.get(METRICS_KEY)


# --------------------------------------------------------------------------------------------
# Several upstreams: one component each.
# --------------------------------------------------------------------------------------------


async def test__grpc_client_providers__two_upstreams_in_two_components__are_two_factories(
    container_factory: ContainerFactory,
) -> None:
    """A service with several clients registers one bundle per upstream, each in its own component."""
    # Arrange
    container = await container_factory(
        *grpc_client_providers(make_settings(target="users:50051"), component="users"),
        *grpc_client_providers(make_settings(target="orders:50051"), component="orders"),
    )

    # Act
    users = await container.get(GrpcClientFactory, component="users")
    orders = await container.get(GrpcClientFactory, component="orders")

    # Assert
    assert users is not orders
    assert users._settings is not None and users._settings.target == "users:50051"
    assert orders._settings is not None and orders._settings.target == "orders:50051"


# --------------------------------------------------------------------------------------------
# The extra.
# --------------------------------------------------------------------------------------------


def test__dishka_module__extra_missing__raises_the_install_hint() -> None:
    """Without dishka the import fails naming the extra, not with a bare ModuleNotFoundError."""
    # Arrange
    with patch.dict("sys.modules", {"dishka": None}):
        sys.modules.pop("grpc_client_kit.dishka", None)

        # Act & Assert
        with pytest.raises(ImportError, match=r"grpc-client-kit\[dishka\]"):
            importlib.import_module("grpc_client_kit.dishka")
