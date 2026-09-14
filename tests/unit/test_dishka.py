"""Unit tests for the Dishka providers: the lifecycle they own, and what a container hands out.

Regression cover for issue #28: every consumer wrote the pool lifecycle themselves — build it from
the settings, park it on an exit stack, close it with the container — and the collector next to it
registered twice when a test suite rebuilt the container. These pin that a container with the bundle
and a ``BaseGrpcClientSettings`` builds without a server, that closing the container closes the pool,
that a second container does not raise, and that two upstreams are two components.

And for issue #31: several upstreams in one container could not share a pool, since the factory
provider always built its own. These pin that a pool provided in the default component is the one
every ``shared_pool`` factory borrows, that the container closes it once and after the factories, and
that a bundle registered as before still owns a pool of its own.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from functools import partial
from typing import Any
from unittest.mock import patch

import pytest
from dishka import AsyncContainer, Provider, make_async_container
from dishka.exceptions import GraphMissingFactoryError

from grpc_client_kit import ChannelPool, GrpcClientFactory
from grpc_client_kit.channel import DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_CHANNELS_PER_TARGET
from grpc_client_kit.dishka import (
    AsyncChannelPoolProvider,
    AsyncGrpcClientProvider,
    GrpcClientSettingsProvider,
    PrometheusGrpcClientMetricsProvider,
    grpc_client_providers,
)
from grpc_client_kit.interceptors.metrics import AsyncClientMetricsInterceptor
from grpc_client_kit.metrics import GrpcClientMetrics, get_grpc_client_metrics
from grpc_client_kit.protocols import GrpcClientMetricsProtocol, GrpcClientSettingsProtocol
from grpc_client_kit.settings import BaseChannelPoolSettings, BaseGrpcClientSettings

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
# One pool for several upstreams: provided in the default component, borrowed by every bundle told to.
# --------------------------------------------------------------------------------------------


async def test__grpc_client_providers__two_components_on_a_shared_pool__are_two_factories_on_one_pool(
    container_factory: ContainerFactory,
) -> None:
    """The reporter's layout: one pool in the default component, one bundle per upstream borrowing it."""
    # Arrange
    container = await container_factory(
        AsyncChannelPoolProvider(BaseChannelPoolSettings(max_channels_per_target=2, idle_timeout=0)),
        *grpc_client_providers(make_settings(target="users:50051"), component="users", shared_pool=True),
        *grpc_client_providers(make_settings(target="orders:50051"), component="orders", shared_pool=True),
    )

    # Act
    pool = await container.get(ChannelPool)
    users = await container.get(GrpcClientFactory, component="users")
    orders = await container.get(GrpcClientFactory, component="orders")

    # Assert
    assert users is not orders
    assert users._pool is orders._pool is pool
    assert not users._owns_pool and not orders._owns_pool
    assert pool._max_channels_per_target == 2


async def test__async_channel_pool_provider__container_closed__closes_the_shared_pool_once_after_the_factories() -> (
    None
):
    """The container owns the pool: drained once, with the pool provider's grace, once every factory has left."""
    # Arrange
    container = make_async_container(
        AsyncChannelPoolProvider(shutdown_grace=2.5),
        *grpc_client_providers(make_settings(target="users:50051"), component="users", shared_pool=True),
        *grpc_client_providers(make_settings(target="orders:50051"), component="orders", shared_pool=True),
    )
    pool = await container.get(ChannelPool)
    users = await container.get(GrpcClientFactory, component="users")
    orders = await container.get(GrpcClientFactory, component="orders")
    await users.create_client(make_stub_class("Stub")).connect()
    await orders.create_client(make_stub_class("Stub")).connect()
    pooled_before_close = len(pool._entries)
    closed: list[str] = []

    async def closing(name: str, original: Callable[..., Awaitable[None]], **kwargs: Any) -> None:
        closed.append(name)
        await original(**kwargs)

    # Act
    with (
        patch.object(users, "close", side_effect=partial(closing, "users", users.close)),
        patch.object(orders, "close", side_effect=partial(closing, "orders", orders.close)),
        patch.object(pool, "close_all", side_effect=partial(closing, "pool", pool.close_all)) as close_all,
    ):
        await container.close()

    # Assert
    assert pooled_before_close == 2
    assert closed == ["orders", "users", "pool"]
    close_all.assert_awaited_once_with(grace=2.5)
    assert pool._entries == {}


async def test__async_grpc_client_provider__shared_pool__closing_the_factory_leaves_the_pool_open(
    container_factory: ContainerFactory,
) -> None:
    """A borrowing factory closes nothing but its checker, exactly what `GrpcClientFactory(pool=)` promises."""
    # Arrange
    container = await container_factory(
        AsyncChannelPoolProvider(), *grpc_client_providers(make_settings(), shared_pool=True)
    )
    pool = await container.get(ChannelPool)
    factory = await container.get(GrpcClientFactory)
    await factory.create_client(make_stub_class("Stub")).connect()

    # Act
    await factory.close()

    # Assert
    assert len(pool._entries) == 1


async def test__grpc_client_providers__component_without_shared_pool__still_builds_its_own(
    container_factory: ContainerFactory,
) -> None:
    """Opt-in per upstream: a bundle registered as before keeps the pool it owns, next to the shared one."""
    # Arrange
    container = await container_factory(
        AsyncChannelPoolProvider(),
        *grpc_client_providers(make_settings(target="users:50051"), component="users", shared_pool=True),
        *grpc_client_providers(make_settings(target="risk:50051"), component="risk"),
    )

    # Act
    pool = await container.get(ChannelPool)
    users = await container.get(GrpcClientFactory, component="users")
    risk = await container.get(GrpcClientFactory, component="risk")

    # Assert
    assert users._pool is pool
    assert risk._pool is not pool
    assert risk._owns_pool


async def test__async_grpc_client_provider__shared_pool_without_a_pool_provider__is_refused_at_container_build() -> (
    None
):
    """The pool is requested, never defaulted: a missing one fails at build, not on the first call."""
    # Act & Assert
    with pytest.raises(GraphMissingFactoryError):
        make_async_container(*grpc_client_providers(make_settings(), component="users", shared_pool=True))


async def test__async_channel_pool_provider__settings_and_registry__size_the_pool_and_feed_its_statistics(
    container_factory: ContainerFactory,
) -> None:
    """One deployment-wide pool section, and the collector the pool gauges are reported into."""
    # Arrange
    metrics = get_grpc_client_metrics("dishka_pool_test")
    container = await container_factory(
        AsyncChannelPoolProvider(BaseChannelPoolSettings(max_channels_per_target=3, idle_timeout=0), metrics=metrics)
    )

    # Act
    pool = await container.get(ChannelPool)

    # Assert
    assert pool._max_channels_per_target == 3
    assert pool._idle_timeout == 0
    assert pool._metrics is metrics


async def test__async_channel_pool_provider__no_settings__is_the_pool_at_its_own_defaults(
    container_factory: ContainerFactory,
) -> None:
    # Arrange
    container = await container_factory(AsyncChannelPoolProvider())

    # Act
    pool = await container.get(ChannelPool)

    # Assert
    assert pool._max_channels_per_target == DEFAULT_MAX_CHANNELS_PER_TARGET
    assert pool._idle_timeout == DEFAULT_IDLE_TIMEOUT
    assert pool._metrics is None


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
