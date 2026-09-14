"""Dishka providers owning the client lifecycle (``[dishka]`` extra).

What an application otherwise writes by hand — build the factory from the settings, start the health
checker with the container, close the pool with it — lives here as three providers, one per thing a
container hands out, and a bundle registering all three::

    from dishka import make_async_container

    from grpc_client_kit import GrpcClientFactory
    from grpc_client_kit.dishka import grpc_client_providers

    container = make_async_container(*grpc_client_providers(settings.users_grpc))
    factory = await container.get(GrpcClientFactory)
    users = factory.create_client(UserStub)
    ...
    await container.close()  # stops the checker, closes the pool

- `GrpcClientSettingsProvider` holds the settings object and provides it as
  `protocols.GrpcClientSettingsProtocol`, so `settings.BaseGrpcClientSettings` or anything structural
  works.
- `PrometheusGrpcClientMetricsProvider` provides ``GrpcClientMetricsProtocol | None``: the cached
  `metrics.GrpcClientMetrics` when ``settings.metrics_enabled``, ``None`` otherwise — ``None`` being
  the kit's own spelling of "no metrics", which the factory leaves the layer out for.
- `AsyncGrpcClientProvider` provides `factory.GrpcClientFactory` in APP scope from an async generator,
  entered when first resolved and left when the container closes.

Several upstreams in one container are Dishka components: every provider takes ``component=``, so
one bundle per upstream, each in its own component, is one factory per upstream::

    container = make_async_container(
        *grpc_client_providers(settings.users_grpc, component="users"),
        *grpc_client_providers(settings.orders_grpc, component="orders"),
    )
    users = await container.get(GrpcClientFactory, component="users")

Dishka resolves a component's dependencies inside that component, which is why each one carries its
own three providers; the collector they hand out is the same instance, cached per prefix, and its
series are told apart by ``service``. A single upstream uses the default component and needs none of
this.

This module needs the ``dishka`` extra (``grpc-client-kit[dishka]``); importing it without raises an
``ImportError`` naming the extra.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

try:
    from dishka import Provider, Scope, provide
except ImportError as exc:
    raise ImportError("Install grpc-client-kit[dishka] (dishka) to use the Dishka providers") from exc

from .factory import GrpcClientFactory
from .interceptors.metrics import HAS_METRICS
from .protocols import GrpcClientMetricsProtocol, GrpcClientSettingsProtocol

logger = logging.getLogger(__name__)


class GrpcClientSettingsProvider(Provider):
    """Provide one upstream's settings object as ``GrpcClientSettingsProtocol``.

    Construct it with anything structurally satisfying the protocol — `settings.BaseGrpcClientSettings`,
    a dataclass, a plain class. The other providers request the settings through the protocol, which
    is what lets a second upstream be a second component rather than a second container.
    """

    scope = Scope.APP

    def __init__(self, settings: GrpcClientSettingsProtocol, *, component: str | None = None) -> None:
        super().__init__(component=component)
        self._settings = settings

    @provide
    def settings(self) -> GrpcClientSettingsProtocol:
        """Provide the settings object as it was given."""
        return self._settings


class PrometheusGrpcClientMetricsProvider(Provider):
    """Provide the registry the factory records into: `metrics.GrpcClientMetrics`, or ``None``.

    ``None`` when ``settings.metrics_enabled`` is off, and ``None`` with a warning when it is on but
    the ``[metrics]`` extra is absent — the degradation the chain builder applies, so the provider can
    always be registered. The collector comes from `metrics.get_grpc_client_metrics`, one instance per
    prefix on the default registry, so a container rebuilt per test never asks Prometheus to register
    the same series twice.
    """

    scope = Scope.APP

    def __init__(self, *, prefix: str | None = None, component: str | None = None) -> None:
        super().__init__(component=component)
        self._prefix = prefix

    @provide
    def metrics(self, settings: GrpcClientSettingsProtocol) -> GrpcClientMetricsProtocol | None:
        """Provide the collector when metrics are enabled and the extra is installed, ``None`` otherwise."""
        if not settings.metrics_enabled:
            return None
        if not HAS_METRICS:
            logger.warning("Metrics requested but grpc-client-kit[metrics] is not installed; metrics are disabled")
            return None
        from .metrics import get_grpc_client_metrics  # noqa: PLC0415 - lazy: needs the [metrics] extra

        return get_grpc_client_metrics(self._prefix)


class AsyncGrpcClientProvider(Provider):
    """Provide `factory.GrpcClientFactory` in APP scope, entered on first use and closed with the container.

    The factory builds and owns the pool and the health checker: resolving it starts the checker and
    waits for its first pass (``ready_timeout``); closing the container stops the checker and closes
    the pool, giving in-flight RPCs ``shutdown_grace`` — exactly what ``async with factory`` does.
    Clients come from the factory as usual, ``factory.create_client(UserStub)``, and are cheap enough
    to be request-scoped.
    """

    scope = Scope.APP

    def __init__(
        self,
        *,
        shutdown_grace: float | None = 5.0,
        ready_timeout: float | None = 10.0,
        component: str | None = None,
    ) -> None:
        super().__init__(component=component)
        self._shutdown_grace = shutdown_grace
        self._ready_timeout = ready_timeout

    @provide
    async def factory(
        self,
        settings: GrpcClientSettingsProtocol,
        metrics: GrpcClientMetricsProtocol | None,
    ) -> AsyncIterator[GrpcClientFactory]:
        """Build the factory from the settings and the registry, and close it with the container."""
        async with GrpcClientFactory(
            settings=settings,
            shutdown_grace=self._shutdown_grace,
            ready_timeout=self._ready_timeout,
            metrics=metrics,
        ) as factory:
            yield factory


def grpc_client_providers(
    settings: GrpcClientSettingsProtocol,
    *,
    component: str | None = None,
    metrics_prefix: str | None = None,
    shutdown_grace: float | None = 5.0,
    ready_timeout: float | None = 10.0,
) -> tuple[Provider, ...]:
    """Return the three providers of one upstream, for one-line registration.

    Args:
        settings: The upstream's settings object; anything satisfying `protocols.GrpcClientSettingsProtocol`.
        component: The Dishka component to register the providers in — one per upstream when a
            container serves several, the default component otherwise.
        metrics_prefix: Prefix for the Prometheus metric names.
        shutdown_grace: Seconds in-flight RPCs get to finish when the container closes.
        ready_timeout: Seconds the factory waits for the first health check pass when resolved.

    Returns:
        The providers, to unpack into ``make_async_container``.
    """
    return (
        GrpcClientSettingsProvider(settings, component=component),
        PrometheusGrpcClientMetricsProvider(prefix=metrics_prefix, component=component),
        AsyncGrpcClientProvider(shutdown_grace=shutdown_grace, ready_timeout=ready_timeout, component=component),
    )


__all__ = [
    "AsyncGrpcClientProvider",
    "GrpcClientSettingsProvider",
    "PrometheusGrpcClientMetricsProvider",
    "grpc_client_providers",
]
