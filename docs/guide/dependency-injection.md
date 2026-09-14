# Dependency injection

The pool is application-scoped, the factory too, and clients are cheap enough
to be request-scoped. What a container has to get right is the lifecycle at
the two ends: start the health checker once the factory is resolved, and close
the pool when the container closes. `grpc_client_kit.dishka` (the `dishka`
extra) ships that as providers, the way `grpc_server_kit.aio.dishka` does for
the server.

## The providers

```python
from dishka import make_async_container

from grpc_client_kit import GrpcClientFactory
from grpc_client_kit.dishka import grpc_client_providers

container = make_async_container(
    *grpc_client_providers(settings.users_grpc),
    MyDomainProvider(),
)

factory = await container.get(GrpcClientFactory)  # enters the factory: starts the checker
users = factory.create_client(UserStub)
...
await container.close()  # leaves it: stops the checker, closes the pool
```

`grpc_client_providers(settings, *, component=None, metrics_prefix=None,
shutdown_grace=5.0, ready_timeout=10.0)` returns the three providers below,
which can also be registered one by one:

| Provider | Provides | Notes |
| :--- | :--- | :--- |
| `GrpcClientSettingsProvider(settings)` | `GrpcClientSettingsProtocol` | Holds the settings object: [`BaseGrpcClientSettings`](configuration.md#from-the-environment) or anything structural |
| `PrometheusGrpcClientMetricsProvider(prefix=None)` | `GrpcClientMetricsProtocol \| None` | The [shipped collector](observability.md#the-shipped-collector) when `settings.metrics_enabled`, `None` otherwise |
| `AsyncGrpcClientProvider(shutdown_grace=5.0, ready_timeout=10.0)` | `GrpcClientFactory` | APP scope, from an async generator: entered on first resolution, left when the container closes |

The factory provider *requests* the settings and the registry through their
protocols rather than taking them in its constructor, so a settings object
provided by your own provider works just as well as `GrpcClientSettingsProvider`,
and a registry of your own replaces the Prometheus one by providing
`GrpcClientMetricsProtocol | None` instead. What is not done for you is
defaulting the registry: a container with `AsyncGrpcClientProvider` and no
provider of `GrpcClientMetricsProtocol | None` is refused when it is built,
which is the moment to find that out.

All of it is `async with GrpcClientFactory(...)` in the container's terms.
Resolving `GrpcClientFactory` enters the factory — with a `health_checker`
block that starts the probes and waits up to `ready_timeout` for the first
pass, for [the reason the factory does](health.md) — and closing the container
leaves it, giving in-flight RPCs `shutdown_grace` before the pool's channels
are closed. Clients are created from the resolved factory with
`create_client` exactly as without a container; a per-request client is
fine, since [recreating one is cheap](quickstart.md#letting-the-factory-wire-it).

## Metrics

`PrometheusGrpcClientMetricsProvider` hands out `get_grpc_client_metrics(prefix)`:
one collector per prefix on the process-wide registry, whatever the number of
containers. That is what keeps a test suite that builds a container per test
from hitting Prometheus's "duplicated timeseries" error on the second one —
the collector is registered once and handed back afterwards. When
`settings.metrics_enabled` is off the provider yields `None`, and the factory
[leaves the metrics layer out](observability.md#turning-layers-on); when it is
on but the `metrics` extra is absent, it yields `None` and logs a warning, so
the provider can always be registered.

Starting the `/metrics` HTTP server is the application's job, as with the
server kit: `prometheus_client.start_http_server(port)` once at startup, not
in a provider.

## Several upstreams

One upstream, one settings object, one factory: that is what the default
component gives you. A service with several clients registers one bundle per
upstream, each in its own Dishka component:

```python
from typing import Annotated

from dishka import FromComponent, make_async_container

container = make_async_container(
    *grpc_client_providers(settings.users_grpc, component="users"),
    *grpc_client_providers(settings.orders_grpc, component="orders"),
)

users = await container.get(GrpcClientFactory, component="users")
orders = await container.get(GrpcClientFactory, component="orders")


class Checkout:
    def __init__(self, orders: Annotated[GrpcClientFactory, FromComponent("orders")]) -> None: ...
```

Dishka resolves a component's dependencies inside that component, which is
why every provider takes `component=` and the bundle registers all three
there: the `users` factory reads the `users` settings and nothing else. The
collector is the exception by design — it is cached per prefix, so both
components hand out the same instance, and the two upstreams' series are told
apart by the `service` label rather than by separate metrics.

## By hand, or with another container

Nothing here is Dishka-specific beyond the provider classes. Any container
that can express "one instance per process, closed at shutdown" runs the same
two `async with` blocks; this is what the providers do, spelled out:

```python
from collections.abc import AsyncIterable

from dishka import Provider, Scope, provide

from grpc_client_kit import GrpcClient, GrpcClientFactory
from grpc_client_kit.metrics import get_grpc_client_metrics


class GrpcProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_factory(self, settings: UpstreamSettings) -> AsyncIterable[GrpcClientFactory]:
        async with GrpcClientFactory(settings=settings, metrics=get_grpc_client_metrics()) as factory:
            yield factory

    @provide(scope=Scope.REQUEST)
    def get_user_client(self, factory: GrpcClientFactory) -> GrpcClient[UserStub]:
        return factory.create_client(UserStub, target="user-service:50051")
```

The `async with` is what matters: entering it starts and stops
[health checking](health.md), and leaving it closes the pool the factory
created — a factory handed a `pool=` of its own
[borrows it and closes nothing](channels.md#ownership-and-shutdown).
