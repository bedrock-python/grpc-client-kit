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
shutdown_grace=5.0, ready_timeout=10.0, shared_pool=False)` returns the first
three providers below, which can also be registered one by one; the fourth is
registered on its own, by the containers whose upstreams
[share one pool](#one-pool-for-several-upstreams):

| Provider | Provides | Notes |
| :--- | :--- | :--- |
| `GrpcClientSettingsProvider(settings)` | `GrpcClientSettingsProtocol` | Holds the settings object: [`BaseGrpcClientSettings`](configuration.md#from-the-environment) or anything structural |
| `PrometheusGrpcClientMetricsProvider(prefix=None)` | `GrpcClientMetricsProtocol \| None` | The [shipped collector](observability.md#the-shipped-collector) when `settings.metrics_enabled`, `None` otherwise |
| `AsyncGrpcClientProvider(shutdown_grace=5.0, ready_timeout=10.0, shared_pool=False)` | `GrpcClientFactory` | APP scope, from an async generator: entered on first resolution, left when the container closes; `shared_pool=True` borrows the default component's `ChannelPool` instead of building one |
| `AsyncChannelPoolProvider(settings=None, metrics=None, shutdown_grace=5.0)` | `ChannelPool` | APP scope, from an async generator: the pool several upstreams share, sized by a `ChannelPoolSettingsProtocol`, drained with `shutdown_grace` when the container closes |

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

### One pool for several upstreams

Registered that way, each component's factory builds a pool of its own, as it
does outside a container. Upstreams whose retry and timeout policy differ but
whose channels should sit in one pool — one set of
[pool statistics](observability.md#pool-statistics), one section configuring
it — register the pool once, in the default component, and tell each bundle to
borrow it:

```python
from grpc_client_kit.dishka import AsyncChannelPoolProvider, grpc_client_providers
from grpc_client_kit.metrics import get_grpc_client_metrics

container = make_async_container(
    AsyncChannelPoolProvider(settings.grpc_pool, metrics=get_grpc_client_metrics()),
    *grpc_client_providers(settings.users_grpc, component="users", shared_pool=True),
    *grpc_client_providers(settings.orders_grpc, component="orders", shared_pool=True),
)

pool = await container.get(ChannelPool)  # the one both factories draw from
```

`AsyncChannelPoolProvider` provides `ChannelPool` in APP scope the way the
factory provider provides the factory: built on first resolution, drained with
`close_all(grace=shutdown_grace)` when the container closes. It is sized by a
`ChannelPoolSettingsProtocol` — a
[`BaseChannelPoolSettings`](configuration.md#from-the-environment) nested at
the top of the service's settings is one deployment-wide section,
`GRPC_POOL__MAX_CHANNELS_PER_TARGET` and `GRPC_POOL__IDLE_TIMEOUT`, next to
the per-upstream ones — and is at the pool's own defaults without one.
`metrics=` is the registry its statistics
are reported into, handed in rather than requested: a container with one
component per upstream has a registry per component and none in the default
one, so there is no single seam for the pool to ask; `get_grpc_client_metrics()`
is the instance the bundles hand out.

A factory with `shared_pool=True` resolves `ChannelPool` from the default
component (`Annotated[ChannelPool, FromComponent("")]`, so anything providing
`ChannelPool` there will do) and is `GrpcClientFactory(pool=...)`: it
[borrows the pool and never closes it](channels.md#ownership-and-shutdown),
and owns nothing but its health checker. The container closes generators in
reverse order of entry, so every factory leaves — stopping its checker —
before the pool is drained, and the pool is drained exactly once. The bundle's
`shutdown_grace` then has nothing to apply to; the pool provider's is the one
in-flight RPCs get. The upstream's own `pool` block is not read either: the
pool was built before the factory asked.

Three things stay per upstream. The settings and the policy they carry, which
is the point. The health checker, because it probes that upstream's
`targets`; two health-checked upstreams sharing a pool are two checkers, each
[marking its own addresses](health.md) in the one pool. And the choice: a
bundle registered without `shared_pool` next to shared ones builds and owns
its own pool as before. A `shared_pool=True` bundle in a container with no
`ChannelPool` in the default component is refused when the container is built,
like a missing registry seam.

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
