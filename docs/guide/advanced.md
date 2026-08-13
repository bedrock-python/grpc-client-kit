# Advanced

Three topics that belong to no single layer.

## Target validation

Targets are validated before a channel is created, because a malformed target
is otherwise reported only as an opaque `UNAVAILABLE` at the first RPC, long
after the misconfiguration was introduced. `ChannelPool.get_channel`, the
balancers and `HealthChecker.start` all validate.

Accepted: `host:port` (DNS name or IPv4 literal), `[ipv6]:port` with brackets,
and the resolver URIs `dns:`, `ipv4:`, `ipv6:`, `unix:`, `unix-abstract:`,
`vsock:`, `xds:`, `google-c2p:` — with or without an `//authority/` part.

Validation is stricter than gRPC in one respect: **a port is always required.**
gRPC silently falls back to 443 for a portless target, which for a plaintext
service turns a typo into a connection to the wrong port. An explicit
`scheme://` that no gRPC resolver understands (typically `http://` copied out
of a REST config) is rejected by name. Underscores in host labels are
tolerated: they are invalid per RFC 1123 but common in container and Compose
service names.

`grpc_client_kit.validation.validate_target` is importable if you want to
check configuration at startup, before anything tries to connect.

## Writing against the plain grpc.aio API

Nothing forces a custom layer through
[`around_call`](interceptors.md#writing-a-custom-interceptor): an interceptor
written against `intercept_unary_unary` and friends still works and travels
through the chain's extra slots untouched. It just has to answer two things
`ClientCall` would have answered for it.

**A continuation resolves to a `Call`, not to a response.** It returns the
moment the RPC is *created*, never raises, and looks identical for a call that
will fail — so a layer built on `await continuation(...)` reports every call
as an instant success and times call creation rather than the call.

**A channel registers an interceptor by class, for one kind only.** Claiming
all four base classes gives you unary-unary and nothing else, silently
([how a chain reaches the channel](interceptors.md#how-a-chain-reaches-the-channel));
one class per kind is the alternative.

Two details hold either way: `client_call_details.method` may be `bytes`, and
`ClientCallDetails` is a `NamedTuple`-shaped structural type, so copy it with
`_replace` rather than mutating it. Keep instances stable — each becomes part
of the [channel identity](channels.md#channel-identity) — and pass a
hand-assembled chain through `flatten_interceptors` before binding it, since
gRPC rejects a logical interceptor with a `ValueError` rather than
half-registering it.

## Dependency injection

The pool is application-scoped, the factory usually too, and clients are cheap
enough to be request-scoped:

```python
from collections.abc import AsyncIterable

from dishka import Provider, Scope, provide

from grpc_client_kit import ChannelPool, GrpcClient, GrpcClientFactory


class GrpcProvider(Provider):
    @provide(scope=Scope.APP)
    async def get_pool(self) -> AsyncIterable[ChannelPool]:
        async with ChannelPool(max_channels_per_target=4) as pool:
            yield pool

    @provide(scope=Scope.APP)
    async def get_factory(self, pool: ChannelPool, settings: UpstreamSettings) -> AsyncIterable[GrpcClientFactory]:
        async with GrpcClientFactory(settings=settings, pool=pool) as factory:
            yield factory

    @provide(scope=Scope.REQUEST)
    def get_user_client(self, factory: GrpcClientFactory) -> GrpcClient[UserStub]:
        return factory.create_client(UserStub, target="user-service:50051")
```

Both `async with` blocks matter: the pool's closes the channels, the factory's
starts and stops [health checking](health.md). Nothing here is
Dishka-specific — any container that can express "one instance per process,
closed at shutdown" works the same way.
