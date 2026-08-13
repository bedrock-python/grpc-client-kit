# Quick start

## A client in three objects

```python
import asyncio

from grpc_client_kit import ChannelPool, GrpcClient, GrpcClientConfig


async def main() -> None:
    async with ChannelPool() as pool:
        client = GrpcClient(
            GreeterStub,
            config=GrpcClientConfig(target="localhost:50051", insecure=True),
            pool=pool,
        )
        async with client as stub:
            print(await stub.Greet(GreetRequest(name="world")))


asyncio.run(main())
```

Three objects with three different lifetimes:

- **`ChannelPool`** owns the channels. Create one per process and share it;
  leaving its `async with` block closes every channel it opened.
- **`GrpcClientConfig`** describes how a channel to this service is built —
  address, security, options, compression.
- **`GrpcClient`** owns nothing. It picks a target, asks the pool for the
  matching channel and constructs a stub, so creating one per request is
  cheap and closing one closes nothing.

`async with client as stub` yields the stub, not the client. Its `__aexit__`
deliberately leaves the channel alone: channels belong to the pool, and idle
connections are parked by gRPC core rather than closed. See
[Channels & pooling](channels.md) for who closes what.

## Letting the factory wire it

Assembling a pool, a balancer, a health checker and an interceptor chain by
hand gets repetitive across a dozen upstream services. `GrpcClientFactory`
does it from one settings object:

```python
from grpc_client_kit import GrpcClientFactory

async with GrpcClientFactory(settings=settings) as factory:
    users = factory.create_client(UserStub)
    orders = factory.create_client(OrderStub, service_name="orders.v1.Orders")

    async with users as stub:
        await stub.GetUser(request)
```

Every client from one factory shares its pool, and recreating a client for
the same stub reuses the same chain and channel — a per-request DI scope does
not open a connection per request. Entering the factory's `async with`
**starts health checking and waits for its first pass** (bounded by
`ready_timeout`): until that pass every target reads unhealthy by design, so
returning earlier would make the first call of every freshly started pod fail
deterministically. A factory used without `async with` logs a warning and
keeps routing traffic to targets nobody has probed.

Leaving the block stops the checker and — only if the factory created the pool
itself — closes it, giving in-flight RPCs `shutdown_grace` seconds (5 by
default) to finish rather than cancelling them mid-flight: a k8s SIGTERM lands
exactly here. Hand in a pool of your own (`GrpcClientFactory(pool=pool)`) and
the factory borrows it without ever closing it.

The settings object is validated **at construction**: one missing required
field raises a `TypeError` naming it, instead of a bare `AttributeError` on
the first RPC in production.

See [Configuration](configuration.md) for the settings object the factory
reads.

## Handling failures

Failures come in two families, and the kit keeps them apart. The server said
no: a plain `grpc.aio.AioRpcError` carrying the server's status. The **kit**
said no — an open breaker, an exhausted [deadline budget](deadlines.md), a
balancer with nothing healthy left, a checker asked before it started — and
every one of those derives from `GrpcClientKitError`:

```python
import grpc.aio

from grpc_client_kit import (
    CircuitBreakerOpenError,
    GrpcClientKitError,
    NoHealthyTargetsError,
)

try:
    async with client as stub:
        return await stub.GetUser(request)
except NoHealthyTargetsError as exc:
    ...  # nothing to call: exc.targets lists what was tried
except CircuitBreakerOpenError:
    ...  # this method is tripped; fail fast, serve a fallback
except GrpcClientKitError:
    ...  # any other local refusal — the network was never the problem
except grpc.aio.AioRpcError as exc:
    if exc.code() is grpc.StatusCode.DEADLINE_EXCEEDED:
        ...  # the whole call ran out of budget, retries included
    raise
```

The two kit errors that stand in for an RPC outcome —
`CircuitBreakerOpenError` and `DeadlineBudgetExhaustedError` — additionally
**are** `AioRpcError`s (carrying `UNAVAILABLE` and `DEADLINE_EXCEEDED`), so
existing handlers and every logging, metrics and tracing layer keep seeing
them as the call failures they are. Order the `except` clauses narrowest
first; the retry layer recognizes a tripped circuit and never retries it,
even though `UNAVAILABLE` is otherwise retryable.

## Where to go next

| Page | What it covers |
| :--- | :--- |
| [Configuration](configuration.md) | `GrpcClientConfig`, the settings protocols, the extras |
| [Channels & pooling](channels.md) | Channel identity, pool limits, who closes what |
| [Interceptors](interceptors.md) | The chain, its order, and writing your own |
| [Resilience](resilience.md) | Timeout budgets, waiting for a connection, retry safety, the breaker |
| [Deadline budgets](deadlines.md) | Propagating the caller's remaining time into every hop |
| [Load balancing](load-balancing.md) | Strategies and how health narrows them |
| [Health checking](health.md) | The probe loop, cold start, backoff |
| [Observability](observability.md) | What the logs, metrics and spans actually contain |
