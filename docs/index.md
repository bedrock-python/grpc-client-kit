# grpc-client-kit

Batteries-optional async gRPC client toolkit: a channel pool keyed by the full
identity of a channel, client-side load balancing, active `grpc.health.v1`
monitoring, and a fixed-order chain of streaming-aware interceptors for
retries, deadlines, circuit breaking, logging, tracing and metrics.

The core depends only on `grpcio`. `import grpc_client_kit` works on a bare
install; every integration is an opt-in extra, so you install exactly what you
use.

## Installation

```bash
pip install grpc-client-kit                       # core (grpcio only)
pip install "grpc-client-kit[health]"             # + grpc.health.v1 monitoring
pip install "grpc-client-kit[metrics,tracing]"    # + Prometheus + OpenTelemetry
pip install "grpc-client-kit[observability]"      # + metrics and tracing together
pip install "grpc-client-kit[deadline]"           # + request budget propagation
pip install "grpc-client-kit[all]"                # everything
```

**Requirements:** Python 3.12+

## Quick start

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

Continue with the [guide](guide/quickstart.md) for the factory, load
balancing, health checks and the interceptor chain.

## Why grpc-client-kit

- **Channels pooled by identity, not by address** — target, security,
  options, compression *and* the interceptor chain form the pool key, because
  `grpc.aio` binds all of them to a channel at creation. A client can never
  inherit another client's retry policy or circuit breaker. See
  [Channel identity](guide/channels.md#channel-identity).
- **A timeout is the budget of the whole call** — converted into a deadline
  once, then divided between retry attempts, so `max_attempts` can never
  multiply your deadline. See [Timeouts](guide/resilience.md#timeouts).
- **A deadline that survives the hop** — install the caller's request budget
  once and every outgoing call is trimmed to what the request still has,
  instead of being issued with its own fresh timeout five services deep. The
  kit supplies the mechanism; installing the budget stays the caller's call.
  See [Deadline budgets](guide/deadlines.md).
- **Retries that refuse to duplicate writes** — the default retryable set
  covers only requests the server never started processing; `INTERNAL` is
  deliberately absent. See [Retry safety](guide/resilience.md#retry-safety).
- **One chain per target** — stateful layers track one backend each, so a
  single failing member of a load-balanced set cannot trip the breaker for its
  healthy peers. See
  [Circuit breaker isolation](guide/resilience.md#circuit-breaker-isolation).
- **Health from evidence only** — an unchecked target is not a healthy
  target, and a checker that was never started says so instead of guessing.
  See [Health checking](guide/health.md).
- **Zero-dependency core** — `grpcio` only; observability integrates through
  SDK-free structural protocols.
