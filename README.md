# grpc-client-kit

Batteries-optional async gRPC client toolkit (channel pool, load balancing, health checking, streaming-aware interceptors, observability)

[![PyPI](https://img.shields.io/pypi/v/grpc-client-kit?color=blue)](https://pypi.org/project/grpc-client-kit/)
[![Python](https://img.shields.io/pypi/pyversions/grpc-client-kit)](https://pypi.org/project/grpc-client-kit/)
[![License](https://img.shields.io/github/license/bedrock-python/grpc-client-kit)](LICENSE)
[![CI](https://github.com/bedrock-python/grpc-client-kit/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/bedrock-python/grpc-client-kit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bedrock-python/grpc-client-kit/graph/badge.svg)](https://codecov.io/gh/bedrock-python/grpc-client-kit)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://bedrock-python.github.io/grpc-client-kit/)

`grpc-client-kit` is the caller-side half of a production gRPC setup: a channel
pool keyed by the full identity of a channel, client-side load balancing,
active `grpc.health.v1` monitoring, and a fixed-order chain of streaming-aware
interceptors for retries, deadlines, circuit breaking, logging, tracing and
metrics.

The core depends only on `grpcio`. Every integration is an opt-in extra, so you
install exactly what you use.

> [!TIP]
> **Building this with an AI assistant?** Hand it
> **[one page](https://bedrock-python.github.io/grpc-client-kit/agents/)** instead of the
> whole site: the whole public API, who owns a channel and who closes it, what a timeout
> and a retry actually cover, which batteries are opt-in — plus the mistakes models make
> with this API and a map of which page to fetch for the rest. Every docs page is also
> served as raw Markdown at its own URL, and a **Copy page** button at the top of each one
> hands it straight to a chat window.

## Why grpc-client-kit

- **Channels are pooled by identity, not by address.** Target, security,
  options, compression *and* the interceptor chain form the pool key, so a
  client never inherits another client's retry policy or circuit breaker.
- **A timeout is the budget of a whole call, not of one attempt.** It becomes a
  deadline once, on entry, and the retry layer divides what is left between
  attempts — `max_attempts × timeout` is not a thing this kit does.
- **A deadline can survive the hop.** Install the caller's request budget once
  and every outgoing call is trimmed to what the request still has, instead of
  being issued with a fresh timeout five services deep. The kit supplies the
  mechanism; installing the budget stays the caller's decision.
- **A layer is one `around_call`, not four interceptor methods.** You write a
  single async generator; the kit expands it into the four adapters a channel
  registers, so streaming calls get the same treatment unary calls get.
- **A failed call marks its target down immediately.** Active probes learn
  about a dead backend one interval late; passive quarantine takes it out of
  the rotation on the first `UNAVAILABLE`, so an outage costs one call, not a
  full share of the traffic.
- **Local refusals are distinguishable from server failures.** Every failure
  the kit raises on its own authority derives from `GrpcClientKitError`, and
  a breaker rejection is `status="rejected"` in the metrics — never `"error"`
  — so "is the breaker open or is the backend down?" has an answer.
- **A bare install is a working install.** `import grpc_client_kit` never
  reaches for an extra: names that need one resolve on first access and raise
  an `ImportError` that says which extra to install.
- **What gRPC core does better stays with gRPC core.** Idle connections are
  parked by `grpc.client_idle_timeout_ms`, native `loadBalancingConfig` and
  `retryPolicy` pass through untouched, and the docs say plainly
  [when to use which](https://bedrock-python.github.io/grpc-client-kit/guide/native-vs-kit/).

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

from my_service_pb2 import GreetRequest
from my_service_pb2_grpc import GreeterStub


async def main() -> None:
    async with ChannelPool() as pool:  # owns the channels
        client = GrpcClient(  # owns nothing, cheap to create
            GreeterStub,
            config=GrpcClientConfig(target="localhost:50051", insecure=True),
            pool=pool,
        )
        async with client as stub:  # a stub on a pooled channel
            print(await stub.Greet(GreetRequest(name="world")))


asyncio.run(main())
```

`GrpcClient` is a stub factory, not a connection: it picks a target, asks the
pool for the matching channel, and hands you a stub. The pool owns the
channels and outlives every client built on it.

## The factory does the wiring

`GrpcClientFactory` maps one settings object onto the pool, the balancer, the
health checker and a per-target interceptor chain, so services declare
behaviour instead of assembling it:

```python
from grpc_client_kit import GrpcClientFactory

async with GrpcClientFactory(settings=settings) as factory:  # starts health checking
    users = factory.create_client(UserStub)  # both share the pool
    orders = factory.create_client(OrderStub)

    async with users as stub:
        await stub.GetUser(request)
```

Any object carrying the fields of `GrpcClientSettingsProtocol` works —
a pydantic model, a dataclass, a plain class. Everything optional
(`credentials`, `options`, `compression`, `metrics_registry`,
`sensitive_headers`) is read through `getattr`, so a minimal settings object
stays valid. See the
[configuration guide](https://bedrock-python.github.io/grpc-client-kit/guide/configuration/)
for the full field list.

## Channels are pooled by identity, not by address

A pooled channel is filed under **everything** gRPC baked into it at creation:
target, `insecure`, credentials, options, compression, and the identity of the
interceptor chain. Asking for `host:50051` over TLS never returns the insecure
channel someone opened for the same address, and a client never inherits
another client's interceptors.

That last field is the deliberate one: `grpc.aio` binds interceptors to a
channel when the channel is created and offers no way to attach them later. A
channel built for one chain therefore *cannot* serve another, so two clients
with different chains get different channels — the alternative is silently
running RPCs through somebody else's retry policy and circuit breaker.

`GrpcClient` caches the chain it builds per target for exactly this reason:
rebuilding it per call would mint a new channel identity every time, and the
pool would open a channel per RPC. Pooling, reuse and idle eviction are covered
in the [channels guide](https://bedrock-python.github.io/grpc-client-kit/guide/channels/).

`ConnectivityConfig` belongs to the same story. Keepalive pings and reconnect
backoff are channel arguments — fixed at creation, spelled in milliseconds,
and part of the pool key — so the kit takes them in seconds, validates them,
and composes them with your explicit `options` by dropping any derived argument
whose key you already used, rather than handing gRPC the same key twice. It is
opt-in: a server left at its own defaults answers a client that pings too often
with `GOAWAY` and `ENHANCE_YOUR_CALM`.

## The interceptor chain

`InterceptorChainBuilder` fixes the order, because where a layer sits decides
what it observes and what it repeats. Outermost first:

| # | Layer | Why here |
| :-: | :--- | :--- |
| 1 | extra outer (e.g. `AsyncClientContextInterceptor`) | metadata must be injected before the layers that correlate on it |
| 2 | `AsyncLoggingInterceptor` | one record per logical call, with `request-id` already attached |
| 3 | tracing | one CLIENT span covering the retries nested below it |
| 4 | metrics | latency as the caller experiences it, retry backoff included |
| 5 | `AsyncTimeoutInterceptor` | installs the budget for the **whole** call |
| 6 | `AsyncDeadlineBudgetInterceptor` | trims that budget to what the caller's *request* has left, before anything divides it |
| 7 | `AsyncWaitForReadyInterceptor` | decides whether a call waits for its connection, reading the deadline the two above settled |
| 8 | `AsyncRetryInterceptor` | divides the resulting budget between attempts |
| 9 | `AsyncCircuitBreakerInterceptor` | sees individual attempts, rejects tripped methods off-network |
| 10 | extra inner | custom layers that must re-run per attempt (credential refresh, ...) |

Layers 6 and 7 appear only when configured (`deadline_budget=`,
`wait_for_ready=`), and layer 6 also needs the `deadline` extra.

**Every layer covers all four call kinds.** A channel files an interceptor into
one of its four lists *by class*, so one object claiming all four kinds is
registered for unary-unary only and every streaming call runs past it. A chain
built here reaches the channel with each layer expanded into four adapters, one
per kind — which is why a stream gets the same log record, span, latency sample,
deadline, retry and breaker verdict a unary call gets, all of them landing when
the stream ends rather than when its first message arrives.

**A layer learns the outcome by awaiting the call.** In `grpc.aio` a
continuation resolves to a `Call` the moment the RPC is *created*, identically
for a call that will succeed and one that will fail, and it never raises. The
kit awaits that call, so a failure — including one halfway through a stream —
reaches every layer as the error it is, instead of being reported as an instant
success.

**A timeout is the budget of a whole call, not of one attempt.** The retry
interceptor converts it into a deadline once, on entry, issues every attempt
with the budget that is left, and abandons a retry whose backoff alone would
outlive it. `max_attempts × timeout` is not a thing this kit does; the
[resilience guide](https://bedrock-python.github.io/grpc-client-kit/guide/resilience/)
walks through deadlines, backoff and the breaker together.

## Your own layer is one async generator

```python
from collections import Counter
from collections.abc import AsyncIterator

import grpc.aio

from grpc_client_kit.interceptors import AsyncAroundClientInterceptor, ClientCall


class FailureCounter(AsyncAroundClientInterceptor):
    """Count the calls of each method that came back with an error."""

    def __init__(self) -> None:
        self.failures: Counter[str] = Counter()

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        try:
            yield  # the whole RPC happens here — a response stream to its last item
        except grpc.aio.AioRpcError:
            self.failures[call.method] += 1
            raise
```

Code before the `yield` runs before the RPC exists, so that is where call
details are rewritten and where raising refuses a call outright. Code after it
runs once the call is over, whichever of the four kinds it was, and cannot
change how it ended: what the teardown raises is logged and dropped. Both sides
of the `yield` run in one `contextvars.Context`, so `token = VAR.set(...)`
before it and `VAR.reset(token)` after it is a supported way to scope a request
id — or an OpenTelemetry context — to a single call. Layers that re-issue a
call — retries — subclass `AsyncClientInterceptor` and issue it themselves; see
the
[advanced guide](https://bedrock-python.github.io/grpc-client-kit/guide/advanced/).

**`INTERNAL` is not retryable by default.** `DEFAULT_RETRYABLE_CODES` holds
`UNAVAILABLE` and `RESOURCE_EXHAUSTED` only — both mean the request was
rejected before the server application saw it. `INTERNAL` is raised by the
handler itself, so retrying it duplicates whatever write it already performed.
Widen the set per method with `idempotent_methods`, not globally.

## Health checking

```python
from grpc_client_kit import HealthChecker, create_balancer  # HealthChecker needs [health]

checker = HealthChecker(check_interval=30.0, timeout=5.0)
await checker.start(["api-1:50051", "api-2:50051"])
await checker.wait_until_ready(timeout=5.0)  # await the first pass

balancer = create_balancer(targets=targets, health_checker=checker)
```

**An unchecked target is not a healthy target.** Health comes from evidence:
`is_healthy()` is `True` only after a check saw `SERVING`, and a checker that
was never started raises `HealthCheckerNotRunningError` instead of guessing.
On a cold start that means the balancer raises `NoHealthyTargetsError` until
the first pass lands — call `wait_until_ready()` after `start()` if the first
RPC must not see it.

`HealthChecker` lives behind the `[health]` extra. It is resolved lazily, so
`import grpc_client_kit` still works without it; touching
`grpc_client_kit.HealthChecker` on a bare install raises an `ImportError` that
names the extra to install.

## What's inside

| Module | Responsibility | Extra |
| :--- | :--- | :--- |
| `config.GrpcClientConfig` | Zero-dependency channel settings dataclass | core |
| `config.ConnectivityConfig` | Keepalive and reconnect backoff in seconds, composed into the channel options | core |
| `channel.ChannelPool` | Channels pooled by full identity, idle eviction, health flags | core |
| `client.GrpcClient` | Stub factory: target selection + channel acquisition | core |
| `factory.GrpcClientFactory` | Settings → pool, balancer, health checker, per-target chains | core |
| `balancers.*` | Round-robin / random / weighted selection with health filtering | core |
| `interceptors.*` | Context, logging, timeout, wait-for-ready, retry, circuit breaker + chain builder | core |
| `deadline` + `interceptors.deadline` | The request budget in a contextvar, and the layer that trims every call to it | `deadline` |
| `interceptors.tracing` | OpenTelemetry CLIENT spans and `traceparent` injection | `tracing` |
| `interceptors.metrics` | Counters, latency histograms and an in-flight gauge | core¹ |
| `health.HealthChecker` | Background `grpc.health.v1` probing with per-target backoff | `health` |
| `protocols` / `validation` / `utils` | Settings seams, target validation, channel and metadata helpers | core |

¹ The metrics interceptor records through `GrpcClientMetricsProtocol` and works
with any backend you pass. The `[metrics]` extra only supplies the usual one.

## Optional dependencies

| Extra | Pulls in | Enables |
| :--- | :--- | :--- |
| `health` | `grpcio-health-checking` | `HealthChecker`, health-aware balancing and pooling |
| `tracing` | `opentelemetry-api` | `AsyncClientTracingInterceptor` (a pass-through without it) |
| `metrics` | `prometheus-client` | the default metrics backend; a custom registry needs no extra |
| `deadline` | `deadline-budget` | `AsyncDeadlineBudgetInterceptor` (the layer is skipped without it) |
| `observability` | `metrics` + `tracing` | both of the above |
| `all` | `deadline` + `health` + `metrics` + `tracing` | everything |

`deadline` is deliberately not part of `observability`: propagating a budget is
resilience, not telemetry, and an observability extra should not pull in a
dependency that changes what calls do.

## Examples

Runnable, self-contained scripts in [`examples/`](examples/) — each one starts the servers
it needs on ephemeral ports, prints what it is doing, and exits on its own:

- [`minimal_client.py`](examples/minimal_client.py) — pool, config, client, stub, and what channel pooling buys you.
- [`custom_interceptor.py`](examples/custom_interceptor.py) — your own layer with `AsyncAroundClientInterceptor.around_call`: one generator covering unary calls, whole streams, mid-stream failures and refusing a call outright.
- [`resilience.py`](examples/resilience.py) — retries against a failing server, the call budget divided between attempts, and the `fail_threshold ≤ max_attempts` trap where a call opens its own circuit.
- [`deadline_propagation.py`](examples/deadline_propagation.py) — one request budget across several calls, measured as the *server* saw each deadline: shrinking deadlines, a call refused before the wire, per-method caps, and `wait_for_ready` waiting out a server that starts late (needs the `deadline` extra).
- [`health_and_balancing.py`](examples/health_and_balancing.py) — `HealthChecker` feeding both the pool and the balancer, routing that follows the verdicts, and `NoHealthyTargetsError` (needs the `health` extra).
- [`observability.py`](examples/observability.py) — the log records and metric samples an RPC actually produces, redaction and request-id correlation included.
- [`factory_setup.py`](examples/factory_setup.py) — one settings object driving the whole factory, and why a breaker per target is not the same as one breaker shared by all of them.

## Documentation

Full documentation at [bedrock-python.github.io/grpc-client-kit](https://bedrock-python.github.io/grpc-client-kit/).

| | |
| :--- | :--- |
| [Quick start](https://bedrock-python.github.io/grpc-client-kit/guide/quickstart/) | pool, config, client, stub — a first call end to end |
| [Configuration](https://bedrock-python.github.io/grpc-client-kit/guide/configuration/) | `GrpcClientConfig`, the settings protocols, and what the factory reads off them |
| [Channels](https://bedrock-python.github.io/grpc-client-kit/guide/channels/) | the pool, channel identity, reuse and idle eviction |
| [Interceptors](https://bedrock-python.github.io/grpc-client-kit/guide/interceptors/) | the fixed chain order, the four call kinds, writing your own with `around_call` |
| [Resilience](https://bedrock-python.github.io/grpc-client-kit/guide/resilience/) | call budgets, waiting for a connection, retry safety and circuit breaker isolation |
| [Deadline budgets](https://bedrock-python.github.io/grpc-client-kit/guide/deadlines/) | propagating the caller's remaining time into every hop |
| [Load balancing](https://bedrock-python.github.io/grpc-client-kit/guide/load-balancing/) | round-robin, random and weighted selection with health filtering |
| [Health](https://bedrock-python.github.io/grpc-client-kit/guide/health/) | `grpc.health.v1` probing, per-target backoff and cold starts |
| [Observability](https://bedrock-python.github.io/grpc-client-kit/guide/observability/) | log records, CLIENT spans and the metrics an RPC emits |
| [Advanced](https://bedrock-python.github.io/grpc-client-kit/guide/advanced/) | interceptors that re-issue calls, target validation, ownership and DI wiring |
| [API reference](https://bedrock-python.github.io/grpc-client-kit/reference/) | generated from the source |
| [For AI agents](https://bedrock-python.github.io/grpc-client-kit/agents/) | the whole API surface, the rules that break code when broken and a map of the rest, on one page to hand to a coding assistant |

## License

Apache 2.0 — see [LICENSE](LICENSE).
