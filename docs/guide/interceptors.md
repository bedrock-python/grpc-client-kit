# Interceptors

Every feature this kit adds to a call — a deadline, a retry, a log record, a
span — is an interceptor. gRPC applies a client interceptor list from the
outside in: the first entry wraps the second, and the last one sits closest to
the wire.

## The chain

`InterceptorChainBuilder` fixes that order regardless of the sequence in which
its `with_*` methods were called, because the position of a layer decides what
it observes and what it repeats.

```python
from grpc_client_kit import (
    CircuitBreakerConfig,
    ObservabilityConfig,
    RetryConfig,
    TimeoutConfig,
    build_interceptors,
)

interceptors = build_interceptors(
    observability=ObservabilityConfig(service_name="orders.v1.Orders", logging=True, tracing=True),
    timeout=TimeoutConfig(default=10.0, per_method={"/orders.v1.Orders/Export": 120.0}),
    retry=RetryConfig(max_attempts=3),
    circuit_breaker=CircuitBreakerConfig(fail_threshold=5),
    extra_interceptors=[AsyncClientContextInterceptor(auth_metadata)],
    extra_inner_interceptors=[CredentialRefreshInterceptor()],
)
```

Outermost first:

1. **Extra outer** — metadata injection lives here. The logging layer
   correlates records by reading `request-id` off the call metadata, so
   anything injected further in is invisible to logs and spans, and running
   outermost keeps one identity across a call's retries.
2. **Logging** — one record per logical call, with the correlation metadata
   and the outcome the caller actually observed; retries are reported
   separately by the retry layer.
3. **Tracing** — one CLIENT span per logical call, covering the retries
   nested below it.
4. **Metrics** — latency as the caller experiences it, i.e. including retry
   backoff.
5. **Timeout** — installs the budget for the whole call, outermost of the
   resilience layers so it covers every attempt instead of being handed out
   fresh to each one.
6. **[Deadline budget](deadlines.md)** — trims that budget to what the
   caller's *request* has left. It has to see the deadline the layer above
   installed, and it has to run before anything divides that deadline further,
   which pins it between the timeout layer and the retry layer.
7. **[Wait-for-ready](resilience.md#waiting-for-a-connection)** — decides
   whether a call waits for its connection or fails fast. Last of the layers
   that shape a call's deadline handling, because that decision is made *from*
   the deadline the two above it settled on.
8. **Retry** — divides the resulting budget between attempts.
9. **Circuit breaker** — innermost, so it sees individual attempts rather
   than one aggregated verdict, and rejects tripped methods without touching
   the network.
10. **Extra inner** — custom layers that must re-run per attempt, closest to
    the wire (a credential refresh, for instance). When the factory wires a
    balancer, its [passive quarantine
    reporter](load-balancing.md#passive-quarantine-a-failed-call-is-fresher-than-any-probe)
    lives here, so it grades each attempt's real transport outcome.

Layers 6 and 7 are only present when configured — `build_interceptors` takes a
`deadline_budget=` and a `wait_for_ready=` alongside the arguments above — and
layer 6 additionally needs the `deadline` extra, without which it is left out
with a warning.

Custom interceptors passed to `build_interceptors(extra_interceptors=...)`,
`InterceptorChainBuilder.with_custom` or
`GrpcClientFactory.create_client(interceptors=...)` land in the **outer**
slot: metadata injection is by far their most common job, and it only works
above the observability layers.

All layers wrap the whole RPC for every call kind: for a response-streaming
call the log record, the span, the latency sample and the breaker verdict all
land when the stream ends, and a mid-stream failure is the failure the whole
chain sees.

The one exception is a response stream the caller walks away from **without
cancelling it**: `grpc.aio` keeps such a call alive, no teardown ever runs, and
the metrics in-flight gauge keeps its slot. A consumer that stops early should
say so — `call.cancel()` ends the RPC, and the call is then recorded as
cancelled like any other outcome.

## How a chain reaches the channel

A channel sorts its interceptor list into four lists — one per RPC kind — by
class, and files each entry into the **first** one it matches. An interceptor
claiming all four kinds therefore ends up registered for unary-unary only, and
streaming calls run past it with no logging, no deadline, no retry and no
breaker. So each layer reaches the channel as four adapters, one per kind, and
`build_interceptors` returns that already-expanded chain — the seven layers the
example above builds arrive as 28 entries, in the same order in all four of the
channel's lists.

Two consequences worth knowing:

- The chain is what a channel accepts, so it can be handed to `GrpcClient`, to
  `GrpcClientFactory` or to `grpc.aio.insecure_channel` unchanged.
- `logical_interceptor(entry)` maps an adapter back to the interceptor it
  stands for, leaving anything else alone. That is how you reach a layer
  inside a built chain — the circuit breaker, say, to call `get_states()` on
  it.

## Chains per target, or one shared chain

`GrpcClient` accepts either, and refuses both:

```python
GrpcClient(Stub, config=config, pool=pool, interceptors=chain)  # one shared chain
GrpcClient(Stub, config=config, pool=pool, interceptor_factory=build_chain)  # a chain per target
```

`GrpcClientFactory` always passes a factory, so every target of a
load-balanced client gets its own stateful layers. A shared `interceptors`
list means a shared circuit breaker across all of them — see
[Circuit breaker isolation](resilience.md#circuit-breaker-isolation).

## Writing a custom interceptor

Subclass `AsyncAroundClientInterceptor` and write one async generator. That
single generator is the layer for all four RPC kinds:

```python
import logging
import time
from collections.abc import AsyncIterator

import grpc.aio

from grpc_client_kit.interceptors import AsyncAroundClientInterceptor, ClientCall

logger = logging.getLogger(__name__)


class TimingInterceptor(AsyncAroundClientInterceptor):
    """Log how long every call took, whichever kind it was."""

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        started = time.perf_counter()
        try:
            yield  # the whole RPC happens here — a response stream to its last item
        except grpc.aio.AioRpcError as error:
            logger.warning("%s failed with %s", call.method, error.code())
            raise
        finally:
            logger.info("%s took %.3fs", call.method, time.perf_counter() - started)
```

- **Before the `yield`** the RPC does not exist yet. Rewrite the call details
  here — `call.details = call.details._replace(timeout=5.0)` — and raise here
  to refuse the call outright: nothing is sent.
- **At the `yield`** the call runs, start to finish. A failure arrives as
  `grpc.aio.AioRpcError`, one halfway through a response stream included.
- **After it** — `except`, `else`, `finally` — the outcome is known and
  nothing done here can change it. Swallowing the exception is not supported:
  there is no response to put in its place. Raising is not either: whatever
  the teardown raises is logged at `ERROR` against the method and dropped, so
  a metrics push to a collector that has gone away cannot take a response the
  server already sent with it.

`call` carries what a layer needs: `method` (already decoded to `str`),
`rpc_type`, `request_streaming` / `response_streaming`, the mutable `details`,
`response` once a unary one has arrived, and `underlying_call` for the
`grpc.aio.Call` itself.

### Scoping a value to one call

Both sides of the `yield` run in one `contextvars.Context`, so the pair that
scopes something for the length of a call is written the obvious way and works
on all four RPC kinds:

```python
from contextvars import ContextVar

REQUEST_ID: ContextVar[str | None] = ContextVar("request_id", default=None)


class RequestId(AsyncAroundClientInterceptor):
    """Tag every outgoing call with an id the layers below it can read."""

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        token = REQUEST_ID.set(new_request_id())
        try:
            yield
        finally:
            REQUEST_ID.reset(token)
```

What the setup sets is visible to every layer below and to the RPC itself. It
is not visible to your own calling code: `grpc.aio` runs a chain in a task of
its own, so a chain has never been able to write into the caller's context.
OpenTelemetry's `attach` and `detach` work across the `yield` for the same
reason the token pair does.

The kit pays for that by pinning a context per call on the three kinds whose
teardown finishes somewhere else — after the last item of a response stream,
or once a streaming request's outcome arrives — which costs an `asyncio.Task`
or two per call, per layer. A unary-unary call pins nothing and pays nothing:
its setup, RPC and teardown are one coroutine already.

## When `intercept` is the right seam

A layer that re-issues a call rather than merely wrapping it — a retry —
subclasses `AsyncClientInterceptor` and implements `intercept(call)`, issuing
the call itself with `await call.invoke_unary()` or
`await call.invoke_stream()`. So does a layer that only rewrites the call
details and never looks at the outcome, as the context and timeout layers do:
`around_call` would stay open until the last message of a response stream,
costing a wrapper the layer never uses. Rule of thumb — `around_call` when the
outcome matters, `intercept` when the call has to be issued by hand, or not at
all.

Writing an interceptor straight against gRPC's own `intercept_*` methods stays
possible too; [Advanced](advanced.md#writing-against-the-plain-grpcaio-api)
covers what you then have to handle yourself.
