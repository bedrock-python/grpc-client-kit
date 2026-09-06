# grpc-client-kit for AI agents

> One page holding everything a coding assistant needs to configure and drive
> grpc-client-kit correctly, plus a map of where the rest of the documentation keeps the
> details it leaves out. Give an agent this page rather than the whole site.

| | |
|---|---|
| Package | `grpc-client-kit` on PyPI, import root `grpc_client_kit` |
| Requires | Python 3.12+, `grpcio` 1.78+ — and nothing else on a bare install |
| Install | `pip install grpc-client-kit` · extras: `health`, `tracing`, `metrics`, `deadline`, `observability` (= `metrics` + `tracing`), `all` |
| Async | All of it. Every entry point is `grpc.aio`, and the pool, the balancers and the checker are coroutines. |
| Sync | None. There is no sync mirror and no thread-safe surface; this kit is for an event loop. |
| Source | <https://github.com/bedrock-python/grpc-client-kit> |

## How to read this page

Every page of this site is also served as raw Markdown at its own URL with `.md` in place of
the trailing slash — this page is `/agents.md`, the resilience guide is
`/guide/resilience.md` — so anything the map below points at can be fetched as plain text
rather than scraped out of HTML. The **Copy page** control at the top of a page does the same
thing for a human with a chat window open. The one exception is the API reference: its
Markdown is a handful of instructions to a docstring renderer rather than the API, so it
carries neither the control nor a `.md` twin — read it as HTML, or read the docstrings in the
source, which is where the reasoning behind every decision below actually lives.

Top to bottom before writing code. [Rules that hold or break the code](#rules-that-hold-or-break-the-code)
is the section correctness lives in — those are the things the library will not save you
from. Every name used below is in the public API; if you need something not listed here,
fetch the page the [documentation map](#documentation-map) points at rather than guessing a
method that sounds plausible.

## Scope

**It does** hold gRPC channels in a pool keyed by everything gRPC bakes into a channel at
creation, pick a target from a list of addresses (round-robin, random, weighted), probe those
addresses over `grpc.health.v1` and let health decide eligibility, and wrap every outgoing
call in a fixed-order chain of interceptors — logging, tracing, metrics, a whole-call
deadline, request-budget propagation, wait-for-ready, retries, a circuit breaker — that
covers all four RPC kinds, streams included.

**It does not** implement a gRPC server, generate stubs or touch `.proto` files; it does not
resolve DNS or watch a service registry; it does not do connection-level load balancing
(gRPC core does that better, see [Native gRPC or the kit?](guide/native-vs-kit.md)); it does
not schedule anything of its own beyond the health loop; it does not start a request budget
for you; it never closes a channel you are holding; and it has no sync API.

## Mental model

Four nouns, each with a different lifetime.

* **`ChannelPool`** owns the channels. One per process. A channel is filed under a
  `ChannelKey` — target, `insecure`, credentials, options, compression *and* a token for the
  interceptor chain — because `grpc.aio` binds every one of those to a channel when it is
  created and none of them can be changed afterwards. Two callers share a channel only when
  all six match.
* **`GrpcClientConfig`** describes how one channel is built: address, security, raw channel
  `options`, compression, and `connectivity` (keepalive and reconnect backoff spelled in
  seconds instead of as `grpc.*_ms` argument pairs).
* **`GrpcClient[T]`** owns nothing. It picks a target — from `config.target` or from a
  balancer — asks the pool for the matching channel and constructs a stub. Creating one per
  request is cheap; closing one closes nothing.
* **`GrpcClientFactory`** wires the rest from one settings object: the pool, a balancer over
  `settings.targets`, a `HealthChecker`, and one interceptor chain per target. It owns the
  pool only when it created the pool itself.

The interceptor chain is the other half of the design. `grpc.aio` applies a client
interceptor list from the outside in, and `InterceptorChainBuilder` fixes the order because
position decides what a layer observes and what it repeats:

    extra outer → logging → tracing → metrics → timeout → deadline budget
      → wait-for-ready → retry → circuit breaker → extra inner

A **logical** interceptor (`AsyncClientInterceptor` / `AsyncAroundClientInterceptor`) is one
implementation covering all four RPC kinds. A channel, however, files interceptors into four
lists by class and puts each entry in the *first* list it matches, so a chain reaches the
channel as four thin adapters per layer — `build_interceptors` and `flatten_interceptors`
do that expansion, and `logical_interceptor(entry)` maps an adapter back.

Two facts about `grpc.aio` explain most of the code: a continuation resolves to a `Call` the
moment the RPC is *created*, never raises, and looks identical for a call that will fail —
so the outcome only appears when that `Call` is awaited or iterated. `ClientCall.invoke_unary`
and `ClientCall.invoke_stream` are where that is handled once, for every layer.

## Wiring

The smallest correct client:

```python
import asyncio

from grpc_client_kit import ChannelPool, GrpcClient, GrpcClientConfig


async def main() -> None:
    async with ChannelPool() as pool:  # owns the channels
        client = GrpcClient(
            GreeterStub,  # your generated stub class
            config=GrpcClientConfig(target="localhost:50051", insecure=True),
            pool=pool,
        )
        async with client as stub:  # yields the STUB, not the client
            print(await stub.Greet(GreetRequest(name="world")))


asyncio.run(main())
```

A hand-built chain, which is what you need for per-method budgets — request-budget
propagation and wait-for-ready are reachable from a settings object too, through their
own optional blocks:

```python
from grpc_client_kit import (
    ChannelPool,
    CircuitBreakerConfig,
    DeadlineBudgetConfig,
    GrpcClient,
    GrpcClientConfig,
    ObservabilityConfig,
    RetryConfig,
    TimeoutConfig,
    WaitForReadyConfig,
    build_interceptors,
)

chain = build_interceptors(
    observability=ObservabilityConfig(service_name="orders.v1.Orders", logging=True),
    timeout=TimeoutConfig(default=5.0, per_method={"/orders.v1.Orders/Export": 120.0}),
    deadline_budget=DeadlineBudgetConfig(),  # needs the [deadline] extra
    wait_for_ready=WaitForReadyConfig(),
    retry=RetryConfig(max_attempts=3),
    circuit_breaker=CircuitBreakerConfig(fail_threshold=5),
)

pool = ChannelPool()
client = GrpcClient(
    OrdersStub,
    config=GrpcClientConfig(target="orders.internal:50051"),
    pool=pool,
    interceptors=chain,  # built once, reused: it is part of the pool key
)
```

And the factory, which builds all of that per target from one settings object:

```python
from grpc_client_kit import GrpcClientFactory

async with GrpcClientFactory(settings=settings) as factory:  # starts health checks, waits for the first pass
    users = factory.create_client(UserStub)
    async with users as stub:
        await stub.GetUser(request)
```

## The API

Everything below is importable from `grpc_client_kit` unless a row says otherwise.

### The four objects

| Name | Signature | Notes |
|---|---|---|
| `ChannelPool` | `(max_channels_per_target=1, idle_timeout=300.0, health_checker=None, metrics=None)` | `max_channels_per_target <= 0` raises `ValueError` |
| `GrpcClientConfig` | `(target=None, insecure=False, credentials=None, options=None, compression=None, connectivity=None)` | mutable dataclass; `insecure=True` with `credentials` raises `ValueError` |
| `GrpcClient[T]` | `(stub_class, config, pool, balancer=None, interceptors=None, interceptor_factory=None)` | generic in the stub type |
| `GrpcClientFactory` | `(settings=None, pool=None, shutdown_grace=5.0, ready_timeout=10.0)` | `settings` validated eagerly against `GrpcClientSettingsProtocol` |

| Method | Returns | What it does |
|---|---|---|
| `await pool.get_channel(target, insecure=False, credentials=None, options=None, compression=None, interceptors=None, key=None)` | `grpc.aio.Channel` | round-robin among healthy channels of that identity; `RuntimeError` while the pool is draining |
| `pool.make_key(target, ...)` | `ChannelKey` | validates the target and builds the identity; cache it and pass it back as `key=` |
| `await pool.close_all(grace=None)` | `None` | closes every channel; the pool stays usable |
| `await pool.health_check(target=None)` | `bool` | the pool itself, or one target through its checker |
| `await pool.update_channel_health(target, is_healthy)` | `None` | flags **every** pooled channel of that address |
| `await client.connect()` | `T` | picks a target, gets the channel, returns a new stub |
| `client.interceptors_for(target)` | `list[grpc.aio.ClientInterceptor]` | the cached chain for one target |
| `await client.circuit_breaker_states()` | `dict[str, dict[str, Any]]` | target → method → status, for chains already built |
| `factory.create_client(stub_class, target=None, service_name=None, metrics=None, interceptors=None)` | `GrpcClient[T]` | `service_name` defaults to `stub_class.__name__` |
| `await factory.wait_until_ready(timeout=None)` | `bool` | `True` immediately when no checker is configured |
| `await factory.close(grace=None)` | `None` | stops the checker; closes the pool only if the factory made it |
| `factory.health_checker` | `HealthChecker | None` | property |
| `await factory.circuit_breaker_states()` | `dict[str, dict[str, Any]]` | keyed `"service -> target"` |

Context managers: `async with pool` yields the pool and closes it with no grace on exit;
`async with client` yields **the stub** and closes nothing; `async with factory` yields the
factory, starts the checker and waits for its first pass, and on exit closes with
`shutdown_grace`.

### Channel construction

| Name | Fields (defaults) |
|---|---|
| `ConnectivityConfig` | `keepalive_time=30.0`, `keepalive_timeout=10.0`, `permit_without_calls=False`, `max_pings_without_data=2`, `initial_reconnect_backoff=1.0`, `min_reconnect_backoff=None`, `max_reconnect_backoff=30.0` — all seconds; `None` keeps gRPC's own default |
| `ChannelKey` | `target`, `insecure`, `credentials`, `options`, `compression`, `interceptors_token`; `ChannelKey.build(...)` classmethod |

`GrpcClientConfig.channel_options()` returns explicit `options` first and verbatim, then the
arguments `connectivity` stands for minus any key the caller already used.
`ConnectivityConfig.to_options()` is that derived list on its own. Both are pure functions in
a fixed order, which is what keeps a channel's identity stable.

### The chain

```python
build_interceptors(
    timeout=None, retry=None, circuit_breaker=None, observability=None,
    extra_interceptors=None, extra_inner_interceptors=None,
    deadline_budget=None, wait_for_ready=None,
) -> list[grpc.aio.ClientInterceptor]
```

`InterceptorChainBuilder()` is the same thing incrementally: `.with_observability(config)`,
`.with_resilience(timeout=None, retry=None, circuit_breaker=None, deadline_budget=None, wait_for_ready=None)`,
`.with_extra_outer(seq)`, `.with_extra_inner(seq)`, `.with_custom(seq)` (an alias of
`with_extra_outer`), `.build()`. The order is a property of `build()`, not of the call
sequence.

| Config | Fields (defaults) |
|---|---|
| `TimeoutConfig` | `default=10.0`, `per_method={}` — budget of the **whole call**; `None`/`0` disables |
| `RetryConfig` | `max_attempts=3`, `initial_backoff=0.1`, `max_backoff=10.0`, `backoff_multiplier=2.0`, `jitter=0.1`, `retryable_codes=None`, `retry_streaming=False`, `idempotent_methods=None`, `on_retry=None`, `metrics=None` |
| `CircuitBreakerConfig` | `fail_threshold=5`, `recovery_timeout=60.0`, `half_open_max_calls=1`, `max_methods=1000`, `metrics=None` |
| `WaitForReadyConfig` | `default=True`, `per_method={}`, `require_deadline=True` |
| `DeadlineBudgetConfig` | `reserve_for_next=0.0` — needs the `deadline` extra |
| `ObservabilityConfig` | `tracing=False`, `metrics=False`, `logging=True`, `service_name="unknown"`, `metrics_registry=None`, `sensitive_methods=None`, `sensitive_patterns=None`, `sensitive_headers=None`, `log_request_payload=False`, `log_response_payload=False`, `enable_method_label=True`, `success_log_level=logging.INFO` |

Layer classes, all exported and all usable on their own:
`AsyncClientContextInterceptor(metadata_provider)`, `AsyncLoggingInterceptor`,
`AsyncTimeoutInterceptor`, `AsyncWaitForReadyInterceptor`, `AsyncRetryInterceptor`,
`AsyncCircuitBreakerInterceptor`. The breaker exposes `await get_states()` returning
`dict[str, CircuitBreakerStatus]`, alongside the `CircuitState` enum
(`CLOSED` / `OPEN` / `HALF_OPEN`).

### Writing a layer

| Name | Use |
|---|---|
| `AsyncAroundClientInterceptor` | subclass and write one `async def around_call(self, call) -> AsyncIterator[None]` that yields exactly once — the whole RPC happens at the `yield`, a response stream to its last item included |
| `AsyncClientInterceptor` | subclass and implement `async def intercept(self, call)` when the call must be issued by hand, re-issued, or not issued at all |
| `ClientCall` | `method` (already decoded `str`), `rpc_type`, `details`, `request`, `request_streaming`, `response_streaming`, `response`, `underlying_call`, `await invoke_unary()`, `await invoke_stream()` |
| `flatten_interceptors(interceptors)` | expands a mixed chain into what a channel accepts |
| `logical_interceptor(entry)` | an adapter back to its interceptor; anything else unchanged |

Rewrite the call before the `yield` with `call.details = call.details._replace(timeout=...)`;
raise before the `yield` to refuse the call outright. Swallowing an exception after it is not
supported.

### Balancing

| Name | Signature |
|---|---|
| `create_balancer(targets, config=None, health_checker=None)` | round-robin unless `config` says otherwise |
| `LoadBalancerConfig` | `strategy=LoadBalancingStrategy.ROUND_ROBIN`, `weights=None` |
| `LoadBalancingStrategy` | `ROUND_ROBIN` / `RANDOM` / `WEIGHTED` (a `StrEnum`: `"round_robin"`, `"random"`, `"weighted"`) |
| `RoundRobinLoadBalancer(targets, health_checker=None)` | re-gathers health on every pick |
| `RandomLoadBalancer(targets, health_checker=None)` | caches the healthy set for 1 s |
| `WeightedLoadBalancer(targets, weights, health_checker=None)` | ditto; a missing weight is `1.0`, negatives and an all-zero total raise `ValueError` |
| `await balancer.select_target()` | the address, or `NoHealthyTargetsError` |
| `balancer.report_failure(target, quarantine=5.0)` | takes a target out of the rotation now, without waiting for a probe |

### Health

`HealthChecker` needs the `health` extra and resolves on first attribute access, so
`import grpc_client_kit` works without it.

```python
HealthChecker(
    check_interval=30.0,
    timeout=5.0,
    max_backoff=300.0,
    on_status_change=None,
    insecure=False,
    credentials=None,
    pool=None,
    fail_fast_callback=False,
    options=None,
    compression=None,
    service="",
)
```

`await start(targets)`, `await stop(timeout=5.0)`, `await wait_until_ready(timeout=None) -> bool`,
`await check_health(target) -> bool` (one probe, published like any other),
`await is_healthy(target) -> bool` (from cache), `is_running` property. `service=""` asks
about the server as a whole; naming a service asks about that service alone.

### Deadline budgets

| Name | What it is |
|---|---|
| `use_budget(budget)` | context manager installing a budget for the current task; `use_budget(None)` detaches |
| `current_budget()` | the installed budget, or `None` |
| `DeadlineBudgetProtocol` | `timeout_for_call(call_name, reserve_for_next=0.0)`, `remaining()`, `expired()` — `runtime_checkable`, so any object of that shape works |

### Protocols and helpers

Settings and collaborator protocols, all `runtime_checkable` and all exported:
`GrpcClientSettingsProtocol`, `ChannelPoolSettingsProtocol`, `TimeoutSettingsProtocol`,
`RetrySettingsProtocol`, `CircuitBreakerSettingsProtocol`, `LoadBalancerSettingsProtocol`,
`HealthCheckerSettingsProtocol`, `ChannelProviderProtocol`, `HealthCheckerProtocol`,
`HealthStatusCallbackProtocol`, `GrpcClientMetricsProtocol`, `RetryMetricsProtocol`,
`CircuitBreakerMetricsProtocol`, and the three that describe the optional settings blocks —
`GrpcChannelExtrasProtocol`, `GrpcObservabilityExtrasProtocol`,
`FullGrpcClientSettingsProtocol`. Also `metadata_to_dict(metadata)` and `__version__`.

Names that exist but are **not** re-exported at package level — import them from the module
named beside them:

| Name | Module |
|---|---|
| `DEFAULT_RETRYABLE_CODES` | `grpc_client_kit.interceptors` |
| `AsyncDeadlineBudgetInterceptor`, `HAS_DEADLINE_BUDGET` | `grpc_client_kit.interceptors.deadline` |
| `AsyncClientTracingInterceptor`, `HAS_TRACING` | `grpc_client_kit.interceptors.tracing` |
| `AsyncClientMetricsInterceptor`, `HAS_METRICS` | `grpc_client_kit.interceptors.metrics` |
| `AsyncPassiveOutlierInterceptor`, `DEFAULT_QUARANTINE_SECONDS` | `grpc_client_kit.interceptors.outlier` |
| `validate_target`, `MIN_PORT`, `MAX_PORT` | `grpc_client_kit.validation` |
| `create_aio_channel` | `grpc_client_kit.utils` |

`ChannelWrapper` and `chain_token` in `grpc_client_kit.channel`, and `MethodCircuitState` in
`grpc_client_kit.interceptors.circuit_breaker`, are internals left out of the public surface on
purpose: exporting them would freeze those implementations into the compatibility contract. Do
not build on them — a breaker's state is read through `get_states()`, which returns
`CircuitBreakerStatus` snapshots.

### Settings objects

`GrpcClientFactory` reads a settings object structurally, so a pydantic model, a dataclass or
a plain class with nested classes all work. **Required** (`GrpcClientSettingsProtocol`):
`target`, `targets`, `insecure`, `tracing_enabled`, `metrics_enabled`, `logging_enabled`,
`pool`, `circuit_breaker`, `retry`, `timeout`, `balancer`, `health_checker`. A missing one
raises `TypeError` naming it, at construction.

Read with `getattr` and therefore optional: `credentials`, `options`, `compression`,
`connectivity`, `metrics_registry`, `sensitive_headers`, `enable_method_label`,
`success_log_level`, plus two whole blocks — `wait_for_ready` (`default`, `per_method`,
`require_deadline`) and `deadline_budget` (`reserve_for_next`). The `retry` block's `jitter`,
`retryable_codes`, `retry_streaming`, `idempotent_methods` and `on_retry`, the
`circuit_breaker` block's `max_methods` and the `health_checker` block's `service` are picked
up the same way. Because they are read with `getattr`, a **typo in an optional name silently
yields the default** — pydantic users should set `extra="forbid"`.

Per-method budgets are the one thing settings cannot express: `timeout` carries only
`default`. That needs a hand-built chain.

## Rules that hold or break the code

1. **`async with client` yields the stub, and closes nothing.** `GrpcClient.__aexit__`
   deliberately leaves the channel alone — channels belong to the pool. Only
   `ChannelPool.close_all()` closes anything, and `GrpcClientFactory` closes the pool only
   when it created the pool itself. A factory handed `pool=` borrows it.
2. **The pool never closes an idle channel.** `idle_timeout` becomes
   `grpc.client_idle_timeout_ms` and gRPC core *parks* the connection once no RPC is active,
   redialling transparently on the next call. That is why a stub you hold stays valid for
   life and a week-long stream is safe. `idle_timeout=0` or less disables parking entirely.
3. **`close_all()` drains the pool, it does not retire it.** Every channel is closed, the
   pool stays usable, and its `async with` block can be entered again. A `get_channel`
   issued *while* the drain runs raises `RuntimeError` rather than handing out a channel the
   pool no longer tracks.
4. **A timeout is the budget of the entire call, retries included.** The retry layer converts
   it into a monotonic deadline once, on entry, and issues each attempt with what is left;
   `max_attempts × timeout` is not how long a call can take. A backoff that would outlive the
   budget abandons the retry and re-raises the original error.
5. **A `None` settings block means "do not add that layer", never "use defaults".** No
   `timeout` block means no timeout interceptor and therefore **no deadline at all**. So does
   `TimeoutConfig(default=None)` with no `per_method` entries — a pass-through layer would
   cost a hop per call and, since the chain is part of the pool key, a separate channel.
6. **`DEFAULT_RETRYABLE_CODES` is `{UNAVAILABLE, RESOURCE_EXHAUSTED}` and nothing else.**
   `INTERNAL`, `UNKNOWN`, `ABORTED` and `DEADLINE_EXCEEDED` are never retried unless you put
   them in `retryable_codes`. Even the two defaults are a compromise: a server dying
   mid-handler surfaces as `UNAVAILABLE`, and a handler may abort with `RESOURCE_EXHAUSTED`
   after a write. Where a duplicate is unaffordable, set `idempotent_methods` — once that
   whitelist exists, nothing outside it is retried, whatever the code.
7. **Streaming requests are never retried; streaming responses need two opt-ins.** A
   stream-unary or stream-stream call's request iterator is consumed by the first attempt and
   cannot be replayed. A unary-stream call is restartable only with `retry_streaming=True`
   **and** the method listed in `idempotent_methods` — and a restart replays items the
   consumer has already seen, which is logged as a warning.
8. **The breaker counts attempts, not calls, so `fail_threshold` must exceed `max_attempts`.**
   The breaker is innermost and the retry layer above it manufactures the attempts, so one
   logical call contributes up to `max_attempts` consecutive failures. With
   `max_attempts=3, fail_threshold=2` a single unlucky call opens its own circuit and the
   caller is told `CircuitBreakerOpenError` instead of what the server actually said. A
   tripped circuit is never retried, even though its error carries `UNAVAILABLE`.
9. **A budget only ever tightens a deadline.** Every layer that touches the deadline takes
   the smaller of what it finds and what it knows; a request with 50 seconds left does not
   entitle a method configured for 5 to more than 5.
10. **Nothing propagates a deadline until you install a budget.** The kit supplies the
    contextvar and the interceptor; `use_budget(BudgetContext.create(...))` is the caller's
    job, normally at the point a request enters the process. Without it the layer is a
    pass-through that touches no call. Without the `deadline` extra the layer is not in the
    chain at all — logged as a warning, never raised. Per-call caps are keyed by the full
    method name, `/package.Service/Method`, and nothing else.
11. **A task created outside a `use_budget` block carries no budget**, however deep inside
    the block its `await` happens: `asyncio` copies the context at `create_task` time.
12. **The interceptor chain is part of the channel's identity — build it once per target.**
    The token is per interceptor *instance*, so a chain rebuilt per call mints a new identity
    every time and the pool opens a channel per RPC. `GrpcClient` caches what
    `interceptor_factory` returns, keyed by target; do the same by hand. For the same reason,
    reuse one credentials object: `ChannelKey` compares credentials by **identity**, because
    gRPC credentials define no equality.
13. **Never hand a logical interceptor straight to a channel.** `grpc.aio` raises
    `ValueError`. Pass hand-assembled chains through `flatten_interceptors` first. And an
    interceptor you write against gRPC's own `intercept_*` methods that inherits all four
    base classes is registered for **unary-unary only**, silently — the channel files each
    entry into the first list it matches.
14. **`create_client(interceptors=...)` puts your layers in the outer slot**, above logging,
    tracing, metrics and the timeout. That is right for metadata injection and wrong for
    anything that has to read or reshape the deadline; those belong in a hand-built chain
    given to `GrpcClient(interceptors=...)`.
15. **`interceptors` and `interceptor_factory` are mutually exclusive, and so are `balancer`
    and `config.target`.** Both pairs raise `ValueError` at construction rather than picking a
    winner at run time. A shared `interceptors` list also means one circuit breaker shared
    across every target of that client; the factory always passes a factory instead.
16. **An unchecked target is not a healthy target.** Every target reads unhealthy until the
    first health pass lands, so enter the factory's `async with` (or `await
    checker.wait_until_ready()`) before the first RPC, or every fresh pod fails its first
    call with `NoHealthyTargetsError`. A checker that was never started raises
    `HealthCheckerNotRunningError` from `is_healthy`, and balancers gather health with
    `return_exceptions=True`, so that mistake otherwise looks exactly like a cluster that is
    entirely down.
17. **A port is always required.** Targets are validated before a channel exists, more
    strictly than gRPC — which silently falls back to 443 for a portless target. `[::1]:50051`
    must be bracketed; `http://` is rejected by name.
18. **Never stack kit retries on a native `retryPolicy`.** Service-config retries run inside
    the channel, below every interceptor, so the two multiply: 3 × 3 = 9 requests reach the
    server, invisibly to the kit's logs and metrics. `GrpcClient` warns once when it sees
    both. Native retries *without* kit retries are fully supported.
19. **Batteries are opt-in, and a missing one is a warning, not an error.**
    `import grpc_client_kit` never reaches for an extra. `HealthChecker` resolves on first
    attribute access and raises `ImportError` naming `[health]` — an `ImportError` and not an
    `AttributeError`, so a broken install says so rather than looking like a name that never
    existed. The cost is that `hasattr(grpc_client_kit, "HealthChecker")` **propagates that
    ImportError instead of returning `False`**, and so does `getattr` with a default; probe with
    `importlib.util.find_spec("grpc_health")` or catch the ImportError. Tracing, metrics and the
    deadline budget layers are left out of the chain, with a log line, when their extra is
    absent — the chain still builds and the calls still run.
20. **There is no sync API and no thread safety.** Everything here assumes one event loop.

## Common mistakes

```python
# WRONG — treating the client as the owner of the connection
async with GrpcClient(Stub, config=config, pool=pool) as client:
    await client.GetUser(request)  # `client` here is the stub, not the client
# ...and expecting this to have closed anything

# RIGHT
async with ChannelPool() as pool:  # the pool owns the channels
    client = GrpcClient(Stub, config=config, pool=pool)
    async with client as stub:  # yields the stub
        await stub.GetUser(request)
```

```python
# WRONG — a per-attempt timeout, and a threshold below the attempt count
TimeoutConfig(default=10.0)  # "so 3 attempts get 30 seconds"
CircuitBreakerConfig(fail_threshold=2)  # with max_attempts=3: one call opens its own circuit

# RIGHT — 10 seconds is the whole call, and the breaker outlives a single call
TimeoutConfig(default=10.0)  # retries are issued out of these 10 seconds
RetryConfig(max_attempts=3)
CircuitBreakerConfig(fail_threshold=5)  # > max_attempts
```

```python
# WRONG — widening the retryable set to "be resilient"
RetryConfig(retryable_codes={grpc.StatusCode.INTERNAL, grpc.StatusCode.UNAVAILABLE})
# INTERNAL is raised by the handler: the write has very likely been applied already.

# RIGHT — widen per method, behind the whitelist
RetryConfig(
    retryable_codes={grpc.StatusCode.INTERNAL, grpc.StatusCode.UNAVAILABLE},
    idempotent_methods={"/users.v1.Users/GetUser"},  # nothing outside this is retried
)
```

```python
# WRONG — a fresh chain per call: a new channel identity, so a new channel, per RPC
async def get_stub(pool, target):
    chain = build_interceptors(retry=RetryConfig())
    return UserStub(await pool.get_channel(target, interceptors=chain))


# RIGHT — build once, reuse; or let GrpcClient cache it per target
chain = build_interceptors(retry=RetryConfig())
client = GrpcClient(UserStub, config=config, pool=pool, interceptors=chain)
```

```python
# WRONG — a logical interceptor handed to a channel, which raises ValueError
grpc.aio.insecure_channel("host:50051", interceptors=[MyAroundInterceptor()])

# RIGHT — expand it into the four adapters a channel files correctly
from grpc_client_kit import flatten_interceptors

grpc.aio.insecure_channel("host:50051", interceptors=flatten_interceptors([MyAroundInterceptor()]))
```

```python
# WRONG — configuring the layer and expecting deadlines to propagate
build_interceptors(deadline_budget=DeadlineBudgetConfig())
await client_a.fetch(...)  # no budget installed: nothing is trimmed

# RIGHT — the caller installs the budget; the layer spends it
from deadline_budget import BudgetContext

from grpc_client_kit import use_budget

with use_budget(BudgetContext.create(total_seconds=3.0)):
    await client_a.fetch(...)  # 3.0s
    await client_b.charge(...)  # whatever the first call left
```

```python
# WRONG — a factory used without its context manager
factory = GrpcClientFactory(settings=settings)
client = factory.create_client(UserStub)  # logs a warning; health checks are NOT running

# RIGHT — entering the block starts the checker and waits for its first pass
async with GrpcClientFactory(settings=settings) as factory:
    client = factory.create_client(UserStub)
```

## Errors

`GrpcClientKitError` is the base of every failure the kit raises on its own authority, as
opposed to a `grpc.aio.AioRpcError` carrying a server's status. `except GrpcClientKitError`
catches exactly the local family.

| Error | Import from | Means |
|---|---|---|
| `GrpcClientKitError` | `grpc_client_kit` | base class; catch it to separate local refusals from server answers |
| `NoHealthyTargetsError` | `grpc_client_kit` | the balancer had nothing eligible left; `.targets` lists what it tried. Raised before any channel is touched |
| `HealthCheckerNotRunningError` | `grpc_client_kit` | health was asked for a target with no verdict and no loop to produce one; also a `RuntimeError`; `.target` names it. Deliberately outside the `health` extra so catching it needs no extra |
| `CircuitBreakerOpenError` | `grpc_client_kit` | the circuit for this method is open, or every half-open trial slot is taken. **Also** an `AioRpcError` carrying `UNAVAILABLE`, so logs, spans and metrics see a failed call — but metrics label it `status="rejected"`, never `"error"` |
| `DeadlineBudgetExhaustedError` | `grpc_client_kit` | the request budget was spent, so the RPC was never created. **Also** an `AioRpcError` carrying `DEADLINE_EXCEEDED`, with the budget library's `DeadlineExceededError` as `__cause__` |

Configuration mistakes raise plain `ValueError` at construction — a malformed target,
`insecure=True` with credentials, both a `balancer` and a `config.target`, both `interceptors`
and an `interceptor_factory`, a negative backoff, a non-positive `fail_threshold`,
contradictory reconnect bounds. A settings object missing a required field raises `TypeError`
naming the field. `HealthChecker` without the `health` extra raises `ImportError` naming the
extra.

## Documentation map

Fetch a page when the task is the one named beside it.

| Page | Read it when |
|---|---|
| [Overview](index.md) | placing the library at all: what it is, the extras, a first call |
| [Quick start](guide/quickstart.md) | writing the first integration: three objects, the factory, the two error families |
| [Configuration](guide/configuration.md) | building `GrpcClientConfig` or a settings object, and picking extras |
| [Channels & pooling](guide/channels.md) | channel identity, keepalive and reconnect tuning, pool limits, who closes what |
| [Interceptors](guide/interceptors.md) | the chain order, how it reaches the channel, writing your own layer |
| [Resilience](guide/resilience.md) | call budgets, wait-for-ready, retry safety, sizing the breaker against the retries |
| [Deadline budgets](guide/deadlines.md) | propagating the caller's remaining time; tasks, fan-out and per-call caps |
| [Load balancing](guide/load-balancing.md) | strategies, health-narrowed eligibility, passive quarantine |
| [Native gRPC or the kit?](guide/native-vs-kit.md) | deciding which layer owns LB, retries, idling and health |
| [Health checking](guide/health.md) | the probe loop, cold starts, backoff, status callbacks |
| [Observability](guide/observability.md) | what a log record, a metric sample and a span actually contain |
| [Advanced](guide/advanced.md) | target validation, plain `grpc.aio` interceptors, dependency injection |
| [API reference](reference/index.md) | an exact signature, field or docstring — HTML only, see above |
| [Changelog](changelog.md) | what changed between versions |
