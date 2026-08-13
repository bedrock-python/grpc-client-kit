# Configuration

There are two ways to configure a client, and they compose:

- **`GrpcClientConfig`** — a stdlib dataclass describing one channel. This is
  what `GrpcClient` and `ChannelPool` actually consume.
- **A settings object** — anything carrying the fields of
  `GrpcClientSettingsProtocol`. `GrpcClientFactory` maps it onto a pool, a
  balancer, a health checker and an interceptor chain.

## GrpcClientConfig

```python
from grpc_client_kit import ConnectivityConfig, GrpcClientConfig

config = GrpcClientConfig(
    target="api.internal:50051",  # host:port, or a gRPC resolver URI
    insecure=False,  # False + no credentials = system trust store
    credentials=grpc.ssl_channel_credentials(),
    options=[("grpc.max_receive_message_length", 8 * 1024 * 1024)],
    compression=grpc.Compression.Gzip,
    connectivity=ConnectivityConfig(keepalive_time=30.0),  # None = gRPC's defaults
)
```

`insecure=True` together with `credentials` raises `ValueError` at
construction — a channel cannot be both. With `insecure=False` and no
credentials, `grpc.ssl_channel_credentials()` is used, i.e. the system trust
store.

Reuse one credentials object rather than rebuilding it per call: gRPC
credentials define no equality, so the pool compares them by identity and a
fresh object is a fresh channel. See
[Channel identity](channels.md#channel-identity).

`connectivity` spells the keepalive and reconnect channel arguments in seconds
instead of milliseconds; explicit `options` win over it, key by key, and the
composed list is what the channel is opened with. See
[Keepalive and reconnect backoff](channels.md#keepalive-and-reconnect-backoff).

Targets are validated before a channel is created, which turns a typo into an
error at configuration time instead of an opaque `UNAVAILABLE` at the first
RPC — see [Target validation](advanced.md#target-validation).

## Settings objects

The factory reads a settings object through structural protocols, so a
pydantic model, a dataclass or a plain class all work. Protocol fields are
declared as read-only properties rather than attributes, which makes them
covariant: a pydantic model may narrow `strategy` to a `Literal` and still
satisfy the protocol.

These fields are **required** (`GrpcClientSettingsProtocol`):

| Field | Type | Meaning |
| :--- | :--- | :--- |
| `target` | `str \| None` | Single target; mutually exclusive with `targets` |
| `targets` | `list[str] \| None` | Targets to [load-balance](load-balancing.md) across |
| `insecure` | `bool` | Plaintext channels |
| `tracing_enabled` | `bool` | Add the [tracing](observability.md#tracing) layer |
| `metrics_enabled` | `bool` | Add the [metrics](observability.md#metrics) layer |
| `logging_enabled` | `bool` | Add the [logging](observability.md#logging) layer |
| `pool` | block or `None` | [Channel pool](channels.md#inside-the-pool) |
| `timeout` | block or `None` | [Timeouts](resilience.md#timeouts) |
| `retry` | block or `None` | [Retries](resilience.md#retries) |
| `circuit_breaker` | block or `None` | [Circuit breaker](resilience.md#circuit-breaker) |
| `balancer` | block or `None` | [Load balancer](load-balancing.md#choosing-a-strategy) |
| `health_checker` | block or `None` | [Health checker](health.md#what-the-factory-wires-for-you) |

**A `None` block means "do not add that layer at all", not "use defaults".**
No `timeout` block, no timeout interceptor — and therefore no deadline.

These are **optional** and read through `getattr`, so a minimal settings
object stays valid: `credentials`, `options`, `compression`
(`GrpcChannelExtrasProtocol`), plus `sensitive_headers` and
`metrics_registry` (`GrpcObservabilityExtrasProtocol`). Both protocols live in
`grpc_client_kit.protocols`, along with `FullGrpcClientSettingsProtocol` for
settings that carry everything.

`connectivity` is read the same way — a `ConnectivityConfig` under that name
[tunes the channels](channels.md#from-a-settings-object), its absence leaves
gRPC's defaults.

Splitting the optional fields off is what keeps
`isinstance(settings, GrpcClientSettingsProtocol)` a meaningful check instead
of one that fails on fields nobody defines.

## A settings object end to end

```python
import grpc


class UpstreamSettings:
    target = None
    targets = ["api-1.prod:443", "api-2.prod:443"]
    insecure = False
    credentials = grpc.ssl_channel_credentials()

    tracing_enabled = True
    metrics_enabled = True
    logging_enabled = True
    metrics_registry = my_registry

    class pool:
        max_channels_per_target = 4
        idle_timeout = 300.0

    class timeout:
        default = 10.0

    class retry:
        max_attempts = 3
        initial_backoff = 0.1
        max_backoff = 5.0
        backoff_multiplier = 2.0

    class circuit_breaker:
        fail_threshold = 5
        recovery_timeout = 60.0
        half_open_max_calls = 1

    class balancer:
        strategy = "round_robin"
        weights = None

    class health_checker:
        check_interval = 30.0
        timeout = 5.0
```

The blocks a settings object can carry are deliberately narrower than the
kit's own config dataclasses: `timeout` only exposes `default`, and the
retry block's `jitter`, `retryable_codes`, `retry_streaming`,
`idempotent_methods` and `on_retry` are picked up only when it happens to
define them. Two more optional blocks join on the same duck-typed terms — a
`wait_for_ready` block (`default`, `per_method`, `require_deadline`) and a
`deadline_budget` block (`reserve_for_next`) — so every one of the
[five resilience layers](resilience.md) is reachable from settings alone.
Anything finer — per-method budgets above all — still needs a hand-built
chain, as described in [Interceptors](interceptors.md#the-chain):

```python
chain = build_interceptors(
    observability=ObservabilityConfig(service_name="users.v1.Users"),
    timeout=TimeoutConfig(default=5.0, per_method={"/users.v1.Users/Export": 60.0}),
    deadline_budget=DeadlineBudgetConfig(),
    wait_for_ready=WaitForReadyConfig(),
    retry=RetryConfig(max_attempts=3),
)
client = GrpcClient(UserStub, config=config, pool=pool, interceptors=chain)
```

**Not through `create_client(interceptors=...)`.** That argument adds custom
layers to the [outer slot](interceptors.md#the-chain) of the chain the factory
builds — above logging, tracing, metrics and the timeout — which is the wrong
side of every layer a deadline-shaping interceptor needs to read.

The required surface of the settings object is validated when the factory is
constructed: a missing required field raises `TypeError` naming it. The
optional blocks stay optional, which also means a **typo** in an optional
field name silently yields the default — pydantic users should set
`model_config = ConfigDict(extra="forbid")` on their settings models so typos
fail at model construction instead.

## Optional dependencies

| Extra | Pulls in | Enables |
| :--- | :--- | :--- |
| `health` | `grpcio-health-checking` | `HealthChecker`, health-aware balancing and pooling |
| `tracing` | `opentelemetry-api` | `AsyncClientTracingInterceptor` (a pass-through without it) |
| `metrics` | `prometheus-client` | the default metrics backend; a custom registry needs no extra |
| `deadline` | `deadline-budget` | [deadline budget propagation](deadlines.md) (the layer is skipped without it) |
| `observability` | `metrics` + `tracing` | both of the above |
| `all` | `deadline` + `health` + `metrics` + `tracing` | everything |

`deadline` is deliberately **not** part of `observability`: propagating a
budget is resilience, not telemetry, and an observability extra should not pull
in a dependency that changes what calls do.

`import grpc_client_kit` never requires an extra. `HealthChecker` is the one
gated export: it is resolved on first attribute access, and without
`[health]` that access raises an `ImportError` naming the extra to install.
