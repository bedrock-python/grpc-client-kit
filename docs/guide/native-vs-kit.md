# Native gRPC or the kit?

gRPC core ships load balancing, retries and health checking of its own,
implemented in C below the Python layer. For some jobs it is simply better
than anything a client library can build on top — and the kit passes the
configuration through untouched, so using it costs nothing. This page says
plainly which layer to use for what, because pretending the kit should do
everything would make it worse at the things it is genuinely for.

## What to delegate to gRPC core

**Connection-level load balancing over a plain list of addresses.** Native
`round_robin` runs on subchannels *inside one channel*: it sees a broken
connection the instant the transport does and takes the subchannel out of the
picker without losing a single call. The kit's balancer, living above the
channel, learns about failures from probes and failed calls — measurably
slower. The recipe:

```python
import json

service_config = json.dumps({"loadBalancingConfig": [{"round_robin": {}}]})

client = factory.create_client(
    GreeterStub,
    target="ipv4:10.0.0.5:50051,10.0.0.6:50051",  # or dns:///greeter.internal:50051
)
# with options on the settings object:
#   options = [("grpc.service_config", service_config)]
```

**Idle connection management.** The pool already delegates this: its
`idle_timeout` becomes `grpc.client_idle_timeout_ms`, and core parks idle
connections without invalidating channels or killing streams.

**Simple transparent retries.** A native `retryPolicy` in the service config
retries below every interceptor. If that is all you need, use it and skip the
kit's `RetryConfig` entirely.

!!! warning "One source of retries, never two"

    Native `retryPolicy` and the kit's retry layer **multiply**: the kit's
    attempts each get retried natively, so 3 × 3 = 9 requests reach the
    server — a retry storm the kit's own logs and metrics cannot see. The
    client logs a warning when it detects both; heed it. Pick the native
    policy for simple code-based retries, the kit's for budget arithmetic,
    idempotency whitelists and retry metrics.

**Native health-checking of a watched service.**
`healthCheckConfig` in the service config makes the channel itself consult the
standard health service per subchannel. It composes with native LB and, like
everything above, passes through the kit unmodified.

## What the kit is for

These do not exist in gRPC core, and they are the reason this library exists:

- **A circuit breaker** — per method and per target, with half-open trials.
  gRPC has nothing comparable at any layer.
- **Deadline budgets** — [propagating the remaining time of the *request*
  being served](deadlines.md) into every outgoing call.
- **A timeout that is the budget of the whole call**, retries included, rather
  than of each attempt.
- **Uniform observability** — structured logs, metrics with honest statuses
  (`rejected` is not `error`), OpenTelemetry spans that parent correctly, all
  four RPC kinds covered.
- **Weighted balancing and active probing with passive quarantine** — for the
  cases where eligibility must follow application-level health, not just
  connectivity.
- **The `around_call` seam** — one async generator instead of four interceptor
  classes, applied to every RPC kind. Native `grpc.aio` interceptors are
  registered per kind and famously easy to get wrong.

## The composition that usually wins

Let core own the connections; let the kit own policy and visibility:

- targets via `dns:///` or `ipv4:` with native `round_robin`, so failover is
  instant and connection-level;
- the kit's chain on top for the breaker, budgets, logging, metrics and
  tracing;
- native `retryPolicy` **or** kit retries — one of them, chosen by whether you
  need budget arithmetic and idempotency whitelists;
- the kit's health checker only when eligibility must follow application
  health (a service that is *up* but declares itself `NOT_SERVING`).
