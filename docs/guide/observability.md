# Observability

Three layers report what the client is doing: logging, metrics and tracing.
All of them sit above the resilience layers in
[the chain](interceptors.md#the-chain), so what they describe is the *logical*
call the caller made — retries and their backoff included — not an individual
network attempt.

## Turning layers on

`tracing_enabled`, `metrics_enabled` and `logging_enabled` on the settings
object toggle the three layers; `ObservabilityConfig` is the same choice for a
hand-built chain. Two rules govern what actually ends up in the chain:

- **Metrics need a registry.** `metrics_enabled=True` with no
  `metrics_registry` (and no `metrics=` argument to `create_client`) would
  record nothing, so the layer is left out entirely and a warning is logged —
  "no metrics" is visible in the chain instead of being a silent hop. An
  explicit `create_client(metrics=...)` wins over `settings.metrics_registry`.
- **A layer that cannot work is announced, not silently added.** With
  `tracing_enabled=True` but no `[tracing]` extra, the interceptor is omitted
  and the omission logged.

The kit never talks to Prometheus or OpenTelemetry directly: metrics go
through `GrpcClientMetricsProtocol` (`record_request`, `record_inflight_delta`,
`record_pool_stats`), so any backend — or a test double — satisfies it.

## Logging

One record per logical call, written once the call has an outcome, to the
logger `grpc.client.<service_name>`. At DEBUG a second record marks the start;
the fields are built only when the logger is actually enabled for DEBUG, since
this is a hot path.

Every record carries a freshly built `extra` mapping, so nothing leaks between
records of the same call:

| Field | On | Value |
| :--- | :--- | :--- |
| `grpc.service`, `grpc.method` | every record | The configured service name and the full method path |
| `request_id` | when present | From `request-id` or `x-request-id` in the call metadata |
| `grpc.metadata` | start (DEBUG) | The metadata with sensitive values replaced by `***` |
| `grpc.request` | start (DEBUG) | Only with `log_request_payload`; `<stream_request>` for a streaming request |
| `grpc.duration_ms`, `grpc.status` | terminal | How long the call took, and how it ended |
| `grpc.error` | failures | The gRPC details string |
| `grpc.response` | success | Only with `log_response_payload`, and never for a stream |
| `grpc.sensitive` | sensitive methods | `True` |

Payloads are truncated to 1000 characters and are off by default; they are
only reachable through a hand-built `ObservabilityConfig`.

The severity of the terminal record follows the outcome:

- **OK** → `INFO` by default; `success_log_level` lowers it. One line per
  successful RPC is a flood at high QPS — a client doing 10k calls a second
  writes 10k lines a second about nothing going wrong — so high-volume clients
  set `success_log_level=logging.DEBUG` and keep the failures.
- **Cancelled** → `INFO`, status `CANCELLED`. A caller abandoning a response
  stream arrives as `GeneratorExit` and is reported here too, so no call goes
  unaccounted for.
- **Refused by the open circuit breaker** → `WARNING`, one line per rejection.
  Rejections are the breaker doing its job while the circuit recovers, not
  fresh failures: reporting each as `ERROR` would bury the handful of genuine
  state-transition records under thousands of identical lines.
- **`INTERNAL`, `UNKNOWN`, `DATA_LOSS`, or an error carrying no code** →
  `logger.exception`, with the traceback: these point at a broken callee, or
  at us.
- **Any other gRPC status** → `logger.error` without a traceback. A
  `NOT_FOUND` is an expected outcome and does not deserve a stack trace.

A logger that will emit nothing costs nothing: when the logger's level is
above `WARNING` the interceptor skips metadata normalization and record
assembly entirely, so leaving the layer in the chain is free in a process that
has client logging turned off.

A streaming call says "stream" where a unary one says "call", and its record
lands when the stream ends.

Redaction covers the usual credential-bearing headers by default
(`authorization`, `cookie`, `x-api-key`, `token`, `password`, …);
`sensitive_headers` replaces that set. Whole methods can be marked sensitive
too, by exact name or by regex — such a call logs no metadata, no payloads and
no error details, only that it happened and how it ended.

## Metrics

Three measurements per call, recorded through the registry:

| Metric | Type | Meaning |
| :--- | :--- | :--- |
| `grpc_client_requests_total` | Counter | One increment per finished call |
| `grpc_client_request_duration_seconds` | Histogram | The RPC as the caller experienced it |
| `grpc_client_requests_in_flight` | Gauge | Calls currently running |

Labels are `service`, `method`, `rpc_type`, `status` and `grpc_code`.
`status` is one of:

- `success` — the call completed with `OK`;
- `error` — the server (or the wire) failed it;
- `rejected` — the **local circuit breaker** refused it without touching the
  network. Kept apart from `error` on purpose: during an incident the first
  question is "is the breaker open, or is the backend down?", and a shared
  label pair would make that unanswerable on a dashboard;
- `cancelled` — the caller cancelled or abandoned it.

`service` and `method` are parsed from the method path, so
`/orders.v1.Orders/Export` becomes `service="Orders"`, `method="Export"`; set
`enable_method_label=False` — on `ObservabilityConfig` or as
`enable_method_label` on your settings object — to collapse `method` to
`total` when a service has thousands of methods and cardinality matters.

The in-flight gauge is incremented before the call and decremented in a
`finally`, so it is balanced however the call ends — drained, failed,
cancelled, or abandoned mid-stream. Every RPC that was created is counted
exactly once, which is what keeps the counter and the gauge from drifting
apart. An exception raised by the registry itself is caught and logged: a
metrics hiccup never fails the call it was measuring.

### Pool statistics

`record_pool_stats(active_channels, idle_targets)` is reported by the
[channel pool](channels.md#inside-the-pool) whenever it changes:
`active_channels` counts pooled channels, `idle_targets` counts the distinct
addresses they lead to (one address can back several channel identities).
`settings.metrics_registry` feeds this independently of `metrics_enabled`.

### Opting into retry and breaker visibility

The request metrics sit **above** the retry layer and record one entry per
*logical* call, so three wire attempts collapsing into one success look like a
single request — a retry storm is invisible through them by construction. Two
optional extension protocols close that gap; a registry opts in simply by
implementing the methods:

| Protocol | Method | Told about |
| :--- | :--- | :--- |
| `RetryMetricsProtocol` | `record_retry(service, method, attempt, grpc_code)` | Every scheduled retry, the moment it is scheduled |
| `CircuitBreakerMetricsProtocol` | `record_circuit_state(method, state)` | Every state transition, by name (`closed` / `open` / `half-open`) |
| | `record_circuit_rejection(method)` | Every call the open circuit refused locally |

The kit checks for these once, with `isinstance`, when the chain is built — a
registry that implements only the base protocol is never poked with methods it
does not have. Live snapshots are also available without metrics at all:
`GrpcClient.circuit_breaker_states()` and
`GrpcClientFactory.circuit_breaker_states()` return the current state of every
breaker, per target and method.

## Tracing

One CLIENT span per logical call, named after the full method, with the
OpenTelemetry semantic-convention attributes `rpc.system` (`"grpc"`),
`rpc.service` (the configured service name) and `rpc.method`. A failed call
also carries `rpc.grpc.status_code`.

The parent is the span active in the **current** context — typically the
server span of the request being handled — and is deliberately not extracted
from the outgoing metadata: nothing has written a `traceparent` there yet at
that point, because this interceptor is what writes it. Extracting would yield
an empty context and quietly turn every client span into a root span.

Installing `opentelemetry-api` without configuring an SDK — the default state
of the `[tracing]` extra — hands out non-recording spans, and the interceptor
checks for exactly that: a non-recording span skips the attributes, the status
bookkeeping and the metadata rebuild entirely, so the layer costs next to
nothing until a real tracer provider is configured.

Propagation runs the other way: `traceparent`, `tracestate` and `baggage` are
injected into the outgoing metadata from the new span, replacing any stale
values the caller left under those keys, so the callee continues the same
trace.

A span is red only for statuses that mean something went wrong —
`INTERNAL`, `UNKNOWN`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `UNAUTHENTICATED`,
`PERMISSION_DENIED`, `DATA_LOSS`. Ordinary application outcomes such as
`NOT_FOUND` leave the span OK with the code recorded as an attribute, so they
do not inflate error rates in the tracing backend. The span closes when the
unary response arrives or the response stream ends, never when the call was
merely created.

Without the `[tracing]` extra the interceptor is a documented pass-through:
calls run untouched, no metadata is added, no span is produced, and the
degradation is announced once per instance.
