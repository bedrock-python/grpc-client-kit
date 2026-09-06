# Resilience

Five layers decide how long a call may take, whether it may wait for a
connection, how often it may be repeated, and when it should not be attempted
at all. They nest in that order — timeout, deadline budget, wait-for-ready,
retry, circuit breaker — and that nesting is the whole design; see
[the chain](interceptors.md#the-chain).

Propagating the caller's remaining time is a topic of its own, since half of it
is the caller's job rather than the kit's:
[Deadline budgets](deadlines.md) covers it.

## Timeouts

**A timeout is the budget of an entire call, retries included.**
`AsyncTimeoutInterceptor` runs once per call; the retry layer nested below it
divides what that budget leaves. Coming from a per-attempt model, this is the
one thing to unlearn: `max_attempts × timeout` is not how long a call can
take.

```python
from grpc_client_kit import TimeoutConfig

TimeoutConfig(
    default=10.0,
    per_method={
        "/orders.v1.Orders/Export": 120.0,
        "/orders.v1.Orders/Stream": None,  # explicitly unbounded
    },
)
```

- A deadline the caller already set wins when it is **smaller**: a per-call
  deadline may tighten the configured budget, never loosen it.
- `None` or `0` means "no deadline". A per-method `None` disables the budget
  for that method only, overriding `default`.
- `TimeoutConfig(default=None)` with no `per_method` entries adds **no
  interceptor at all** — a pass-through layer would still cost a hop per call
  and, since the chain is part of the pool key, a separate channel.

A settings `timeout` block only carries `default`. Per-method budgets need a
hand-built chain.

Two layers may narrow the deadline further before the call is issued: the
[request budget](deadlines.md), which trims it to what the caller's request has
left, and gRPC itself, which never lets a call outlive the deadline its own
details carry. Nothing ever widens it.

## Waiting for a connection

A `grpc.aio` channel connects lazily, so a call made before the connection is
up fails immediately with `UNAVAILABLE`. That is the burst of errors every pod
produces in the first second of its life, and every client produces again after
a backend restart: failures that describe the *channel's age* rather than the
service's health. gRPC's answer is the `wait_for_ready` flag, and this layer is
where it gets configured — globally or per method, the way deadlines are.

```python
from grpc_client_kit import WaitForReadyConfig

WaitForReadyConfig(
    default=True,
    per_method={
        "/orders.v1.Orders/Probe": False,  # this one must fail fast
        "/orders.v1.Orders/Export": None,  # exempt: left exactly as it arrives
    },
    require_deadline=True,
)
```

- A method resolving to `True` is issued with `wait_for_ready=True`; `False`
  is issued fail-fast explicitly; `None` — globally or per method — leaves the
  call untouched.
- **A value the caller set at the call site always wins.** That is the only way
  to opt one call out of a policy set for the whole client.

### Without a deadline, this is how you hang a call forever

On its own the flag does not remove a failure mode, it swaps one for another:
the call no longer fails fast, it waits — and a wait for a backend that never
comes back never ends by itself. `UNAVAILABLE` in 200 ms is a bad answer;
nothing at all, for the rest of the process's life, is a worse one.

Bounded by a deadline, the same trade is one-sided: the call either connects
and runs, or ends in `DEADLINE_EXCEEDED` after exactly the time it was allowed
— which is what the caller asked for either way. Hence `require_deadline=True`,
the default: **waiting is enabled only for calls that carry a deadline.** A
call without one is left fail-fast and reported once per interceptor instance,
at `WARNING`, naming the method and the two ways out (configure a timeout for
it, or accept the risk with `require_deadline=False`).

A chain built by this kit carries a deadline by default, so the interlock
rarely bites — but a `TimeoutConfig(default=None)` with no per-method entry,
or no `timeout` block at all, is exactly the configuration in which it does.

### What it changes downstream

Turning waiting on quietly rewrites which status a broken backend produces, and
two layers below read that status:

- **Connection failures stop being retryable.** `UNAVAILABLE` is in
  [`DEFAULT_RETRYABLE_CODES`](#retries); `DEADLINE_EXCEEDED` deliberately is
  not. A call that used to burn three attempts on a backend that was down now
  spends its deadline waiting for that backend to come back instead — usually
  the better bargain, and never the same one.
- **The circuit breaker is unaffected.** Its failure set is wider than the
  retryable set and contains both codes, so a backend that stays away still
  trips its circuit; see
  [sizing the breaker](#sizing-the-breaker-against-the-retries).
- **A dead backend now costs a full deadline instead of milliseconds.** That is
  the price of the trade, paid by every call while the backend is away, and the
  reason the deadline bounding it should be one you would actually be willing
  to wait.

A settings object reaches this layer the same way it reaches
[deadline budgets](deadlines.md#adding-the-layer): an optional `wait_for_ready`
block with `default`, `per_method` and `require_deadline`, read with `getattr`
and turned into the interceptor by the factory. A hand-built chain asks for it
as `build_interceptors(wait_for_ready=...)`. Either way it sits below the layers
that settle the deadline — it reads that deadline to decide — and above retry,
where one pass suffices, since each attempt is rebuilt from the details this
layer already wrote.

`examples/deadline_propagation.py` shows both halves against one address: a
call refusing to wait reports `UNAVAILABLE` in 0.1 s, and the same call with
the flag waits for a server started 0.4 s later, paying for the wait out of its
request budget.

## Retries

```python
from grpc_client_kit import RetryConfig

RetryConfig(
    max_attempts=3,  # total, first attempt included
    initial_backoff=0.1,
    max_backoff=10.0,
    backoff_multiplier=2.0,
    jitter=0.1,  # backoff × (1 ± jitter)
    retryable_codes=None,  # None = DEFAULT_RETRYABLE_CODES; empty set = never retry
    retry_streaming=False,
    idempotent_methods=None,
)
```

`grpc_client_kit.interceptors.DEFAULT_RETRYABLE_CODES` is
`{UNAVAILABLE, RESOURCE_EXHAUSTED}` and nothing else. Both mean the attempt
was rejected before the server application saw the request: `UNAVAILABLE`
comes from connection failures and from servers that are draining,
`RESOURCE_EXHAUSTED` from quota and flow-control checks that run ahead of the
handler.

### Retry safety

Retrying an RPC the server already executed duplicates its side effects, so
what gets retried is deliberately narrow — and the default is a **compromise,
not a guarantee**.

- **"Usually" is not "always".** Measured against a live server, both default
  codes can follow a request that *was* executed: a server dying mid-handler
  surfaces as `UNAVAILABLE` (the retry then re-executes the same logical
  request on the restarted server), and a handler is free to abort with
  `RESOURCE_EXHAUSTED` after a write. Where a duplicate write is unaffordable,
  set `idempotent_methods` — with the whitelist in place, nothing outside it
  is ever retried.
- **`INTERNAL` is deliberately absent** from the default set. It is raised by
  the handler itself, so the write has very likely been applied — retrying
  duplicates it with certainty rather than in the corner cases. The same
  reasoning excludes `UNKNOWN`, `ABORTED` and `DEADLINE_EXCEEDED`.
- **`idempotent_methods` is a whitelist for every call kind.** When it is set,
  a method outside it is not retried even on a retryable code — the tool for
  widening `retryable_codes` per method rather than across your whole API.
- **Streaming responses need that whitelist.** Restarting a unary-stream call
  replays items the consumer has already seen, so `retry_streaming=True` alone
  is not enough: the method must also appear in `idempotent_methods`, and each
  restart is logged as a warning.
- **Streaming requests are never retried.** The request iterator is consumed
  by the first attempt and cannot be replayed without buffering it whole.
- **A tripped circuit is not retried.** `CircuitBreakerOpenError` carries
  `UNAVAILABLE`, which is retryable by default; the retry layer recognizes the
  type and re-raises it at once, instead of hammering a breaker that exists to
  stop exactly that.
- **Never stack kit retries on a native `retryPolicy`.** A service config's
  retries run inside the channel, below every interceptor, so the two layers
  multiply — 3 × 3 = 9 requests reach the server, invisibly to the kit's logs
  and metrics. The client warns when it sees both configured; see
  [Native gRPC or the kit?](native-vs-kit.md).

### Retries inside the call budget

The relative timeout on the call details is the budget for the whole call, so
the retry layer converts it into a monotonic deadline **once**, on entry.
Before each attempt it recomputes what is left and re-expresses it as the
relative timeout gRPC understands; before each backoff it checks whether the
wait alone would outlive the budget, and abandons the retry if it would,
propagating the original error. Without this, three attempts of a ten-second
call would stretch it to thirty.

Backoff is `initial_backoff × backoff_multiplier^(attempt-1)`, multiplied by
`(1 ± jitter)` and capped at `max_backoff`.

## Circuit breaker

```python
from grpc_client_kit import CircuitBreakerConfig

CircuitBreakerConfig(
    fail_threshold=5,  # consecutive failures CLOSED → OPEN
    recovery_timeout=60.0,  # seconds OPEN before a trial call is allowed
    half_open_max_calls=1,  # concurrent trial calls in HALF-OPEN
    max_methods=1000,  # LRU bound on tracked methods
    metrics=my_registry,  # optional state gauge
)
```

States are the usual three. CLOSED counts consecutive failures and resets the
count on any success. At `fail_threshold` the circuit goes OPEN and calls fail
immediately with `CircuitBreakerOpenError` — no network involved. After
`recovery_timeout` the next call moves it to HALF-OPEN, where at most
`half_open_max_calls` trial calls run concurrently: one success closes the
circuit, one failure reopens it. A streaming call holds its trial slot until
the stream ends, which is also when its verdict is recorded.

Only failures that say something about the server's health count:
`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, `RESOURCE_EXHAUSTED`,
`ABORTED`, `UNKNOWN`, `DATA_LOSS`. Application outcomes such as `NOT_FOUND`
never trip anything, however many of them there are. Non-gRPC exceptions always
count; a cancellation counts as nothing, since a caller walking away says
nothing about the server.

An LRU of `max_methods` entries bounds what is tracked, so a process calling
generated method names forever cannot leak — but eviction **never discards a
protecting circuit**. An OPEN state silently evicted would re-close the
breaker: the next call to the "protected" method would go out to a backend the
breaker had declared down, and it would take another full threshold of real
failures to open it again. The victim is therefore always a clean CLOSED
state; when every state is protecting something, the map grows past the limit
instead, with a warning — memory yields to correctness.

Live snapshots are available at every level: the interceptor's
`get_states()`, `GrpcClient.circuit_breaker_states()` per target, and
`GrpcClientFactory.circuit_breaker_states()` across everything the factory
built. During an incident that is the first question — "is the breaker open,
or is the backend down?" — and the [metrics](observability.md#metrics) keep
the two apart as well: a local rejection is `status="rejected"`, never
`"error"`.

### Circuit breaker isolation

The breaker keeps state per method, in the interceptor instance. Since an
instance belongs to exactly one channel, and a channel to exactly one target,
giving each target its own instance makes the state effectively per
`(target, method)` — so one failing member of a load-balanced set cannot trip
the breaker for its healthy peers. `GrpcClientFactory` does this by handing
`GrpcClient` an `interceptor_factory`; a shared `interceptors` list gives up
the isolation on purpose.

### Sizing the breaker against the retries

The breaker is the innermost layer, so it counts **attempts, not calls** —
and the retry layer above it is what manufactures those attempts. One logical
call therefore contributes up to `max_attempts` consecutive failures to its
counter.

That makes `fail_threshold ≤ max_attempts` a configuration that defeats
itself:

```python
RetryConfig(max_attempts=3)
CircuitBreakerConfig(fail_threshold=2)  # a single call can open its own circuit
```

Attempt 1 fails with `UNAVAILABLE` and is counted. The retry layer issues
attempt 2, which fails and reaches the threshold — the circuit opens. The
retry layer then tries attempt 3, and the breaker it just tripped refuses it
with `CircuitBreakerOpenError`. Since a tripped circuit is never retried, that
error is what propagates: the caller is told
`UNAVAILABLE: Circuit breaker for /pkg.Service/Method is open` instead of the
status the server actually returned, the last attempt never reaches the wire,
and every following call fails fast for `recovery_timeout` seconds on the
evidence of one unlucky request.

Keep `fail_threshold` above `max_attempts`. With the defaults (3 attempts,
threshold 5) no single call can open the circuit, and it takes roughly
`fail_threshold / max_attempts` consecutively failing calls to do so — the
number to reason about when tuning either value.

The inverse pairing is harmless but worth knowing: the breaker's failure set
is wider than the retryable set, so an attempt can count against the circuit
without ever being retried — a method that keeps hitting its deadline opens
its circuit with no retry issued at all.
