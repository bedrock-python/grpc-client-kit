# Channels & pooling

A gRPC channel is a long-lived, multiplexed HTTP/2 connection. Opening one per
call is wasteful, and sharing one indiscriminately is worse — so the pool sits
between the two, handing out a channel only to callers that asked for exactly
the same thing.

## Channel identity

A pooled channel is filed under everything gRPC baked into it when it was
created — `ChannelKey`:

| Field | Compared by | Why it is part of the key |
| :--- | :--- | :--- |
| `target` | value | Different address, different connection |
| `insecure` | value | Plaintext and TLS channels are not interchangeable |
| `credentials` | **identity** | gRPC credentials define no equality |
| `options` | value (as a tuple) | Options are fixed at channel creation |
| `compression` | value | Likewise fixed at creation |
| `interceptors_token` | value | Interceptors are bound at creation and cannot be added later |

The first five are unremarkable: gRPC lets you configure them only while
building the channel, so a channel built for one combination cannot serve
another. The interesting one is the last.

## Why the chain is part of the key

**`grpc.aio` binds interceptors to a channel when the channel is created.**
There is no API to attach one afterwards, and none to run a call through a
different chain than the channel's. So the chain is not an attribute of the
call — it is an attribute of the connection. Two clients with different chains
must get different channels, and the alternative would be far worse than an
extra connection: whichever client asked first would silently impose its retry
policy, deadlines and circuit breaker on everybody else who happened to name
the same address.

The token itself is per interceptor **instance**. Each instance is tagged once
through a `WeakKeyDictionary` and keeps its tag for life, so a chain that is
reused keeps hitting the same pooled channel, and `chain_token` returns `None`
for an empty chain (a channel with no interceptors is interchangeable with any
other such channel).

The practical consequence when you build chains by hand: **build the chain
once per target and reuse it.** `GrpcClient` does this for you — it caches
what `interceptor_factory` returns, keyed by target — because rebuilding a
chain per call would mint a new identity every time and the pool would open a
channel per RPC. The two ways to hand a client its chain are covered in
[Interceptors](interceptors.md#chains-per-target-or-one-shared-chain).

## Keepalive and reconnect backoff

Two channel behaviours are left at their defaults far more often than they are
chosen: whether a channel pings its peer to notice a connection that has
silently gone away, and how long it waits before dialling again after losing
one. Both are gRPC *channel arguments* — fixed when the channel is created,
unchangeable afterwards, and spelled as an untyped list of
`("grpc.keepalive_time_ms", 30000)` pairs everyone is expected to remember in
the right unit. `ConnectivityConfig` is that list, named, in seconds, and
validated:

```python
from grpc_client_kit import ConnectivityConfig, GrpcClientConfig

config = GrpcClientConfig(
    target="api.internal:50051",
    connectivity=ConnectivityConfig(
        keepalive_time=30.0,  # ping after this much inactivity; None = never
        keepalive_timeout=10.0,  # unanswered for this long → the connection is dead
        permit_without_calls=False,  # do not ping while no call is in flight
        max_pings_without_data=2,  # 0 = no limit
        initial_reconnect_backoff=1.0,
        min_reconnect_backoff=None,  # None = keep gRPC's own default
        max_reconnect_backoff=30.0,
    ),
)
```

Why each half is worth setting:

- **Keepalive** detects a connection that has silently gone away — a dropped
  NAT mapping, a load balancer that closed one side, a peer that vanished. TCP
  alone can leave that undetected until a call has already hung on it.
- **Reconnect backoff** decides how long a channel that lost its connection
  waits before dialling again. gRPC's own upper bound is two minutes, which is
  a long time for a backend that restarts in seconds: the client keeps failing
  long after the server is back. The kit's integration suite measures the
  difference on a doomed call — around 2 s at gRPC's defaults against roughly
  0.1 s with the backoff tuned down.

`min_reconnect_backoff` is the one to know about: besides bounding the wait
*between* attempts it also bounds how long a single connect attempt may sit
there before it is written off, which is why it, rather than the maximum, is
what decides how quickly an unreachable address reports back.

**Opt-in on purpose.** `connectivity=None`, the default, leaves every one of
these arguments at gRPC's default, because pinging is a conversation the server
has to agree to: a server enforces its own minimum interval and answers a
client that pings too often with `GOAWAY` and `ENHANCE_YOUR_CALM`. A client
library that switched keepalive on for everybody would be a way to get
connections dropped by servers nobody had configured for it. The values above
are what you get once you have decided to tune the channel at all — conservative
enough for a server left at its own defaults, which permits pings on a
connection with calls in flight. `permit_without_calls=True` is the one to
agree with whoever runs the server before shipping.

Durations must be positive or `None`, `max_pings_without_data` non-negative,
and `min_reconnect_backoff ≤ max_reconnect_backoff`; anything else is a
`ValueError` at construction rather than a channel that quietly behaves unlike
the one you described.

### Explicit options win, without being duplicated

`options` and `connectivity` compose in `GrpcClientConfig.channel_options()`:
explicit options come first and verbatim, and any derived argument whose key
the caller already used is **dropped, not appended**.

```python
GrpcClientConfig(
    target="api.internal:50051",
    options=[("grpc.keepalive_time_ms", 60_000)],  # this one is used
    connectivity=ConnectivityConfig(keepalive_time=30.0),  # dropped: same key
)
```

Dropping is the only sense in which "explicit wins" can be honoured: gRPC
receives the argument once, with your value, so there is no duplicate for it to
resolve one way or the other.

### Tuning is part of the channel identity

The options list is part of `ChannelKey`, so this tuning decides which channel a
client gets:

- **Two clients tuned alike share one channel.** The list is a pure function of
  the configuration, emitted in a fixed order, so it compares equal however
  many times it is rebuilt.
- **Two clients tuned differently get a channel each** — correct rather than
  wasteful, since these arguments are baked in at creation: a client that wants
  to ping every 15 s genuinely cannot use a channel built to ping every 25.
- **A client with no `connectivity` keeps the identity it always had.** Its own
  `options` are handed back unchanged, `None` included, so adding the feature
  moved nobody's channel.

The order is fixed and deliberately not sorted: sorting would equate two option
lists that differ only in the order of conflicting duplicate keys, which gRPC
does not treat as identical.

### From a settings object

`GrpcClientFactory` reads `connectivity` with `getattr`, like the other
optional channel fields ([Configuration](configuration.md#settings-objects)), so
a settings object carrying one has its channels tuned and one that does not is
left with gRPC's defaults:

```python
class UpstreamSettings:
    target = "api.internal:50051"
    insecure = False
    connectivity = ConnectivityConfig(keepalive_time=30.0, max_reconnect_backoff=5.0)
    ...
```

## Inside the pool

| Field | Default | Meaning |
| :--- | :--- | :--- |
| `max_channels_per_target` | `1` | Channels per **identity**, not per address |
| `idle_timeout` | `300.0` | Seconds without active RPCs before gRPC core parks the connection; `0` never parks |

One target can back several identities (different security, options or
interceptor chains), and the limit applies to each of them separately. A
single HTTP/2 channel multiplexes concurrent RPCs perfectly well, so raise the
limit only when you have measured head-of-line blocking.

Within one identity the pool cycles through the channels it holds, skipping
those a health checker has marked down. Two edge cases are decided
deliberately:

- **Below the limit with nothing healthy**, the pool opens another channel —
  a fresh connection is the cheapest way to find out whether the backend came
  back.
- **At the limit with nothing healthy**, it serves the oldest channel anyway.
  It may have recovered since the last probe, and a stale channel beats no
  channel at all.

Idleness is delegated to gRPC core, never enforced by closing channels. The
pool translates `idle_timeout` into `grpc.client_idle_timeout_ms` on the
channels it creates (an explicit value of that option in your own `options`
wins), and core parks the connection only once **no RPC is active**, then
reconnects transparently on the next call. The distinction matters: a pool
that closed idle channels itself would invalidate the stub you are holding and
kill any stream that outlives the timeout — with core idling, a held stub stays
valid for life and a week-long stream is safe, while an idle connection still
releases its socket on both ends.

Pass `metrics=` to the pool — or set `metrics_registry` on your settings — and
every change reports the [pool gauges](observability.md#pool-statistics).

## Health is per address, not per identity

Health belongs to the server, not to the channel configuration, so
`update_channel_health(target, ...)` marks **every** pooled channel sharing
that address at once — the plaintext one, the TLS one, and each chain-specific
one. This is what a [health checker](health.md) calls when a probe changes a
target's status.

## Ownership and shutdown

| Object | Owns | On exit |
| :--- | :--- | :--- |
| `ChannelPool` | Its channels | `close_all(grace=...)` closes them; the pool stays usable |
| `GrpcClient` | Nothing | `__aexit__` closes nothing — channels belong to the pool |
| `GrpcClientFactory` | The pool **only if it created it**, plus its health checker | Stops the checker; closes the pool only when it owns it |

```python
await pool.close_all(grace=30.0)  # let in-flight RPCs finish
```

`close_all` drains the pool rather than retiring it: it closes every channel
and leaves the pool ready to serve again, so a shared pool survives one
shutdown and its `async with` block can be entered a second time.
A channel requested *while* the drain runs is refused with `RuntimeError`
rather than handed out — a channel the pool no longer tracks would leak past
shutdown.

A factory handed an existing pool (`GrpcClientFactory(pool=pool)`) borrows it
and never closes it: whoever created the pool closes it. And a factory used
without `async with` never starts its health checker — `create_client` warns
about exactly that, because the resulting client would route traffic by
unverified health data.
