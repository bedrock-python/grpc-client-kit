# Load balancing

**If all you have is a list of addresses or a DNS name, consider gRPC's own
load balancing first.** It runs on subchannels inside one channel, sees a
broken connection the instant it breaks, and involves no Python in the hot
path — see [Native gRPC or the kit?](native-vs-kit.md) for the recipe and the
trade-offs. The kit's balancer earns its keep when you need what the native
one does not have: weighted distribution, a circuit breaker per target, or an
active health checker driving eligibility.

Client-side balancing spreads calls across a list of addresses you already
know — a headless service, a static set of pods, a pair of regional
endpoints. Point the settings at `targets` instead of `target` and the factory
builds a balancer for you; or build one directly:

```python
from grpc_client_kit import LoadBalancerConfig, LoadBalancingStrategy, create_balancer

targets = ["api-1:50051", "api-2:50051", "api-3:50051"]

balancer = create_balancer(targets=targets)  # round-robin (default)

balancer = create_balancer(  # weighted
    targets=targets,
    config=LoadBalancerConfig(
        strategy=LoadBalancingStrategy.WEIGHTED,
        weights={"api-1:50051": 1.0, "api-2:50051": 2.0, "api-3:50051": 1.0},
    ),
)

client = GrpcClient(
    GreeterStub,
    config=GrpcClientConfig(insecure=True),  # no target: the balancer picks
    pool=pool,
    balancer=balancer,
)
```

A client takes either a `target` or a `balancer` — passing both is ambiguous
and raises `ValueError` at construction rather than picking a winner at run
time. Every target is [validated](advanced.md#target-validation) when the
balancer is built, so a typo in the list fails before any traffic moves.

## Choosing a strategy

| Strategy | Selection | Suited to |
| :--- | :--- | :--- |
| `round_robin` (default) | Fixed cyclic order, skipping unhealthy targets | Instances of equal capacity |
| `random` | Uniform pick among healthy targets | Many targets, order irrelevant |
| `weighted` | Weighted pick among healthy targets | Heterogeneous instance sizes |

`weighted` requires `weights`; a target missing from the mapping defaults to
`1.0`, while negative weights and an all-zero total are rejected at
construction. If every *healthy* target happens to weigh zero, selection falls
back to a uniform pick among them rather than failing — the weights say how to
prefer backends, not whether to use them.

## Selection happens per connect

A target is chosen inside `client.connect()`, which is what `async with client`
calls. Consecutive blocks therefore land on different backends, and a client
held for the lifetime of a process still spreads its traffic. Nothing is
sticky: there is no session affinity, and a retry inside one call stays on the
channel that call already picked.

Because the factory builds
[one interceptor chain per target](interceptors.md#chains-per-target-or-one-shared-chain),
each backend also gets its own circuit breaker — a single sick instance cannot
fail-fast the calls destined for its healthy peers.

## Health decides eligibility

Give the balancer a [health checker](health.md) and only targets whose last
check reported `SERVING` are eligible:

```python
balancer = create_balancer(targets=targets, health_checker=checker)
```

Health is gathered concurrently for the whole list, with `return_exceptions=True`
so a checker that raises never breaks selection — that target is simply not
eligible. A target that has never been checked is not eligible either. If
nothing is left, the balancer raises `NoHealthyTargetsError` carrying the list
it tried, **before** any channel is touched.

The strategies read that data differently, and the difference is deliberate:

- `round_robin` re-gathers health on every selection, so a target that just
  went down is skipped immediately.
- `random` and `weighted` cache the healthy set for one second. They would
  otherwise pay for a full concurrent gather to make a decision that a
  one-second-old snapshot answers just as well.

## Passive quarantine: a failed call is fresher than any probe

An active checker learns about a dead backend one probe interval late. With
the default 30-second interval that is a long window in which the balancer
keeps routing a full share of traffic into an address nothing answers at —
each of those calls burning its entire deadline before failing.

The factory therefore installs a passive reporter at the innermost position of
every per-target chain: a call that fails with `UNAVAILABLE` or
`DEADLINE_EXCEEDED` quarantines its target on the balancer **immediately**,
for a few seconds — long enough for the next probe (or a recovered backend) to
have the casting vote. Counting `DEADLINE_EXCEEDED` is a deliberate trade: a
slow-but-alive server sits out one short quarantine, while a freshly dead one
— which often burns whole deadlines before the transport settles on
`UNAVAILABLE` — stops eating them at once.

Quarantine only ever narrows the choice, never empties it: when every
candidate is quarantined, the quarantine is ignored — degraded service beats
refusing to route, and the next failure simply renews the verdict. Building a
balancer by hand? `balancer.report_failure(target, quarantine=5.0)` is the
same seam, callable from anywhere that learns about a failure.

## Without a health checker

Every target starts eligible — the balancer has no evidence to filter on and
does not pretend otherwise. `round_robin` cycles the list, `random` picks
uniformly, `weighted` picks by weight. Passive quarantine still applies: the
first call that finds a backend dead takes it out of the rotation, so even
checker-less balancing loses roughly one call per outage rather than a full
share of the traffic.

That is a reasonable setup when something upstream (a service mesh, a load
balancer, a DNS record with health-aware records) already removes dead
backends from the list. When nothing does, add a checker: see
[Health checking](health.md), and in particular why the first pass has to land
before the first RPC.
