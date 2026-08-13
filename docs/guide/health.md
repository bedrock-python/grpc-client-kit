# Health checking

Load balancing without health data spreads traffic evenly over live and dead
backends alike. `HealthChecker` (the `[health]` extra) probes
`grpc.health.v1.Health/Check` in the background and feeds both the balancer
and the channel pool.

## Starting a checker

```python
from grpc_client_kit import ChannelPool, HealthChecker, create_balancer

checker = HealthChecker(check_interval=30.0, timeout=5.0)
await checker.start(targets)
await checker.wait_until_ready(timeout=5.0)  # await the first pass

balancer = create_balancer(targets=targets, health_checker=checker)
pool = ChannelPool(health_checker=checker)  # pooled channels get health flags too
...
await checker.stop()  # closes the probe channels
```

Each probe asks for the **overall** server status — the empty service name of
the Health v1 protocol — rather than a per-service one, so a target is healthy
when the process behind it says it is serving at all.

`HealthChecker` is resolved lazily, so `import grpc_client_kit` works on a
bare install; touching `grpc_client_kit.HealthChecker` without the extra
raises an `ImportError` naming it. `HealthCheckerNotRunningError` lives in
`grpc_client_kit.health`, which likewise needs the extra.

## An unchecked target is not a healthy target

Health is reported from evidence only. `is_healthy()` returns `True` after a
check saw `SERVING`, `False` before that, and — when the target has no
recorded status *and* no loop is running to ever produce one — raises
`HealthCheckerNotRunningError` rather than claiming anything.

That last case is the one worth designing for. Balancers gather health with
`return_exceptions=True`, so the error surfaces merely as "not eligible", and
a checker nobody started looks exactly like a cluster that is entirely down.
The checker therefore also logs a one-time warning naming the real cause.

On a cold start the balancer raises `NoHealthyTargetsError` until the first
pass lands — which is why `wait_until_ready()` exists. It blocks until every
monitored target has a verdict and returns `False` if it timed out or the loop
is not running. Use it whenever the first RPC follows `start()` immediately.

## Inside the probe loop

`start()` spawns one background loop for all monitored targets. Each tick it
collects the targets whose next-check time has arrived and probes those
concurrently, then sleeps a second before looking again. Calling `start()` on
a running checker is ignored with a warning rather than starting a second
loop.

- **Scheduling is monotonic.** Wall-clock jumps (NTP, DST) would otherwise
  stall every target's next check or stampede them all at once.
- **Failures back off per target.** The next check is delayed by
  `check_interval × 2^(failures-1)`, capped at `max_backoff` (300s), so a dead
  backend is not probed at full rate while a live one keeps its interval. One
  success resets the counter.
- **A failed probe drops its channel.** A half-open connection would otherwise
  keep failing forever; the next check reconnects. The channel is dropped
  before the result is published, so a callback that raises cannot leave a
  broken connection behind.
- **Results fan out.** Each result updates the cache, marks the pooled
  channels of that target, and — only on a change — invokes
  `on_status_change`.
- **Channels of targets that are no longer monitored** are closed every five
  minutes, and every probe channel is closed by `stop()`.

`check_health(target)` is the same probe as a one-shot call: it runs the RPC,
publishes the result like any scheduled check, and returns the verdict.

## Probes run on their own channels, never on pooled ones

Three reasons, each sufficient on its own:

- a pooled channel carries the client's interceptor chain, so probing through
  it would pollute client metrics and count towards circuit breakers;
- pooled channels belong to application traffic and its lifecycle, while
  monitoring must run on its own schedule;
- a failed probe closes its channel to force a reconnect, which must never
  happen to a channel that application RPCs are holding.

## What the factory wires for you

| Field | Default | Meaning |
| :--- | :--- | :--- |
| `check_interval` | `30.0` | Seconds between checks of a healthy target |
| `timeout` | `5.0` | Budget for one probe RPC |
| `max_backoff` | `300.0` | Cap on the backoff of a failing target |

The factory builds a checker only when the settings carry both a
`health_checker` block **and** `targets`, and passes it `check_interval`,
`timeout`, `insecure` and `credentials`. It does **not** forward `options` or
`compression`: if your application channels need specific channel options for
the probes to negotiate HTTP/2 identically, construct `HealthChecker`
yourself and hand it to the pool and the balancer.

Entering the factory's `async with` starts the checker; leaving it stops the
checker and closes its probe channels. A factory used without that block warns
from `create_client` — see [Quick start](quickstart.md#letting-the-factory-wire-it).

## Reacting to changes

```python
async def on_change(target: str, is_healthy: bool) -> None:
    logger.warning("%s is now %s", target, "up" if is_healthy else "down")


checker = HealthChecker(on_status_change=on_change)
```

The callback fires only on a transition, not on every probe, so it is a
suitable place for an alert or a cache invalidation. A callback that raises is
logged and swallowed, because a broken notification must not stop the
monitoring that produced it; pass `fail_fast_callback=True` to have it
propagate instead — useful in tests, where a silently failing callback is
worse than a loud one.
