# Deadline budgets

A [timeout](resilience.md#timeouts) says how long *this call* may take. A
budget says how long *the request* may take — and a request that fans out to
five services spends one deadline in five places. Without something connecting
the two, the fifth hop is issued with its own fresh ten seconds however little
of the request's three are left, and a request that was supposed to fail at
three seconds spends far longer than that failing.

This layer connects them: every outgoing call is trimmed to what the request
still has. It needs the `deadline` extra, which supplies
[`deadline-budget`](https://pypi.org/project/deadline-budget/):

```bash
pip install "grpc-client-kit[deadline]"
```

## The kit ships a mechanism; the caller closes the chain

Say this part plainly, because it is the part that gets assumed: **a service
that never installs a budget propagates nothing**, however many interceptors it
runs.

`deadline-budget` deliberately has no ambient context. A `BudgetContext` is
created by whoever starts the request and handed from one call site to the next
by argument — the right default for a budget library, and the wrong shape for a
client kit, where the interceptor sits far below the code that knows what the
budget is and none of the layers in between would carry it. So the kit supplies
the one piece that was missing, a context variable, plus the two functions that
write and read it:

```python
from deadline_budget import BudgetContext

from grpc_client_kit import use_budget

with use_budget(BudgetContext.create(total_seconds=3.0)):
    await orders.fetch(order_id)  # issued with 3.0s
    await payments.charge(order)  # issued with whatever the first call left
```

Outside that block every call behaves exactly as it did before: the layer finds
no budget and touches nothing.

The budget is normally created where the request enters the process, out of the
deadline the caller sent — that is the hop that makes the chain a chain:

```python
class OrdersServicer(orders_pb2_grpc.OrdersServicer):
    async def Submit(self, request, context):
        left = context.time_remaining()  # None when the caller sent no deadline
        budget = BudgetContext.create(total_seconds=left) if left else None
        with use_budget(budget):
            return await self._submit.execute(request)
```

`use_budget(None)` is a valid call — it installs nothing — so the handler above
needs no second branch. Note `left` may be `None` (an unbounded caller) and
`BudgetContext.create` rejects a non-positive total, which is why the guard
tests the value rather than passing it straight through.

## Adding the layer

```python
from grpc_client_kit import DeadlineBudgetConfig, RetryConfig, TimeoutConfig, build_interceptors

interceptors = build_interceptors(
    timeout=TimeoutConfig(default=5.0),
    deadline_budget=DeadlineBudgetConfig(reserve_for_next=0.0),
    retry=RetryConfig(max_attempts=3),
)
```

`reserve_for_next` is seconds withheld from every call for the work that
follows it — the caller's own response handling, or a compensating call after a
failure. It is passed straight to the budget, which subtracts it from what it
grants.

Two things worth knowing before you wire it:

- **A settings object can carry this layer too.** A `deadline_budget` block
  with `reserve_for_next` is read from
  [the settings object](configuration.md#settings-objects) with `getattr` — so
  a factory-built chain gets the layer in
  [its proper place](#where-it-sits-in-the-chain), with no hand-built chain
  involved. What settings cannot express is anything finer — per-method caps
  above all; that is what `build_interceptors` is for, and the chain it returns
  goes to `GrpcClient(interceptors=...)`. Not to
  `create_client(interceptors=...)`, which places what it is given in the outer
  slot, above the layers this one has to read —
  [why](configuration.md#a-settings-object-end-to-end).
- **Without the extra the layer is skipped**, with a warning. The chain is
  built and works; deadlines are simply not trimmed. Nothing raises at import
  time, and `import grpc_client_kit` never needs the extra.

## What it does to a call

| The context holds | The call is issued with |
| :--- | :--- |
| no budget | exactly what it arrived with |
| a budget with time left | the smaller of its own deadline and what the budget grants |
| an exhausted budget | nothing — the RPC is never created |

**A budget may only tighten a deadline, never extend one.** A request with 50
seconds left does not entitle a method configured for 5 to more than 5, and a
call the caller already bounded at 1 stays at 1: every layer that touches the
deadline takes the smaller of what it finds and what it knows.

**An exhausted budget fails locally.** Dialing out with nothing left can only
produce a `DEADLINE_EXCEEDED` one round trip later, so the call is refused
before it touches the network. What is raised is
`DeadlineBudgetExhaustedError` — a `grpc.aio.AioRpcError` carrying
`DEADLINE_EXCEEDED`, so existing handlers, logs, spans and metrics treat it
like any other late call, with the budget library's own
`DeadlineExceededError` kept as its `__cause__`:

```python
from grpc_client_kit.interceptors.deadline import DeadlineBudgetExhaustedError
```

Catching it by name is optional; `except grpc.aio.AioRpcError` with a
`DEADLINE_EXCEEDED` check already covers it, which is the point of the choice.

**A nearly spent budget still issues one call.** `BudgetContext` refuses to
hand out a timeout below its own `min_timeout` (0.1 s by default), so a budget
down to its last milliseconds yields that floor rather than a deadline too
short for anything to answer within. Only a budget with *nothing* left —
`remaining()` at or below zero — refuses the call. The kit does not
second-guess that floor; raise `min_timeout` if a call of yours is pointless
below, say, half a second.

## Per-call caps are keyed by the full method name

```python
BudgetContext.create(
    total_seconds=3.0,
    call_caps={"/orders.v1.Orders/Audit": 0.5},
)
```

The kit asks the budget for a timeout under the RPC's own full method name,
`/package.Service/Method`, which is how `BudgetContext` keys `call_caps`. A cap
stored under that name applies to that method and nothing else — and a cap
stored under a friendlier name (`"audit"`) applies to nothing at all.

## Where it sits in the chain

Timeout → **deadline budget** → [wait-for-ready](resilience.md#waiting-for-a-connection)
→ retry → circuit breaker.

- **Above retry, and that part is forced.** The retry layer converts the call's
  timeout into a deadline once, on entry, then hands out slices of it. A budget
  applied below that would arrive after the division had already been made from
  the untrimmed value — `max_attempts` attempts of a deadline the request could
  not afford even once.
- **Below timeout, which is a choice.** Both layers narrow the deadline and
  both take the smaller of what they find and what they know, so either order
  ends at the same number. This one reads in the order the two things are
  decided: the budget configured for the method, then how much of the request
  is left to spend on it.

The trimmed deadline is therefore what retry divides, and a refused call is
still logged, traced and measured like any other failure.

## Budgets, tasks and fan-out

The budget lives in a `ContextVar`, which gives it exactly the semantics a
deadline wants:

- **A task inherits the budget of the context it was created in.** `asyncio`
  copies the context at `create_task` time, so a fan-out started inside the
  block shares one deadline:

    ```python
    with use_budget(budget):
        calls = [asyncio.create_task(fetch(target)) for target in targets]

    results = await asyncio.gather(*calls)  # still budgeted: they were created inside
    ```

    The corollary is the mistake to avoid — a task created *outside* the block
    carries no budget, however deep inside it the `await` happens.

- **A budget installed inside a task cannot leak out of it**, and is invisible
  to that task's siblings. Nesting works too: `use_budget` restores the
  previous value on the way out, exception or not.

- **`use_budget(None)` detaches.** Background work that must outlive the
  request which spawned it belongs in a block of its own, or it inherits a
  deadline that is about to expire.

- **`current_budget()`** returns what is installed, or `None`. Useful for your
  own code — logging what a request has left, or skipping optional work when
  it clearly no longer fits.

## Bringing your own budget object

The contextvar is typed against `DeadlineBudgetProtocol`
(`timeout_for_call`, `remaining`, `expired`), not against
`BudgetContext`. That is what keeps `deadline-budget` optional — the kit never
imports it to *describe* a budget — and it leaves the door open for a caller
who already tracks deadlines their own way: anything with that shape works, and
the protocol is `runtime_checkable` if you want to assert it.

## What the server actually receives

`examples/deadline_propagation.py` measures it where it cannot be faked — the
demo server records `context.time_remaining()` for every call. One run of its
second scene, three sequential calls under a 3 s budget against a chain
configured for 5 s:

```text
--- 2. three calls out of one 3s request budget ---
[server] Fetch(3.00s), Fetch(2.65s), Fetch(2.29s)
[note]   each call carries what its predecessors left, not a fresh 5s
```
