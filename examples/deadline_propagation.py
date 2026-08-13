"""One request budget, several calls, and the deadline the server actually received.

Requires the `deadline` extra (``pip install "grpc-client-kit[deadline]"``), which supplies
`deadline-budget` and with it `BudgetContext`.

A deadline that stops at the process boundary is not a deadline. Five hops into a request
that started with three seconds, the last hop is usually issued with its own fresh
configured timeout -- so a request that was supposed to answer in three seconds can spend
far longer than that failing. This example closes that gap and *measures* it: the demo
server records `context.time_remaining()` for every call, which is the deadline as it
crossed the wire, so nothing here is an assertion about the client's internals.

Six scenes, one server, one budget at a time:

1. **No budget installed: nothing happens.** The kit ships a mechanism, not a policy. The
   call arrives with the deadline the chain was configured with, exactly as before.
2. **Three calls out of one budget.** The deadlines the server observes shrink from call to
   call: each one is issued with what its predecessors left, not with a fresh timeout.
3. **A budget wider than the configured deadline.** The deadline wins -- a budget may only
   tighten a call, never loosen one.
4. **A spent budget.** The call is refused before the RPC is created: dialing out with
   nothing left can only produce `DEADLINE_EXCEEDED` a round trip later.
5. **Per-call caps.** They are keyed by the full method name (``/package.Service/Method``),
   because that is the name the kit asks the budget for.
6. **The deadline is also what makes waiting safe.** A call issued against a server that
   does not exist yet fails instantly without `wait_for_ready` and waits for it with the
   flag on -- bounded by the budget, and paying for the wait out of it.

Run with: `python examples/deadline_propagation.py`. It prints what each call was issued
with and what the server saw, and exits 0. (The file is named for the subject rather than
for the library: a script called ``deadline_budget.py`` would shadow the package it imports,
since the directory a script is run from goes on ``sys.path`` ahead of everything else.)
"""

# ruff: noqa: T201 -- an example is a program whose output IS the documentation.

from __future__ import annotations

import asyncio
import socket
import time
from dataclasses import dataclass, field

import grpc
import grpc.aio
from deadline_budget import BudgetContext

from grpc_client_kit import (
    ChannelPool,
    ConnectivityConfig,
    DeadlineBudgetConfig,
    GrpcClient,
    GrpcClientConfig,
    TimeoutConfig,
    WaitForReadyConfig,
    build_interceptors,
    use_budget,
)
from grpc_client_kit.interceptors.deadline import DeadlineBudgetExhaustedError

SERVICE = "example.Accounts"
FETCH = f"/{SERVICE}/Fetch"
AUDIT = f"/{SERVICE}/Audit"

# The deadline this client is configured with. Deliberately far wider than any budget below,
# so a call that arrives carrying it is unmistakably a call no budget touched.
CONFIGURED_TIMEOUT = 5.0

# What the request as a whole may spend, counted from the moment the budget is created.
REQUEST_BUDGET = 3.0

# How long the server holds each call in scene 2, so that the budget visibly drains.
SERVER_WORK = 0.35

# Scene 5 caps this one method, to show that caps are keyed by the RPC's own path.
AUDIT_CAP = 0.5

# Scene 6: how long the address stays empty after the call has been made.
SERVER_DELAY = 0.4

# Scene 6 dials an address nothing serves yet, which costs gRPC a couple of seconds per
# connect attempt at its own defaults. Tuning the reconnect is what makes the difference
# between demonstrating the flag and demonstrating the backoff.
FAST_RECONNECT = ConnectivityConfig(
    initial_reconnect_backoff=0.1,
    min_reconnect_backoff=0.1,
    max_reconnect_backoff=0.1,
)

SHUTDOWN_GRACE = 0.5


def show(seconds: float | None) -> str:
    """Render a deadline the way the server read it, or say there was none."""
    return "none" if seconds is None else f"{seconds:.2f}s"


@dataclass(slots=True)
class Ledger:
    """The demo service, and its record of what each call arrived with.

    Attributes:
        work_seconds: How long a handler holds the call before answering.
        observed: One ``(method, deadline)`` pair per call, in arrival order. The deadline is
            `context.time_remaining()` -- the budget as it crossed the wire. gRPC computes it
            against the wall clock, whose tick is ~15 ms on Windows, so a reading may sit a
            hair above the deadline that was actually sent.
    """

    work_seconds: float = 0.0
    observed: list[tuple[str, float | None]] = field(default_factory=list)

    def reset(self, work_seconds: float = 0.0) -> None:
        """Start a new scene with a clean record."""
        self.work_seconds = work_seconds
        self.observed.clear()

    def report(self) -> str:
        """Render every call of the current scene as ``Method(deadline)``."""
        return ", ".join(f"{method}({show(deadline)})" for method, deadline in self.observed)

    async def _serve(self, method: str, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """Record the deadline this call arrived with, then answer it."""
        self.observed.append((method, context.time_remaining()))

        if self.work_seconds:
            await asyncio.sleep(self.work_seconds)

        return b"ok:" + request

    async def fetch(self, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """Unary handler standing in for the work a request fans out to."""
        return await self._serve("Fetch", request, context)

    async def audit(self, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """A second method, so a per-method cap has something to single out."""
        return await self._serve("Audit", request, context)


class AccountsStub:
    """What protoc would have generated: multicallables bound to one channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind one multicallable per method to `channel`."""
        self.fetch: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(FETCH)
        self.audit: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(AUDIT)


async def start_server(ledger: Ledger, port: int = 0) -> tuple[grpc.aio.Server, str]:
    """Start the demo server, by default on an ephemeral port.

    Args:
        ledger: The service implementation to expose.
        port: Port to bind; 0 lets the OS pick a free one.

    Returns:
        The running server and the address clients connect to.
    """
    handler = grpc.method_handlers_generic_handler(
        SERVICE,
        {
            "Fetch": grpc.unary_unary_rpc_method_handler(ledger.fetch),
            "Audit": grpc.unary_unary_rpc_method_handler(ledger.audit),
        },
    )
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((handler,))
    bound = server.add_insecure_port(f"127.0.0.1:{port}")
    await server.start()
    return server, f"127.0.0.1:{bound}"


def free_port() -> int:
    """Reserve a loopback port, release it, and report which one it was.

    It is the address of a server that does not exist yet, which is the only way to make a
    client dial one. The window between releasing the port and binding it again is a race in
    principle; on loopback, in a script that binds it immediately, it is a safe convenience.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def client_for(
    pool: ChannelPool,
    target: str,
    chain: list[grpc.aio.ClientInterceptor],
    connectivity: ConnectivityConfig | None = None,
) -> GrpcClient[AccountsStub]:
    """Build a client on `chain`.

    Each scene builds its own chain, and a chain is part of a channel's identity, so every
    scene also gets its own pooled channel.
    """
    config = GrpcClientConfig(target=target, insecure=True, connectivity=connectivity)
    return GrpcClient(AccountsStub, config, pool, interceptors=chain)


def budgeted_chain(wait_for_ready: bool = False) -> list[grpc.aio.ClientInterceptor]:
    """Build a chain that installs a deadline and then trims it to the request budget.

    `DeadlineBudgetConfig` adds the layer; the budget itself arrives from the ambient
    context, so a chain built here and never given a budget behaves like one without it.
    """
    return build_interceptors(
        timeout=TimeoutConfig(default=CONFIGURED_TIMEOUT),
        deadline_budget=DeadlineBudgetConfig(),
        wait_for_ready=WaitForReadyConfig() if wait_for_ready else None,
    )


async def scene_no_budget(pool: ChannelPool, target: str, ledger: Ledger) -> None:
    """Without `use_budget` the layer is a pass-through: the configured deadline arrives."""
    print("\n--- 1. no budget installed: the layer touches nothing ---")
    ledger.reset()

    client = client_for(pool, target, budgeted_chain())
    async with client as stub:
        await stub.fetch(b"1")

    print(f"[server] {ledger.report()}")
    print(f"[note]   the configured {CONFIGURED_TIMEOUT:.0f}s deadline, untouched: the kit ships the mechanism,")
    print("[note]   and the chain only closes when the caller installs a budget")


async def scene_one_budget_many_calls(pool: ChannelPool, target: str, ledger: Ledger) -> None:
    """Three sequential calls out of one budget: each is issued with what is left of it."""
    print(f"\n--- 2. three calls out of one {REQUEST_BUDGET:.0f}s request budget ---")
    ledger.reset(work_seconds=SERVER_WORK)

    client = client_for(pool, target, budgeted_chain())

    started = time.perf_counter()
    # The budget's clock starts here, at the top of the request -- not at the first call.
    with use_budget(BudgetContext.create(total_seconds=REQUEST_BUDGET)):
        async with client as stub:
            for number in range(1, 4):
                await stub.fetch(str(number).encode())
    elapsed = time.perf_counter() - started

    print(f"[server] {ledger.report()}")
    print(f"[note]   each call carries what its predecessors left, not a fresh {CONFIGURED_TIMEOUT:.0f}s")
    print(f"[client] the request spent {elapsed:.2f}s of its {REQUEST_BUDGET:.0f}s")


async def scene_budget_only_tightens(pool: ChannelPool, target: str, ledger: Ledger) -> None:
    """A request with more time left than this call is allowed does not extend the call."""
    print("\n--- 3. a budget wider than the configured deadline ---")
    ledger.reset()

    client = client_for(pool, target, budgeted_chain())
    with use_budget(BudgetContext.create(total_seconds=CONFIGURED_TIMEOUT * 10)):
        async with client as stub:
            await stub.fetch(b"1")

    print(f"[server] {ledger.report()}")
    print(
        f"[note]   the smaller of the two wins: 50s of request budget buys no more than the {CONFIGURED_TIMEOUT:.0f}s"
    )
    print("[note]   this method is configured for")


async def scene_spent_budget(pool: ChannelPool, target: str, ledger: Ledger) -> None:
    """An exhausted budget refuses the call locally instead of dialing out to be told so."""
    print("\n--- 4. a budget that is already spent ---")
    ledger.reset()

    client = client_for(pool, target, budgeted_chain())
    budget = BudgetContext.create(total_seconds=0.15)
    await asyncio.sleep(0.3)  # the request outlives its budget before this call is even made

    with use_budget(budget):
        try:
            async with client as stub:
                await stub.fetch(b"1")
        except DeadlineBudgetExhaustedError as error:
            # An AioRpcError carrying DEADLINE_EXCEEDED, so every handler already written for
            # a late call treats it normally; the budget library's own error is the cause.
            print(f"[client] {type(error).__name__}: {error.code().name}")
            print(f"[client] details: {error.details()}")
            print(f"[client] cause:   {type(error.__cause__).__name__}")

    print(f"[server] calls received: {len(ledger.observed)} -- the RPC was never created")


async def scene_per_call_cap(pool: ChannelPool, target: str, ledger: Ledger) -> None:
    """Caps are keyed by the full method name, which is what the kit asks the budget for."""
    print(f"\n--- 5. a per-call cap of {AUDIT_CAP}s on {AUDIT} ---")
    ledger.reset()

    client = client_for(pool, target, budgeted_chain())
    budget = BudgetContext.create(total_seconds=REQUEST_BUDGET, call_caps={AUDIT: AUDIT_CAP})

    with use_budget(budget):
        async with client as stub:
            await stub.fetch(b"1")
            await stub.audit(b"1")

    print(f"[server] {ledger.report()}")
    print("[note]   Fetch got what the request had left; Audit got its cap, because the cap is")
    print("[note]   stored under /package.Service/Method and looked up by that same name")


async def scene_wait_for_ready(pool: ChannelPool, ledger: Ledger) -> None:
    """The flag decides whether a cold channel fails or waits; the deadline bounds the wait."""
    print("\n--- 6. a call made before its server exists ---")
    ledger.reset()

    port = free_port()
    target = f"127.0.0.1:{port}"

    # Fail-fast first, against the same empty address: this is the UNAVAILABLE burst every
    # client produces while a backend is restarting.
    fail_fast = client_for(pool, target, budgeted_chain(), FAST_RECONNECT)
    started = time.perf_counter()
    try:
        async with fail_fast as stub:
            await stub.fetch(b"1")
    except grpc.aio.AioRpcError as error:
        print(f"[client] without the flag: {error.code().name} after {time.perf_counter() - started:.2f}s")

    waiting = client_for(pool, target, budgeted_chain(wait_for_ready=True), FAST_RECONNECT)

    async def call_and_wait() -> bytes:
        """Issue the call that has to outlive the missing server."""
        async with waiting as stub:
            return bytes(await stub.fetch(b"1"))

    with use_budget(BudgetContext.create(total_seconds=REQUEST_BUDGET)):
        # Created inside the block on purpose: a task inherits the context it was created in,
        # so this call runs under the same budget without anything being passed to it.
        call = asyncio.create_task(call_and_wait())

    await asyncio.sleep(SERVER_DELAY)
    print(f"[client] with the flag: still waiting after {SERVER_DELAY:.1f}s (call done: {call.done()})")

    late_server, late_target = await start_server(ledger, port=port)
    print(f"[server] a server finally appears on {late_target}")
    try:
        print(f"[client] the waiting call answered: {await call!r}")
        print(f"[server] {ledger.report()}")
        print(f"[note]   less than the {REQUEST_BUDGET:.0f}s budget: the wait was paid for out of it, which is")
        print("[note]   why waiting is only enabled for calls something eventually ends")
    finally:
        await late_server.stop(grace=SHUTDOWN_GRACE)


async def main() -> None:
    ledger = Ledger()
    server, target = await start_server(ledger)
    print(f"[server] listening on {target}")

    try:
        async with ChannelPool() as pool:
            await scene_no_budget(pool, target, ledger)
            await scene_one_budget_many_calls(pool, target, ledger)
            await scene_budget_only_tightens(pool, target, ledger)
            await scene_spent_budget(pool, target, ledger)
            await scene_per_call_cap(pool, target, ledger)
            await scene_wait_for_ready(pool, ledger)
    finally:
        await server.stop(grace=SHUTDOWN_GRACE)

    print("\n[server] stopped")


if __name__ == "__main__":
    asyncio.run(main())
