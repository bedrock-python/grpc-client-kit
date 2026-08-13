"""Retries, the call budget, and the breaker setting that defeats itself.

Three layers decide how long a call may take, how often it may be repeated, and when it
should not be attempted at all. They nest in that order -- timeout, retry, circuit breaker
-- and the nesting is the whole design.

This example runs four scenes against one deliberately unreliable server, which counts
every attempt that reaches it and reports how much deadline each one arrived with:

1. **A retry rides out a blip.** The server rejects the first two attempts with
   `UNAVAILABLE` and answers the third; the caller sees one successful call.
2. **The budget belongs to the call, not to the attempt.** With a 2s budget and three
   attempts, the deadlines the *server* observed shrink from attempt to attempt: 2s, then
   what is left after the first failure and its backoff, and so on. A per-attempt budget
   would hand out a fresh 2s three times and stretch the call to 6s.
3. **The trap: `fail_threshold` at or below `max_attempts`.** The breaker is the innermost
   layer, so it counts *attempts*, and the retry layer above it is what manufactures them.
   One unlucky call then trips its own circuit, the last attempt is refused by the breaker
   the earlier attempts just opened, and the caller is told about the circuit instead of
   about the server. Every later call fails fast for `recovery_timeout` seconds -- all on
   the evidence of a single request.
4. **The fix.** With `fail_threshold` above `max_attempts`, the same failing server
   produces the status it actually returned, and the circuit stays closed.

Run with: `python examples/resilience.py`. It prints what the client saw and what the
server saw in each scene, and exits 0.
"""

# ruff: noqa: T201 -- an example is a program whose output IS the documentation.

from __future__ import annotations

import asyncio
import logging
import sys
import time
from dataclasses import dataclass, field

import grpc
import grpc.aio

from grpc_client_kit import (
    AsyncCircuitBreakerInterceptor,
    ChannelPool,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    GrpcClient,
    GrpcClientConfig,
    RetryConfig,
    TimeoutConfig,
    build_interceptors,
)
from grpc_client_kit.interceptors import logical_interceptor

SERVICE = "example.Payments"
CHARGE = f"/{SERVICE}/Charge"

CALL_BUDGET_SECONDS = 2.0
SHUTDOWN_GRACE = 0.5


@dataclass(slots=True)
class ChargeControl:
    """The server's knobs and its record of what actually arrived.

    Attributes:
        fail_times: How many of the first attempts are rejected; 0 rejects every attempt.
        attempts: How many attempts reached the handler since the last `reset`.
        deadlines: Seconds left on each attempt's deadline, as the *server* saw it. This is
            the call budget as it crossed the wire, so it shows what the retry layer handed
            out without having to ask the client side.
    """

    fail_times: int = 0
    attempts: int = 0
    deadlines: list[float | None] = field(default_factory=list)

    def reset(self, fail_times: int) -> None:
        """Start a new scene with a clean record."""
        self.fail_times = fail_times
        self.attempts = 0
        self.deadlines.clear()

    async def charge(self, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """Record the attempt, then reject it or answer it."""
        self.attempts += 1
        self.deadlines.append(context.time_remaining())

        if self.fail_times == 0 or self.attempts <= self.fail_times:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "payment gateway unreachable")

        return b"charged:" + request


class ChargeStub:
    """What protoc would have generated: one multicallable bound to one channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind the Charge multicallable to `channel`."""
        self.charge: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(CHARGE)


async def start_server(control: ChargeControl) -> tuple[grpc.aio.Server, str]:
    """Start the unreliable demo server on an ephemeral port.

    Returns:
        The running server and the address clients connect to.
    """
    handler = grpc.method_handlers_generic_handler(
        SERVICE,
        {"Charge": grpc.unary_unary_rpc_method_handler(control.charge)},
    )
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((handler,))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


def find_breaker(chain: list[grpc.aio.ClientInterceptor]) -> AsyncCircuitBreakerInterceptor:
    """Reach the breaker inside a built chain.

    A chain holds four per-kind adapters per logical interceptor; `logical_interceptor`
    maps an adapter back to the interceptor it stands for and leaves anything else alone.

    Args:
        chain: The chain `build_interceptors` returned.

    Returns:
        The circuit breaker layer of that chain.

    Raises:
        LookupError: If the chain was built without a circuit breaker.
    """
    for entry in chain:
        logical = logical_interceptor(entry)
        if isinstance(logical, AsyncCircuitBreakerInterceptor):
            return logical

    raise LookupError("this chain has no circuit breaker")


async def call_charge(client: GrpcClient[ChargeStub]) -> None:
    """Make one call and print the outcome the caller was given."""
    try:
        async with client as stub:
            response = await stub.charge(b"order-1")
    except CircuitBreakerOpenError as error:
        # A subclass of AioRpcError carrying UNAVAILABLE, so it has to be caught first to
        # be told apart from the server's own UNAVAILABLE.
        print(f"[client] refused by the breaker: {error.code().name}: {error.details()}")
    except grpc.aio.AioRpcError as error:
        print(f"[client] failed: {error.code().name}: {error.details()}")
    else:
        print(f"[client] Charge -> {response!r}")


async def scene_retry_rides_out_a_blip(pool: ChannelPool, target: str, control: ChargeControl) -> None:
    """Two rejected attempts, one answered: the caller sees a single successful call."""
    print("\n--- 1. a retry rides out a blip (server rejects the first two attempts) ---")
    control.reset(fail_times=2)

    chain = build_interceptors(retry=RetryConfig(max_attempts=3, initial_backoff=0.05, jitter=0.0))
    client = GrpcClient(ChargeStub, GrpcClientConfig(target=target, insecure=True), pool, interceptors=chain)

    await call_charge(client)
    print(f"[server] attempts received: {control.attempts}")


async def scene_call_budget(pool: ChannelPool, target: str, control: ChargeControl) -> None:
    """One budget for the whole call, divided between the attempts."""
    print(f"\n--- 2. the {CALL_BUDGET_SECONDS:.0f}s budget covers every attempt, it is not handed out per attempt ---")
    control.reset(fail_times=0)

    chain = build_interceptors(
        timeout=TimeoutConfig(default=CALL_BUDGET_SECONDS),
        # jitter=0 only so the numbers below are reproducible; leave it at 0.1 in real use.
        retry=RetryConfig(max_attempts=3, initial_backoff=0.3, backoff_multiplier=2.0, jitter=0.0),
    )
    client = GrpcClient(ChargeStub, GrpcClientConfig(target=target, insecure=True), pool, interceptors=chain)

    started = time.perf_counter()
    await call_charge(client)
    elapsed = time.perf_counter() - started

    observed = ", ".join(f"{left:.2f}s" if left is not None else "none" for left in control.deadlines)
    print(f"[server] deadline each attempt arrived with: {observed}")
    print(f"[client] the whole call took {elapsed:.2f}s, inside its {CALL_BUDGET_SECONDS:.0f}s budget")


async def scene_breaker_trap(pool: ChannelPool, target: str, control: ChargeControl) -> None:
    """fail_threshold (2) at or below max_attempts (3): one call opens its own circuit."""
    print("\n--- 3. the trap: fail_threshold=2 with max_attempts=3 ---")
    control.reset(fail_times=0)

    chain = build_interceptors(
        retry=RetryConfig(max_attempts=3, initial_backoff=0.05, jitter=0.0),
        circuit_breaker=CircuitBreakerConfig(fail_threshold=2, recovery_timeout=30.0),
    )
    client = GrpcClient(ChargeStub, GrpcClientConfig(target=target, insecure=True), pool, interceptors=chain)

    await call_charge(client)

    state = (await find_breaker(chain).get_states())[CHARGE]
    print(f"[server] attempts received: {control.attempts} (the third never left the process)")
    print(f"[breaker] circuit is {state['state'].upper()} after {state['failure_count']} failed attempts")
    print("[note]    the caller was told about the breaker, not about the server, and every")
    print("[note]    following call now fails fast for recovery_timeout seconds")


async def scene_breaker_sized(pool: ChannelPool, target: str, control: ChargeControl) -> None:
    """fail_threshold (5) above max_attempts (3): the server's own status survives."""
    print("\n--- 4. sized correctly: fail_threshold=5 with max_attempts=3 ---")
    control.reset(fail_times=0)

    chain = build_interceptors(
        retry=RetryConfig(max_attempts=3, initial_backoff=0.05, jitter=0.0),
        circuit_breaker=CircuitBreakerConfig(fail_threshold=5, recovery_timeout=30.0),
    )
    client = GrpcClient(ChargeStub, GrpcClientConfig(target=target, insecure=True), pool, interceptors=chain)

    await call_charge(client)

    state = (await find_breaker(chain).get_states())[CHARGE]
    print(f"[server] attempts received: {control.attempts} (every attempt reached the wire)")
    print(f"[breaker] circuit is {state['state'].upper()} after {state['failure_count']} failed attempts")


async def main() -> None:
    # The kit announces state changes through the standard logging module. Sending them to
    # stdout keeps them in order with this script's own output instead of racing it on
    # stderr; scene 3 below is where one of them shows up.
    logging.basicConfig(level=logging.WARNING, stream=sys.stdout, format="[kit-log] %(levelname)s %(message)s")

    control = ChargeControl()
    server, target = await start_server(control)
    print(f"[server] listening on {target}")

    try:
        # Every scene builds its own chain, so each gets a fresh circuit breaker -- and,
        # since the chain is part of a channel's identity, its own pooled channel.
        async with ChannelPool() as pool:
            await scene_retry_rides_out_a_blip(pool, target, control)
            await scene_call_budget(pool, target, control)
            await scene_breaker_trap(pool, target, control)
            await scene_breaker_sized(pool, target, control)
    finally:
        await server.stop(grace=SHUTDOWN_GRACE)

    print("\n[server] stopped")


if __name__ == "__main__":
    asyncio.run(main())
