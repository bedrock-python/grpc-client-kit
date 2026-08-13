"""Driving calls against the echo stub, and labelling how they ended.

A call that the circuit breaker refuses and a call the server really failed look alike from the
outside — `CircuitBreakerOpenError` derives from `AioRpcError` and carries UNAVAILABLE — so the
tests need one place that tells the two apart, and one place that keeps calling an open circuit
until it admits a trial.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass

import grpc
import grpc.aio
import pytest

from grpc_client_kit import ChannelPool, CircuitBreakerOpenError, ConnectivityConfig, WaitForReadyConfig

from .chains import ResilienceChain, resilience_chain
from .echo_bench import EchoStub, client_for_target
from .waiting import POLL_INTERVAL, WAIT_TIMEOUT


async def requests(items: Iterable[bytes]) -> AsyncIterator[bytes]:
    """Feed `items` to a streaming-request call as the async iterator gRPC expects."""
    for item in items:
        yield item


@dataclass(frozen=True, slots=True)
class RpcKind:
    """One RPC kind: how it is invoked, what it must answer, how metrics label it."""

    method: str
    rpc_type: str
    invoke: Callable[[EchoStub], Awaitable[bytes | list[bytes]]]
    expected: bytes | list[bytes]


async def _call_unary_unary(stub: EchoStub) -> bytes | list[bytes]:
    return await stub.echo(b"hello")


async def _call_unary_stream(stub: EchoStub) -> bytes | list[bytes]:
    return [item async for item in stub.stream(b"go")]


async def _call_stream_unary(stub: EchoStub) -> bytes | list[bytes]:
    return await stub.collect(requests([b"ab", b"cd"]))


async def _call_stream_stream(stub: EchoStub) -> bytes | list[bytes]:
    return [item async for item in stub.chat(requests([b"ab", b"cd"]))]


UNARY_UNARY = RpcKind("Echo", "unary_unary", _call_unary_unary, b"hello")
UNARY_STREAM = RpcKind("Stream", "unary_stream", _call_unary_stream, [b"one", b"two", b"three"])
STREAM_UNARY = RpcKind("Collect", "stream_unary", _call_stream_unary, b"abcd")
STREAM_STREAM = RpcKind("Chat", "stream_stream", _call_stream_stream, [b"AB", b"CD"])

EVERY_KIND = [pytest.param(kind, id=kind.rpc_type) for kind in (UNARY_UNARY, UNARY_STREAM, STREAM_UNARY, STREAM_STREAM)]


async def drive(call: Awaitable[bytes]) -> bytes:
    """Await a call inside a task, so cancelling that task reaches the RPC."""
    return await call


async def connect_attempts_within(
    pool: ChannelPool,
    connectivity: ConnectivityConfig | None,
    *,
    window: float,
) -> int:
    """Count how many times a channel dials an address that hangs up on every connection.

    The listener completes the TCP handshake and closes at once, so every dial fails the same way
    on every platform — after the transport connected, before HTTP/2 settled. How soon the channel
    dials again is decided by gRPC's reconnect backoff and by nothing else the kit sets, which
    makes the dial count the one thing a test can measure the connectivity tuning by. Timing the
    failure instead would measure the operating system: Linux refuses a dead loopback port in
    microseconds, Windows takes a second, and neither number says anything about the backoff.

    A wait-for-ready call pins the channel to its redial loop for the whole window; its deadline
    is the window, so DEADLINE_EXCEEDED is the outcome that proves the channel was still trying.

    Args:
        pool: The pool the client draws its channel from.
        connectivity: The tuning under test, or None for gRPC's own defaults.
        window: Seconds the pending call keeps the channel dialing; also the call's deadline.

    Returns:
        Number of connections the channel opened within the window.

    Raises:
        AssertionError: If the call ended in anything other than DEADLINE_EXCEEDED, which would
            mean the channel gave up dialing before the window closed.
    """
    dials = 0

    def _hang_up(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal dials
        dials += 1
        writer.close()

    listener = await asyncio.start_server(_hang_up, "127.0.0.1", 0)
    port = listener.sockets[0].getsockname()[1]
    # One attempt, and the kit's own wait-for-ready layer: a retry would add its own connect
    # cycles to what is being counted, and a fail-fast call would end at the first dial.
    chain = resilience_chain(timeout=window, max_attempts=1, wait_for_ready=WaitForReadyConfig(default=True))
    stub = await client_for_target(pool, f"127.0.0.1:{port}", chain.interceptors, connectivity).connect()

    try:
        await stub.echo(b"hello")
    except grpc.aio.AioRpcError as error:
        code = error.code()
        assert code == grpc.StatusCode.DEADLINE_EXCEEDED, f"expected DEADLINE_EXCEEDED, got {code}"
        return dials
    finally:
        listener.close()
        await listener.wait_closed()

    raise AssertionError("a call to an address that hangs up on every dial must not succeed")


async def outcome(stub: EchoStub) -> str:
    """Label how one Echo call ended: its status name, or ``circuit-open`` when the breaker refused it.

    `CircuitBreakerOpenError` derives from `AioRpcError` and carries UNAVAILABLE, so it has to be
    caught first to stay distinguishable from a server that really is unavailable.
    """
    try:
        await stub.echo(b"hello")
    except CircuitBreakerOpenError:
        return "circuit-open"
    except grpc.aio.AioRpcError as error:
        return error.code().name
    return "ok"


@dataclass(frozen=True, slots=True)
class TrialOutcome:
    """What it took for an open circuit to let one call through.

    Attributes:
        response: The response of the call the circuit finally admitted.
        rejections: Calls refused before that one; zero means the circuit was never really shut.
        waited: Seconds spent waiting for the circuit to admit a trial.
    """

    response: bytes
    rejections: int
    waited: float


async def wait_for_trial(stub: EchoStub) -> TrialOutcome:
    """Call until the circuit stops refusing, and report what that cost.

    Args:
        stub: The stub to call.

    Returns:
        The admitted call's response, and what it took to get it.

    Raises:
        AssertionError: If the circuit admitted no trial within `WAIT_TIMEOUT`.
    """
    started = time.perf_counter()
    deadline = started + WAIT_TIMEOUT
    rejections = 0

    while True:
        response = await _attempt(stub)
        if response is not None:
            return TrialOutcome(response=response, rejections=rejections, waited=time.perf_counter() - started)

        rejections += 1
        if time.perf_counter() >= deadline:
            raise AssertionError(f"the circuit admitted no trial call within {WAIT_TIMEOUT}s")
        await asyncio.sleep(POLL_INTERVAL)


async def trial_in_flight(stub: EchoStub, chain: ResilienceChain) -> asyncio.Task[bytes]:
    """Keep calling until the circuit admits a trial into the hanging handler, and return that task.

    Args:
        stub: The stub to call; its Echo handler must be hanging.
        chain: The chain whose breaker is questioned about the half-open slot.

    Returns:
        The task holding the admitted trial, still running.

    Raises:
        AssertionError: If the circuit admitted no trial within `WAIT_TIMEOUT`.
    """
    deadline = time.perf_counter() + WAIT_TIMEOUT

    while time.perf_counter() < deadline:
        task = asyncio.create_task(drive(stub.echo(b"trial")))

        # A refused call finishes at once; an admitted one blocks in the handler and shows up as the
        # half-open slot the breaker handed it.
        while not task.done():
            status = await chain.circuit()
            if status is not None and status["half_open_calls"] == 1:
                return task
            await asyncio.sleep(POLL_INTERVAL)

        with suppress(CircuitBreakerOpenError):
            await task

    raise AssertionError(f"the circuit admitted no trial call within {WAIT_TIMEOUT}s")


async def _attempt(stub: EchoStub) -> bytes | None:
    """Call Echo, returning None when the circuit rejected it instead of dialing out."""
    try:
        return await stub.echo(b"trial")
    except CircuitBreakerOpenError:
        return None
