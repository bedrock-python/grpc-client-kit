"""A call made before its server exists: does it wait for one, or fail on the spot?

The scenario is the one that produces the errors this flag is for — a client dialing a backend that
has not finished starting — and it is reproduced literally: the client is pointed at a port nothing
is listening on, the call is made, and only then is a server started there.

What separates the two behaviours is the status a call ends with. A call that waits can only finish
by succeeding once the server appears or by running out of deadline; ``UNAVAILABLE`` is reachable
exclusively by a call that refused to wait, and only while nothing is serving the address. That last
condition is not pedantry — a fail-fast call issued against an empty address still sits inside
gRPC's own connect machinery for a while, and a server appearing inside that window is picked up by
the connect already in flight, so the call succeeds after all. Hence the shape of the tests below:
the waiting one is given a server to wait for, and the fail-fast ones are given none.
"""

from __future__ import annotations

import asyncio
import time

import grpc
import grpc.aio
import pytest

from grpc_client_kit import ChannelPool, ConnectivityConfig, WaitForReadyConfig

from .calls import drive
from .chains import resilience_chain
from .echo_bench import StartEchoServer, client_for_target, free_port

# How long the address stays empty after the call has been made.
_SERVER_DELAY = 0.2

# The deadline bounding every call here. It has to cover gRPC's reconnect machinery, which is what a
# channel that has just failed to connect waits out before it tries the address again.
_CALL_TIMEOUT = 10.0

# By when a call that refused to wait must have reported. Far below the deadline, so "gave up" and
# "ran out of time" cannot be confused.
_GAVE_UP_BY = _CALL_TIMEOUT / 2

# Every channel here dials an address nothing serves yet, which by default costs gRPC a couple of
# seconds per connect attempt. The kit's own reconnect tuning cuts that to a tenth, which is the
# difference between a test that measures the flag and a test that mostly measures the backoff.
_FAST_RECONNECT = ConnectivityConfig(
    initial_reconnect_backoff=0.1,
    min_reconnect_backoff=0.1,
    max_reconnect_backoff=0.1,
)

# One attempt per call, so what these tests measure is the flag and nothing else. Retries would blur
# it in both directions: a second attempt landing while the channel is connecting waits for that
# connect to resolve, and a later one might find the server already up.
_ONE_ATTEMPT = 1


async def test__wait_for_ready__server_started_after_the_call__waits_for_it_and_succeeds(
    pool: ChannelPool,
    start_echo_server: StartEchoServer,
) -> None:
    # Arrange
    port = free_port()
    chain = resilience_chain(
        timeout=_CALL_TIMEOUT,
        max_attempts=_ONE_ATTEMPT,
        wait_for_ready=WaitForReadyConfig(),
    )
    stub = await client_for_target(pool, f"127.0.0.1:{port}", chain.interceptors, _FAST_RECONNECT).connect()

    # Act
    call = asyncio.create_task(drive(stub.echo(b"hello")))
    await asyncio.sleep(_SERVER_DELAY)
    still_waiting = not call.done()
    server = await start_echo_server(port)

    # Assert
    assert still_waiting, "the call gave up before the server existed"
    assert await asyncio.wait_for(call, _CALL_TIMEOUT) == b"hello"
    assert server.service.unary_unary.calls == 1


async def test__wait_for_ready__not_configured__gives_up_instead_of_waiting(
    pool: ChannelPool,
) -> None:
    # Arrange
    # The same address and the same deadline as the test above; the chain differs by one layer.
    port = free_port()
    chain = resilience_chain(timeout=_CALL_TIMEOUT, max_attempts=_ONE_ATTEMPT)
    stub = await client_for_target(pool, f"127.0.0.1:{port}", chain.interceptors, _FAST_RECONNECT).connect()

    # Act
    started = time.perf_counter()
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await asyncio.wait_for(asyncio.create_task(drive(stub.echo(b"hello"))), _CALL_TIMEOUT)
    elapsed = time.perf_counter() - started

    # Assert
    # This is the flapping the flag exists to remove: the call reports the service unavailable while
    # most of its deadline is still unspent, having declined to wait for a connection at all. The
    # same call in the test above spends that time waiting, and answers.
    assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
    assert elapsed < _GAVE_UP_BY


async def test__wait_for_ready__call_without_a_deadline__is_left_fail_fast(
    pool: ChannelPool,
) -> None:
    # Arrange
    # Waiting is enabled, but nothing bounds this call, so the interlock leaves it fail-fast: an
    # unbounded wait for an address nothing serves would never end at all.
    port = free_port()
    chain = resilience_chain(timeout=None, max_attempts=_ONE_ATTEMPT, wait_for_ready=WaitForReadyConfig())
    stub = await client_for_target(pool, f"127.0.0.1:{port}", chain.interceptors, _FAST_RECONNECT).connect()

    # Act
    started = time.perf_counter()
    # The bound is the assertion's teeth: without the interlock this call would hang for good, and
    # a hanging suite says far less than a failing one.
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await asyncio.wait_for(asyncio.create_task(drive(stub.echo(b"hello"))), _CALL_TIMEOUT)
    elapsed = time.perf_counter() - started

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
    assert elapsed < _GAVE_UP_BY
