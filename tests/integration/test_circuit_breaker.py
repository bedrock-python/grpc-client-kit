"""The circuit breaker on a live channel: what trips it, what it refuses, and what it lets through.

A refusal is proved by the server's call count standing still, not by an interceptor saying so, and
recovery is proved by a trial call that really reaches the server. The mid-stream case is the one a
mock cannot reach at all: the failure arrives long after the RPC was created, which is the moment an
unaware breaker would already have written down a success.
"""

from __future__ import annotations

import asyncio
import time

import grpc
import grpc.aio
import pytest

from grpc_client_kit import ChannelPool, CircuitBreakerOpenError, CircuitState

from .calls import drive, outcome, trial_in_flight, wait_for_trial
from .chains import resilience_chain
from .echo_bench import STREAM, ClientFactory, RunningServer, client_for
from .waiting import eventually, until_async

# A call the breaker refuses never touches the network, so it returns orders of magnitude faster than
# the real round trip it replaced. The bound is loose on purpose; the sharp assertion is that the
# server's call count did not move.
_REJECTION_BUDGET = 0.05

_RECOVERY_TIMEOUT = 0.2


async def test__circuit_breaker__failures_reach_the_threshold__stops_reaching_the_server(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    # One attempt per call, so every call the server counts is one the breaker let out.
    chain = resilience_chain(max_attempts=1, fail_threshold=2)
    stub = await make_client(chain.interceptors).connect()

    # Act
    outcomes = [await outcome(stub) for _ in range(4)]

    # Assert
    assert outcomes == ["UNAVAILABLE", "UNAVAILABLE", "circuit-open", "circuit-open"]
    assert control.calls == 2


async def test__circuit_breaker__open__rejects_the_call_without_touching_the_network(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(max_attempts=1, fail_threshold=1)
    stub = await make_client(chain.interceptors).connect()
    assert await outcome(stub) == "UNAVAILABLE"

    # Act
    started = time.perf_counter()
    refused = await outcome(stub)
    elapsed = time.perf_counter() - started

    # Assert
    assert refused == "circuit-open"
    assert control.calls == 1
    assert elapsed < _REJECTION_BUDGET


async def test__circuit_breaker__stream_fails_mid_flight__the_next_stream_never_reaches_the_server(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # The failure arrives after the first item, i.e. long after the RPC was created — the moment the
    # continuation resolved and an unaware breaker would already have written down a success.
    control = echo_server.service.unary_stream
    control.mid_stream_abort = grpc.StatusCode.INTERNAL
    control.mid_stream_after = 1
    chain = resilience_chain(max_attempts=1, fail_threshold=1)
    stub = await make_client(chain.interceptors).connect()
    items: list[bytes] = []

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        async for item in stub.stream(b"go"):
            items.append(item)
    with pytest.raises(CircuitBreakerOpenError):
        [item async for item in stub.stream(b"go")]

    # Assert
    assert items == [b"one"]
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL
    assert control.calls == 1
    status = await chain.circuit(STREAM)
    assert status is not None
    assert status["state"] == CircuitState.OPEN.value


async def test__circuit_breaker__recovery_timeout_elapses__admits_a_trial_that_closes_the_circuit(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(max_attempts=1, fail_threshold=1, recovery_timeout=_RECOVERY_TIMEOUT)
    stub = await make_client(chain.interceptors).connect()
    assert await outcome(stub) == "UNAVAILABLE"
    control.abort_code = None

    # Act
    trial = await wait_for_trial(stub)

    # Assert
    assert trial.response == b"trial"
    # Calls were refused before the window elapsed, so the trial was let out by the recovery timeout
    # and not by a circuit that never shut in the first place.
    assert trial.rejections >= 1
    assert control.calls == 2
    status = await chain.circuit()
    assert status is not None
    assert status["state"] == CircuitState.CLOSED.value
    assert status["failure_count"] == 0


async def test__circuit_breaker__one_target_failing__the_healthy_target_keeps_answering(
    echo_server: RunningServer,
    spare_echo_server: RunningServer,
    pool: ChannelPool,
) -> None:
    # Arrange
    # A chain each, which is what GrpcClient's interceptor_factory exists to produce: the breaker is
    # bound to a channel, hence to a target, so its verdict must not travel.
    echo_server.service.unary_unary.abort_code = grpc.StatusCode.UNAVAILABLE
    failing_chain = resilience_chain(max_attempts=1, fail_threshold=1)
    healthy_chain = resilience_chain(max_attempts=1, fail_threshold=1)
    failing = await client_for(pool, echo_server, failing_chain.interceptors).connect()
    healthy = await client_for(pool, spare_echo_server, healthy_chain.interceptors).connect()

    # Act
    outcomes = [await outcome(failing) for _ in range(3)]
    response = await healthy.echo(b"still-here")

    # Assert
    assert outcomes == ["UNAVAILABLE", "circuit-open", "circuit-open"]
    assert response == b"still-here"
    assert spare_echo_server.service.unary_unary.calls == 1
    healthy_status = await healthy_chain.circuit()
    assert healthy_status is not None
    assert healthy_status["state"] == CircuitState.CLOSED.value


async def test__circuit_breaker__one_chain_shared_by_two_targets__rejects_the_healthy_target_too(
    echo_server: RunningServer,
    spare_echo_server: RunningServer,
    pool: ChannelPool,
) -> None:
    # Arrange
    echo_server.service.unary_unary.abort_code = grpc.StatusCode.UNAVAILABLE
    shared = resilience_chain(max_attempts=1, fail_threshold=1)
    failing = await client_for(pool, echo_server, shared.interceptors).connect()
    healthy = await client_for(pool, spare_echo_server, shared.interceptors).connect()

    # Act
    first = await outcome(failing)
    second = await outcome(healthy)

    # Assert
    assert first == "UNAVAILABLE"
    # The breaker keys its state on the method name alone, so one shared chain makes a healthy
    # backend pay for its failing peer. This is the cost of passing `interceptors` instead of
    # `interceptor_factory`, and the reason the latter exists.
    assert second == "circuit-open"
    assert spare_echo_server.service.unary_unary.calls == 0


async def test__circuit_breaker__client_cancels_the_call__the_cancellation_is_not_a_failure(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.hang = True
    chain = resilience_chain(timeout=None, max_attempts=1, fail_threshold=2)
    stub = await make_client(chain.interceptors).connect()
    call = asyncio.create_task(drive(stub.echo(b"hello")))
    await eventually(lambda: control.calls == 1, message="the call never reached the server")

    # Act
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    control.hang = False
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    failed = await outcome(stub)

    # Assert
    assert failed == "UNAVAILABLE"
    status = await chain.circuit()
    assert status is not None
    # Only the server's failure counted. Had the cancellation counted as well, this second failure
    # would have been the threshold's and the circuit would be open.
    assert status["failure_count"] == 1
    assert status["state"] == CircuitState.CLOSED.value


async def test__circuit_breaker__client_cancels_a_half_open_trial__the_trial_slot_is_released(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(timeout=None, max_attempts=1, fail_threshold=1, recovery_timeout=_RECOVERY_TIMEOUT)
    stub = await make_client(chain.interceptors).connect()
    assert await outcome(stub) == "UNAVAILABLE"
    # The next trial the circuit admits blocks in the handler, which is how it can be caught in
    # flight and cancelled while it still holds the only half-open slot.
    control.abort_code = None
    control.hang = True
    trial = await trial_in_flight(stub, chain)

    # Act
    trial.cancel()
    with pytest.raises(asyncio.CancelledError):
        await trial

    # Assert
    async def slot_released() -> bool:
        status = await chain.circuit()
        return status is not None and status["half_open_calls"] == 0

    await until_async(slot_released, message="the cancelled trial never gave its half-open slot back")

    # A leaked slot would leave the circuit permanently half-open and refusing every trial, so the
    # only proof that it was really released is that the circuit can still close.
    control.hang = False
    control.release()
    follow_up = await wait_for_trial(stub)
    assert follow_up.response == b"trial"
    status = await chain.circuit()
    assert status is not None
    assert status["state"] == CircuitState.CLOSED.value
