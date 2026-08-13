"""Retries against a real server: what gets reissued, what does not, and what the server counts.

The server's own call count is the witness in every test here, so a reissued call is one the server
saw twice, not one an interceptor claims to have made. That any of it is observable is the kit's
doing: a `grpc.aio` continuation resolves to a `Call` when the RPC is created and never raises, so a
retry layer that does not await its own call has nothing to retry on.

The last test covers the failure that must *not* be reissued however retryable it looks: the circuit
breaker's own refusal, which carries UNAVAILABLE.
"""

from __future__ import annotations

import time

import grpc
import grpc.aio
import pytest

from .calls import outcome, requests
from .chains import resilience_chain
from .echo_bench import STREAM, ClientFactory, RunningServer

# Backoff for the test that proves a circuit rejection is not retried: one retry of it would have to
# sit this out first, which is what makes the timing assertion able to tell the two apart.
_BLOCKING_BACKOFF = 0.5

# A call the breaker refuses never touches the network, so it returns orders of magnitude faster than
# the real round trip it replaced. The bound is loose on purpose; the sharp assertion is that the
# server's call count did not move.
_REJECTION_BUDGET = 0.05


async def test__retry__transient_failures_then_success__reissues_the_call_until_the_server_answers(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    control.fail_times = 2
    chain = resilience_chain(max_attempts=3)
    stub = await make_client(chain.interceptors).connect()

    # Act
    response = await stub.echo(b"hello")

    # Assert
    assert response == b"hello"
    assert control.calls == 3
    assert chain.attempts == 3


async def test__retry__server_never_recovers__gives_up_after_max_attempts(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(max_attempts=3)
    stub = await make_client(chain.interceptors).connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.echo(b"hello")

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
    assert control.calls == 3


async def test__retry__non_retryable_status__calls_the_server_once(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # Only meaningful with the outcome awaited: the shipped chain retries nothing at all, so it
    # would pass this by never having had the chance to get it wrong.
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.PERMISSION_DENIED
    chain = resilience_chain(max_attempts=3)
    stub = await make_client(chain.interceptors).connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.echo(b"hello")

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert control.calls == 1
    assert chain.attempts == 1


async def test__retry__streaming_request__calls_the_server_once(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # A request iterator is consumed by the first attempt and cannot be replayed, so the retry layer
    # refuses to repeat a streaming-request call — and now that it is installed on stream-unary calls
    # too, that refusal is the only thing keeping the count below at one.
    control = echo_server.service.stream_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(max_attempts=3)
    stub = await make_client(chain.interceptors).connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.collect(requests([b"ab", b"cd"]))

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
    assert control.calls == 1


async def test__retry__streaming_response__reissues_the_stream_until_the_server_answers(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # Restarting a stream replays items the consumer has already seen, so it takes both switches:
    # retry_streaming and the method on the idempotent whitelist.
    control = echo_server.service.unary_stream
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    control.fail_times = 2
    chain = resilience_chain(max_attempts=3, retry_streaming=True, idempotent_methods={STREAM})
    stub = await make_client(chain.interceptors).connect()

    # Act
    items = [item async for item in stub.stream(b"go")]

    # Assert
    assert items == [b"one", b"two", b"three"]
    assert control.calls == 3


async def test__retry__circuit_breaker_rejects_the_call__does_not_reissue_it(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(max_attempts=3, initial_backoff=_BLOCKING_BACKOFF, fail_threshold=1)
    stub = await make_client(chain.interceptors).connect()
    # The first call trips the circuit on its first attempt and is refused on its second.
    assert await outcome(stub) == "circuit-open"

    # Act
    started = time.perf_counter()
    refused = await outcome(stub)
    elapsed = time.perf_counter() - started

    # Assert
    assert refused == "circuit-open"
    assert control.calls == 1
    # CircuitBreakerOpenError carries UNAVAILABLE, which is retryable. Without the explicit guard
    # for it the retry layer would sit out _BLOCKING_BACKOFF twice before giving up, and would spend
    # the wait hammering a circuit whose whole job is to stop the hammering.
    assert elapsed < _REJECTION_BUDGET
