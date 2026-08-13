"""What the canonical chain does when the server misbehaves: aborts, mid-stream failures, silence.

The failures are genuine — the server aborts, gives up halfway through a stream or never answers at
all — and the chain under them is the shipped one, so what these tests read is what a caller of the
kit would see. That the failures arrive at all is the kit's doing: a `grpc.aio` continuation resolves
to a `Call` the moment the RPC is created and never raises, so a chain that does not await its own
call would report every one of these as a success.
"""

from __future__ import annotations

import asyncio
import logging

import grpc
import grpc.aio
import pytest

from .calls import requests
from .chains import CLIENT_SERVICE_NAME, canonical_chain
from .echo_bench import ClientFactory, RunningServer
from .recording import RecordingMetrics

# Upper bound for a call that must have been cut by its own deadline. It only has to outlive the
# configured budget: it exists to fail a test whose deadline never fires, not to wait for anything.
_HUNG_CALL_GUARD = 2.0


async def test__full_chain__server_aborts__client_sees_the_status(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.PERMISSION_DENIED
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.echo(b"hello")

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert exc_info.value.details() == "aborted by test"
    # PERMISSION_DENIED says nothing about the server's health, so it must not be retried.
    assert control.calls == 1


async def test__full_chain__server_aborts__call_is_not_logged_as_successful(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrange
    caplog.set_level(logging.INFO, logger=f"grpc.client.{CLIENT_SERVICE_NAME}")
    echo_server.service.unary_unary.abort_code = grpc.StatusCode.PERMISSION_DENIED
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await stub.echo(b"hello")

    # Assert
    messages = [record.getMessage() for record in caplog.records]
    assert "gRPC call successful" not in messages


async def test__full_chain__server_aborts_mid_stream__client_keeps_the_items_it_already_got(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_stream
    control.mid_stream_abort = grpc.StatusCode.INTERNAL
    control.mid_stream_after = 1
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()
    items: list[bytes] = []

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        async for item in stub.stream(b"go"):
            items.append(item)

    # Assert
    assert items == [b"one"]
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


async def test__full_chain__bidirectional_stream__server_consumes_every_request_item(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.stream_stream
    control.mid_stream_abort = grpc.StatusCode.RESOURCE_EXHAUSTED
    control.mid_stream_after = 1
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()
    items: list[bytes] = []

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        async for item in stub.chat(requests([b"ab", b"cd"])):
            items.append(item)

    # Assert
    assert items == [b"AB"]
    assert exc_info.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    # Both request items crossed the wire before the server gave up on the response side.
    assert control.received == [b"ab", b"cd"]


async def test__full_chain__transient_unavailable__retry_recovers_within_one_logical_call(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    control.fail_times = 1
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    response = await stub.echo(b"hello")

    # Assert
    assert response == b"hello"
    assert control.calls == 2
    # Metrics sit above the retry layer, so two attempts still make one measured call.
    assert metrics.only_request("Echo").status == "success"


async def test__full_chain__server_keeps_failing__circuit_breaker_stops_calling_it(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    # One attempt per call, so the server's call count is the number of attempts the breaker let out.
    client = make_client(canonical_chain(metrics, max_attempts=1, fail_threshold=1))
    stub = await client.connect()

    # Act
    for _ in range(3):
        with pytest.raises(grpc.aio.AioRpcError):
            await stub.echo(b"hello")

    # Assert
    # The circuit opens on the first failure, so the two calls after it never reach the server.
    assert control.calls == 1


async def test__full_chain__unresponsive_server__timeout_budget_ends_the_call(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    echo_server.service.unary_unary.hang = True
    client = make_client(canonical_chain(metrics, timeout=0.2))
    stub = await client.connect()

    # Act
    # The outer guard turns a budget that never fires into a failure instead of a hung test run.
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        async with asyncio.timeout(_HUNG_CALL_GUARD):
            await stub.echo(b"hello")

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED


async def test__full_chain__unresponsive_server__timeout_budget_ends_a_stream(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    echo_server.service.unary_stream.hang = True
    client = make_client(canonical_chain(metrics, timeout=0.2))
    stub = await client.connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        async with asyncio.timeout(_HUNG_CALL_GUARD):
            [item async for item in stub.stream(b"go")]

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
