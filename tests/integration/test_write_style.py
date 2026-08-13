"""The write()/done_writing() API style for streaming-request calls.

grpc.aio gives streaming-request calls two API styles: pass a request iterator, or call the stub
with no argument and push requests through ``call.write()``. The second style parks ``write()``
until the interceptors task has returned, so any interceptor that awaits the response inside that
task deadlocks the call — the response cannot arrive before the requests are written. None of the
original suite used ``write()``, which is exactly how that deadlock shipped.
"""

from __future__ import annotations

import asyncio

import grpc
import grpc.aio

from .chains import canonical_chain
from .echo_bench import ClientFactory, RunningServer
from .recording import RecordingMetrics
from .waiting import until


async def test__stream_unary__write_style_through_the_canonical_chain__completes_promptly(
    echo_server: RunningServer,
    make_client: ClientFactory,
    metrics: RecordingMetrics,
) -> None:
    # Arrange
    stub = await make_client(canonical_chain(metrics)).connect()

    # Act
    call = stub.collect()
    await call.write(b"a")
    await call.write(b"b")
    await call.done_writing()
    response = await asyncio.wait_for(call, timeout=2.0)

    # Assert
    assert response == b"ab"
    assert await call.code() is grpc.StatusCode.OK
    assert echo_server.service.stream_unary.received == [b"a", b"b"]
    # The outcome is observed from a task, so the record lands a tick later — but it must land.
    await until(
        lambda: any(record.method == "Collect" and record.status == "success" for record in metrics.requests),
        message="the write()-style call was never recorded",
    )


async def test__stream_unary__write_style_server_abort__fails_the_awaiting_caller(
    echo_server: RunningServer,
    make_client: ClientFactory,
    metrics: RecordingMetrics,
) -> None:
    # Arrange
    echo_server.service.stream_unary.abort_code = grpc.StatusCode.PERMISSION_DENIED
    stub = await make_client(canonical_chain(metrics)).connect()

    # Act
    call = stub.collect()
    await call.done_writing()
    try:
        await asyncio.wait_for(call, timeout=2.0)
        raised: grpc.aio.AioRpcError | None = None
    except grpc.aio.AioRpcError as error:
        raised = error

    # Assert
    assert raised is not None
    assert raised.code() is grpc.StatusCode.PERMISSION_DENIED
    await until(
        lambda: any(record.method == "Collect" and record.status == "error" for record in metrics.requests),
        message="the failed write()-style call was never recorded",
    )
