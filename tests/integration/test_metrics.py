"""What the metrics layer records off a live channel, and when the in-flight gauge comes back down.

Every record here is produced by a real call through the shipped chain, which is the only way to see
the streaming cases at all: a metrics layer that treats the `Call` a continuation resolves to as the
response would label every stream a success the instant it was created.

Two cases stay out of reach and are marked ``xfail``: see `ABANDONED_CALL_UNREACHABLE`.
"""

from __future__ import annotations

import grpc
import grpc.aio
import pytest

from .calls import EVERY_KIND, RpcKind
from .chains import canonical_chain
from .echo_bench import ClientFactory, RunningServer
from .recording import RecordingMetrics
from .waiting import eventually

# A caller that walks away from a response stream without cancelling it leaves grpc.aio holding the
# Call: the interceptors task and the call's own done-callback reference each other, so the object
# is not collected however hard gc is run, and nothing ever drives the interceptor teardown that
# would release the in-flight slot. Measured, not deduced — the same stream cancelled first, or
# iterated to the end, balances the gauge immediately.
ABANDONED_CALL_UNREACHABLE = "grpc.aio keeps an abandoned Call alive, so no teardown ever runs for it"


@pytest.mark.parametrize("kind", EVERY_KIND)
async def test__full_chain__every_rpc_kind__records_one_metric(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
    kind: RpcKind,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    await kind.invoke(stub)

    # Assert
    record = metrics.only_request(kind.method)
    assert record.service == "EchoService"
    assert record.rpc_type == kind.rpc_type
    assert record.status == "success"
    assert record.grpc_code == "OK"
    assert metrics.total_inflight() == 0


async def test__full_chain__server_aborts__metrics_record_the_error(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    echo_server.service.unary_unary.abort_code = grpc.StatusCode.PERMISSION_DENIED
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await stub.echo(b"hello")

    # Assert
    record = metrics.only_request("Echo")
    assert record.status == "error"
    assert record.grpc_code == "PERMISSION_DENIED"
    assert metrics.total_inflight() == 0


async def test__metrics__stream_iterated_to_the_end__records_success_and_balances_inflight(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    items = [item async for item in stub.stream(b"go")]

    # Assert
    assert items == [b"one", b"two", b"three"]
    assert metrics.only_request("Stream").status == "success"
    assert metrics.total_inflight() == 0


async def test__metrics__stream_cancelled_mid_iteration__records_it_and_balances_inflight(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()
    call = stub.stream(b"go")

    # Act
    # A consumer that stops early and says so. Cancelling ends the RPC and, with it, the reference
    # cycle grpc keeps around a live call, so dropping the call now really does collect it and the
    # interceptor teardown runs. That collection is the line the two xfails below sit on the wrong
    # side of: without the cancel, the same `del` collects nothing.
    first = None
    async for item in call:
        first = item
        break
    call.cancel()
    del call

    # Assert
    assert first == b"one"
    await eventually(lambda: metrics.total_inflight() == 0, message=f"in-flight left at {metrics.inflight}")
    assert metrics.only_request("Stream").status == "cancelled"


@pytest.mark.xfail(reason=ABANDONED_CALL_UNREACHABLE, strict=True)
async def test__metrics__stream_abandoned_mid_iteration__inflight_returns_to_zero(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()
    call = stub.stream(b"go")

    # Act
    first = None
    async for item in call:
        first = item
        break
    await eventually(lambda: metrics.total_inflight() == 1, message="the open stream holds no slot")

    # The caller walks away after the first item: nothing drives the wrappers to their `finally`,
    # so only the finalizer can still balance the gauge.
    del call

    # Assert
    assert first == b"one"
    await eventually(lambda: metrics.total_inflight() == 0, message=f"in-flight left at {metrics.inflight}")


@pytest.mark.xfail(reason=ABANDONED_CALL_UNREACHABLE, strict=True)
async def test__metrics__stream_never_iterated__inflight_returns_to_zero(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    # The stream is created and dropped without a single iteration, the case no `finally` can cover.
    call = stub.stream(b"go")
    await eventually(lambda: metrics.total_inflight() == 1, message="the open stream holds no slot")
    del call

    # Assert
    await eventually(lambda: metrics.total_inflight() == 0, message=f"in-flight left at {metrics.inflight}")
