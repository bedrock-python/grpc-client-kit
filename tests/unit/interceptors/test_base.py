"""Unit tests for the interceptor base: one logical interceptor, four channel adapters.

Every test drives the seam the way a channel does — through the adapters in
`AsyncClientInterceptor.adapters`, with a continuation shaped like grpc's: it resolves to a `Call`
object without raising, and the outcome only surfaces when that Call is awaited or iterated.
"""

from __future__ import annotations

import logging

import grpc
import grpc.aio
import pytest

from grpc_client_kit.interceptors.base import flatten_interceptors, logical_interceptor
from grpc_client_kit.interceptors.circuit_breaker import CircuitBreakerOpenError
from grpc_client_kit.interceptors.retry import AsyncRetryInterceptor
from tests.helpers import (
    AROUND_LOGGER,
    METHOD,
    REQUEST_ID,
    RPC_KINDS,
    FakeStreamCall,
    FakeUnaryCall,
    RaisingTeardown,
    TokenAcrossYield,
    Wire,
    collect,
    make_call_details,
    make_rpc_error,
    refusing_wire,
)

from .conftest import (
    OldStyleInterceptor,
    Probe,
    Recorder,
    context_reading_wire,
    drive_call,
    response_for,
    run_call,
    start_call,
    wire_for,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# Dispatch: what the adapters tell the logical interceptor about the call.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rpc_type", "request_streaming", "response_streaming"),
    [
        ("unary_unary", False, False),
        ("unary_stream", False, True),
        ("stream_unary", True, False),
        ("stream_stream", True, True),
    ],
)
async def test__adapters__every_rpc_kind__label_the_call_with_their_own_kind(
    rpc_type: str,
    request_streaming: bool,
    response_streaming: bool,
) -> None:
    """Each adapter labels the call with its kind, so the logic can tell streams from unaries."""
    # Arrange
    probe = Probe()
    wire = Wire(FakeStreamCall() if response_streaming else FakeUnaryCall("ok"))

    # Act
    await start_call(probe, wire, rpc_type)

    # Assert
    assert probe.kinds == [(rpc_type, request_streaming, response_streaming)]
    assert wire.attempts == 1


async def test__client_call__binary_method_path__decodes_it_once_for_every_layer() -> None:
    """Live call details carry the method as bytes; every interceptor used to decode it by hand."""
    # Arrange
    probe = Probe()

    # Act
    await start_call(probe, Wire(FakeUnaryCall("ok")), details=make_call_details(METHOD.encode()))

    # Assert
    assert probe.methods == [METHOD]


# --------------------------------------------------------------------------------------------
# The around_call seam: one generator, four RPC kinds.
# --------------------------------------------------------------------------------------------


async def test__around_call__setup_rewrites_the_deadline__the_rpc_is_issued_with_it() -> None:
    """Rewriting the call details is only possible while the RPC does not exist yet."""
    # Arrange
    recorder = Recorder(timeout=2.5)
    wire = Wire(FakeUnaryCall("ok"))

    # Act
    response = await run_call(recorder, wire)

    # Assert
    assert response == "ok"
    assert wire.timeouts == [2.5]
    assert recorder.events[0] == ("before", METHOD, "unary_unary")


async def test__around_call__unary_call__sees_the_response_and_the_status() -> None:
    """The status only exists after the Call is awaited, and that is what reaches the generator."""
    # Arrange
    ok = Recorder()
    failed = Recorder()

    # Act
    response = await run_call(ok, Wire(FakeUnaryCall("answer")))
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(failed, Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.PERMISSION_DENIED))))

    # Assert
    assert response == "answer"
    assert ok.events == [("before", METHOD, "unary_unary"), ("succeeded", "answer"), "closed"]
    assert failed.events == [
        ("before", METHOD, "unary_unary"),
        ("failed", grpc.StatusCode.PERMISSION_DENIED),
        "closed",
    ]


async def test__around_call__streaming_response__stays_open_until_the_last_item() -> None:
    """A streaming call is not over when it is created; the teardown has to wait for the last item."""
    # Arrange
    recorder = Recorder()
    wire = Wire(FakeStreamCall("a", "b"))

    # Act
    stream = await start_call(recorder, wire, "unary_stream")
    opened = list(recorder.events)
    delivered = await collect(stream)

    # Assert
    assert opened == [("before", METHOD, "unary_stream")], "the call was closed at creation time"
    assert delivered == ["a", "b"]
    assert recorder.events[-2:] == [("succeeded", None), "closed"]


async def test__around_call__failure_mid_stream__reaches_the_generator() -> None:
    """The error arrives after items were delivered, which is where a wrapper-less layer misses it."""
    # Arrange
    recorder = Recorder()
    wire = Wire(FakeStreamCall("a", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    stream = await start_call(recorder, wire, "unary_stream")
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    assert recorder.events == [
        ("before", METHOD, "unary_stream"),
        ("failed", grpc.StatusCode.UNAVAILABLE),
        "closed",
    ]


async def test__around_call__abandoned_stream__still_runs_the_teardown() -> None:
    """Abandoning a stream still has to run the teardown, or in-flight gauges leak forever."""
    # Arrange
    recorder = Recorder()
    wire = Wire(FakeStreamCall("a", "b", "c"))

    # Act
    stream = await start_call(recorder, wire, "unary_stream")
    async for _ in stream:
        break
    await stream.aclose()

    # Assert
    assert "closed" in recorder.events


async def test__around_call__setup_refuses_the_call__nothing_is_dialled_out() -> None:
    """Raising in the setup is how a breaker rejects a call without touching the network."""
    # Arrange
    recorder = Recorder(refuse=CircuitBreakerOpenError(METHOD))
    wire = Wire(FakeUnaryCall("ok"))

    # Act
    with pytest.raises(CircuitBreakerOpenError):
        await run_call(recorder, wire)

    # Assert
    assert wire.attempts == 0


async def test__around_call__streaming_response__creates_the_rpc_before_returning_its_iterator() -> None:
    """grpc binds the Call an interceptor made to the iterator it returns; a lazy one binds None."""
    # Arrange
    wire = Wire(FakeStreamCall("a"))

    # Act
    await start_call(Recorder(), wire, "unary_stream")

    # Assert
    assert wire.attempts == 1


async def test__around_call__stream_refused_by_an_inner_layer__still_runs_the_teardown() -> None:
    """An inner layer may refuse the call outright; the teardown still has to run."""
    # Arrange
    recorder = Recorder()

    # Act
    with pytest.raises(CircuitBreakerOpenError):
        await start_call(recorder, refusing_wire(CircuitBreakerOpenError(METHOD)), "unary_stream")

    # Assert
    assert recorder.events == [
        ("before", METHOD, "unary_stream"),
        ("failed", grpc.StatusCode.UNAVAILABLE),
        "closed",
    ]


# --------------------------------------------------------------------------------------------
# What the two sides of the yield are promised: one context, and no say in the outcome.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__around_call__contextvar_token__is_reset_in_the_context_the_setup_ran_in(rpc_type: str) -> None:
    """Scoping a value to one call is the shortest thing this seam is for, and it has to work.

    `contextvars` compares contexts by identity, so a teardown stepped from another task than the
    setup cannot reset what the setup set — which is where three of the four kinds finish.
    """
    # Arrange
    interceptor = TokenAcrossYield()
    seen: list[str | None] = []

    # Act
    received = await drive_call(interceptor, context_reading_wire(wire_for(rpc_type), seen), rpc_type)

    # Assert
    assert interceptor.reset_error is None, "the teardown ran in a different context than the setup"
    assert seen == ["req-42"], "what the setup set was invisible to the layers below it"
    assert received == response_for(rpc_type)
    assert REQUEST_ID.get() is None, "the value escaped the call and reached the caller's context"


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__around_call__teardown_raises__the_response_still_reaches_the_caller(
    rpc_type: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An interceptor is observability: a metrics push that fails must not take the response with it."""
    # Arrange
    caplog.set_level(logging.ERROR, logger=AROUND_LOGGER)
    interceptor = RaisingTeardown()

    # Act
    received = await drive_call(interceptor, wire_for(rpc_type), rpc_type)

    # Assert
    assert received == response_for(rpc_type)
    assert interceptor.teardowns == 1
    reported = [record for record in caplog.records if record.name == AROUND_LOGGER]
    assert [record.getMessage() for record in reported] == [f"around_call teardown failed for {METHOD}"]
    assert reported[0].exc_info is not None, "the failure was reported without the traceback that explains it"
    assert reported[0].exc_info[0] is RuntimeError


async def test__around_call__teardown_raises_on_a_failed_call__the_server_status_survives(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A status is an outcome too: the teardown must not replace what the server answered either."""
    # Arrange
    caplog.set_level(logging.ERROR, logger=AROUND_LOGGER)
    wire = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.PERMISSION_DENIED)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError) as raised:
        await drive_call(RaisingTeardown(), wire)

    # Assert
    assert raised.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert [record.name for record in caplog.records] == [AROUND_LOGGER]


# --------------------------------------------------------------------------------------------
# Assembling a chain out of both interceptor generations.
# --------------------------------------------------------------------------------------------


def test__flatten_interceptors__mixed_generations__expands_the_logical_ones_and_keeps_the_rest() -> None:
    """A chain may mix both generations while the kit is being migrated."""
    # Arrange
    old_style = OldStyleInterceptor()
    first, second = AsyncRetryInterceptor(), Recorder()

    # Act
    flat = flatten_interceptors([first, old_style, second])

    # Assert
    assert flat == [*first.adapters, old_style, *second.adapters]
    assert all(isinstance(entry, grpc.aio.ClientInterceptor) for entry in flat)


def test__flatten_interceptors__two_logical_layers__keeps_chain_order_within_every_rpc_kind() -> None:
    """A channel appends in list order, so four adapters of A ahead of B keep A outermost."""
    # Arrange
    first, second = AsyncRetryInterceptor(), Recorder()

    # Act
    flat = flatten_interceptors([first, second])

    # Assert
    for abc_class in RPC_KINDS.values():
        of_kind = [logical_interceptor(entry) for entry in flat if isinstance(entry, abc_class)]
        assert of_kind == [first, second], f"order lost for {abc_class.__name__}"


def test__logical_interceptor__old_style_entry__is_returned_unchanged() -> None:
    """Questioning a flattened chain must work whichever generation an entry came from."""
    # Arrange
    old_style = OldStyleInterceptor()

    # Act
    resolved = logical_interceptor(old_style)

    # Assert
    assert resolved is old_style
