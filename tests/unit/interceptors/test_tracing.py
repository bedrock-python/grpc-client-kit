"""Unit tests for the tracing interceptor.

Every test drives the interceptor the way a channel does: through the adapters in
`AsyncClientInterceptor.adapters`, with a continuation that behaves like grpc's — it resolves to a
`Call` object without raising, and the status only surfaces when that Call is awaited or iterated.
A continuation that raises by itself is what let the old suite believe the span carried the real
outcome, while on a live connection every span was closed as OK.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import patch

import grpc.aio
import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from grpc_client_kit.interceptors.tracing import AsyncClientTracingInterceptor
from tests.helpers import (
    METHOD,
    RPC_KINDS,
    STREAMING_RESPONSE,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    await_result,
    collect,
    make_call_details,
    make_rpc_error,
    refusing_wire,
)

from .conftest import (
    PARENT_SPAN_ID,
    PARENT_TRACE_ID,
    STALE_TRACEPARENT,
    FakeTracer,
    run_call,
    settle,
    start_call,
    traceparents,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# Reaching every RPC kind: a channel files an interceptor by class, not by capability.
# --------------------------------------------------------------------------------------------


def test__tracing_interceptor__built__offers_one_adapter_per_rpc_kind() -> None:
    """One object per gRPC base class, or the channel registers the layer for unary-unary only."""
    # Arrange
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=FakeTracer())

    # Act
    adapters = interceptor.adapters

    # Assert
    for abc_class in RPC_KINDS.values():
        matching = [entry for entry in adapters if isinstance(entry, abc_class)]
        assert len(matching) == 1, f"expected exactly one adapter for {abc_class.__name__}, got {matching}"

    assert [sum(isinstance(entry, abc) for abc in RPC_KINDS.values()) for entry in adapters] == [1, 1, 1, 1]


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__tracing__every_rpc_kind__gets_a_span_and_a_traceparent(
    current_trace: trace.SpanContext,
    rpc_type: str,
) -> None:
    """Streaming calls used to run entirely untraced: no span, no traceparent, no status."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    streaming = rpc_type in STREAMING_RESPONSE
    wire = Wire(FakeStreamCall("item") if streaming else FakeUnaryCall("response"))

    # Act
    result = await start_call(interceptor, wire, rpc_type, request="request")
    if streaming:
        assert await collect(result) == ["item"]
    else:
        await await_result(result)
    # Streaming-request spans are closed from an observer task, not from the awaiting caller.
    await settle()

    # Assert
    assert tracer.names == [METHOD]
    assert tracer.kinds == [SpanKind.CLIENT]
    assert traceparents(wire), "the callee cannot join a trace it was never told about"
    assert tracer.spans[0].statuses[-1].status_code is StatusCode.OK
    assert tracer.spans[0].ended == 1


# --------------------------------------------------------------------------------------------
# The span status: the outcome only exists once the Call is awaited or iterated.
# --------------------------------------------------------------------------------------------


async def test__tracing__failure_surfacing_only_on_awaiting_the_call__is_recorded_on_the_span(
    current_trace: trace.SpanContext,
) -> None:
    """The whole point: a continuation that resolves happily still hides a failed RPC."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    error = make_rpc_error(grpc.StatusCode.INTERNAL)

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, Wire(FakeUnaryCall(error=error)))

    # Assert
    span = tracer.spans[0]
    assert span.statuses[-1].status_code is StatusCode.ERROR
    assert span.statuses[-1].description == "boom"
    assert span.attributes["rpc.grpc.status_code"] == "INTERNAL"
    assert span.exceptions == [error]
    assert span.ended == 1


async def test__tracing__failed_stream_unary_call__is_recorded_on_the_span(
    current_trace: trace.SpanContext,
) -> None:
    """stream-unary is the kind whose return value grpc hands to the caller untouched."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    error = make_rpc_error(grpc.StatusCode.UNAVAILABLE, "gone")

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, Wire(FakeUnaryCall(error=error)), "stream_unary", request="requests")

    # Assert
    span = tracer.spans[0]
    assert span.statuses[-1].status_code is StatusCode.ERROR
    assert span.attributes["rpc.grpc.status_code"] == "UNAVAILABLE"


async def test__tracing__stream_unary_call__hands_back_the_call_not_the_bare_response(
    current_trace: trace.SpanContext,
) -> None:
    """A bare response would break the caller's ``await`` and the call's own finalizer."""
    # Arrange
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=FakeTracer())

    # Act
    result = await start_call(interceptor, Wire(FakeUnaryCall("collected")), "stream_unary", request="requests")

    # Assert
    assert hasattr(result, "__await__")
    assert await result == "collected"


async def test__tracing__expected_grpc_code__keeps_the_span_green(
    current_trace: trace.SpanContext,
) -> None:
    """NOT_FOUND is an application outcome, not a failure of the call."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    error = make_rpc_error(grpc.StatusCode.NOT_FOUND, "missing")

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, Wire(FakeUnaryCall(error=error)))

    # Assert
    span = tracer.spans[0]
    assert span.statuses[-1].status_code is StatusCode.OK
    assert span.attributes["rpc.grpc.status_code"] == "NOT_FOUND"
    # OpenTelemetry drops a description that comes with a non-error status, and warns for every one.
    assert span.statuses[-1].description is None


async def test__tracing__cancellation_and_unexpected_errors__end_the_span(
    current_trace: trace.SpanContext,
) -> None:
    """Neither outcome is a gRPC status, and both still have to close the span they opened."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)

    # Act
    with pytest.raises(asyncio.CancelledError):
        await run_call(interceptor, Wire(FakeUnaryCall(error=asyncio.CancelledError())))
    with pytest.raises(RuntimeError, match="boom"):
        await run_call(interceptor, Wire(FakeUnaryCall(error=RuntimeError("boom"))), "stream_unary")

    # Assert
    cancelled, failed = tracer.spans
    assert cancelled.statuses[-1].status_code is StatusCode.ERROR
    assert cancelled.ended == 1
    assert failed.statuses[-1].status_code is StatusCode.ERROR
    assert [type(error) for error in failed.exceptions] == [RuntimeError]
    assert failed.ended == 1


async def test__tracing__call_that_cannot_even_be_created__ends_its_span(
    current_trace: trace.SpanContext,
) -> None:
    """An interceptor further in may refuse outright instead of resolving to a Call."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    refused = refusing_wire(make_rpc_error(grpc.StatusCode.UNAVAILABLE, "no circuit"))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await start_call(interceptor, refused, "unary_stream", request="request")

    # Assert
    assert tracer.spans[0].statuses[-1].status_code is StatusCode.ERROR
    assert tracer.spans[0].ended == 1


# --------------------------------------------------------------------------------------------
# Streaming responses: the span is only over when the last item has been delivered.
# --------------------------------------------------------------------------------------------


async def test__tracing__streaming_response__keeps_the_span_open_until_the_last_item(
    current_trace: trace.SpanContext,
) -> None:
    """A stream is not over when it is created, and neither is the span describing it."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)

    # Act
    stream = await start_call(interceptor, Wire(FakeStreamCall("item-1", "item-2")), "unary_stream")
    span = tracer.spans[0]
    while_open = span.ended
    delivered = await collect(stream)

    # Assert
    assert while_open == 0
    assert delivered == ["item-1", "item-2"]
    assert span.ended == 1
    assert span.statuses[-1].status_code is StatusCode.OK


async def test__tracing__streaming_response__creates_the_rpc_before_returning_its_iterator(
    current_trace: trace.SpanContext,
) -> None:
    """grpc binds the Call an interceptor made to the iterator it returns; a lazy one binds None."""
    # Arrange
    wire = Wire(FakeStreamCall("item-1"))
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=FakeTracer())

    # Act
    await start_call(interceptor, wire, "unary_stream")

    # Assert
    assert wire.attempts == 1, "the RPC must exist by the time the iterator reaches grpc"


async def test__tracing__failure_mid_stream__is_recorded_on_the_span(
    current_trace: trace.SpanContext,
) -> None:
    """The status of a stream arrives after its items, which is where a lazy layer misses it."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    error = make_rpc_error(grpc.StatusCode.UNAVAILABLE, "gone")
    wire = Wire(FakeStreamCall("item-1", error=error))

    # Act
    stream = await start_call(interceptor, wire, "stream_stream", request="r")
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    span = tracer.spans[0]
    assert span.statuses[-1].status_code is StatusCode.ERROR
    assert span.attributes["rpc.grpc.status_code"] == "UNAVAILABLE"
    assert span.ended == 1


async def test__tracing__unexpected_error_mid_stream__is_recorded_on_the_span(
    current_trace: trace.SpanContext,
) -> None:
    """An error with no gRPC status of its own is still recorded, exception and all."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)

    # Act
    stream = await start_call(interceptor, Wire(FakeStreamCall("item-1", error=RuntimeError("boom"))), "unary_stream")
    with pytest.raises(RuntimeError, match="boom"):
        await collect(stream)

    # Assert
    span = tracer.spans[0]
    assert span.statuses[-1].status_code is StatusCode.ERROR
    assert [type(error) for error in span.exceptions] == [RuntimeError]
    assert span.ended == 1


async def test__tracing__cancellation_mid_stream__ends_the_span(
    current_trace: trace.SpanContext,
) -> None:
    """A consumer that walked away still leaves a closed span behind."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    wire = Wire(FakeStreamCall("item-1", error=asyncio.CancelledError()))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    with pytest.raises(asyncio.CancelledError):
        await collect(stream)

    # Assert
    span = tracer.spans[0]
    assert span.statuses[-1].status_code is StatusCode.ERROR
    assert span.ended == 1


# --------------------------------------------------------------------------------------------
# Trace propagation: the parent comes from the ambient context, never from the wire.
# --------------------------------------------------------------------------------------------


async def test__tracing__ambient_span__is_continued_and_propagated(
    current_trace: trace.SpanContext,
) -> None:
    """The client span must be a child of the ambient context, and travel in outgoing metadata."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    wire = Wire()

    # Act
    response = await run_call(interceptor, wire, details=make_call_details(metadata=[("x-request-id", "req-1")]))

    # Assert
    assert response == "response"
    # No context argument: the tracer resolves the parent from the current context itself.
    assert tracer.contexts == [None]
    assert tracer.kinds == [SpanKind.CLIENT]
    assert tracer.names == [METHOD]

    outgoing = wire.metadata
    version, trace_id, span_id, flags = outgoing["traceparent"].split("-")
    assert version == "00"
    assert trace_id == f"{PARENT_TRACE_ID:032x}"
    assert span_id == f"{tracer.spans[0].get_span_context().span_id:016x}"
    assert span_id != f"{PARENT_SPAN_ID:016x}"
    assert flags == "01"
    # Metadata set by the caller survives injection.
    assert outgoing["x-request-id"] == "req-1"


async def test__tracing__stale_traceparent_on_the_wire__is_replaced_not_used_as_the_parent(
    current_trace: trace.SpanContext,
) -> None:
    """A traceparent an earlier hop left behind would silently reparent the whole trace."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    wire = Wire()
    metadata = [("traceparent", STALE_TRACEPARENT), ("authorization", "bearer x")]

    # Act
    await run_call(interceptor, wire, details=make_call_details(metadata=metadata))

    # Assert
    sent = traceparents(wire)
    assert len(sent) == 1
    assert sent[0] != STALE_TRACEPARENT
    assert sent[0].split("-")[1] == f"{PARENT_TRACE_ID:032x}"
    assert ("authorization", "bearer x") in wire.metadata_pairs


async def test__tracing__no_ambient_span__starts_a_fresh_trace_and_still_propagates() -> None:
    """With no active span the call starts a fresh trace and still carries a traceparent."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)
    wire = Wire()

    # Act
    await run_call(interceptor, wire)

    # Assert
    sent = traceparents(wire)
    assert len(sent) == 1
    assert sent[0].split("-")[1] == f"{tracer.spans[0].get_span_context().trace_id:032x}"


async def test__tracing__successful_call__carries_the_standard_rpc_attributes(
    current_trace: trace.SpanContext,
) -> None:
    """The span speaks the OpenTelemetry RPC conventions, and nothing beyond them."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)

    # Act
    await run_call(interceptor, Wire())

    # Assert
    span = tracer.spans[0]
    assert span.attributes == {
        "rpc.system": "grpc",
        "rpc.service": "test-service",
        "rpc.method": METHOD,
    }
    assert span.statuses[-1].status_code is StatusCode.OK
    assert span.ended == 1


async def test__tracing__binary_method_path__is_decoded_for_the_span_name(
    current_trace: trace.SpanContext,
) -> None:
    """gRPC hands the method path over as bytes; the call decodes it once, for every layer."""
    # Arrange
    tracer = FakeTracer()
    interceptor = AsyncClientTracingInterceptor(service_name="test-service", tracer=tracer)

    # Act
    await run_call(interceptor, Wire(), details=make_call_details(method=METHOD.encode()))

    # Assert
    assert tracer.names == [METHOD]
    assert tracer.spans[0].attributes["rpc.method"] == METHOD


# --------------------------------------------------------------------------------------------
# Without the tracing extra.
# --------------------------------------------------------------------------------------------


async def test__tracing_interceptor__opentelemetry_missing__passes_the_call_through_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without the tracing extra the interceptor is an announced no-op, not a silent one."""
    # Arrange
    wire = Wire()
    details = make_call_details(metadata=[("x-request-id", "req-1")])

    # Act
    with patch("grpc_client_kit.interceptors.tracing.HAS_TRACING", False), caplog.at_level(logging.WARNING):
        interceptor = AsyncClientTracingInterceptor(service_name="test-service")
        response = await run_call(interceptor, wire, details=details)

    # Assert
    assert response == "response"
    # Call details are forwarded untouched: no span, no traceparent.
    assert wire.details is details
    assert "grpc-client-kit[tracing]" in caplog.text


async def test__tracing_interceptor__opentelemetry_missing__still_delivers_a_stream() -> None:
    """The streaming path of the no-op must stay a working pass-through as well."""
    # Arrange
    wire = Wire(FakeStreamCall("item-1", "item-2"))

    # Act
    with patch("grpc_client_kit.interceptors.tracing.HAS_TRACING", False):
        interceptor = AsyncClientTracingInterceptor(service_name="test-service")
        stream = await start_call(interceptor, wire, "unary_stream")
        delivered = await collect(stream)

    # Assert
    assert delivered == ["item-1", "item-2"]


async def test__tracing_interceptor__no_tracer_given__resolves_the_default_one() -> None:
    """A caller that installed a global tracer provider should not have to pass a tracer too."""
    # Arrange
    interceptor = AsyncClientTracingInterceptor(service_name="test-service")

    # Act & Assert
    assert interceptor._tracer is not None
