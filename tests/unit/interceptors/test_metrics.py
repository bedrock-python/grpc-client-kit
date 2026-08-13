"""Unit tests for the client metrics interceptor.

Every test drives the interceptor the way a channel does: through the adapters in
`AsyncClientInterceptor.adapters`, with a continuation that behaves like grpc's — it resolves to a
`Call` without raising, and the outcome only surfaces when that Call is awaited or iterated. Feeding
the interceptor a continuation that raises by itself is what let the old suite pass while every
failed call on a live connection was recorded as ``status=success, grpc_code=OK``.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import patch

import grpc.aio
import pytest

from grpc_client_kit.interceptors.circuit_breaker import CircuitBreakerOpenError
from grpc_client_kit.interceptors.metrics import AsyncClientMetricsInterceptor
from tests.helpers import (
    METHOD,
    RPC_KINDS,
    STREAMING_RESPONSE,
    CodelessRpcError,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    collect,
    make_call_details,
    make_metrics,
    make_rpc_error,
)

from .conftest import inflight_deltas, make_metrics_interceptor, request_record, run_call, settle, start_call

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# Dispatch: a channel files interceptors by class, so measuring streams needs an object per kind.
# --------------------------------------------------------------------------------------------


def test__metrics_interceptor__built__offers_one_adapter_per_rpc_kind() -> None:
    """Without one adapter per ABC the channel would register the layer for unary-unary only."""
    # Arrange
    interceptor = make_metrics_interceptor(make_metrics())

    # Act
    adapters = interceptor.adapters

    # Assert
    for abc_class in RPC_KINDS.values():
        matching = [entry for entry in adapters if isinstance(entry, abc_class)]
        assert len(matching) == 1, f"expected exactly one adapter for {abc_class.__name__}, got {matching}"

    assert [sum(isinstance(entry, abc) for abc in RPC_KINDS.values()) for entry in adapters] == [1, 1, 1, 1]


def test__metrics_interceptor__on_its_own__is_not_a_grpc_interceptor() -> None:
    """Handing the logical object to a channel must fail loudly, not register it for unary only."""
    # Act & Assert
    assert not isinstance(make_metrics_interceptor(make_metrics()), grpc.aio.ClientInterceptor)


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__metrics__every_rpc_kind__is_measured_under_its_own_type(rpc_type: str) -> None:
    """Streaming calls used to reach no metrics layer at all; each kind now labels itself."""
    # Arrange
    metrics = make_metrics()
    interceptor = make_metrics_interceptor(metrics)
    streaming = rpc_type in STREAMING_RESPONSE
    wire = Wire(FakeStreamCall("item") if streaming else FakeUnaryCall("ok"))

    # Act
    result = await start_call(interceptor, wire, rpc_type)
    delivered = await collect(result) if streaming else await result
    # Streaming-request outcomes are recorded from an observer task, not from the awaiting caller.
    await settle()

    # Assert
    assert delivered == (["item"] if streaming else "ok")
    record = request_record(metrics)
    assert (record["service"], record["method"], record["rpc_type"]) == ("Service", "Method", rpc_type)
    assert record["status"] == "success"
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__binary_method_path__is_decoded_before_the_labels_are_built() -> None:
    """gRPC hands the method path over as bytes; `base` decodes it before the labels are built."""
    # Arrange
    metrics = make_metrics()

    # Act
    await run_call(
        make_metrics_interceptor(metrics), Wire(FakeUnaryCall("ok")), details=make_call_details(METHOD.encode())
    )

    # Assert
    record = request_record(metrics)
    assert (record["service"], record["method"]) == ("Service", "Method")


def test__parse_method_name__method_path__splits_it_into_service_and_method() -> None:
    """Labels carry the short service name, and an unparsable path degrades instead of crashing."""
    # Arrange
    interceptor = make_metrics_interceptor(make_metrics())

    # Act & Assert
    assert interceptor._parse_method_name("/auth.v1.AuthService/Login") == ("AuthService", "Login")
    assert interceptor._parse_method_name("/ServiceName/Method") == ("ServiceName", "Method")
    assert interceptor._parse_method_name("invalid") == ("unknown", "unknown")


# --------------------------------------------------------------------------------------------
# The outcome: a continuation resolves to a Call and never raises.
# --------------------------------------------------------------------------------------------


async def test__metrics__failed_call__records_the_status_the_caller_saw() -> None:
    """The whole point: the continuation resolves happily for a call that is going to fail."""
    # Arrange
    metrics = make_metrics()
    wire = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.PERMISSION_DENIED)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(make_metrics_interceptor(metrics), wire)

    # Assert
    record = request_record(metrics)
    assert record["status"] == "error"
    assert record["grpc_code"] == "PERMISSION_DENIED"
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__successful_call__records_it_as_a_success() -> None:
    """A call that resolves is recorded once, with the status gRPC gives a healthy call."""
    # Arrange
    metrics = make_metrics()

    # Act
    response = await run_call(make_metrics_interceptor(metrics), Wire(FakeUnaryCall("response")))

    # Assert
    assert response == "response"
    metrics.record_request.assert_called_once()
    record = request_record(metrics)
    assert (record["status"], record["grpc_code"]) == ("success", "OK")
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__breaker_rejection__is_recorded_apart_from_real_failures() -> None:
    """An open breaker never touched the network; sharing the 'error' label with genuine server
    failures would make "is the breaker open or is the backend down?" unanswerable on a dashboard."""
    # Arrange
    metrics = make_metrics()
    wire = Wire(FakeUnaryCall(error=CircuitBreakerOpenError(METHOD)))

    # Act
    with pytest.raises(CircuitBreakerOpenError):
        await run_call(make_metrics_interceptor(metrics), wire)

    # Assert
    record = request_record(metrics)
    assert (record["status"], record["grpc_code"]) == ("rejected", "UNAVAILABLE")


async def test__metrics__non_grpc_failure__is_recorded_as_unknown() -> None:
    """An error with no status of its own still has to be counted, under a label that says so."""
    # Arrange
    metrics = make_metrics()
    wire = Wire(FakeUnaryCall(error=RuntimeError("generic error")))

    # Act
    with pytest.raises(RuntimeError, match="generic error"):
        await run_call(make_metrics_interceptor(metrics), wire)

    # Assert
    record = request_record(metrics)
    assert (record["status"], record["grpc_code"]) == ("error", "UNKNOWN")
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__error_without_a_usable_code__is_recorded_as_unknown() -> None:
    """A gRPC-shaped error from a custom layer need not expose the ``code()`` grpc's own errors do."""
    # Arrange
    metrics = make_metrics()

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(make_metrics_interceptor(metrics), Wire(FakeUnaryCall(error=CodelessRpcError())))

    # Assert
    assert request_record(metrics)["grpc_code"] == "UNKNOWN"


async def test__metrics__cancelled_call__is_recorded_as_cancelled() -> None:
    """A caller that walked away is neither a success nor a failure of the callee."""
    # Arrange
    metrics = make_metrics()
    wire = Wire(FakeUnaryCall(error=asyncio.CancelledError()))

    # Act
    with pytest.raises(asyncio.CancelledError):
        await run_call(make_metrics_interceptor(metrics), wire)

    # Assert
    record = request_record(metrics)
    assert (record["status"], record["grpc_code"]) == ("cancelled", "CANCELLED")
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__slow_rpc__measures_the_rpc_and_not_the_creation_of_the_call() -> None:
    """The continuation returns immediately; only awaiting the Call takes as long as the RPC does."""
    # Arrange
    metrics = make_metrics()

    # Act
    await run_call(make_metrics_interceptor(metrics), Wire(FakeUnaryCall("ok", delay=0.05)))

    # Assert
    assert request_record(metrics)["duration"] >= 0.04


# --------------------------------------------------------------------------------------------
# Streaming responses: the call is not over when it is created.
# --------------------------------------------------------------------------------------------


async def test__metrics__streaming_response__is_recorded_when_its_last_item_has_been_delivered() -> None:
    """An open stream holds its in-flight slot, and is only counted once it is done."""
    # Arrange
    metrics = make_metrics()
    interceptor = make_metrics_interceptor(metrics)

    # Act
    stream = await start_call(interceptor, Wire(FakeStreamCall("item1", "item2")), "unary_stream")
    while_open = inflight_deltas(metrics)
    not_yet_recorded = metrics.record_request.call_args_list == []
    delivered = await collect(stream)

    # Assert
    assert not_yet_recorded
    assert while_open == [1], "the open stream must hold its in-flight slot"
    assert delivered == ["item1", "item2"]
    metrics.record_request.assert_called_once()
    assert request_record(metrics)["status"] == "success"
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__failure_mid_stream__is_recorded_with_its_status() -> None:
    """Items are delivered first and the status arrives last: a wrapper-less layer misses it."""
    # Arrange
    metrics = make_metrics()
    wire = Wire(FakeStreamCall("item1", error=make_rpc_error(grpc.StatusCode.INTERNAL)))

    # Act
    stream = await start_call(make_metrics_interceptor(metrics), wire, "unary_stream")
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    record = request_record(metrics)
    assert (record["status"], record["grpc_code"]) == ("error", "INTERNAL")
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__streaming_response__is_measured_across_every_item() -> None:
    """The duration of a stream is the whole delivery, not the time it took to create it."""
    # Arrange
    metrics = make_metrics()
    wire = Wire(FakeStreamCall("a", "b", delay=0.03))

    # Act
    await collect(await start_call(make_metrics_interceptor(metrics), wire, "unary_stream"))

    # Assert
    assert request_record(metrics)["duration"] >= 0.05


async def test__metrics__stream_cancelled_midway__is_recorded_as_cancelled() -> None:
    """A cancellation reaching the consumer mid-stream is labelled as one, under its own rpc type."""
    # Arrange
    metrics = make_metrics()
    wire = Wire(FakeStreamCall("item1", error=asyncio.CancelledError()))

    # Act
    stream = await start_call(make_metrics_interceptor(metrics), wire, "stream_stream")
    with pytest.raises(asyncio.CancelledError):
        await collect(stream)

    # Assert
    record = request_record(metrics)
    assert (record["status"], record["rpc_type"]) == ("cancelled", "stream_stream")
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__abandoned_stream__frees_its_in_flight_slot() -> None:
    """Abandoning a stream ends the call: the slot is freed and the call is reported as cancelled."""
    # Arrange
    metrics = make_metrics()

    # Act
    stream = await start_call(make_metrics_interceptor(metrics), Wire(FakeStreamCall("a", "b")), "unary_stream")
    async for _ in stream:
        break
    await stream.aclose()

    # Assert
    assert inflight_deltas(metrics) == [1, -1]
    assert request_record(metrics)["status"] == "cancelled"


async def test__metrics__stream_that_is_never_iterated__frees_its_in_flight_slot() -> None:
    """The RPC exists before the iterator is handed out, so dropping it still has to balance out."""
    # Arrange
    metrics = make_metrics()

    # Act
    stream = await start_call(make_metrics_interceptor(metrics), Wire(FakeStreamCall("a")), "unary_stream")
    while_open = inflight_deltas(metrics)

    del stream
    await settle()

    # Assert
    assert while_open == [1]
    assert inflight_deltas(metrics) == [1, -1]
    assert request_record(metrics)["status"] == "cancelled"


async def test__metrics__drained_stream_then_collected__is_reported_exactly_once() -> None:
    """Draining a stream and then collecting it must not double-count either measurement."""
    # Arrange
    metrics = make_metrics()

    # Act
    stream = await start_call(make_metrics_interceptor(metrics), Wire(FakeStreamCall("a")), "unary_stream")
    delivered = await collect(stream)

    del stream
    await settle()

    # Assert
    assert delivered == ["a"]
    assert inflight_deltas(metrics) == [1, -1]
    metrics.record_request.assert_called_once()


# --------------------------------------------------------------------------------------------
# Configuration and safety.
# --------------------------------------------------------------------------------------------


def test__metrics_interceptor__no_collector__announces_that_nothing_is_measured(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A metrics interceptor without a collector must say so instead of failing silently."""
    # Act
    with caplog.at_level(logging.WARNING):
        AsyncClientMetricsInterceptor(service_name="test-service")

    # Assert
    assert "test-service" in caplog.text
    assert "will not be measured" in caplog.text
    assert "pass metrics=" in caplog.text


def test__metrics_interceptor__prometheus_not_installed__names_the_extra_to_install(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """HAS_METRICS decides which of the two possible mistakes the user is told about."""
    # Act
    with (
        patch("grpc_client_kit.interceptors.metrics.HAS_METRICS", False),
        caplog.at_level(logging.WARNING),
    ):
        AsyncClientMetricsInterceptor(service_name="test-service")

    # Assert
    assert "grpc-client-kit[metrics]" in caplog.text


def test__metrics_interceptor__collector_present__warns_about_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A correctly configured interceptor has nothing to complain about."""
    # Act
    with caplog.at_level(logging.WARNING):
        make_metrics_interceptor(make_metrics())

    # Assert
    assert caplog.text == ""


async def test__metrics__no_collector__still_passes_the_call_through() -> None:
    """Without a collector the layer measures nothing, but it must not break the call."""
    # Arrange
    interceptor = AsyncClientMetricsInterceptor(service_name="test-service", metrics=None)
    wire = Wire(FakeUnaryCall("response"))

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "response"
    assert wire.attempts == 1


async def test__metrics__recorder_raising__never_breaks_the_call() -> None:
    """Recording problems are logged, not propagated to the caller."""
    # Arrange
    metrics = make_metrics()
    metrics.record_request.side_effect = RuntimeError("registry down")
    metrics.record_inflight_delta.side_effect = RuntimeError("registry down")

    # Act
    response = await run_call(make_metrics_interceptor(metrics), Wire(FakeUnaryCall("response")))

    # Assert
    assert response == "response"
    assert inflight_deltas(metrics) == [1, -1]


async def test__metrics__method_label_disabled__records_every_call_under_one_label() -> None:
    """A service with unbounded method names would otherwise blow up the label cardinality."""
    # Arrange
    metrics = make_metrics()

    # Act
    await run_call(make_metrics_interceptor(metrics, enable_method_label=False), Wire(FakeUnaryCall("ok")))

    # Assert
    assert request_record(metrics)["method"] == "total"
    assert metrics.record_inflight_delta.call_args.kwargs["method"] == "total"
