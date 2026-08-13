"""Unit tests for the client logging interceptor.

Every test drives the interceptor the way a channel does: through the adapters in
`AsyncClientInterceptor.adapters`, with a continuation that behaves like grpc's — it resolves to a
`Call` without raising, and the outcome only surfaces when that Call is awaited or iterated. Feeding
the interceptor a continuation that raises by itself is what let the old suite pass while a call the
server had aborted was logged as "gRPC call successful".

Records are read off `caplog` rather than off a mocked logger, so what is asserted is the message a
handler would actually format and the fields it would actually see.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import patch

import grpc
import grpc.aio
import pytest

from grpc_client_kit.interceptors import client_logging
from grpc_client_kit.interceptors.client_logging import AsyncLoggingInterceptor
from tests.helpers import (
    METHOD,
    RPC_KINDS,
    STREAMING_RESPONSE,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    collect,
    make_call_details,
    make_rpc_error,
    requests,
)

from .conftest import (
    LOGGER,
    SERVICE,
    counting_metadata_to_dict,
    final_record,
    messages,
    run_call,
    settle,
    start_call,
    start_record,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# Dispatch: a channel files interceptors by class, so logging streams needs an object per kind.
# --------------------------------------------------------------------------------------------


def test__logging_interceptor__built__offers_one_adapter_per_rpc_kind() -> None:
    """Without one adapter per ABC the channel would register the layer for unary-unary only."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    adapters = interceptor.adapters

    # Assert
    for abc_class in RPC_KINDS.values():
        matching = [entry for entry in adapters if isinstance(entry, abc_class)]
        assert len(matching) == 1, f"expected exactly one adapter for {abc_class.__name__}, got {matching}"

    assert [sum(isinstance(entry, abc) for abc in RPC_KINDS.values()) for entry in adapters] == [1, 1, 1, 1]


def test__logging_interceptor__on_its_own__is_not_a_grpc_interceptor() -> None:
    """Handing the logical object to a channel must fail loudly, not register it for unary only."""
    # Act & Assert
    assert not isinstance(AsyncLoggingInterceptor(service_name=SERVICE), grpc.aio.ClientInterceptor)


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__logging__every_rpc_kind__gets_a_start_and_a_completion_record(
    debug_logging: pytest.LogCaptureFixture,
    rpc_type: str,
) -> None:
    """Streaming calls used to reach no logging layer at all; each kind now gets its own records."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    streaming = rpc_type in STREAMING_RESPONSE
    wire = Wire(FakeStreamCall("item") if streaming else FakeUnaryCall("ok"))

    # Act
    result = await start_call(interceptor, wire, rpc_type)
    delivered = await collect(result) if streaming else await result
    # Streaming-request outcomes are logged from an observer task, not from the awaiting caller.
    await settle()

    # Assert
    assert delivered == (["item"] if streaming else "ok")
    subject = "stream" if streaming else "call"
    assert messages(debug_logging) == ["gRPC call started", f"gRPC {subject} successful"]
    assert final_record(debug_logging).__dict__["grpc.status"] == "OK"


async def test__logging__binary_method_path__is_decoded_on_every_record(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """gRPC hands the method path over as bytes; `base` decodes it before it reaches a record."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("ok")), details=make_call_details(METHOD.encode()))

    # Assert
    assert start_record(debug_logging).__dict__["grpc.method"] == METHOD
    assert final_record(debug_logging).__dict__["grpc.method"] == METHOD


# --------------------------------------------------------------------------------------------
# The outcome: a continuation resolves to a Call and never raises.
# --------------------------------------------------------------------------------------------


async def test__logging__failed_call__is_never_reported_as_successful(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The whole point: the continuation resolves happily for a call that is going to fail."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    wire = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.PERMISSION_DENIED, "not for you")))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire)

    # Assert
    assert "gRPC call successful" not in messages(debug_logging)
    record = final_record(debug_logging)
    assert record.getMessage() == "gRPC call failed with status PERMISSION_DENIED: not for you"
    assert record.__dict__["grpc.status"] == "PERMISSION_DENIED"
    assert record.__dict__["grpc.error"] == "not for you"


async def test__logging__critical_and_expected_codes__only_the_critical_one_gets_a_traceback(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A broken callee is worth a traceback; NOT_FOUND is an answer, not an incident."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.INTERNAL, "broken"))))
    critical = final_record(debug_logging)

    debug_logging.clear()
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(
            interceptor, Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.NOT_FOUND, "nothing here")))
        )
    expected = final_record(debug_logging)

    # Assert
    assert critical.getMessage() == "gRPC call failed with critical error"
    assert critical.exc_info is not None
    assert expected.getMessage() == "gRPC call failed with status NOT_FOUND: nothing here"
    assert expected.exc_info is None


async def test__logging__failure_mid_stream__is_reported_with_its_status(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Items are delivered first and the status arrives last: a wrapper-less layer misses it."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    wire = Wire(FakeStreamCall("item", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE, "gone")))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    record = final_record(debug_logging)
    assert record.getMessage() == "gRPC stream failed with status UNAVAILABLE: gone"
    assert record.__dict__["grpc.status"] == "UNAVAILABLE"


async def test__logging__stream_dying_of_a_critical_code__is_reported_as_critical(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A stream lost to DATA_LOSS is an incident, and is reported as one."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    wire = Wire(FakeStreamCall("item", error=make_rpc_error(grpc.StatusCode.DATA_LOSS, "lost")))

    # Act
    stream = await start_call(interceptor, wire, "stream_stream")
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    record = final_record(debug_logging)
    assert record.getMessage() == "gRPC stream failed with critical error"
    assert record.__dict__["grpc.status"] == "DATA_LOSS"


async def test__logging__non_grpc_failure__is_reported_as_internal(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """An error carrying no status is still a failed call, and gets a traceback."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    with pytest.raises(RuntimeError, match="oops"):
        await run_call(interceptor, Wire(FakeUnaryCall(error=RuntimeError("oops"))))

    # Assert
    record = final_record(debug_logging)
    assert record.getMessage() == "gRPC call failed with unexpected error"
    assert record.__dict__["grpc.status"] == "INTERNAL"
    assert record.__dict__["grpc.error"] == "oops"
    assert record.exc_info is not None


async def test__logging__non_grpc_failure_mid_stream__is_reported_as_internal(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The streaming path classifies an unexpected error the same way the unary one does."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    wire = Wire(FakeStreamCall("item", error=RuntimeError("boom")))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    with pytest.raises(RuntimeError, match="boom"):
        await collect(stream)

    # Assert
    record = final_record(debug_logging)
    assert record.getMessage() == "gRPC stream failed with unexpected error"
    assert record.__dict__["grpc.status"] == "INTERNAL"


async def test__logging__cancelled_call__is_reported_as_cancelled(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A caller that walked away is not a failure of the callee, and is labelled accordingly."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    with pytest.raises(asyncio.CancelledError):
        await run_call(interceptor, Wire(FakeUnaryCall(error=asyncio.CancelledError())))

    # Assert
    record = final_record(debug_logging)
    assert record.getMessage() == "gRPC call cancelled"
    assert record.__dict__["grpc.status"] == "CANCELLED"


async def test__logging__cancelled_stream__is_reported_as_cancelled(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The streaming path labels a cancellation the same way the unary one does."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    wire = Wire(FakeStreamCall("item", error=asyncio.CancelledError()))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    with pytest.raises(asyncio.CancelledError):
        await collect(stream)

    # Assert
    assert final_record(debug_logging).getMessage() == "gRPC stream cancelled"


async def test__logging__abandoned_stream__still_gets_a_terminal_record(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Abandoning a stream ends the call, so it gets a terminal record like any other outcome."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    stream = await start_call(interceptor, Wire(FakeStreamCall("a", "b")), "unary_stream")
    async for _ in stream:
        break
    await stream.aclose()

    # Assert
    record = final_record(debug_logging)
    assert record.getMessage() == "gRPC stream cancelled"
    assert record.__dict__["grpc.status"] == "CANCELLED"


async def test__logging__slow_rpc__measures_the_rpc_and_not_the_creation_of_the_call(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The continuation returns immediately; only awaiting the Call takes as long as the RPC does."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("ok", delay=0.05)))

    # Assert
    assert final_record(debug_logging).__dict__["grpc.duration_ms"] >= 40.0


async def test__logging__streaming_response__is_only_finished_once_its_last_item_is_delivered(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A stream is not over when it is created, so its completion record has to wait."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act
    stream = await start_call(interceptor, Wire(FakeStreamCall("item")), "unary_stream")
    while_open = messages(debug_logging)
    delivered = await collect(stream)

    # Assert
    assert while_open == ["gRPC call started"]
    assert delivered == ["item"]
    assert messages(debug_logging) == ["gRPC call started", "gRPC stream successful"]


# --------------------------------------------------------------------------------------------
# Record contents: correlation, redaction and payloads.
# --------------------------------------------------------------------------------------------


async def test__logging__sensitive_metadata__is_redacted_and_the_request_id_kept(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Correlation is worth logging; the credential that travelled next to it is not."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    details = make_call_details(metadata=[("authorization", "bearer secret"), ("x-request-id", "req-123")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    extra = start_record(debug_logging).__dict__
    assert extra["request_id"] == "req-123"
    assert "'authorization': '***'" in extra["grpc.metadata"]
    assert "bearer secret" not in extra["grpc.metadata"]


async def test__logging__proxy_authorization_header__is_redacted_by_default(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The default set of sensitive headers covers more than plain ``authorization``."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(SERVICE, log_metadata=True)
    details = make_call_details(metadata=[("proxy-authorization", "secret")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    assert "'proxy-authorization': '***'" in start_record(debug_logging).__dict__["grpc.metadata"]


async def test__logging__custom_sensitive_header__is_redacted(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A deployment knows its own secret headers, and can add them to the default set."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, sensitive_headers={"x-custom-token"})
    details = make_call_details(metadata=[("x-custom-token", "secret-value")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    assert "'x-custom-token': '***'" in start_record(debug_logging).__dict__["grpc.metadata"]


async def test__logging__metadata_logging_disabled__leaves_the_field_out(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Redaction is not the only answer: a deployment may keep metadata out of the logs entirely."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(SERVICE, log_metadata=False)
    details = make_call_details(metadata=[("authorization", "secret")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    assert "grpc.metadata" not in start_record(debug_logging).__dict__


async def test__logging__request_id_sent_as_bytes__reaches_the_record_decoded(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Byte values arrive decoded, because metadata is normalized before extraction."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    details = make_call_details(metadata=[("x-request-id", b"req-bytes")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    assert start_record(debug_logging).__dict__["request_id"] == "req-bytes"


def test__extract_request_id__known_spellings__finds_the_correlation_id() -> None:
    """Callers spell the correlation header in several ways, and none of them is worth losing."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)

    # Act & Assert
    assert interceptor._extract_request_id({"request-id": "id1"}) == "id1"
    assert interceptor._extract_request_id({"x-request-id": "id2"}) == "id2"
    assert interceptor._extract_request_id({"X-Request-ID": "id3"}) == "id3"
    assert interceptor._extract_request_id({}) is None
    assert interceptor._extract_request_id({"other": "value"}) is None


async def test__logging__long_request_payload__is_truncated(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A payload is logged as a sample, not as a copy of the message."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, log_request_payload=True)

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), request="a" * 2000)

    # Assert
    assert len(start_record(debug_logging).__dict__["grpc.request"]) == 1000


async def test__logging__long_response_payload__is_truncated(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The response side is sampled the same way the request side is."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, log_response_payload=True)

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("b" * 2000)))

    # Assert
    assert len(final_record(debug_logging).__dict__["grpc.response"]) == 1000


async def test__logging__streaming_response__carries_no_response_payload(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A streaming response has no single payload, so the field is left out rather than faked."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, log_response_payload=True)

    # Act
    await collect(await start_call(interceptor, Wire(FakeStreamCall("a")), "unary_stream"))

    # Assert
    assert "grpc.response" not in final_record(debug_logging).__dict__


async def test__logging__streaming_request__is_not_materialized(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Draining a request iterator to log it would consume the very messages meant for the server."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, log_request_payload=True)

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), "stream_unary", request=requests("req"))

    # Assert
    assert start_record(debug_logging).__dict__["grpc.request"] == "<stream_request>"


# --------------------------------------------------------------------------------------------
# Sensitive methods.
# --------------------------------------------------------------------------------------------


async def test__logging__sensitive_method__keeps_payloads_and_metadata_out(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """A sensitive method is marked as such, and everything that could leak is left out."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(
        service_name=SERVICE,
        sensitive_methods={"/auth.AuthService/Login"},
        log_request_payload=True,
    )
    details = make_call_details("/auth.AuthService/Login", metadata=[("authorization", "bearer secret")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details, request="password")

    # Assert
    extra = start_record(debug_logging).__dict__
    assert extra["grpc.sensitive"] is True
    assert "grpc.request" not in extra
    assert "grpc.metadata" not in extra
    assert final_record(debug_logging).getMessage() == "gRPC call successful (SENSITIVE)"


async def test__logging__method_matching_a_sensitive_pattern__is_marked_sensitive(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Naming every sensitive method is impractical, so a pattern may stand in for a list."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, sensitive_patterns=[r".*Password.*"])
    details = make_call_details("/users.UserService/UpdatePassword")

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    assert start_record(debug_logging).__dict__["grpc.sensitive"] is True


async def test__logging__sensitive_method_failing__withholds_the_error_details(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The status is safe to log; the message explaining it may quote what the caller sent."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, sensitive_methods={"/auth.AuthService/Login"})
    error = make_rpc_error(grpc.StatusCode.UNAUTHENTICATED, "bad password for bob")

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(
            interceptor,
            Wire(FakeUnaryCall(error=error)),
            details=make_call_details("/auth.AuthService/Login"),
        )

    # Assert
    record = final_record(debug_logging)
    assert record.__dict__["grpc.status"] == "UNAUTHENTICATED"
    assert "grpc.error" not in record.__dict__


async def test__logging__sensitive_method_raising_an_unexpected_error__withholds_its_message(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """An unexpected error may quote the request too, so the same rule applies to it."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, sensitive_methods={"/auth.AuthService/Login"})
    wire = Wire(FakeUnaryCall(error=RuntimeError("bad password for bob")))

    # Act
    with pytest.raises(RuntimeError):
        await run_call(interceptor, wire, details=make_call_details("/auth.AuthService/Login"))

    # Assert
    assert "grpc.error" not in final_record(debug_logging).__dict__


async def test__is_sensitive__more_methods_than_the_cap__evicts_the_least_recently_used() -> None:
    """Pattern matching is cached per method, and the cache is bounded like every other one here."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, max_cache_size=2)

    # Act
    interceptor._is_sensitive("m1")
    interceptor._is_sensitive("m2")
    cached = "m1" in interceptor._sensitivity_cache
    interceptor._is_sensitive("m3")

    # Assert
    assert cached
    assert "m1" not in interceptor._sensitivity_cache


# --------------------------------------------------------------------------------------------
# Record hygiene and cost.
# --------------------------------------------------------------------------------------------


async def test__logging__start_and_completion_records__do_not_inherit_fields_from_each_other(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Each record owns its `extra`: start-only fields must not leak into the completion record."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, log_request_payload=True)
    details = make_call_details(metadata=[("x-request-id", "req-123")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    start = start_record(debug_logging).__dict__
    done = final_record(debug_logging).__dict__
    # The start record cannot know the outcome yet...
    assert "grpc.duration_ms" not in start
    assert "grpc.status" not in start
    # ...and the completion record must not drag the request payload along.
    assert "grpc.request" in start
    assert "grpc.request" not in done
    assert done["grpc.status"] == "OK"
    # Call-wide fields are present on both.
    assert start["grpc.method"] == done["grpc.method"] == METHOD
    assert start["request_id"] == done["request_id"] == "req-123"


async def test__logging__stream_completion_record__does_not_inherit_the_start_fields(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """The streaming path builds its records the same way, metadata included."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    details = make_call_details("/Service/Stream", metadata=[("authorization", "bearer secret")])

    # Act
    await collect(await start_call(interceptor, Wire(FakeStreamCall("item")), "unary_stream", details=details))

    # Assert
    assert "grpc.metadata" in start_record(debug_logging).__dict__
    done = final_record(debug_logging).__dict__
    assert "grpc.metadata" not in done
    assert done["grpc.status"] == "OK"


async def test__logging__completion_record__holds_only_strings_booleans_and_a_float_duration(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Handlers serialize these fields, so nothing exotic may end up in one."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, sensitive_methods={METHOD})

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")))

    # Assert
    done = final_record(debug_logging).__dict__
    fields = {key: value for key, value in done.items() if key.startswith("grpc.") or key == "request_id"}
    assert done["grpc.sensitive"] is True
    assert isinstance(done["grpc.duration_ms"], float)
    assert all(isinstance(value, str | bool | float) for value in fields.values())


async def test__logging__one_call__converts_its_metadata_exactly_once(
    debug_logging: pytest.LogCaptureFixture,
) -> None:
    """Metadata normalization is a hot path: correlation id and log field share one conversion."""
    # Arrange
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE)
    conversions: list[object] = []
    details = make_call_details(metadata=[("x-request-id", "req-123")])

    # Act
    with patch.object(client_logging, "metadata_to_dict", counting_metadata_to_dict(conversions)):
        await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    assert len(conversions) == 1


async def test__logging__debug_disabled__skips_the_start_record(caplog: pytest.LogCaptureFixture) -> None:
    """Nothing is formatted for a record that would be dropped anyway."""
    # Arrange
    caplog.set_level(logging.INFO, logger=LOGGER)
    interceptor = AsyncLoggingInterceptor(service_name=SERVICE, log_request_payload=True)
    details = make_call_details(metadata=[("x-request-id", "req-123")])

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("resp")), details=details)

    # Assert
    assert messages(caplog) == ["gRPC call successful"]
