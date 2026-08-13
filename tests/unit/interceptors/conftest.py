"""Shared fixtures and doubles for the interceptor tests.

Every interceptor is driven the way a channel drives it: through the adapters in
`AsyncClientInterceptor.adapters`, never by calling `intercept` directly. `start_call` and
`run_call` are the two entry points that do this, so a test never has to know which of the four
gRPC base classes its RPC kind is filed under.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any
from unittest.mock import MagicMock

import grpc
import grpc.aio
import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace

from grpc_client_kit.interceptors import client_logging
from grpc_client_kit.interceptors.base import (
    AsyncAroundClientInterceptor,
    AsyncClientInterceptor,
    ClientCall,
    Continuation,
)
from grpc_client_kit.interceptors.circuit_breaker import (
    AsyncCircuitBreakerInterceptor,
    CircuitState,
    MethodCircuitState,
)
from grpc_client_kit.interceptors.metrics import AsyncClientMetricsInterceptor
from tests.helpers import (
    METHOD,
    RPC_KINDS,
    FakeUnaryCall,
    Wire,
    adapter_for,
    await_result,
    make_call_details,
    make_rpc_error,
)

# The service name the observability interceptors are built with, and the logger it gives them.
SERVICE = "test"
LOGGER = f"grpc.client.{SERVICE}"

# Recovery window used by the half-open tests, and the wait that outlives it. Short enough to keep
# the suite quick, wide enough to survive the 15.6 ms tick of the Windows monotonic clock.
RECOVERY_TIMEOUT = 0.1
BEYOND_RECOVERY = 0.15

# A caller's trace that is already in progress when the client call is made.
PARENT_TRACE_ID = 0x0AF7651916CD43DD8448EB211C80319C
PARENT_SPAN_ID = 0x00F067AA0BA902B7

# A traceparent left on the outgoing metadata by an earlier hop; it must never become the parent.
STALE_TRACEPARENT = "00-11111111111111111111111111111111-2222222222222222-01"

# The trace a span is filed under when nothing was active at the time it was started.
FRESH_TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736


# --------------------------------------------------------------------------------------------
# Driving an interceptor through the adapters a channel files it under.
# --------------------------------------------------------------------------------------------


async def start_call(
    interceptor: AsyncClientInterceptor,
    wire: Continuation,
    rpc_type: str = "unary_unary",
    details: Any = None,
    request: Any = "req",
) -> Any:
    """Enter the interceptor through its adapter and return whatever it handed the channel back."""
    adapter = adapter_for(interceptor, RPC_KINDS[rpc_type])
    given = make_call_details() if details is None else details
    return await getattr(adapter, f"intercept_{rpc_type}")(wire, given, request)


async def run_call(
    interceptor: AsyncClientInterceptor,
    wire: Continuation,
    rpc_type: str = "unary_unary",
    details: Any = None,
    request: Any = "req",
) -> Any:
    """Drive a unary-response call end to end: through the adapter, then awaiting the Call."""
    try:
        return await await_result(await start_call(interceptor, wire, rpc_type, details, request))
    finally:
        # The assertions that follow must see the completed teardown of deferred outcomes.
        await settle()


def nested_wire(
    interceptor: AsyncClientInterceptor,
    wire: Continuation,
    rpc_type: str = "unary_unary",
) -> Continuation:
    """A continuation that enters `interceptor` first, the way the next layer of a chain does."""
    adapter = adapter_for(interceptor, RPC_KINDS[rpc_type])

    async def _nested(details: Any, request: Any) -> Any:
        return await getattr(adapter, f"intercept_{rpc_type}")(wire, details, request)

    return _nested


def stack_depth() -> int:
    """Count the frames currently on the stack."""
    depth = 0
    frame: Any = sys._getframe()
    while frame is not None:
        depth += 1
        frame = frame.f_back
    return depth


def depth_recording_stream(depths: list[int], error: BaseException) -> Callable[[], AsyncIterator[Any]]:
    """A stream outcome noting the stack depth it was started at, then failing with `error`."""

    async def _stream() -> AsyncIterator[Any]:
        depths.append(stack_depth())
        yield "item"
        raise error

    return _stream


async def settle() -> None:
    """Collect what the caller dropped and let the loop finalize the generators it held."""
    for _ in range(5):
        gc.collect()
        await asyncio.sleep(0)


# --------------------------------------------------------------------------------------------
# Logical interceptors written against the `base` seam.
# --------------------------------------------------------------------------------------------


class Probe(AsyncClientInterceptor):
    """A minimal logical interceptor writing down how `base` described every call to it.

    Attributes:
        kinds: Per call, its RPC kind together with the two streaming flags derived from it.
        methods: The decoded method name of every call.
    """

    def __init__(self) -> None:
        """Start with nothing observed."""
        self.kinds: list[tuple[str, bool, bool]] = []
        self.methods: list[str] = []

    async def intercept(self, call: ClientCall) -> Any:
        self.kinds.append((call.rpc_type, call.request_streaming, call.response_streaming))
        self.methods.append(call.method)

        if call.response_streaming:
            return await call.invoke_stream()
        return await call.invoke_unary()


class Recorder(AsyncAroundClientInterceptor):
    """An `around_call` interceptor that writes down what it saw and when.

    Attributes:
        events: Everything the generator observed, in the order it observed it.
    """

    def __init__(self, *, timeout: float | None = None, refuse: BaseException | None = None) -> None:
        """Optionally rewrite the deadline before the call, or refuse the call outright."""
        self._timeout = timeout
        self._refuse = refuse
        self.events: list[Any] = []

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        if self._refuse is not None:
            raise self._refuse
        if self._timeout is not None:
            call.details = call.details._replace(timeout=self._timeout)

        self.events.append(("before", call.method, call.rpc_type))
        try:
            yield
        except grpc.aio.AioRpcError as error:
            self.events.append(("failed", error.code()))
            raise
        else:
            self.events.append(("succeeded", call.response))
        finally:
            self.events.append("closed")


class OldStyleInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """An interceptor written against grpc's ``intercept_*`` methods, the generation before `base`."""

    async def intercept_unary_unary(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        return await continuation(client_call_details, request)


# --------------------------------------------------------------------------------------------
# Metadata providers the context interceptor enriches a call from.
# --------------------------------------------------------------------------------------------


async def fresh_token_provider() -> dict[str, str | bytes]:
    """A provider that has to await something before it can answer, as a token refresh would."""
    return {"authorization": "bearer fresh"}


def failing_metadata_provider() -> dict[str, str | bytes]:
    """A provider that blows up, as one talking to an unreachable token endpoint would."""
    raise RuntimeError("no token today")


# --------------------------------------------------------------------------------------------
# Circuit breaker: reaching a given state without duplicating the drive code in every test.
# --------------------------------------------------------------------------------------------


async def fail_unary(
    interceptor: AsyncCircuitBreakerInterceptor,
    code: grpc.StatusCode = grpc.StatusCode.UNAVAILABLE,
    method: str = METHOD,
) -> Wire:
    """Run one unary call that fails with `code`, and hand back the wire it went through."""
    wire = Wire(FakeUnaryCall(error=make_rpc_error(code)))
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire, details=make_call_details(method))
    return wire


async def open_circuit(interceptor: AsyncCircuitBreakerInterceptor, method: str = METHOD) -> MethodCircuitState:
    """Trip a breaker configured with ``fail_threshold=1`` and return the method's state."""
    await fail_unary(interceptor, method=method)
    state = await interceptor._get_method_state(method)
    assert state.state == CircuitState.OPEN
    return state


# --------------------------------------------------------------------------------------------
# Logging: records are read off caplog, so what is asserted is what a handler would see.
# --------------------------------------------------------------------------------------------


@pytest.fixture
def debug_logging(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Capture everything the interceptor writes, the start record included."""
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    return caplog


def client_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Every record this interceptor emitted, ignoring anything else on the root logger."""
    return [record for record in caplog.records if record.name == LOGGER]


def start_record(caplog: pytest.LogCaptureFixture) -> logging.LogRecord:
    """The DEBUG record written before the call was issued."""
    return next(record for record in client_records(caplog) if record.levelno == logging.DEBUG)


def final_record(caplog: pytest.LogCaptureFixture) -> logging.LogRecord:
    """The single record saying how the call ended."""
    terminal = [record for record in client_records(caplog) if record.levelno > logging.DEBUG]
    assert len(terminal) == 1, f"expected exactly one terminal record, got {[r.getMessage() for r in terminal]}"
    return terminal[0]


def messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The formatted message of every record, as a handler would see it."""
    return [record.getMessage() for record in client_records(caplog)]


def counting_metadata_to_dict(conversions: list[Any]) -> Callable[[Any], dict[str, str]]:
    """A `metadata_to_dict` stand-in recording every conversion it is asked to perform."""
    real_metadata_to_dict = client_logging.metadata_to_dict

    def _convert(metadata: Any) -> dict[str, str]:
        conversions.append(metadata)
        return real_metadata_to_dict(metadata)

    return _convert


# --------------------------------------------------------------------------------------------
# Metrics: building the interceptor and reading back what it recorded.
# --------------------------------------------------------------------------------------------


def make_metrics_interceptor(metrics: MagicMock, **kwargs: Any) -> AsyncClientMetricsInterceptor:
    """Build a metrics interceptor reporting into `metrics`."""
    return AsyncClientMetricsInterceptor(service_name="test-service", metrics=metrics, **kwargs)


def inflight_deltas(metrics: MagicMock) -> list[int]:
    """Every in-flight delta recorded, in order."""
    return [call.kwargs["delta"] for call in metrics.record_inflight_delta.call_args_list]


def request_record(metrics: MagicMock) -> dict[str, Any]:
    """The labels of the last recorded request."""
    return dict(metrics.record_request.call_args.kwargs)


# --------------------------------------------------------------------------------------------
# Tracing: a tracer that produces real W3C contexts and records what is done to its spans.
# --------------------------------------------------------------------------------------------


def make_span_context(trace_id: int, span_id: int, *, is_remote: bool = True) -> trace.SpanContext:
    """Build a sampled span context, remote by default as an inherited parent would be."""
    return trace.SpanContext(
        trace_id=trace_id,
        span_id=span_id,
        is_remote=is_remote,
        trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
    )


class FakeSpan(trace.NonRecordingSpan):
    """Span with a real W3C context that also records what the interceptor does to it.

    Attributes:
        attributes: Every attribute set on the span, by key.
        statuses: Every status set on the span, in order.
        exceptions: Every exception recorded on the span, in order.
        ended: How many times the span was ended.
    """

    def __init__(self, span_context: trace.SpanContext) -> None:
        """Wrap a context and start with nothing recorded."""
        super().__init__(span_context)
        self.attributes: dict[str, Any] = {}
        self.statuses: list[Any] = []
        self.exceptions: list[BaseException] = []
        self.ended = 0

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_status(self, status: Any, description: str | None = None) -> None:
        self.statuses.append(status)

    def record_exception(
        self,
        exception: BaseException,
        attributes: Any = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:
        self.exceptions.append(exception)

    def end(self, end_time: int | None = None) -> None:
        self.ended += 1

    def is_recording(self) -> bool:
        return True


class FakeTracer:
    """Tracer producing children of whichever context OpenTelemetry resolves for the call.

    Attributes:
        spans: Every span this tracer started, in order.
        contexts: The context argument each span was started with.
        kinds: The span kind each span was started with.
        names: The name each span was started with.
    """

    def __init__(self) -> None:
        """Start with no spans, handing out span ids from a fixed sequence."""
        self.spans: list[FakeSpan] = []
        self.contexts: list[Any] = []
        self.kinds: list[Any] = []
        self.names: list[str] = []
        self._next_span_id = 0x0BB0

    def start_span(self, name: str, context: Any = None, kind: Any = None, **kwargs: Any) -> FakeSpan:
        parent = trace.get_current_span(context).get_span_context()
        self._next_span_id += 1
        span = FakeSpan(
            make_span_context(
                parent.trace_id if parent.is_valid else FRESH_TRACE_ID,
                self._next_span_id,
                is_remote=False,
            )
        )
        self.names.append(name)
        self.contexts.append(context)
        self.kinds.append(kind)
        self.spans.append(span)
        return span


@pytest.fixture
def current_trace() -> Iterator[trace.SpanContext]:
    """Run the test as if a caller's span were already active in this context."""
    parent = trace.NonRecordingSpan(make_span_context(PARENT_TRACE_ID, PARENT_SPAN_ID))
    token = otel_context.attach(trace.set_span_in_context(parent))
    try:
        yield parent.get_span_context()
    finally:
        otel_context.detach(token)


def traceparents(wire: Wire) -> list[str]:
    """Every traceparent header the last attempt carried."""
    return [value for key, value in wire.metadata_pairs if key == "traceparent"]
