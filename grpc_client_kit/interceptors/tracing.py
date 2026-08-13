"""Tracing interceptor: one OpenTelemetry CLIENT span per gRPC call, of whichever kind."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import (
    Any,
    Protocol,
    runtime_checkable,
)

import grpc.aio

from .base import AsyncClientInterceptor, ClientCall, _spawn_background

# HAS_TRACING tells whether `opentelemetry-api` (the `tracing` extra) is importable. When it is
# False, AsyncClientTracingInterceptor degrades to an announced pass-through (see its docstring).
try:
    from opentelemetry import propagate, trace
    from opentelemetry.trace import Span, SpanKind, Status
    from opentelemetry.trace import StatusCode as OtelStatusCode

    HAS_TRACING = True
except ImportError:  # pragma: no cover - only reachable without the "tracing" extra
    HAS_TRACING = False
    propagate = None  # type: ignore[assignment]
    trace = None  # type: ignore[assignment]

    # Stand-ins keep the module importable and its signatures evaluable without OpenTelemetry.
    class SpanKind:  # type: ignore[no-redef]
        CLIENT = 2
        INTERNAL = 1

    class Span:  # type: ignore[no-redef]
        pass

    class Status:  # type: ignore[no-redef]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    class OtelStatusCode:  # type: ignore[no-redef]
        OK = 0
        ERROR = 1


logger = logging.getLogger(__name__)

# Codes that make a client span red. Everything else (NOT_FOUND, ALREADY_EXISTS, ...) is a normal
# application outcome and must not inflate error rates in the tracing backend.
_ERROR_STATUS_CODES = frozenset(
    {
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.DATA_LOSS,
    }
)


def _metadata_key(key: str | bytes) -> str:
    """Return a metadata key as text (gRPC accepts both ``str`` and ``bytes`` keys)."""
    return key.decode("utf-8") if isinstance(key, bytes) else key


@runtime_checkable
class TracerProtocol(Protocol):
    """Protocol for OpenTelemetry Tracer."""

    def start_span(
        self,
        name: str,
        context: Any | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Any | None = None,
        links: Any | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> Span:
        """Start a new span."""
        ...


class AsyncClientTracingInterceptor(AsyncClientInterceptor):
    """Async client tracing interceptor for gRPC clients using OpenTelemetry.

    For every gRPC call the interceptor:

    - Starts a CLIENT span whose parent is the span active in the **current** OpenTelemetry
      context (typically the server span of the request being handled).
    - Injects that new span context into the **outgoing** metadata (``traceparent``,
      ``tracestate``, ``baggage``), so the callee continues the very same trace.
    - Records the standard gRPC attributes (``rpc.system``, ``rpc.service``, ``rpc.method``).
    - Maps the status the call really ended with to an OpenTelemetry span status.
    - Keeps the span open until a streaming response has been fully consumed.

    Note:
        The parent is taken from the ambient context and is deliberately **not** extracted from the
        outgoing metadata: nothing has written a ``traceparent`` there yet at this point (this
        interceptor is what writes it), so extracting would yield an empty context and turn every
        client span into a root span, silently detaching it from the caller's trace.

    Note:
        A continuation resolves to a `grpc.aio.Call` when the RPC is *created* and never raises, so
        a layer that treats it as the response closes every span as OK. The span here is closed by
        `ClientCall.invoke_unary`, which awaits that Call, or by the wrapper around a response
        stream — the two places where the real status exists.

    Note:
        This layer implements `base.AsyncClientInterceptor.intercept` rather than the simpler
        ``around_call`` seam, because the span must be made current only while the RPC is being
        created. ``around_call`` resumes when the *whole* call is over, and for a streaming response
        that happens in whichever task drains the stream, while gRPC runs the interceptor chain in a
        task of its own: detaching the OpenTelemetry context there fails ("Failed to detach
        context") and leaves the client span current in the task that started the call.

    Note:
        Without the ``tracing`` extra (``opentelemetry-api``) the interceptor is a documented
        pass-through: calls run untouched, no metadata is added and no span is produced. The
        degradation is announced with a warning once per interceptor instance.
    """

    def __init__(self, service_name: str, tracer: TracerProtocol | None = None) -> None:
        """Initialize the tracing interceptor.

        Args:
            service_name: Name of the service for span attributes.
            tracer: Optional OpenTelemetry tracer instance. If not provided,
                    it will attempt to get one using the package name.
        """
        self._service_name = service_name
        self._tracer = tracer

        if not HAS_TRACING:
            logger.warning(
                "opentelemetry-api is not installed: tracing for %r is disabled and calls pass through "
                "untouched (no spans, no traceparent). Install grpc-client-kit[tracing] to enable it.",
                service_name,
            )
            return

        if self._tracer is None:
            self._tracer = trace.get_tracer(__name__)  # type: ignore[assignment]

    def _prepare_call_details(self, client_call_details: Any, span: Any) -> Any:
        """Return call details carrying the propagation headers of ``span``.

        Args:
            client_call_details: The original gRPC call details.
            span: The client span whose context has to travel to the callee.

        Returns:
            Call details with ``traceparent``/``tracestate``/``baggage`` set from the span.
        """
        injected: dict[str, str] = {}
        propagate.inject(injected, context=trace.set_span_in_context(span))

        # Anything the caller already put under those keys is stale by now (this span is the parent
        # of the outgoing call), and duplicates would be joined into an unparseable header value.
        replaced = {key.lower() for key in injected}
        metadata = [
            (key, value)
            for key, value in (client_call_details.metadata or [])
            if _metadata_key(key).lower() not in replaced
        ]
        metadata.extend(injected.items())

        return client_call_details._replace(metadata=metadata)

    async def _issue(self, call: ClientCall) -> Any:
        """Issue the call and hold on to it for as long as one span can.

        Args:
            call: The call to issue.

        Returns:
            The response of a unary call, or the response iterator of a streaming one — the RPC is
            created either way, so a failure to even start it is raised here rather than later.

        Raises:
            grpc.aio.AioRpcError: If a unary call failed, or if the call could not be created.
        """
        if call.response_streaming:
            return await call.invoke_stream()

        return await call.invoke_unary()

    async def intercept(self, call: ClientCall) -> Any:
        """Run one RPC inside a CLIENT span that ends with the status the call really produced."""
        if not HAS_TRACING or self._tracer is None:
            return await self._issue(call)

        # No explicit context: the tracer resolves the parent from the current context.
        span = self._tracer.start_span(name=call.method, kind=SpanKind.CLIENT)

        # opentelemetry-api installed without a configured SDK — the default state of the
        # [tracing] extra — hands out non-recording spans. Attributes, status, the metadata
        # rebuild for inject(): all of it would be measurable CPU per call producing literally
        # nothing, so the no-op path skips straight to issuing the call.
        if not span.is_recording():
            return await self._issue(call)

        span.set_attribute("rpc.system", "grpc")
        span.set_attribute("rpc.service", self._service_name)
        # OTel convention: full method name is preferred
        span.set_attribute("rpc.method", call.method)

        call.details = self._prepare_call_details(call.details, span)

        # Activating the span makes anything the inner interceptors do a child of this call.
        # Exception bookkeeping stays here, hence use_span must not duplicate it.
        with trace.use_span(span, end_on_exit=False, record_exception=False, set_status_on_exception=False):
            try:
                result = await self._issue(call)
            except grpc.aio.AioRpcError as e:
                self._record_exception(span, e)
                span.end()
                raise
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                span.set_status(Status(OtelStatusCode.ERROR, "Cancelled"))
                span.end()
                raise
            except Exception as e:
                span.set_status(Status(OtelStatusCode.ERROR, str(e)))
                span.record_exception(e)
                span.end()
                raise

        if call.response_streaming:
            # The span belongs to the stream now and is closed when the stream ends.
            return self._wrap_stream(result, span)

        if call.request_streaming:
            # `invoke_unary` hands the Call back promptly for streaming requests (awaiting it here
            # would deadlock the write()-style API), so the span closes from an observer instead.
            _spawn_background(self._end_span_with_outcome(result, span))
            return result

        span.set_status(Status(OtelStatusCode.OK))
        span.end()
        return result

    async def _end_span_with_outcome(self, deferred: Any, span: Any) -> None:
        """Await a deferred unary outcome and end the span with whatever it turned out to be."""
        try:
            if hasattr(deferred, "__await__"):
                await deferred
        except grpc.aio.AioRpcError as e:
            self._record_exception(span, e)
        except asyncio.CancelledError:
            span.set_status(Status(OtelStatusCode.ERROR, "Cancelled"))
        except Exception as e:
            span.set_status(Status(OtelStatusCode.ERROR, str(e)))
            span.record_exception(e)
        else:
            span.set_status(Status(OtelStatusCode.OK))
        finally:
            span.end()

    async def _wrap_stream(self, stream: Any, span: Any) -> AsyncIterator[Any]:
        """Wrap streaming response to maintain span lifecycle."""
        try:
            async for item in stream:
                yield item
            span.set_status(Status(OtelStatusCode.OK))
        except grpc.aio.AioRpcError as e:
            self._record_exception(span, e)
            raise
        except asyncio.CancelledError:
            span.set_status(Status(OtelStatusCode.ERROR, "Cancelled"))
            raise
        except Exception as e:
            span.set_status(Status(OtelStatusCode.ERROR, str(e)))
            span.record_exception(e)
            raise
        finally:
            span.end()

    def _record_exception(self, span: Any, e: grpc.aio.AioRpcError) -> None:
        """Map gRPC error to OTEL status and record exception."""
        code_func = getattr(e, "code", None)
        code = code_func() if callable(code_func) else None

        status_code = OtelStatusCode.ERROR if code in _ERROR_STATUS_CODES else OtelStatusCode.OK

        details_func = getattr(e, "details", None)
        details = details_func() if callable(details_func) else str(e)

        # OpenTelemetry drops the description of a non-error status and warns about it once per
        # call, so an expected code (NOT_FOUND, ALREADY_EXISTS, ...) is reported by the attribute
        # below instead. Passing it anyway used to be invisible: this branch was only reachable when
        # the continuation itself raised, which over a live channel it never does.
        span.set_status(Status(status_code, details) if status_code == OtelStatusCode.ERROR else Status(status_code))
        span.record_exception(e)
        span.set_attribute("rpc.grpc.status_code", code.name if code else "UNKNOWN")


__all__ = [
    "HAS_TRACING",
    "AsyncClientTracingInterceptor",
    "TracerProtocol",
]
