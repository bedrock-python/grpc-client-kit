"""Logging interceptor: one structured record per call, written once the call has an outcome.

In `grpc.aio` a continuation resolves to a `Call` the moment the RPC is *created* — identically for
a call that will succeed and one that will fail — and it never raises. A logger that treated that
object as the response announced "gRPC call successful" for calls the server had aborted, and timed
how long creating the RPC took rather than the RPC.

This interceptor is therefore an `AsyncAroundClientInterceptor`: its ``yield`` spans the awaited call
and the whole of a response stream, so a failure arrives as the `grpc.aio.AioRpcError` it is —
mid-stream failures included — and the duration is the call the caller waited for. Being a logical
interceptor also means the channel registers it for all four RPC kinds instead of unary-unary alone,
which is what used to leave streaming calls with no records at all.

One call produces at most two records: the DEBUG one at the start, and exactly one terminal record
saying how it ended. A caller who walks away from a stream ends it too — that arrives at the
``yield`` as `GeneratorExit` and is reported as a cancellation, so no call is left unaccounted for.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Mapping

import grpc.aio

from ..utils import metadata_to_dict
from .base import AsyncAroundClientInterceptor, ClientCall
from .circuit_breaker import CircuitBreakerOpenError

logger = logging.getLogger(__name__)

DEFAULT_SENSITIVE_HEADERS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "api-key",
    "x-api-key",
    "token",
    "x-auth-token",
    "secret",
    "password",
    "passwd",
}

# Codes that point at a broken callee (or at us) rather than at an expected application outcome:
# only those are worth a stack trace in the client log.
_CRITICAL_STATUS_CODES = frozenset({grpc.StatusCode.INTERNAL, grpc.StatusCode.UNKNOWN, grpc.StatusCode.DATA_LOSS})

_MAX_PAYLOAD_CHARS = 1000

# Values that end up in a LogRecord: strings, the duration in milliseconds and boolean flags.
type _LogExtra = dict[str, str | bool | float]


class AsyncLoggingInterceptor(AsyncAroundClientInterceptor):
    """Async logging interceptor for gRPC clients with sensitive data filtering.

    Provides comprehensive structured logging for every gRPC call, including
    start time, duration, status, and metadata.

    Features:
    - **Real Outcomes**: The terminal record carries the status the caller observed, whether it
      arrived at the end of a unary call or in the middle of a response stream.
    - **Sensitive Data Redaction**: Automatically masks common authentication headers.
    - **Method-Level Sensitivity**: Entire methods or regex patterns can be marked
      as sensitive to prevent payload logging.
    - **Request ID Correlation**: Automatically extracts and includes `request-id`
      from metadata in all log entries.
    - **Payload Logging**: Optional logging of request and response bodies (truncated to 1KB).
    - **LRU Cache**: Efficiently caches sensitivity check results for method names.
    - **Structured Logging**: Uses the standard Python `logging` module with `extra` dict.

    Note:
        Every log record gets a freshly built `extra` mapping derived from the call-wide fields
        (service, method, request id, sensitivity). Records therefore never inherit fields from an
        earlier stage of the same call, and handlers can retain the mapping they were given.
    """

    def __init__(
        self,
        service_name: str,
        sensitive_methods: set[str] | None = None,
        sensitive_patterns: list[str] | None = None,
        sensitive_headers: set[str] | None = None,
        log_request_payload: bool = False,
        log_response_payload: bool = False,
        log_metadata: bool = True,
        max_cache_size: int = 1000,
        success_log_level: int = logging.INFO,
    ) -> None:
        """Initialize the logging interceptor.

        Args:
            service_name: Name of the service for log categorization.
            sensitive_methods: Set of full method names to treat as sensitive.
            sensitive_patterns: List of regex patterns for sensitive method names.
            sensitive_headers: Set of header names (case-insensitive) to redact.
            log_request_payload: Whether to log the request object.
            log_response_payload: Whether to log the response object.
            log_metadata: Whether to log gRPC metadata (redacted).
            max_cache_size: Size of the sensitivity check LRU cache.
            success_log_level: Level of the record a successful call emits. One INFO line per
                successful RPC is a flood at high QPS — drop this to ``logging.DEBUG`` there, or
                keep the default and set the logger's own level instead.
        """
        self._service_name = service_name
        self._sensitive_methods = sensitive_methods or set()
        self._sensitive_patterns = [re.compile(pattern) for pattern in (sensitive_patterns or [])]
        self._sensitive_headers = {h.lower() for h in (sensitive_headers or DEFAULT_SENSITIVE_HEADERS)}
        self._log_request_payload = log_request_payload
        self._log_response_payload = log_response_payload
        self._log_metadata = log_metadata
        self._success_log_level = success_log_level

        # The service name never changes, so neither does the logger: resolving it per call costs
        # an f-string plus logging's module lock, on the hot path, for nothing.
        self._log = logging.getLogger(f"grpc.client.{service_name}")

        # LRU cache for sensitivity checks. No lock on purpose: the critical section below contains
        # no await, so it is atomic with respect to the event loop — and asyncio.Lock offers no
        # cross-thread protection anyway. Keep it await-free or bring the lock back.
        self._sensitivity_cache: OrderedDict[str, bool] = OrderedDict()
        self._max_cache_size = max_cache_size

    def _is_sensitive(self, method: str) -> bool:
        """Check if method is sensitive, with LRU caching."""
        if method in self._sensitivity_cache:
            self._sensitivity_cache.move_to_end(method)
            return self._sensitivity_cache[method]

        is_sensitive = method in self._sensitive_methods or any(
            pattern.match(method) for pattern in self._sensitive_patterns
        )

        if len(self._sensitivity_cache) >= self._max_cache_size:
            self._sensitivity_cache.popitem(last=False)

        self._sensitivity_cache[method] = is_sensitive
        return is_sensitive

    def _extract_request_id(self, metadata: Mapping[str, str]) -> str | None:
        """Extract the request ID used for log correlation.

        Args:
            metadata: Metadata already normalized by
                :func:`~grpc_client_kit.utils.metadata_to_dict`.

        Returns:
            The correlation id, or None when the call carries none.
        """
        for key, value in metadata.items():
            if key.lower() in ("request-id", "x-request-id"):
                return value
        return None

    def _redact(self, metadata: Mapping[str, str]) -> dict[str, str]:
        """Return a copy of the metadata with sensitive header values masked."""
        return {key: ("***" if key.lower() in self._sensitive_headers else value) for key, value in metadata.items()}

    def _finish_extra(self, base_extra: _LogExtra, start_time: float, status: str) -> _LogExtra:
        """Build the `extra` mapping of a terminal record: call-wide fields plus duration/status."""
        return {
            **base_extra,
            "grpc.duration_ms": (time.perf_counter() - start_time) * 1000,
            "grpc.status": status,
        }

    def _failure_extra(
        self,
        base_extra: _LogExtra,
        start_time: float,
        error: BaseException,
        is_sensitive: bool,
    ) -> tuple[_LogExtra, bool, str]:
        """Describe a failed call.

        Args:
            base_extra: Call-wide log fields.
            start_time: Value of `time.perf_counter()` taken when the call started.
            error: The gRPC error raised by the call.
            is_sensitive: Whether the method is marked sensitive (suppresses error details).

        Returns:
            A tuple of the record's `extra` mapping, whether the failure deserves a stack
            trace, and the gRPC error details.
        """
        code_func = getattr(error, "code", None)
        code: grpc.StatusCode | None = code_func() if callable(code_func) else None
        details_func = getattr(error, "details", None)
        details = str(details_func() if callable(details_func) else error)

        extra = self._finish_extra(base_extra, start_time, code.name if code else "UNKNOWN")
        if not is_sensitive:
            extra["grpc.error"] = details

        return extra, code in _CRITICAL_STATUS_CODES or code is None, details

    def _log_start(
        self,
        log: logging.Logger,
        base_extra: _LogExtra,
        metadata: Mapping[str, str],
        call: ClientCall,
        is_sensitive: bool,
    ) -> None:
        """Emit the call-start debug record."""
        if is_sensitive:
            log.debug("gRPC call started (SENSITIVE)", extra=dict(base_extra))
            return

        extra = dict(base_extra)
        if self._log_metadata and metadata:
            extra["grpc.metadata"] = str(self._redact(metadata))

        if self._log_request_payload:
            if call.request_streaming:
                extra["grpc.request"] = "<stream_request>"
            else:
                extra["grpc.request"] = str(call.request)[:_MAX_PAYLOAD_CHARS]

        log.debug("gRPC call started", extra=extra)

    def _log_rpc_failure(
        self,
        log: logging.Logger,
        extra: _LogExtra,
        is_critical: bool,
        details: str,
        subject: str,
    ) -> None:
        """Emit the terminal record of a call that came back with a gRPC status."""
        if is_critical:
            log.exception("gRPC %s failed with critical error", subject, extra=extra)
        else:
            # error() instead of exception() to avoid long tracebacks for expected errors
            log.error("gRPC %s failed with status %s: %s", subject, extra["grpc.status"], details, extra=extra)

    def _log_success(
        self,
        log: logging.Logger,
        extra: _LogExtra,
        call: ClientCall,
        is_sensitive: bool,
        subject: str,
    ) -> None:
        """Emit the terminal record of a call that ended with OK."""
        if is_sensitive:
            log.log(self._success_log_level, "gRPC %s successful (SENSITIVE)", subject, extra=extra)
            return

        # A streaming response has no single payload to show: `call.response` stays None for it.
        if self._log_response_payload and not call.response_streaming:
            extra["grpc.response"] = str(call.response)[:_MAX_PAYLOAD_CHARS]
        log.log(self._success_log_level, "gRPC %s successful", subject, extra=extra)

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Log one RPC: a record when it starts, and exactly one saying how it ended."""
        log = self._log

        # A logger that will emit nothing must cost nothing: no metadata normalization, no
        # sensitivity lookup, no extra dicts. WARNING is the lowest level any record of this
        # interceptor is emitted at when things go wrong, so it is the gate.
        if not log.isEnabledFor(logging.WARNING):
            yield
            return

        is_sensitive = self._is_sensitive(call.method)

        # Metadata is normalized exactly once per call: both the correlation id and the redacted
        # metadata field read from this dict instead of walking the raw pairs again.
        metadata = metadata_to_dict(call.details.metadata)
        request_id = self._extract_request_id(metadata)

        base_extra: _LogExtra = {
            "grpc.service": self._service_name,
            "grpc.method": call.method,
        }
        if request_id:
            base_extra["request_id"] = request_id
        if is_sensitive:
            base_extra["grpc.sensitive"] = True

        # Formatting metadata and payloads is pure waste when DEBUG is off, and this is a hot path.
        if log.isEnabledFor(logging.DEBUG):
            self._log_start(log, base_extra, metadata, call, is_sensitive)

        # Streaming responses are reported as streams: they end when their last item is delivered.
        subject = "stream" if call.response_streaming else "call"
        start_time = time.perf_counter()

        try:
            yield
        except (asyncio.CancelledError, GeneratorExit):
            # GeneratorExit is a caller walking away from a stream, which ends the call as surely as
            # a cancellation does.
            if log.isEnabledFor(logging.INFO):
                log.info("gRPC %s cancelled", subject, extra=self._finish_extra(base_extra, start_time, "CANCELLED"))
            raise
        except CircuitBreakerOpenError as error:
            # A rejection by the local breaker is expected behaviour while the circuit recovers,
            # not a fresh server failure: one WARNING per rejection, never an ERROR flood that
            # drowns the handful of genuine transition records.
            extra = self._finish_extra(base_extra, start_time, "UNAVAILABLE")
            log.warning("gRPC %s refused by the open circuit breaker: %s", subject, error.details(), extra=extra)
            raise
        except grpc.aio.AioRpcError as error:
            extra, is_critical, details = self._failure_extra(base_extra, start_time, error, is_sensitive)
            self._log_rpc_failure(log, extra, is_critical, details, subject)
            raise
        except Exception as error:
            extra = self._finish_extra(base_extra, start_time, "INTERNAL")
            if not is_sensitive:
                extra["grpc.error"] = str(error)
            log.exception("gRPC %s failed with unexpected error", subject, extra=extra)
            raise
        else:
            if log.isEnabledFor(self._success_log_level):
                extra = self._finish_extra(base_extra, start_time, "OK")
                self._log_success(log, extra, call, is_sensitive, subject)


__all__ = [
    "DEFAULT_SENSITIVE_HEADERS",
    "AsyncLoggingInterceptor",
]
