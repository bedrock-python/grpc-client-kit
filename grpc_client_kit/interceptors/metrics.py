"""Metrics interceptor: one measurement per RPC, taken where the outcome is finally known.

In `grpc.aio` a continuation resolves to a `Call` the moment the RPC is *created* — identically for
a call that will succeed and one that will fail — and it never raises. Measuring around that
produced a latency histogram of how long it takes to *create* a call and a counter that labelled
every RPC ``status=success, grpc_code=OK``, a server aborting with `PERMISSION_DENIED` included.

This interceptor is therefore an `AsyncAroundClientInterceptor`: its ``yield`` spans the awaited
call and the whole of a response stream, so the duration is the RPC itself and the status is the one
the caller observed, mid-stream failures included. Being a logical interceptor also means the
channel registers it for all four RPC kinds instead of unary-unary alone, which is what used to
leave every streaming call unmeasured.

The in-flight gauge is balanced in a ``finally``, which runs however the call ends: drained, failed,
cancelled, or abandoned by a caller who walks away from a stream — that last case arrives at the
``yield`` as `GeneratorExit`. Every RPC that was created is therefore reported exactly once, so the
request counter and the gauge cannot drift apart.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass

import grpc.aio

from ..protocols import GrpcClientMetricsProtocol
from .base import AsyncAroundClientInterceptor, ClientCall
from .circuit_breaker import CircuitBreakerOpenError

logger = logging.getLogger(__name__)

# HAS_METRICS tells whether `prometheus-client` (the `metrics` extra) is importable. The interceptor
# never talks to Prometheus itself - it records through GrpcClientMetricsProtocol, so any backend
# works - the flag only sharpens the diagnostic when no collector was supplied: it tells "you forgot
# to pass one" apart from "the batteries for the usual one are not installed either".
try:
    import prometheus_client  # noqa: F401

    HAS_METRICS = True
except ImportError:  # pragma: no cover - only reachable without the "metrics" extra
    HAS_METRICS = False


@dataclass(frozen=True, slots=True)
class _CallLabels:
    """The label set of one call, resolved once and reused by every record it produces.

    Attributes:
        service: gRPC service name, taken from the method path.
        method: gRPC method name, or ``"total"`` when the method label is disabled.
        rpc_type: Which of the four RPC kinds the call is.
    """

    service: str
    method: str
    rpc_type: str


def _code_name(error: BaseException) -> str:
    """Return the gRPC status name an error carries, or ``UNKNOWN`` when it carries none."""
    code_func = getattr(error, "code", None)
    if not callable(code_func):
        return "UNKNOWN"

    code: grpc.StatusCode | None = code_func()
    return code.name if code is not None else "UNKNOWN"


class AsyncClientMetricsInterceptor(AsyncAroundClientInterceptor):
    """Async interceptor for recording metrics for gRPC calls.

    Records the following metrics through the injected collector:
    - `grpc_client_requests_total`: Counter of all gRPC requests.
    - `grpc_client_request_duration_seconds`: Histogram of request latencies.
    - `grpc_client_requests_in_flight`: Gauge of calls currently in progress.

    Labels used:
    - `service`: gRPC service name.
    - `method`: gRPC method name, or ``total`` when `enable_method_label` is off.
    - `rpc_type`: One of 'unary_unary', 'unary_stream', 'stream_unary', 'stream_stream'.
    - `status`: 'success', 'error', 'rejected' (refused locally by an open circuit breaker,
      without touching the network) or 'cancelled'.
    - `grpc_code`: gRPC status code name (e.g., 'OK', 'UNAVAILABLE').

    Features:
    - Support for all gRPC call types (unary and streaming).
    - The duration is the RPC, a streaming response up to its last item included.
    - The status is the one the caller observed, not the one creating the call suggested.
    - Configurable method-level granularity.
    - Safe failure handling (exceptions in metrics recording don't crash the call).
    - The in-flight gauge is balanced for every call, streams the caller abandons included.

    Note:
        The interceptor is backend-agnostic: it only needs an object implementing
        :class:`~grpc_client_kit.protocols.GrpcClientMetricsProtocol`. The ``metrics`` extra
        (``prometheus-client``) is only needed by the collector implementation you pass in. If
        ``metrics`` is ``None`` the interceptor records nothing and says so once, at construction
        time, instead of failing silently.
    """

    def __init__(
        self,
        service_name: str,
        metrics: GrpcClientMetricsProtocol | None = None,
        enable_method_label: bool = True,
    ) -> None:
        """Initialize the metrics interceptor.

        Args:
            service_name: Name of the client service.
            metrics: An object implementing GrpcClientMetricsProtocol.
            enable_method_label: Whether to include the method name as a label.
                                 Disable this if you have thousands of methods to
                                 prevent high cardinality in Prometheus.
        """
        self._service_name = service_name
        self._metrics = metrics
        self._enable_method_label = enable_method_label

        if metrics is None:
            hint = (
                "pass metrics=<GrpcClientMetricsProtocol>"
                if HAS_METRICS
                else "install grpc-client-kit[metrics] and pass metrics=<GrpcClientMetricsProtocol>"
            )
            logger.warning("No metrics collector for %r: gRPC calls will not be measured (%s).", service_name, hint)

    def _parse_method_name(self, method_path: str) -> tuple[str, str]:
        """Parse gRPC method path into service and method names.

        Example: /package.Service/Method -> (Service, Method)
        """
        parts = method_path.strip("/").split("/")
        if len(parts) == 2:
            service = parts[0].split(".")[-1]
            return service, parts[1]
        return "unknown", "unknown"

    def _labels_for(self, call: ClientCall) -> _CallLabels:
        """Resolve the labels of one call; `base` hands over the method path already decoded."""
        service, method = self._parse_method_name(call.method)
        return _CallLabels(
            service=service,
            method=method if self._enable_method_label else "total",
            rpc_type=call.rpc_type,
        )

    def _record_inflight(self, labels: _CallLabels, delta: int) -> None:
        """Move the in-flight gauge, or do nothing at all when no collector was supplied."""
        if self._metrics is None:
            return

        try:
            self._metrics.record_inflight_delta(
                service=labels.service,
                method=labels.method,
                rpc_type=labels.rpc_type,
                delta=delta,
            )
        except Exception:
            logger.exception("Failed to record gRPC in-flight delta")

    def _record_request(self, labels: _CallLabels, status: str, grpc_code: str, started: float) -> None:
        """Report one finished call, timing it from `started`."""
        if self._metrics is None:
            return

        try:
            self._metrics.record_request(
                service=labels.service,
                method=labels.method,
                rpc_type=labels.rpc_type,
                status=status,
                grpc_code=grpc_code,
                duration=time.perf_counter() - started,
            )
        except Exception:
            logger.exception("Failed to record gRPC metrics")

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Measure one RPC from the moment it is issued to the moment its last item is delivered."""
        labels = self._labels_for(call)
        self._record_inflight(labels, 1)
        # Taken after the gauge, and after nothing else: what this clock measures is the RPC.
        started = time.perf_counter()

        try:
            yield
        except CircuitBreakerOpenError as error:
            # A local rejection never touched the network. Sharing the 'error'/'UNAVAILABLE' label
            # pair with genuine server failures would make an open breaker indistinguishable from a
            # dead backend on every dashboard — the one question an operator asks during an outage.
            self._record_request(labels, "rejected", _code_name(error), started)
            raise
        except grpc.aio.AioRpcError as error:
            self._record_request(labels, "error", _code_name(error), started)
            raise
        except (asyncio.CancelledError, GeneratorExit, KeyboardInterrupt, SystemExit):
            # GeneratorExit is a caller walking away from a stream. The RPC did happen and holds an
            # in-flight slot, so it is reported like any other call rather than silently dropped.
            self._record_request(labels, "cancelled", "CANCELLED", started)
            raise
        except Exception:
            self._record_request(labels, "error", "UNKNOWN", started)
            raise
        else:
            self._record_request(labels, "success", "OK", started)
        finally:
            self._record_inflight(labels, -1)


__all__ = [
    "HAS_METRICS",
    "AsyncClientMetricsInterceptor",
]
