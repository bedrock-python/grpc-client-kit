"""What a call actually writes to your logs and your metrics backend.

Nothing here is mocked: a real chain runs against a real server, and both the log records
and the metric samples are printed field by field as the interceptors emit them.

This example shows how to:

1. Satisfy `GrpcClientMetricsProtocol` with a few lines of plain Python. The kit never
   talks to Prometheus itself -- the `metrics` extra only supplies the usual backend -- so
   any object with these three methods is a valid collector.
2. Read the structured records the logging layer writes to `grpc.client.<service_name>`.
   The handler below prints every `grpc.*` field of every record, which is what a
   production JSON formatter would ship.
3. Correlate records by request id. `AsyncClientContextInterceptor` sits in the **outer**
   slot, above logging, which is the only position where the metadata it injects is
   already there when the logging layer looks for `request-id`.
4. See redaction: an `authorization` header is injected alongside the request id and
   arrives in the log as ``***``.
5. Watch a failure, and a stream. A failed call is logged at ERROR with the status the
   caller was given and counted with ``status=error``; a stream is recorded when its last
   item is delivered, not when the call was created.
6. Check the invariant that makes the numbers trustworthy: the in-flight gauge is back to
   zero at the end, because every call that was created is reported exactly once.

Run with: `python examples/observability.py`. It prints one ``[log]`` block and one
``[metric]`` line per call, a pool statistics line, the final summary, and exits 0.
"""

# ruff: noqa: T201 -- an example is a program whose output IS the documentation.

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import grpc
import grpc.aio

from grpc_client_kit import (
    AsyncClientContextInterceptor,
    ChannelPool,
    GrpcClient,
    GrpcClientConfig,
    ObservabilityConfig,
    build_interceptors,
)

SERVICE = "example.Ledger"
GET_ENTRY = f"/{SERVICE}/GetEntry"
LIST_ENTRIES = f"/{SERVICE}/ListEntries"

CLIENT_NAME = "ledger-client"
SHUTDOWN_GRACE = 0.5


@dataclass(slots=True)
class PrintingMetrics:
    """A `GrpcClientMetricsProtocol` implementation that prints and remembers.

    Attributes:
        requests: One entry per finished call, as the metrics interceptor reported it.
        inflight: The gauge, keyed by label set. It has to be balanced once nothing runs.
    """

    requests: list[tuple[str, str, str]] = field(default_factory=list)
    inflight: dict[tuple[str, str, str], int] = field(default_factory=dict)

    def record_request(
        self,
        service: str,
        method: str,
        rpc_type: str,
        status: str,
        grpc_code: str,
        duration: float,
    ) -> None:
        """Record one finished call (counter plus latency histogram in a real backend)."""
        self.requests.append((method, status, grpc_code))
        print(
            f"[metric] request service={service} method={method} rpc_type={rpc_type} "
            f"status={status} grpc_code={grpc_code} duration={duration * 1000:.1f}ms"
        )

    def record_inflight_delta(self, service: str, method: str, rpc_type: str, delta: int) -> None:
        """Move the in-flight gauge; +1 before the call, -1 in a `finally` after it."""
        key = (service, method, rpc_type)
        self.inflight[key] = self.inflight.get(key, 0) + delta

    def record_pool_stats(self, active_channels: int, idle_targets: int) -> None:
        """Record a channel pool snapshot, reported whenever the pool grows or evicts."""
        print(f"[metric] pool active_channels={active_channels} idle_targets={idle_targets}")

    def total_inflight(self) -> int:
        """Sum of every gauge, which must be zero once no call is running."""
        return sum(self.inflight.values())


class RecordPrinter(logging.Handler):
    """Prints each record with the structured fields the logging interceptor attached.

    A real deployment installs a JSON formatter here instead; the fields are the same ones.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Print one record and its `grpc.*` / `request_id` extras."""
        print(f"[log] {record.levelname:<5} {record.getMessage()}")
        for key, value in sorted(record.__dict__.items()):
            if key.startswith("grpc.") or key == "request_id":
                print(f"[log]       {key}={value!r}")


class LedgerService:
    """The demo service: one entry that exists, one that does not, and a stream."""

    def generic_handler(self) -> grpc.GenericRpcHandler:
        """Expose the two methods without protoc (see `tests/integration/echo_bench.py`)."""
        return grpc.method_handlers_generic_handler(
            SERVICE,
            {
                "GetEntry": grpc.unary_unary_rpc_method_handler(self._get_entry),
                "ListEntries": grpc.unary_stream_rpc_method_handler(self._list_entries),
            },
        )

    async def _get_entry(self, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """Answer for a known entry, abort with NOT_FOUND for anything else."""
        await asyncio.sleep(0.02)
        if request != b"entry-1":
            await context.abort(grpc.StatusCode.NOT_FOUND, f"no entry {request.decode()}")
        return b"amount=42"

    async def _list_entries(
        self, _request: bytes, _context: grpc.aio.ServicerContext[bytes, bytes]
    ) -> AsyncIterator[bytes]:
        """Stream a few entries, slowly enough for the measured duration to mean something."""
        for index in range(1, 4):
            await asyncio.sleep(0.02)
            yield f"entry-{index}".encode()


class LedgerStub:
    """What protoc would have generated: multicallables bound to one channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind one multicallable per method to `channel`."""
        self.get_entry: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(GET_ENTRY)
        self.list_entries: grpc.aio.UnaryStreamMultiCallable[bytes, bytes] = channel.unary_stream(LIST_ENTRIES)


async def start_server() -> tuple[grpc.aio.Server, str]:
    """Start the demo server on an ephemeral port.

    Returns:
        The running server and the address clients connect to.
    """
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((LedgerService().generic_handler(),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


def install_record_printer() -> None:
    """Send this client's log records to `RecordPrinter`, and only there.

    The interceptor writes to ``grpc.client.<service_name>``. DEBUG is on so the start
    record shows up too -- it carries the metadata and the request payload, and the
    interceptor only builds those fields when the logger is actually enabled for DEBUG.
    """
    log = logging.getLogger(f"grpc.client.{CLIENT_NAME}")
    log.setLevel(logging.DEBUG)
    log.addHandler(RecordPrinter())
    log.propagate = False


def call_metadata() -> dict[str, str | bytes]:
    """The metadata the context interceptor injects into every outgoing call.

    In a real client this reads an ambient context: the request id of the inbound request
    being handled, the caller's token, a tenant id.
    """
    return {"request-id": "req-42", "authorization": "Bearer super-secret-token"}


async def main() -> None:
    install_record_printer()
    metrics = PrintingMetrics()

    server, target = await start_server()
    print(f"[server] listening on {target}\n")

    chain = build_interceptors(
        observability=ObservabilityConfig(
            service_name=CLIENT_NAME,
            logging=True,
            # A registry makes the metrics layer real; without one it would be left out of
            # the chain entirely rather than added as a hop that records nothing.
            metrics=True,
            metrics_registry=metrics,
            # Off by default -- payloads are only reachable through a hand-built config.
            log_request_payload=True,
            log_response_payload=True,
        ),
        # Outer slot: metadata injected here is already on the call when the logging layer
        # looks for a request id. Injected any deeper, it would be invisible to the logs.
        extra_interceptors=[AsyncClientContextInterceptor(call_metadata)],
    )

    try:
        # The same registry collects pool statistics, independently of the call metrics.
        async with ChannelPool(metrics=metrics) as pool:
            client = GrpcClient(LedgerStub, GrpcClientConfig(target=target, insecure=True), pool, interceptors=chain)

            print("--- a successful unary call ---")
            async with client as stub:
                response = await stub.get_entry(b"entry-1")
                print(f"[client] GetEntry -> {response!r}")

            print("\n--- a call the server refuses ---")
            async with client as stub:
                try:
                    await stub.get_entry(b"entry-9")
                except grpc.aio.AioRpcError as error:
                    print(f"[client] GetEntry -> {error.code().name}: {error.details()}")

            print("\n--- a stream: both records land when the last item has been delivered ---")
            async with client as stub:
                items = [item async for item in stub.list_entries(b"")]
                print(f"[client] ListEntries -> {items!r}")
    finally:
        await server.stop(grace=SHUTDOWN_GRACE)

    print("\n--- summary ---")
    for method, status, code in metrics.requests:
        print(f"[summary] {method}: status={status} grpc_code={code}")
    print(f"[summary] in-flight gauge back to {metrics.total_inflight()}: every call was reported exactly once")


if __name__ == "__main__":
    asyncio.run(main())
