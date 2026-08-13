"""Writing your own interceptor: one async generator instead of four methods.

`AsyncAroundClientInterceptor` is the seam this kit is built around. You implement a single
`around_call` generator and it covers all four RPC kinds:

- code **before** the ``yield`` runs before the RPC exists -- rewrite `call.details` here,
  or raise here to refuse the call outright, in which case nothing reaches the network;
- the RPC happens **at** the ``yield``, and for a streaming response the generator stays
  suspended there until the last item has been delivered;
- code **after** the ``yield`` -- ``except``, ``else``, ``finally`` -- runs once the
  outcome is known. A failure arrives as `grpc.aio.AioRpcError`, one that happens halfway
  through a response stream included.

This example shows how to:

1. Write `TimingInterceptor`, whose one generator times a unary call, a whole stream, and
   a call that fails mid-stream after two items have already been consumed.
2. Write `RequestSizeGuard`, which refuses a call *before* the ``yield``. The server's own
   call counter proves the refused request never left the process.
3. Put both into a chain with `build_interceptors(extra_interceptors=[...])`, which is the
   outer slot -- so `TimingInterceptor`, listed first, wraps the guard and observes the
   refusal as the failure the caller sees.

Run with: `python examples/custom_interceptor.py`. It prints one ``[timing]`` line per
call, the server-side call count, and exits 0.
"""

# ruff: noqa: T201 -- an example is a program whose output IS the documentation.

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

import grpc
import grpc.aio

from grpc_client_kit import ChannelPool, GrpcClient, GrpcClientConfig, build_interceptors
from grpc_client_kit.interceptors import AsyncAroundClientInterceptor, ClientCall

SERVICE = "example.Timed"
SLOW_ECHO = f"/{SERVICE}/SlowEcho"
SLOW_COUNT_UP = f"/{SERVICE}/SlowCountUp"

MAX_REQUEST_BYTES = 16
SHUTDOWN_GRACE = 0.5


class TimingInterceptor(AsyncAroundClientInterceptor):
    """Times every RPC and reports how it ended -- one generator for every call kind."""

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Measure one RPC, whether it is a unary call or a whole response stream."""
        kind = "stream" if call.response_streaming else "unary"
        started = time.perf_counter()
        try:
            # For a streaming response this yield spans the ENTIRE stream, so the elapsed
            # time below is the stream, not the time it took to create the call.
            yield
        except grpc.aio.AioRpcError as error:
            elapsed_ms = (time.perf_counter() - started) * 1000
            print(f"[timing] {call.method} ({kind}) FAILED {error.code().name} after {elapsed_ms:.1f}ms")
            raise
        else:
            elapsed_ms = (time.perf_counter() - started) * 1000
            print(f"[timing] {call.method} ({kind}) OK in {elapsed_ms:.1f}ms")


class RequestSizeGuard(AsyncAroundClientInterceptor):
    """Refuses oversized requests before the RPC exists.

    Raising before the ``yield`` means the call is never created: no connection is used, no
    server handler runs. The error is shaped like any other gRPC failure -- exactly how the
    kit's own `CircuitBreakerOpenError` refuses calls -- so the layers above this one see a
    normal `grpc.aio.AioRpcError` rather than something they have to special-case.
    """

    def __init__(self, max_bytes: int) -> None:
        """Store the largest request this layer will let through."""
        self._max_bytes = max_bytes

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Reject a too-large request, otherwise let the call run untouched."""
        # A streaming request is an iterator, not a payload: there is nothing to measure
        # here without consuming what the call itself needs.
        if not call.request_streaming and len(call.request) > self._max_bytes:
            raise grpc.aio.AioRpcError(
                code=grpc.StatusCode.INVALID_ARGUMENT,
                initial_metadata=grpc.aio.Metadata(),
                trailing_metadata=grpc.aio.Metadata(),
                details=f"request of {len(call.request)} bytes exceeds the {self._max_bytes} byte client limit",
            )

        yield


class TimedService:
    """The demo service, counting the calls that actually reached a handler."""

    def __init__(self) -> None:
        """Start with an empty call counter."""
        self.calls = 0

    def generic_handler(self) -> grpc.GenericRpcHandler:
        """Expose the two methods without protoc (see `tests/integration/echo_bench.py`)."""
        return grpc.method_handlers_generic_handler(
            SERVICE,
            {
                "SlowEcho": grpc.unary_unary_rpc_method_handler(self._slow_echo),
                "SlowCountUp": grpc.unary_stream_rpc_method_handler(self._slow_count_up),
            },
        )

    async def _slow_echo(self, request: bytes, _context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """Unary handler with an artificial delay, so the measured time is not zero."""
        self.calls += 1
        await asyncio.sleep(0.05)
        return b"echo:" + request

    async def _slow_count_up(
        self, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]
    ) -> AsyncIterator[bytes]:
        """Streaming handler: one delayed item per number, aborting after two on request."""
        self.calls += 1
        fail_mid_stream = request == b"fail"

        for number in range(1, 4):
            await asyncio.sleep(0.03)
            if fail_mid_stream and number == 3:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "backend went away mid-stream")
            yield str(number).encode()


class TimedStub:
    """What protoc would have generated: multicallables bound to one channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind one multicallable per method to `channel`."""
        self.slow_echo: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(SLOW_ECHO)
        self.slow_count_up: grpc.aio.UnaryStreamMultiCallable[bytes, bytes] = channel.unary_stream(SLOW_COUNT_UP)


async def start_server(service: TimedService) -> tuple[grpc.aio.Server, str]:
    """Start the demo server on an ephemeral port.

    Returns:
        The running server and the address clients connect to.
    """
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((service.generic_handler(),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


async def main() -> None:
    service = TimedService()
    server, target = await start_server(service)
    print(f"[server] listening on {target}")

    # The outer slot, in order: the timer wraps the guard, so a refused call is reported by
    # the timer too. `build_interceptors` returns what a channel accepts -- each logical
    # interceptor expanded into its four per-kind adapters, order preserved.
    interceptors = build_interceptors(
        extra_interceptors=[TimingInterceptor(), RequestSizeGuard(MAX_REQUEST_BYTES)],
    )

    try:
        async with ChannelPool() as pool:
            client = GrpcClient(
                TimedStub,
                config=GrpcClientConfig(target=target, insecure=True),
                pool=pool,
                interceptors=interceptors,
            )

            print("\n--- unary call ---")
            async with client as stub:
                response = await stub.slow_echo(b"hi")
                print(f"[client] SlowEcho -> {response!r}")

            print("\n--- streaming call: the timing spans the whole stream ---")
            async with client as stub:
                items = [item async for item in stub.slow_count_up(b"ok")]
                print(f"[client] SlowCountUp -> {items!r}")

            print("\n--- streaming call that fails after two items ---")
            async with client as stub:
                delivered: list[bytes] = []
                try:
                    async for item in stub.slow_count_up(b"fail"):
                        delivered.append(item)
                except grpc.aio.AioRpcError as error:
                    print(f"[client] SlowCountUp got {delivered!r}, then {error.code().name}: {error.details()}")

            print("\n--- refused before the RPC exists ---")
            calls_before = service.calls
            async with client as stub:
                try:
                    await stub.slow_echo(b"x" * (MAX_REQUEST_BYTES + 1))
                except grpc.aio.AioRpcError as error:
                    print(f"[client] SlowEcho refused: {error.code().name}: {error.details()}")
            print(f"[server] handlers entered during the refused call: {service.calls - calls_before}")
    finally:
        await server.stop(grace=SHUTDOWN_GRACE)

    print(f"\n[server] stopped after serving {service.calls} calls")


if __name__ == "__main__":
    asyncio.run(main())
