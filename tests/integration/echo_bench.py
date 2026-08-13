"""The echo bench: a controllable grpc.aio server and the hand-written stub that calls it.

The test service is registered through generic bytes-in/bytes-out handlers, so the whole bench runs
without protoc and without compiled ``.proto`` files: a hand-written stub builds the four
multicallables straight off the channel, exactly the shape generated code has.

Every handler is driven by a `MethodControl` the test writes before it calls, which is what lets one
server stand in for a healthy backend, a failing one and an unresponsive one.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field

import grpc
import grpc.aio

from grpc_client_kit import ChannelPool, ConnectivityConfig, GrpcClient, GrpcClientConfig

SERVICE = "test.EchoService"

ECHO = f"/{SERVICE}/Echo"
STREAM = f"/{SERVICE}/Stream"
COLLECT = f"/{SERVICE}/Collect"
CHAT = f"/{SERVICE}/Chat"


@dataclass(slots=True)
class MethodControl:
    """Knobs of one test method, set by the test before it calls.

    Attributes:
        abort_code: Status the handler aborts with on entry; None means the method always answers.
        abort_details: Details sent with any abort of this method.
        fail_times: How many of the first calls abort; 0 means every call aborts.
        hang: Whether the handler blocks until `release()`, standing in for an unresponsive server.
        items: Payloads a streaming response yields.
        mid_stream_abort: Status a streaming response aborts with after `mid_stream_after` items.
        mid_stream_after: How many items are delivered before the mid-stream abort.
        calls: How many times the method was entered.
        metadata: Metadata of every call, in arrival order.
        deadlines: Seconds left on each call's deadline as the *server* saw it, in arrival order.
            This is the call budget as it crossed the wire, so a test can read what the retry layer
            handed out without asking the client side about it. None for a call sent without one.
        received: Every request payload the streaming-request methods consumed.
        released: Set by `release()` to unblock a hanging handler.
    """

    abort_code: grpc.StatusCode | None = None
    abort_details: str = "aborted by test"
    fail_times: int = 0
    hang: bool = False
    items: tuple[bytes, ...] = (b"one", b"two", b"three")
    mid_stream_abort: grpc.StatusCode | None = None
    mid_stream_after: int = 1
    calls: int = 0
    metadata: list[dict[str, str | bytes]] = field(default_factory=list)
    deadlines: list[float | None] = field(default_factory=list)
    received: list[bytes] = field(default_factory=list)
    released: asyncio.Event = field(default_factory=asyncio.Event)

    def release(self) -> None:
        """Let a hanging handler proceed."""
        self.released.set()

    async def enter(self, context: grpc.aio.ServicerContext[bytes, bytes]) -> None:
        """Record the call, apply the configured delay and abort if the method is failing.

        Args:
            context: The servicer context of the call being served.
        """
        self.calls += 1
        self.metadata.append(dict(context.invocation_metadata() or ()))
        self.deadlines.append(context.time_remaining())

        if self.hang:
            await self.released.wait()

        code = self.abort_code
        if code is not None and (self.fail_times == 0 or self.calls <= self.fail_times):
            await context.abort(code, self.abort_details)


class EchoService:
    """Bytes-in/bytes-out test service exposing one method of each of the four RPC kinds."""

    def __init__(self) -> None:
        self.unary_unary = MethodControl()
        self.unary_stream = MethodControl()
        self.stream_unary = MethodControl()
        self.stream_stream = MethodControl()

    @property
    def controls(self) -> tuple[MethodControl, ...]:
        """Every method control of this service."""
        return (self.unary_unary, self.unary_stream, self.stream_unary, self.stream_stream)

    def release_all(self) -> None:
        """Unblock every hanging handler, so teardown never waits on one."""
        for control in self.controls:
            control.release()

    def generic_handler(self) -> grpc.GenericRpcHandler:
        """Build the generic handler exposing the four test methods.

        Returns:
            A handler registering the service without any compiled protos.
        """
        return grpc.method_handlers_generic_handler(
            SERVICE,
            {
                "Echo": grpc.unary_unary_rpc_method_handler(self._echo),
                "Stream": grpc.unary_stream_rpc_method_handler(self._stream),
                "Collect": grpc.stream_unary_rpc_method_handler(self._collect),
                "Chat": grpc.stream_stream_rpc_method_handler(self._chat),
            },
        )

    async def _echo(self, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        await self.unary_unary.enter(context)
        return request

    async def _stream(self, request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> AsyncIterator[bytes]:
        control = self.unary_stream
        await control.enter(context)

        for index, item in enumerate(control.items):
            if control.mid_stream_abort is not None and index >= control.mid_stream_after:
                await context.abort(control.mid_stream_abort, control.abort_details)
            yield item

    async def _collect(
        self, request_iterator: AsyncIterator[bytes], context: grpc.aio.ServicerContext[bytes, bytes]
    ) -> bytes:
        control = self.stream_unary
        await control.enter(context)

        chunks = [chunk async for chunk in request_iterator]
        control.received.extend(chunks)
        return b"".join(chunks)

    async def _chat(
        self, request_iterator: AsyncIterator[bytes], context: grpc.aio.ServicerContext[bytes, bytes]
    ) -> AsyncIterator[bytes]:
        control = self.stream_stream
        await control.enter(context)

        delivered = 0
        async for chunk in request_iterator:
            control.received.append(chunk)
            if control.mid_stream_abort is not None and delivered >= control.mid_stream_after:
                await context.abort(control.mid_stream_abort, control.abort_details)
            delivered += 1
            yield chunk.upper()


def free_port() -> int:
    """Reserve a loopback port, release it, and report which one it was.

    It is the address of a server that does not exist yet: the only way to make a client dial one.
    The window between releasing the port and binding it again is a race in principle, which is why
    this stays a loopback-only bench convenience.

    Returns:
        A port number nothing was listening on a moment ago.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@dataclass(slots=True)
class RunningServer:
    """A started grpc.aio server together with the controls of the service it serves."""

    server: grpc.aio.Server
    service: EchoService
    port: int

    @classmethod
    async def start(cls, port: int = 0) -> RunningServer:
        """Start a server serving the controllable echo service.

        Args:
            port: The port to bind, or 0 for an ephemeral one. Naming it is what lets a test start a
                server at an address a client is already dialing.

        Returns:
            The running server, ready to take calls.

        Raises:
            RuntimeError: If the port could not be bound, which would otherwise leave the test
                waiting on a server that never came up.
        """
        service = EchoService()
        server = grpc.aio.server()
        server.add_generic_rpc_handlers((service.generic_handler(),))
        bound = server.add_insecure_port(f"127.0.0.1:{port}")
        if bound == 0:
            raise RuntimeError(f"could not bind 127.0.0.1:{port}")

        await server.start()
        return cls(server=server, service=service, port=bound)

    async def stop(self) -> None:
        """Stop the server, releasing every hanging handler first so the stop waits on none."""
        self.service.release_all()
        await self.server.stop(grace=None)

    @property
    def target(self) -> str:
        """The address clients connect to."""
        return f"127.0.0.1:{self.port}"


class EchoStub:
    """Hand-written stub: what generated code looks like, minus protoc.

    Without serializers gRPC moves the payloads as raw bytes, which is all the bench needs. The
    channel is kept as an attribute so tests can compare the identity of pooled channels.
    """

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind one multicallable per RPC kind to `channel`."""
        self.channel = channel
        self.echo: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(ECHO)
        self.stream: grpc.aio.UnaryStreamMultiCallable[bytes, bytes] = channel.unary_stream(STREAM)
        # Only the two unary-request multicallables are generic in grpcio; the streaming ones are not.
        self.collect: grpc.aio.StreamUnaryMultiCallable = channel.stream_unary(COLLECT)
        self.chat: grpc.aio.StreamStreamMultiCallable = channel.stream_stream(CHAT)


def client_for_target(
    pool: ChannelPool,
    target: str,
    interceptors: list[grpc.aio.ClientInterceptor],
    connectivity: ConnectivityConfig | None = None,
) -> GrpcClient[EchoStub]:
    """Build a client aimed at one address, listening or not.

    Args:
        pool: The pool the client draws its channel from.
        target: The address to dial; nothing needs to be serving it yet.
        interceptors: The chain the client's channel is built with.
        connectivity: Keepalive and reconnect tuning for the channel, or None for gRPC's defaults.

    Returns:
        The client, not yet connected.
    """
    return GrpcClient(
        stub_class=EchoStub,
        config=GrpcClientConfig(target=target, insecure=True, connectivity=connectivity),
        pool=pool,
        interceptors=interceptors,
    )


def client_for(
    pool: ChannelPool,
    server: RunningServer,
    interceptors: list[grpc.aio.ClientInterceptor],
    connectivity: ConnectivityConfig | None = None,
) -> GrpcClient[EchoStub]:
    """Build a client aimed at one server, drawing its channel from the shared pool.

    Args:
        pool: The pool the client draws its channel from.
        server: The server the client is aimed at.
        interceptors: The chain the client's channel is built with.
        connectivity: Keepalive and reconnect tuning for the channel, or None for gRPC's defaults.

    Returns:
        The client, not yet connected.
    """
    return client_for_target(pool, server.target, interceptors, connectivity)


type ClientFactory = Callable[[list[grpc.aio.ClientInterceptor]], GrpcClient[EchoStub]]
type StartEchoServer = Callable[[int], Awaitable[RunningServer]]
