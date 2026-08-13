"""The smallest real grpc-client-kit client -- server included.

This example shows how to:

1. Start a `grpc.aio` server on an ephemeral port (``127.0.0.1:0``), so the script never
   collides with whatever is already listening on this machine. The service is registered
   through generic bytes-in/bytes-out handlers, which is how this kit's own integration
   tests (`tests/integration/echo_bench.py`) run without protoc. Only the example needs
   this trick: a real client talks to a server that already exists.
2. Hand-write the stub protoc would have generated. A stub is nothing but a set of
   multicallables bound to one channel, which is exactly what generated code builds.
3. Open a `ChannelPool` and let a `GrpcClient` draw its channel from it. The client is a
   lightweight wrapper: it selects a target and asks the pool for a channel, and closing
   it closes nothing -- the pool owns the channels.
4. Make one unary call and one server-streaming call.
5. See what the pool is actually for: two stubs built from the same channel identity get
   the *same* channel back, so the second one costs no connection at all.

Run with: `python examples/minimal_client.py`. It prints the bound port, both responses
and the channel identity check, then stops the server and exits 0.
"""

# ruff: noqa: T201 -- an example is a program whose output IS the documentation.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import grpc
import grpc.aio

from grpc_client_kit import ChannelPool, GrpcClient, GrpcClientConfig

SERVICE = "example.Echo"
ECHO = f"/{SERVICE}/Echo"
COUNT_UP = f"/{SERVICE}/CountUp"

# Stopping with a short grace lets the open connections wind down in order. `grace=None`
# tears them down under the client, and grpc's C core logs the GOAWAY it was handed
# mid-flight -- noise that has nothing to do with the example.
SHUTDOWN_GRACE = 0.5


async def echo(request: bytes, _context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
    """Unary handler: bytes in, bytes out."""
    return b"echo:" + request


async def count_up(request: bytes, _context: grpc.aio.ServicerContext[bytes, bytes]) -> AsyncIterator[bytes]:
    """Server-streaming handler: one item per number up to the requested count."""
    for number in range(1, int(request) + 1):
        yield str(number).encode()


def build_generic_handler() -> grpc.GenericRpcHandler:
    """Expose the two methods without protoc.

    A real service registers generated code instead::

        add_EchoServicer_to_server(EchoServicer(), server)
    """
    return grpc.method_handlers_generic_handler(
        SERVICE,
        {
            "Echo": grpc.unary_unary_rpc_method_handler(echo),
            "CountUp": grpc.unary_stream_rpc_method_handler(count_up),
        },
    )


async def start_server() -> tuple[grpc.aio.Server, str]:
    """Start the demo server on an ephemeral port.

    Returns:
        The running server and the address clients connect to.
    """
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((build_generic_handler(),))
    # Port 0: the OS picks a free port and `add_insecure_port` reports which one.
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


class EchoStub:
    """What protoc would have generated: multicallables bound to one channel.

    Without serializers gRPC moves payloads as raw bytes, which is all this example needs.
    The channel is kept as an attribute so the pooling check below can compare identities.
    """

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind one multicallable per method to `channel`."""
        self.channel = channel
        self.echo: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(ECHO)
        self.count_up: grpc.aio.UnaryStreamMultiCallable[bytes, bytes] = channel.unary_stream(COUNT_UP)


async def main() -> None:
    server, target = await start_server()
    print(f"[server] listening on {target}")

    try:
        async with ChannelPool() as pool:
            client = GrpcClient(
                EchoStub,
                config=GrpcClientConfig(target=target, insecure=True),
                pool=pool,
            )

            print("\n--- unary call ---")
            async with client as stub:
                response = await stub.echo(b"world")
                print(f"[client] Echo -> {response!r}")

            print("\n--- server-streaming call ---")
            async with client as stub:
                items = [item async for item in stub.count_up(b"3")]
                print(f"[client] CountUp -> {items!r}")

            print("\n--- what the pool is for ---")
            # Entering the client twice asks the pool for a channel twice. Both requests
            # carry the same channel identity -- target, security, options, compression and
            # (here, empty) interceptor chain -- so the pool hands back the same channel.
            async with client as first, client as second:
                print(f"[pool] both stubs share one channel: {first.channel is second.channel}")
    finally:
        # The pool closed its channels on the way out of its `async with`; the server is
        # ours to stop, and stopping it is what lets the script exit instead of hanging.
        await server.stop(grace=SHUTDOWN_GRACE)

    print("\n[server] stopped")


if __name__ == "__main__":
    asyncio.run(main())
