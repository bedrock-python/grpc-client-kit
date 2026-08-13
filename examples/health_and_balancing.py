"""Two backends, one of them sick: health checking decides where the traffic goes.

Requires the `health` extra (``pip install "grpc-client-kit[health]"``), which supplies
`grpcio-health-checking` and with it `HealthChecker`.

Two servers are started on ephemeral ports. Both serve the same application method and the
real `grpc.health.v1` protocol, and each one's serving status is a knob this script turns.
A `HealthChecker` probes them in the background, a round-robin balancer asks it before
every routing decision, and a `GrpcClient` built with that balancer -- and no target of its
own -- follows wherever health says it is safe to go.

This example shows how to:

1. Wire the three objects together: the pool holds the channels, the checker publishes its
   verdicts into the pool (so pooled channels get flagged) *and* answers the balancer (so
   dead targets are skipped), and the client asks the balancer for a target per call.
2. Wait for the first pass with `wait_until_ready()`. Before it completes every target is
   reported unhealthy on purpose: an address nobody has probed yet must not get traffic.
3. Watch routing follow the verdicts as the servers' statuses are flipped.
4. See `NoHealthyTargetsError` when the last healthy backend goes away -- the balancer
   refuses to guess rather than sending the call somewhere known to be broken.

Probes travel on the checker's own channels, never on the pooled application ones: that is
why monitoring cannot trip the client's circuit breakers, and why it survives the pool
evicting an idle channel. The per-server counters printed at the end show both kinds of
traffic side by side.

Run with: `python examples/health_and_balancing.py`. It takes a few seconds -- the check
loop re-evaluates its schedule once per second -- and exits 0.
"""

# ruff: noqa: T201 -- an example is a program whose output IS the documentation.

from __future__ import annotations

import asyncio
import time

import grpc
import grpc.aio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from grpc_client_kit import (
    ChannelPool,
    GrpcClient,
    GrpcClientConfig,
    HealthChecker,
    NoHealthyTargetsError,
    create_balancer,
)

SERVICE = "example.Echo"
ECHO = f"/{SERVICE}/Echo"

SERVING = health_pb2.HealthCheckResponse.SERVING
NOT_SERVING = health_pb2.HealthCheckResponse.NOT_SERVING

# The check loop wakes up once a second, so a flipped status becomes visible within roughly
# that much however small `check_interval` is.
VERDICT_TIMEOUT = 10.0
POLL_INTERVAL = 0.05
CALLS_PER_SCENE = 4
SHUTDOWN_GRACE = 0.5


class ControllableHealth(health_pb2_grpc.HealthServicer):
    """The real health servicer, with a status this script flips and a probe counter."""

    def __init__(self, status: health_pb2.HealthCheckResponse.ServingStatus) -> None:
        """Start out answering `status`."""
        self.status = status
        self.probes = 0

    async def Check(
        self,
        request: health_pb2.HealthCheckRequest,
        context: grpc.aio.ServicerContext[health_pb2.HealthCheckRequest, health_pb2.HealthCheckResponse],
    ) -> health_pb2.HealthCheckResponse:
        """Answer one probe with the status currently configured."""
        self.probes += 1
        return health_pb2.HealthCheckResponse(status=self.status)


class DemoServer:
    """One backend: the application method, the health protocol, and counters for both."""

    def __init__(self, name: str, status: health_pb2.HealthCheckResponse.ServingStatus) -> None:
        """Prepare a backend called `name` that starts out reporting `status`."""
        self.name = name
        self.health = ControllableHealth(status)
        self.calls = 0
        self._server = grpc.aio.server()
        self._port = 0

    async def start(self) -> None:
        """Bind an ephemeral port and start serving."""
        handler = grpc.method_handlers_generic_handler(
            SERVICE,
            {"Echo": grpc.unary_unary_rpc_method_handler(self._echo)},
        )
        self._server.add_generic_rpc_handlers((handler,))
        health_pb2_grpc.add_HealthServicer_to_server(self.health, self._server)
        self._port = self._server.add_insecure_port("127.0.0.1:0")
        await self._server.start()

    async def stop(self) -> None:
        """Stop serving."""
        await self._server.stop(grace=SHUTDOWN_GRACE)

    @property
    def target(self) -> str:
        """The address clients and the checker connect to."""
        return f"127.0.0.1:{self._port}"

    def set_status(self, status: health_pb2.HealthCheckResponse.ServingStatus) -> None:
        """Change what every later probe is answered with."""
        self.health.status = status

    async def _echo(self, request: bytes, _context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """Application handler: answers with the name of the backend that served the call."""
        self.calls += 1
        return f"served by {self.name}".encode()


class EchoStub:
    """What protoc would have generated: one multicallable bound to one channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind the Echo multicallable to `channel`."""
        self.echo: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(ECHO)


async def wait_for_verdict(checker: HealthChecker, target: str, expected: bool) -> None:
    """Block until the checker's verdict on `target` is `expected`.

    Args:
        checker: The running checker to poll.
        target: The address whose verdict is awaited.
        expected: The verdict to wait for.

    Raises:
        TimeoutError: If the verdict did not arrive within `VERDICT_TIMEOUT`.
    """
    deadline = time.monotonic() + VERDICT_TIMEOUT
    while await checker.is_healthy(target) is not expected:
        if time.monotonic() >= deadline:
            wanted = "healthy" if expected else "unhealthy"
            raise TimeoutError(f"{target} was not reported {wanted} within {VERDICT_TIMEOUT:.0f}s")
        await asyncio.sleep(POLL_INTERVAL)


async def print_verdicts(checker: HealthChecker, servers: list[DemoServer]) -> None:
    """Print what the checker currently believes about each backend."""
    for server in servers:
        verdict = "healthy" if await checker.is_healthy(server.target) else "unhealthy"
        print(f"[health] {server.name} ({server.target}) is {verdict}")


async def route_calls(client: GrpcClient[EchoStub], count: int) -> None:
    """Make `count` calls through the balancer and print which backend answered each one."""
    for _ in range(count):
        try:
            async with client as stub:
                response = await stub.echo(b"ping")
        except NoHealthyTargetsError as error:
            # Raised while selecting a target, before any channel is touched.
            print(f"[client] no call was made: {error}")
            return

        print(f"[client] {response.decode()}")


async def main() -> None:
    # B is sick from the start: it is reachable and answers probes, it just says it is not
    # serving -- the case a plain connection check would happily route into.
    servers = [DemoServer("A", SERVING), DemoServer("B", NOT_SERVING)]
    for server in servers:
        await server.start()
        print(f"[server] {server.name} listening on {server.target}")

    targets = [server.target for server in servers]

    try:
        async with ChannelPool() as pool:
            # The pool has to exist first: the checker publishes each verdict into it, which
            # is what flags the pooled channels of a target that went bad.
            # insecure=True explicitly: the kit defaults to TLS, and these demo servers speak
            # plaintext — a probe negotiating TLS against them would never report healthy.
            checker = HealthChecker(check_interval=0.1, timeout=1.0, insecure=True, pool=pool)
            # The same checker answers the balancer, so routing and pooling agree on who is
            # alive instead of each keeping its own opinion.
            balancer = create_balancer(targets=targets, health_checker=checker)
            # No `target` in the config: the balancer supplies one per call.
            client = GrpcClient(EchoStub, GrpcClientConfig(insecure=True), pool, balancer=balancer)

            await checker.start(targets)
            try:
                print("\n--- waiting for the first check pass ---")
                # Until this returns, every target is reported unhealthy on purpose.
                ready = await checker.wait_until_ready(timeout=VERDICT_TIMEOUT)
                print(f"[health] first pass complete: {ready}")
                await print_verdicts(checker, servers)

                print(f"\n--- {CALLS_PER_SCENE} calls while only A is serving ---")
                await route_calls(client, CALLS_PER_SCENE)

                print("\n--- A goes down, B recovers ---")
                servers[0].set_status(NOT_SERVING)
                servers[1].set_status(SERVING)
                await wait_for_verdict(checker, servers[0].target, expected=False)
                await wait_for_verdict(checker, servers[1].target, expected=True)
                await print_verdicts(checker, servers)

                print(f"\n--- {CALLS_PER_SCENE} calls after the flip ---")
                await route_calls(client, CALLS_PER_SCENE)

                print("\n--- both backends go down ---")
                servers[1].set_status(NOT_SERVING)
                await wait_for_verdict(checker, servers[1].target, expected=False)
                await route_calls(client, 1)
            finally:
                await checker.stop()
    finally:
        for server in servers:
            await server.stop()

    print("\n--- traffic split ---")
    for server in servers:
        print(f"[server] {server.name}: {server.calls} application calls, {server.health.probes} health probes")
    print("[note]   the probes ran on the checker's own channels, not on the pooled ones, so")
    print("[note]   monitoring can neither trip a client breaker nor be evicted as idle")


if __name__ == "__main__":
    asyncio.run(main())
