"""One settings object drives the whole client -- and gives every target its own breaker.

`GrpcClientFactory` is the production entry point: it reads a settings object, builds the
pool, the balancer and the interceptor chain from it, and hands out clients that share all
three. The settings object is not a kit class -- it is *your* configuration model. The kit
only requires the fields named by `GrpcClientSettingsProtocol` and friends, so a pydantic
settings model, a dataclass (as below) or anything else with those attributes will do.

The reason this example runs against two backends is the second thing the factory does for
you, which is easy to miss and expensive to get wrong. A circuit breaker keeps its state in
the interceptor instance, and an instance belongs to one channel, hence to one target. The
factory therefore hands `GrpcClient` an **interceptor factory** and builds a chain per
target, so each backend gets a breaker of its own.

Passing a ready `interceptors=` list instead shares one breaker across every target of the
client, and a breaker counts *consecutive* failures. A healthy peer's answers then keep
resetting the counter the broken peer is filling: the circuit never opens, and the client
goes on spending calls on a backend it has already been told is down. The two scenes below
send identical traffic through both wirings, and the servers count how many calls each of
them actually received.

Run with: `python examples/factory_setup.py`. Backend A is healthy, backend B fails every
call; the script prints the outcome of each call and the per-backend totals, then exits 0.
"""

# ruff: noqa: T201 -- an example is a program whose output IS the documentation.

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass, field

import grpc
import grpc.aio

from grpc_client_kit import (
    ChannelPool,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    GrpcClient,
    GrpcClientConfig,
    GrpcClientFactory,
    HealthCheckerSettingsProtocol,
    RetrySettingsProtocol,
    build_interceptors,
    create_balancer,
)

SERVICE = "example.Echo"
ECHO = f"/{SERVICE}/Echo"

CALLS_PER_SCENE = 8
FAIL_THRESHOLD = 2
SHUTDOWN_GRACE = 0.5


@dataclass(frozen=True, slots=True)
class PoolSettings:
    """Satisfies `ChannelPoolSettingsProtocol`."""

    max_channels_per_target: int = 1
    idle_timeout: float = 300.0


@dataclass(frozen=True, slots=True)
class CircuitBreakerSettings:
    """Satisfies `CircuitBreakerSettingsProtocol`."""

    fail_threshold: int = FAIL_THRESHOLD
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1


@dataclass(frozen=True, slots=True)
class TimeoutSettings:
    """Satisfies `TimeoutSettingsProtocol`: the budget of a whole call, retries included."""

    default: float = 5.0


@dataclass(frozen=True, slots=True)
class BalancerSettings:
    """Satisfies `LoadBalancerSettingsProtocol`."""

    strategy: str = "round_robin"
    weights: dict[str, float] | None = None


@dataclass(frozen=True, slots=True)
class Settings:
    """Satisfies `GrpcClientSettingsProtocol` -- the shape the factory reads.

    Every field here is one the factory looks at. Optional blocks are `None` when the
    feature is not wanted: no `retry` block means no retry layer in the chain at all, which
    keeps the attempt counting in this example honest.
    """

    targets: list[str]
    target: str | None = None
    insecure: bool = True
    tracing_enabled: bool = False
    metrics_enabled: bool = False
    # Logging is off only to keep this script's output about routing; see observability.py.
    logging_enabled: bool = False
    pool: PoolSettings = field(default_factory=PoolSettings)
    circuit_breaker: CircuitBreakerSettings = field(default_factory=CircuitBreakerSettings)
    retry: RetrySettingsProtocol | None = None
    timeout: TimeoutSettings = field(default_factory=TimeoutSettings)
    balancer: BalancerSettings = field(default_factory=BalancerSettings)
    health_checker: HealthCheckerSettingsProtocol | None = None


class DemoServer:
    """One backend: either healthy or failing every call, and counting what reaches it."""

    def __init__(self, name: str, healthy: bool) -> None:
        """Prepare a backend called `name` that answers or aborts, per `healthy`."""
        self.name = name
        self.healthy = healthy
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
        self._port = self._server.add_insecure_port("127.0.0.1:0")
        await self._server.start()

    async def stop(self) -> None:
        """Stop serving."""
        await self._server.stop(grace=SHUTDOWN_GRACE)

    @property
    def target(self) -> str:
        """The address clients connect to."""
        return f"127.0.0.1:{self._port}"

    async def _echo(self, _request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
        """Answer with this backend's name, or abort if this backend is the broken one."""
        self.calls += 1
        if not self.healthy:
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"backend {self.name} is broken")
        return f"served by {self.name}".encode()


class EchoStub:
    """What protoc would have generated: one multicallable bound to one channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind the Echo multicallable to `channel`."""
        self.echo: grpc.aio.UnaryUnaryMultiCallable[bytes, bytes] = channel.unary_unary(ECHO)


async def run_traffic(client: GrpcClient[EchoStub], servers: list[DemoServer]) -> None:
    """Send `CALLS_PER_SCENE` calls through the balancer and print how each one ended."""
    for server in servers:
        server.calls = 0

    for number in range(1, CALLS_PER_SCENE + 1):
        try:
            async with client as stub:
                response = await stub.echo(b"ping")
        except CircuitBreakerOpenError:
            # Never reached the network: the breaker refused the call before it was created.
            print(f"[client] call {number}: refused by the circuit breaker")
        except grpc.aio.AioRpcError as error:
            print(f"[client] call {number}: {error.code().name}: {error.details()}")
        else:
            print(f"[client] call {number}: {response.decode()}")

    for server in servers:
        print(f"[server] {server.name} handled {server.calls} of the {CALLS_PER_SCENE} calls")


async def scene_factory(settings: Settings, servers: list[DemoServer]) -> None:
    """The factory's wiring: a chain, and therefore a breaker, per target."""
    print("\n--- 1. clients from the factory: one chain, and one breaker, per target ---")

    # The factory owns the pool it creates here, and closes it on the way out.
    async with GrpcClientFactory(settings=settings) as factory:
        client = factory.create_client(EchoStub, service_name="ledger")
        await run_traffic(client, servers)

    print("[note]    B's own circuit opened after two consecutive failures and stopped the")
    print("[note]    traffic to it; A never noticed, because A's breaker is a different one")


async def scene_shared_chain(settings: Settings, servers: list[DemoServer]) -> None:
    """The same traffic through one chain -- and one breaker -- shared by both targets."""
    print("\n--- 2. the same traffic, one chain shared by both targets ---")

    chain = build_interceptors(circuit_breaker=CircuitBreakerConfig(fail_threshold=FAIL_THRESHOLD))
    balancer = create_balancer(targets=settings.targets)

    async with ChannelPool() as pool:
        # `interceptors=` is a ready chain: every target of this client gets the same
        # objects, so both share one circuit breaker instance.
        client = GrpcClient(EchoStub, GrpcClientConfig(insecure=True), pool, balancer=balancer, interceptors=chain)
        await run_traffic(client, servers)

    print("[note]    the circuit never opened: every answer from A reset the counter B's")
    print("[note]    failures were raising, so half of the calls kept going into a backend")
    print("[note]    already known to be broken -- twice as many as in scene 1")


async def main() -> None:
    # The kit reports state changes through the standard logging module; sending them to
    # stdout keeps them in order with this script's own output.
    logging.basicConfig(level=logging.WARNING, stream=sys.stdout, format="[kit-log] %(levelname)s %(message)s")

    servers = [DemoServer("A", healthy=True), DemoServer("B", healthy=False)]
    for server in servers:
        await server.start()
        print(f"[server] {server.name} listening on {server.target} (healthy={server.healthy})")

    settings = Settings(targets=[server.target for server in servers])

    try:
        await scene_factory(settings, servers)
        await scene_shared_chain(settings, servers)
    finally:
        for server in servers:
            await server.stop()

    print("\n[server] stopped")


if __name__ == "__main__":
    asyncio.run(main())
