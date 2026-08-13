"""The health bench: a real ``grpc_health.v1`` service, and the objects assembled around it.

Health is the one subsystem where a mock proves nothing: what matters is whether an *unprobed*
address is treated as dead, and a mock answers before the question can even be asked. So every
target here is a started grpc.aio server on an ephemeral port serving the real health protocol, and
the checker's verdicts, its backoff schedule and the balancers' routing decisions are produced by
the wire.

The check loop re-evaluates its schedule once per second in production, which is also the floor for
``check_interval``. `LOOP_SLEEP` is what the ``fast_health_loop`` fixture patches that granularity
down to, so a test can watch a schedule play out instead of waiting on it.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import grpc
import grpc.aio
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from grpc_client_kit import (
    ChannelPool,
    LoadBalancer,
    LoadBalancerConfig,
    LoadBalancingStrategy,
    create_balancer,
)
from grpc_client_kit.health import HealthChecker
from grpc_client_kit.protocols import HealthCheckerProtocol

from .waiting import POLL_INTERVAL, WAIT_TIMEOUT

SERVING = health_pb2.HealthCheckResponse.SERVING
NOT_SERVING = health_pb2.HealthCheckResponse.NOT_SERVING

# Name of the service the *application* calls, as opposed to the empty name the checker probes with.
# It is what separates a client's traffic from the monitoring traffic on the same server.
APP_SERVICE = "app"

# Gap the checker schedules between two passes over a target. Only distinguishable from a backoff
# because `LOOP_SLEEP` below takes the loop's granularity down with it.
CHECK_INTERVAL = 0.1

# Deadline of a single probe, deliberately far out of reach: the tests that hold a probe inside the
# handler are about a verdict that has not arrived, not about one the checker gave up waiting for.
PROBE_TIMEOUT = 5.0

# Granularity the check loop re-evaluates its schedule with; 10ms keeps a 100ms backoff measurable.
LOOP_SLEEP = 0.01

# Health has to veto a target whichever way the balancer would otherwise have picked it.
EVERY_STRATEGY = [pytest.param(strategy, id=strategy.value) for strategy in LoadBalancingStrategy]


class ControllableHealth(health_pb2_grpc.HealthServicer):
    """The real health servicer, with a status the test flips and a gate the test can hold shut.

    Attributes:
        status: What every later probe is answered with.
        probes: Arrival times of the health probes, on the monotonic clock.
        calls: Every requested service name, in arrival order.
    """

    def __init__(self) -> None:
        """Start out serving, answering immediately."""
        self.status: health_pb2.HealthCheckResponse.ServingStatus = SERVING
        self.probes: list[float] = []
        self.calls: list[str] = []
        self._gate = asyncio.Event()
        self._gate.set()

    @property
    def probe_count(self) -> int:
        """How many health probes have arrived so far."""
        return len(self.probes)

    def set_status(self, status: health_pb2.HealthCheckResponse.ServingStatus) -> None:
        """Set the status every later probe is answered with."""
        self.status = status

    def block(self) -> None:
        """Hold every later call inside the handler: a reachable server that does not answer."""
        self._gate.clear()

    def unblock(self) -> None:
        """Let the held calls answer."""
        self._gate.set()

    async def Check(
        self,
        request: health_pb2.HealthCheckRequest,
        context: grpc.aio.ServicerContext[health_pb2.HealthCheckRequest, health_pb2.HealthCheckResponse],
    ) -> health_pb2.HealthCheckResponse:
        """Answer a health check, recording it first so a blocked call still counts as arrived."""
        self.calls.append(request.service)
        if not request.service:
            self.probes.append(time.monotonic())

        await self._gate.wait()
        return health_pb2.HealthCheckResponse(status=self.status)


@dataclass(slots=True)
class HealthServer:
    """A started grpc.aio server serving the health protocol, and the knobs of its servicer."""

    server: grpc.aio.Server
    servicer: ControllableHealth
    port: int

    @classmethod
    async def start(cls) -> HealthServer:
        """Start a server on an ephemeral port, serving the controllable health service.

        Returns:
            The running server, already answering probes.
        """
        servicer = ControllableHealth()
        server = grpc.aio.server()
        health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        return cls(server=server, servicer=servicer, port=port)

    async def stop(self) -> None:
        """Stop the server, opening the gate first so no held handler outlives it."""
        # A handler still waiting on its gate would otherwise hold up the graceless stop.
        self.servicer.unblock()
        await self.server.stop(grace=None)

    @property
    def target(self) -> str:
        """The address clients and the checker connect to."""
        return f"127.0.0.1:{self.port}"


@dataclass(slots=True)
class StatusRecorder:
    """Every status change the checker announced, in order.

    Attributes:
        changes: One entry per `on_status_change` call, which must mean one entry per change.
    """

    changes: list[tuple[str, bool]] = field(default_factory=list)

    async def __call__(self, target: str, is_healthy: bool) -> None:
        """Record one announced change."""
        self.changes.append((target, is_healthy))


@dataclass(slots=True)
class PoolPublisher:
    """Forwards status changes into a pool that does not exist yet when the checker is built.

    `HealthChecker(pool=...)` and `ChannelPool(health_checker=...)` each want the other at
    construction time, and neither offers a setter, so the two can only be joined through a
    callback whose target is filled in once both exist.

    Attributes:
        pool: The pool to publish into, assigned after both objects are built.
        updates: Every forwarded update, so a test can wait for the pool to have been told.
    """

    pool: ChannelPool | None = None
    updates: list[tuple[str, bool]] = field(default_factory=list)

    async def __call__(self, target: str, is_healthy: bool) -> None:
        """Forward one status change to the pool."""
        assert self.pool is not None, "wire the pool into the publisher before starting the checker"

        await self.pool.update_channel_health(target, is_healthy)
        self.updates.append((target, is_healthy))


def balancer_for(
    strategy: LoadBalancingStrategy,
    targets: list[str],
    *,
    heaviest: str,
    checker: HealthCheckerProtocol,
) -> LoadBalancer:
    """Build a balancer of `strategy` that would prefer `heaviest` if health did not veto it.

    Args:
        strategy: The strategy to build.
        targets: The addresses to balance over.
        heaviest: The address the weighted strategy is told to favour.
        checker: The health checker filtering the targets.

    Returns:
        The configured balancer.
    """
    weights = dict.fromkeys(targets, 1.0)
    weights[heaviest] = 100.0
    return create_balancer(targets, LoadBalancerConfig(strategy=strategy, weights=weights), checker)


def probe_gaps(servicer: ControllableHealth) -> list[float]:
    """Seconds between consecutive probes, as the server timed their arrival."""
    return [later - earlier for earlier, later in itertools.pairwise(servicer.probes)]


async def start_checker(checker: HealthChecker, targets: list[str]) -> None:
    """Start the check loop and wait for its first pass, so later assertions rest on evidence.

    Args:
        checker: The checker to start.
        targets: The addresses to monitor.
    """
    await checker.start(targets)
    assert await checker.wait_until_ready(timeout=WAIT_TIMEOUT) is True


async def until_health(checker: HealthChecker, target: str, expected: bool, *, timeout: float = WAIT_TIMEOUT) -> None:
    """Wait until the checker's verdict on `target` is `expected`.

    Args:
        checker: The running checker to poll.
        target: The target address whose verdict is awaited.
        expected: The verdict to wait for.
        timeout: Upper bound in seconds before the wait is declared failed.

    Raises:
        AssertionError: If the verdict did not arrive within `timeout`.
    """
    deadline = time.monotonic() + timeout
    while await checker.is_healthy(target) is not expected:
        if time.monotonic() >= deadline:
            verdict = "healthy" if expected else "unhealthy"
            raise AssertionError(f"'{target}' was not reported {verdict} within {timeout}s")
        await asyncio.sleep(POLL_INTERVAL)


type StartHealthServer = Callable[[], Awaitable[HealthServer]]
type MakeChecker = Callable[..., HealthChecker]
type MakePool = Callable[..., ChannelPool]
