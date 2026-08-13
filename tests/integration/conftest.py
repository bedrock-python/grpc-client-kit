"""Fixtures for the integration bench: real servers, a real pool, real channels.

Only the fixtures live here; everything they assemble lives next door — the echo bench in
`echo_bench`, the health bench in `health_bench`, the factory's settings in `factory_bench`, the
interceptor chains in `chains`, the collector they report into in `recording`, and the helpers that
drive and await calls in `calls` and `waiting`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import grpc.aio
import pytest

from grpc_client_kit import ChannelPool, GrpcClient, GrpcClientFactory
from grpc_client_kit import health as health_module
from grpc_client_kit.health import HealthChecker
from grpc_client_kit.protocols import HealthCheckerProtocol, HealthStatusCallbackProtocol

from .echo_bench import ClientFactory, EchoStub, RunningServer, StartEchoServer, client_for
from .factory_bench import FactorySettings, MakeFactory
from .health_bench import (
    CHECK_INTERVAL,
    LOOP_SLEEP,
    PROBE_TIMEOUT,
    HealthServer,
    MakeChecker,
    MakePool,
    StartHealthServer,
)
from .recording import RecordingMetrics


@pytest.fixture
async def echo_server() -> AsyncIterator[RunningServer]:
    """A started grpc.aio server on an ephemeral port, serving the controllable echo service."""
    server = await RunningServer.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.fixture
async def spare_echo_server() -> AsyncIterator[RunningServer]:
    """A second live server, for the tests that need one failing and one healthy target."""
    server = await RunningServer.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.fixture
async def start_echo_server() -> AsyncIterator[StartEchoServer]:
    """Factory starting echo servers on a named port, all stopped at teardown.

    The port is an argument because the tests that need this fixture are the ones about a client
    dialing an address before anything serves it.
    """
    started: list[RunningServer] = []

    async def _start(port: int = 0) -> RunningServer:
        server = await RunningServer.start(port)
        started.append(server)
        return server

    try:
        yield _start
    finally:
        for server in started:
            await server.stop()


@pytest.fixture
def metrics() -> RecordingMetrics:
    """The collector both the client interceptors and the pool report into."""
    return RecordingMetrics()


@pytest.fixture
async def pool(metrics: RecordingMetrics) -> AsyncIterator[ChannelPool]:
    """A real channel pool; idle eviction is off so no background task outlives the test."""
    channel_pool = ChannelPool(idle_timeout=0, metrics=metrics)
    try:
        yield channel_pool
    finally:
        await channel_pool.close_all()


@pytest.fixture
def make_client(pool: ChannelPool, echo_server: RunningServer) -> ClientFactory:
    """Factory for clients that draw real channels from the shared pool."""

    def _make(interceptors: list[grpc.aio.ClientInterceptor]) -> GrpcClient[EchoStub]:
        return client_for(pool, echo_server, interceptors)

    return _make


@pytest.fixture
def fast_health_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-evaluate the check schedule every 10ms instead of every second.

    The loop sleeps a flat second between passes, which is also the floor for `check_interval`: a
    100ms interval and a 100ms backoff are indistinguishable at that granularity, and every status
    flip in the health suite would cost a second of wall clock.
    """
    monkeypatch.setattr(health_module, "DEFAULT_LOOP_SLEEP", LOOP_SLEEP)


@pytest.fixture
async def start_health_server() -> AsyncIterator[StartHealthServer]:
    """Factory starting health servers on ephemeral ports, all stopped at teardown."""
    started: list[HealthServer] = []

    async def _start() -> HealthServer:
        server = await HealthServer.start()
        started.append(server)
        return server

    try:
        yield _start
    finally:
        for server in started:
            await server.stop()


@pytest.fixture
async def health_server(start_health_server: StartHealthServer) -> HealthServer:
    """One started health server, serving."""
    return await start_health_server()


@pytest.fixture
async def make_checker() -> AsyncIterator[MakeChecker]:
    """Factory for health checkers; every one of them is stopped at teardown."""
    created: list[HealthChecker] = []

    def _make(
        *,
        check_interval: float = CHECK_INTERVAL,
        timeout: float = PROBE_TIMEOUT,
        on_status_change: HealthStatusCallbackProtocol | None = None,
        pool: ChannelPool | None = None,
    ) -> HealthChecker:
        checker = HealthChecker(
            check_interval=check_interval,
            timeout=timeout,
            on_status_change=on_status_change,
            # The bench servers speak plaintext; the kit itself now defaults to TLS.
            insecure=True,
            pool=pool,
        )
        created.append(checker)
        return checker

    try:
        yield _make
    finally:
        for checker in created:
            await checker.stop()


@pytest.fixture
async def make_pool() -> AsyncIterator[MakePool]:
    """Factory for channel pools without idle eviction; every one of them is drained at teardown."""
    created: list[ChannelPool] = []

    def _make(
        *,
        health_checker: HealthCheckerProtocol | None = None,
        max_channels_per_target: int = 1,
    ) -> ChannelPool:
        channel_pool = ChannelPool(
            max_channels_per_target=max_channels_per_target,
            idle_timeout=0,
            health_checker=health_checker,
        )
        created.append(channel_pool)
        return channel_pool

    try:
        yield _make
    finally:
        for channel_pool in created:
            await channel_pool.close_all()


@pytest.fixture
async def make_factory() -> AsyncIterator[MakeFactory]:
    """Factory for client factories; every one of them is closed at teardown."""
    created: list[GrpcClientFactory] = []

    def _make(settings: FactorySettings) -> GrpcClientFactory:
        factory = GrpcClientFactory(settings=settings)
        created.append(factory)
        return factory

    try:
        yield _make
    finally:
        for factory in created:
            await factory.close()
