"""Shared doubles for the tests of the top-level modules: pool, client, factory, health, balancing.

The gRPC-level doubles these tests build on — channels, pools, interceptors, metrics recorders —
live in `tests.helpers`, because the interceptor suites speak the same vocabulary. What is here is
what only these modules need: settings objects the factory reads, ways to question a built chain,
and stand-ins for the pieces of the health protocol.
"""

from __future__ import annotations

import asyncio
import textwrap
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc.aio
from grpc_health.v1 import health_pb2

from grpc_client_kit.client import GrpcClient
from grpc_client_kit.interceptors import logical_interceptor
from grpc_client_kit.protocols import GrpcClientSettingsProtocol
from tests.helpers import make_interceptor

# The two serving statuses a health check can report to a client.
SERVING = health_pb2.HealthCheckResponse.SERVING
NOT_SERVING = health_pb2.HealthCheckResponse.NOT_SERVING

# A probe run in a subprocess, because grpc_health is already imported in the test process while the
# question is what happens on an install that never had it.
BARE_INSTALL_PROBE = textwrap.dedent(
    """
    import sys


    class Blocker:
        def find_spec(self, name, path=None, target=None):
            if name == "grpc_health" or name.startswith("grpc_health."):
                raise ImportError("simulated bare install")
            return None


    sys.meta_path.insert(0, Blocker())

    import grpc_client_kit

    assert grpc_client_kit.ChannelPool is not None

    try:
        grpc_client_kit.HealthChecker
    except ImportError as exc:
        assert "grpc-client-kit[health]" in str(exc), str(exc)
    else:
        raise AssertionError("HealthChecker must not resolve without the extra")

    # An ImportError rather than an AttributeError, on purpose: a missing extra is an install
    # problem and must say so. hasattr() therefore propagates it instead of answering False,
    # which is why the extra is probed for by name and not through the attribute.
    try:
        hasattr(grpc_client_kit, "HealthChecker")
    except ImportError as exc:
        assert "grpc-client-kit[health]" in str(exc), str(exc)
    else:
        raise AssertionError("hasattr must not swallow the missing extra")
    """
)

# The same question for the deadline extra, and it has to be asked in a subprocess for the same
# reason: `deadline_budget` is installed in the test environment, so the only way to see what an
# install without it does is to make the import fail before the kit ever runs.
NO_DEADLINE_EXTRA_PROBE = textwrap.dedent(
    """
    import sys


    class Blocker:
        def find_spec(self, name, path=None, target=None):
            if name == "deadline_budget" or name.startswith("deadline_budget."):
                raise ImportError("simulated install without the deadline extra")
            return None


    sys.meta_path.insert(0, Blocker())

    import grpc_client_kit
    from grpc_client_kit import DeadlineBudgetConfig, TimeoutConfig, build_interceptors, current_budget, use_budget
    from grpc_client_kit.deadline import HAS_DEADLINE_BUDGET
    from grpc_client_kit.interceptors import logical_interceptor
    from grpc_client_kit.interceptors.deadline import AsyncDeadlineBudgetInterceptor
    from grpc_client_kit.interceptors.timeout import AsyncTimeoutInterceptor

    assert HAS_DEADLINE_BUDGET is False, "the extra was supposed to be unavailable"

    # The ambient context still works; there is simply never a budget in it.
    assert current_budget() is None
    with use_budget(None):
        assert current_budget() is None

    # A chain that asks for propagation is built without it, rather than failing to build.
    chain = build_interceptors(timeout=TimeoutConfig(default=1.0), deadline_budget=DeadlineBudgetConfig())
    layers = [logical_interceptor(entry) for entry in chain]
    assert any(isinstance(layer, AsyncTimeoutInterceptor) for layer in layers), "the rest of the chain is missing"
    assert not any(isinstance(layer, AsyncDeadlineBudgetInterceptor) for layer in layers), "the layer must be skipped"
    """
)


# --------------------------------------------------------------------------------------------
# Settings, as the factory reads them.
# --------------------------------------------------------------------------------------------


class StubCircuitBreakerSettings:
    """Circuit breaker settings with real numbers, which a MagicMock cannot be compared against."""

    fail_threshold = 1
    recovery_timeout = 1.0
    half_open_max_calls = 1
    max_methods = 10


class StubClientSettings:
    """Client settings double exposing every attribute the factory reads, with inert defaults.

    The attributes are plain values rather than mock children, so that the factory can compare and
    forward them; each test overrides only the ones its scenario is about.
    """

    def __init__(self, **overrides: Any) -> None:
        """Start from a single insecure target with observability on, then apply the overrides."""
        self.target: str | None = "localhost:50051"
        self.targets: list[str] | None = None
        self.insecure = True
        self.metrics_enabled = True
        self.logging_enabled = True
        self.tracing_enabled = True
        self.sensitive_headers: set[str] | None = None
        self.pool: Any = None
        self.circuit_breaker: Any = None
        self.retry: Any = None
        self.timeout: Any = None
        self.balancer: Any = None
        self.health_checker: Any = None
        self.credentials: Any = None
        self.options: Any = None
        self.compression: Any = None
        self.metrics_registry: Any = None

        for name, value in overrides.items():
            setattr(self, name, value)


def make_protocol_settings() -> MagicMock:
    """Build a settings double from the public protocol, with a round-robin balancer configured."""
    settings = MagicMock(spec=GrpcClientSettingsProtocol)
    settings.pool.max_channels_per_target = 1
    settings.pool.idle_timeout = 300.0
    settings.metrics_registry = None
    settings.health_checker = None
    settings.targets = ["localhost:50051"]
    settings.target = "localhost:50051"
    settings.insecure = True
    settings.tracing_enabled = True
    settings.sensitive_headers = None
    settings.balancer.strategy = "round_robin"
    settings.balancer.weights = None
    return settings


def make_stub_class(name: str = "MyStub") -> MagicMock:
    """Build a stand-in for a generated gRPC stub class, which the factory names clients after."""
    return MagicMock(__name__=name)


# --------------------------------------------------------------------------------------------
# Questioning the chain a client was built with.
# --------------------------------------------------------------------------------------------


def chain_layers(client: GrpcClient, target: str) -> list[Any]:
    """Every chain entry built for `target`, as the logical interceptor it stands for, in order."""
    return [logical_interceptor(entry) for entry in client.interceptors_for(target)]


def layers_of(client: GrpcClient, target: str, interceptor_type: type) -> list[Any]:
    """The distinct layers of `interceptor_type` in the chain built for `target`.

    One layer reaches the channel as four adapters, so the chain is questioned through the logical
    interceptor each entry stands for and a layer counts as a single hit.
    """
    layers = dict.fromkeys(chain_layers(client, target))
    return [layer for layer in layers if isinstance(layer, interceptor_type)]


def recording_interceptor_factory(built: list[str]) -> Callable[[str], list[grpc.aio.ClientInterceptor]]:
    """An interceptor factory noting every target it was asked to build a fresh chain for."""

    def _build(target: str) -> list[grpc.aio.ClientInterceptor]:
        built.append(target)
        return [make_interceptor()]

    return _build


# --------------------------------------------------------------------------------------------
# The health protocol, and the checkers that speak it.
# --------------------------------------------------------------------------------------------


@contextmanager
def health_stub(*, response: Any = None, error: BaseException | None = None) -> Iterator[MagicMock]:
    """Patch the generated health stub so that its ``Check`` answers with `response` or raises."""
    with patch("grpc_health.v1.health_pb2_grpc.HealthStub") as stub_class:
        stub_class.return_value.Check = AsyncMock(return_value=response, side_effect=error)
        yield stub_class


def make_health_response(status: int) -> MagicMock:
    """Build a health check response double carrying the given serving status."""
    return MagicMock(status=status)


def make_health_checker() -> MagicMock:
    """Build a health checker double whose async lifecycle can be awaited and asserted on."""
    checker = MagicMock()
    checker.start = AsyncMock()
    checker.stop = AsyncMock()
    checker.wait_until_ready = AsyncMock(return_value=True)
    return checker


def make_health_probe(is_healthy: Callable[[str], bool]) -> MagicMock:
    """Build a health checker double answering ``is_healthy`` from the given predicate."""
    checker = MagicMock()
    checker.is_healthy = AsyncMock(side_effect=is_healthy)
    return checker


def never_completing_check(started: asyncio.Event) -> Callable[[str], Awaitable[bool]]:
    """A health check that reports it began and then never answers, as a hung backend would."""

    async def _check(target: str) -> bool:
        started.set()
        await asyncio.sleep(10.0)
        return True

    return _check


# --------------------------------------------------------------------------------------------
# Driving the asynchronous edges: a shutdown in flight, a loop that is never scheduled.
# --------------------------------------------------------------------------------------------


def blocking_close(started: asyncio.Event, finish: asyncio.Event) -> Callable[..., Awaitable[None]]:
    """A channel ``close`` that reports it began and then waits for the test to let it finish."""

    async def _close(grace: float | None = None) -> None:
        started.set()
        await finish.wait()

    return _close


class TaskCapture:
    """Stands in for ``asyncio.create_task``: keeps the coroutine instead of scheduling it.

    Attributes:
        task: The task double every capture hands back.
        coroutines: Every coroutine handed to it, so the test can close them itself.
    """

    def __init__(self) -> None:
        """Start with nothing captured."""
        self.task = MagicMock(spec=asyncio.Task)
        self.coroutines: list[Any] = []

    def __call__(self, coro: Any) -> MagicMock:
        self.coroutines.append(coro)
        return self.task

    def close(self) -> None:
        """Close every captured coroutine, so the loop does not warn about them."""
        for coro in self.coroutines:
            coro.close()
