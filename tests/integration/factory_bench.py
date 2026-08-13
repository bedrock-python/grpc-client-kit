"""The settings a `GrpcClientFactory` is built from, and how to connect a client it made.

The factory reads its configuration off a plain settings object, so the blocks below are the whole
contract: what the tests hand over is the shape a service's own settings would have, with health
checking and balancing turned on and everything the suite is not about turned off.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

from grpc_client_kit import GrpcClient, GrpcClientFactory, LoadBalancingStrategy, NoHealthyTargetsError

from .health_bench import CHECK_INTERVAL, PROBE_TIMEOUT
from .waiting import POLL_INTERVAL, WAIT_TIMEOUT


@dataclass(frozen=True, slots=True)
class HealthSettings:
    """The health block of the settings a factory reads."""

    check_interval: float = CHECK_INTERVAL
    timeout: float = PROBE_TIMEOUT


@dataclass(frozen=True, slots=True)
class BalancerSettings:
    """The load balancing block of the settings a factory reads."""

    strategy: str = LoadBalancingStrategy.ROUND_ROBIN.value
    weights: dict[str, float] | None = None


@dataclass(frozen=True, slots=True)
class PoolSettings:
    """The pool block of the settings a factory reads; idle eviction is off in tests."""

    max_channels_per_target: int = 1
    idle_timeout: float = 0.0


@dataclass(frozen=True, slots=True)
class FactorySettings:
    """The settings object `GrpcClientFactory` builds a health-checked, balanced client from."""

    targets: list[str]
    health_checker: HealthSettings = HealthSettings()
    balancer: BalancerSettings = BalancerSettings()
    pool: PoolSettings = PoolSettings()
    target: str | None = None
    insecure: bool = True
    tracing_enabled: bool = False
    metrics_enabled: bool = False
    logging_enabled: bool = False
    circuit_breaker: None = None
    retry: None = None
    timeout: None = None


def factory_settings(targets: list[str], *, strategy: LoadBalancingStrategy | None = None) -> FactorySettings:
    """Build settings for a factory that health-checks and balances over `targets`."""
    balancer = BalancerSettings(strategy=(strategy or LoadBalancingStrategy.ROUND_ROBIN).value)
    return FactorySettings(targets=list(targets), balancer=balancer)


async def connect_when_ready[T](client: GrpcClient[T], *, timeout: float = WAIT_TIMEOUT) -> T:
    """Connect as soon as the first health pass lets the balancer route.

    Entering a factory starts the check loop but does not await its first pass, and the factory
    keeps its checker private, so the first connect races the first probe and is refused until
    evidence arrives.

    Args:
        client: The balanced client to connect.
        timeout: Upper bound in seconds before the refusal is treated as final.

    Returns:
        The connected stub.

    Raises:
        NoHealthyTargetsError: If no target was confirmed healthy within `timeout`.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            return await client.connect()
        except NoHealthyTargetsError:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(POLL_INTERVAL)


type MakeFactory = Callable[[FactorySettings], GrpcClientFactory]
