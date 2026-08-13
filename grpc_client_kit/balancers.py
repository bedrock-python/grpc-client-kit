from __future__ import annotations

import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum

from .errors import GrpcClientKitError
from .protocols import HealthCheckerProtocol
from .validation import validate_target

logger = logging.getLogger(__name__)


class NoHealthyTargetsError(GrpcClientKitError):
    """Raised when no healthy targets are available."""

    def __init__(self, targets: list[str]) -> None:
        self.targets = targets
        super().__init__(f"No healthy targets available among {targets}")


class LoadBalancingStrategy(StrEnum):
    """Load balancing strategies."""

    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    WEIGHTED = "weighted"


@dataclass(slots=True)
class LoadBalancerConfig:
    """Configuration for load balancing."""

    strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN
    weights: dict[str, float] | None = None


class LoadBalancer(ABC):
    """Base class for gRPC client load balancers.

    Provides a standard interface for selecting a target from a list of addresses,
    with support for health filtering.
    """

    def __init__(self, targets: list[str], health_checker: HealthCheckerProtocol | None = None) -> None:
        """Initialize the load balancer.

        Args:
            targets: List of target addresses (host:port).
            health_checker: Optional health checker for filtering unhealthy targets.

        Raises:
            ValueError: If targets list is empty or any target is invalid.
        """
        if not targets:
            raise ValueError("Targets list cannot be empty. At least one host:port target must be provided.")

        for t in targets:
            validate_target(t)

        self._targets = list(targets)  # Copy list to prevent external mutation
        self._health_checker = health_checker
        # Passive verdicts: target -> monotonic deadline until which it is avoided. An active
        # checker learns about a dead backend one probe interval late; a real call learns
        # immediately, and report_failure() is how that knowledge reaches the balancer.
        self._quarantine: dict[str, float] = {}

    def report_failure(self, target: str, quarantine: float = 5.0) -> None:
        """Quarantine a target that a real call just found unreachable.

        Active probing has an inherent window: with the default check interval a dead backend
        keeps receiving its share of traffic for up to that interval, every call burning a full
        client deadline. A call that got ``UNAVAILABLE`` is fresher evidence than any probe, so
        it takes the target out of the rotation immediately, for `quarantine` seconds — long
        enough for the next probe (or a recovered backend) to have the casting vote.

        Args:
            target: The target address the failed call was routed to.
            quarantine: Seconds to keep the target out of the rotation.
        """
        self._quarantine[target] = time.monotonic() + quarantine
        logger.debug("Target %s quarantined for %.1fs after a failed call", target, quarantine)

    def _without_quarantined(self, candidates: list[str]) -> list[str]:
        """Drop quarantined targets from `candidates` — unless that would drop them all.

        When every candidate is quarantined the quarantine is ignored: degraded service beats
        refusing to route at all, and the next failure simply renews the verdict.
        """
        now = time.monotonic()
        expired = [target for target, deadline in self._quarantine.items() if now >= deadline]
        for target in expired:
            del self._quarantine[target]

        kept = [target for target in candidates if target not in self._quarantine]
        return kept or candidates

    @abstractmethod
    async def select_target(self) -> str:
        """Select a target from the available targets.

        Returns:
            The selected target address (host:port).

        Raises:
            NoHealthyTargetsError: If all targets are unhealthy.
        """
        ...


class RoundRobinLoadBalancer(LoadBalancer):
    """Round-robin load balancer with health check support.

    Selects targets in a fixed cyclic order, skipping unhealthy ones.
    """

    def __init__(self, targets: list[str], health_checker: HealthCheckerProtocol | None = None) -> None:
        """Initialize the Round-Robin balancer.

        Args:
            targets: List of target addresses.
            health_checker: Optional health checker.
        """
        super().__init__(targets, health_checker)
        self._index = 0
        self._lock = asyncio.Lock()

    async def select_target(self) -> str:
        """Select the next target in the round-robin sequence."""
        if not self._health_checker:
            candidates = set(self._without_quarantined(self._targets))
            async with self._lock:
                for _ in range(len(self._targets)):
                    target = self._targets[self._index]
                    self._index = (self._index + 1) % len(self._targets)
                    if target in candidates:
                        logger.debug("Selected target %s using round-robin", target)
                        return target
                # Unreachable in practice — _without_quarantined never empties the candidates —
                # but a plain pick beats an exception if it ever is.
                return self._targets[self._index]

        # With health checker - pre-fetch health statuses outside the lock to avoid blocking
        # but keep it in a small window to maintain some accuracy.
        health_statuses = await asyncio.gather(
            *(self._health_checker.is_healthy(t) for t in self._targets), return_exceptions=True
        )
        healthy = [target for target, status in zip(self._targets, health_statuses, strict=True) if status is True]
        candidates = set(self._without_quarantined(healthy))

        num_targets = len(self._targets)
        async with self._lock:
            for _ in range(num_targets):
                target = self._targets[self._index]
                self._index = (self._index + 1) % num_targets

                if target in candidates:
                    logger.debug("Selected target %s using round-robin (health_checker=True)", target)
                    return target

        # All unhealthy - raise error
        raise NoHealthyTargetsError(self._targets)


class RandomLoadBalancer(LoadBalancer):
    """Random load balancer with health check support.

    Selects a random target from the list of healthy targets.
    Caches healthy targets for 1 second to improve performance.
    """

    def __init__(self, targets: list[str], health_checker: HealthCheckerProtocol | None = None) -> None:
        """Initialize the Random balancer.

        Args:
            targets: List of target addresses.
            health_checker: Optional health checker.
        """
        super().__init__(targets, health_checker)
        self._healthy_targets: list[str] = []
        self._last_health_update = 0.0
        self._health_cache_ttl = 1.0  # 1 second
        self._lock = asyncio.Lock()

    async def select_target(self) -> str:
        """Select a random healthy target."""
        if not self._health_checker:
            target = random.choice(self._without_quarantined(self._targets))  # noqa: S311
            logger.debug("Selected target %s using random", target)
            return target

        # Use cached healthy targets if possible to avoid frequent gathers
        async with self._lock:
            now = time.time()
            if now - self._last_health_update > self._health_cache_ttl:
                # Filter healthy targets in parallel
                health_statuses = await asyncio.gather(
                    *(self._health_checker.is_healthy(t) for t in self._targets), return_exceptions=True
                )

                self._healthy_targets = [
                    t for t, status in zip(self._targets, health_statuses, strict=True) if status is True
                ]
                self._last_health_update = now

            if not self._healthy_targets:
                raise NoHealthyTargetsError(self._targets)

            # Quarantine is applied on every pick, not cached: a passive verdict may arrive (and
            # expire) well within the health cache's TTL.
            target = random.choice(self._without_quarantined(self._healthy_targets))  # noqa: S311
            logger.debug("Selected target %s using random (total_healthy=%d)", target, len(self._healthy_targets))
            return target


class WeightedLoadBalancer(LoadBalancer):
    """Weighted random load balancer with health check support.

    Selects targets based on provided weights, giving higher preference to
    targets with larger weights. Skips unhealthy targets.
    """

    def __init__(
        self,
        targets: list[str],
        weights: dict[str, float],
        health_checker: HealthCheckerProtocol | None = None,
    ) -> None:
        """Initialize the Weighted balancer.

        Args:
            targets: List of target addresses.
            weights: Mapping of target to its weight (default 1.0).
            health_checker: Optional health checker.
        """
        super().__init__(targets, health_checker)
        self._weights_dict = weights

        # Validate weights
        for t in targets:
            weight = weights.get(t, 1.0)
            if weight < 0:
                raise ValueError(f"Weight for target {t} cannot be negative: {weight}")

        if sum(weights.get(t, 1.0) for t in targets) <= 0:
            raise ValueError("Sum of weights must be positive")

        # Pre-calculate weights list for targets to avoid repeated dict lookups
        self._cached_weights = [weights.get(t, 1.0) for t in targets]

        # Health status caching (similar to RandomLoadBalancer)
        self._healthy_targets: list[str] = []
        self._healthy_weights: list[float] = []
        self._last_health_update = 0.0
        self._health_cache_ttl = 1.0  # 1 second
        self._lock = asyncio.Lock()

    def _weighted_pick(self, candidates: list[str]) -> str:
        """Pick among `candidates` by weight, falling back to uniform when all weights are zero."""
        weights = [self._weights_dict.get(target, 1.0) for target in candidates]
        if sum(weights) <= 0:
            return random.choice(candidates)  # noqa: S311
        return random.choices(candidates, weights=weights, k=1)[0]  # noqa: S311

    async def select_target(self) -> str:
        """Select a target using weighted random selection among healthy ones."""
        if not self._health_checker:
            target = self._weighted_pick(self._without_quarantined(self._targets))
            logger.debug(
                "Selected target %s using weighted random (weight=%.2f)",
                target,
                self._weights_dict.get(target, 1.0),
            )
            return target

        # Use cached healthy targets if possible
        async with self._lock:
            now = time.time()
            if now - self._last_health_update > self._health_cache_ttl:
                # Filter healthy targets and their weights in parallel
                health_statuses = await asyncio.gather(
                    *(self._health_checker.is_healthy(t) for t in self._targets), return_exceptions=True
                )

                self._healthy_targets = []
                self._healthy_weights = []

                for t, status in zip(self._targets, health_statuses, strict=True):
                    if status is True:
                        self._healthy_targets.append(t)
                        self._healthy_weights.append(self._weights_dict.get(t, 1.0))

                self._last_health_update = now

            if not self._healthy_targets:
                raise NoHealthyTargetsError(self._targets)

            # Quarantine is applied on every pick, not cached: a passive verdict may arrive (and
            # expire) well within the health cache's TTL.
            target = self._weighted_pick(self._without_quarantined(self._healthy_targets))

            logger.debug(
                "Selected target %s using weighted random (weight=%.2f, total_healthy=%d)",
                target,
                self._weights_dict.get(target, 1.0),
                len(self._healthy_targets),
            )
            return target


def create_balancer(
    targets: list[str],
    config: LoadBalancerConfig | None = None,
    health_checker: HealthCheckerProtocol | None = None,
) -> LoadBalancer:
    """Factory function to create a load balancer from targets and config.

    By default, creates a Round-Robin balancer if no config is provided.

    Args:
        targets: List of target addresses (host:port)
        config: Balancer configuration (strategy and weights)
        health_checker: Optional health checker for filtering unhealthy targets

    Returns:
        A concrete LoadBalancer instance

    Raises:
        ValueError: If targets list is empty or config is invalid.
    """
    if not targets:
        raise ValueError("Targets list cannot be empty for load balancer")

    strategy = config.strategy if config else LoadBalancingStrategy.ROUND_ROBIN

    match strategy:
        case LoadBalancingStrategy.RANDOM:
            return RandomLoadBalancer(targets, health_checker)
        case LoadBalancingStrategy.WEIGHTED:
            if not config or not config.weights:
                raise ValueError(f"Weights must be provided for {strategy} strategy")
            return WeightedLoadBalancer(targets, config.weights, health_checker)
        case LoadBalancingStrategy.ROUND_ROBIN:
            return RoundRobinLoadBalancer(targets, health_checker)
        case _:
            raise ValueError(f"Unknown load balancing strategy: {strategy}")


__all__ = [
    "LoadBalancer",
    "LoadBalancerConfig",
    "LoadBalancingStrategy",
    "NoHealthyTargetsError",
    "RandomLoadBalancer",
    "RoundRobinLoadBalancer",
    "WeightedLoadBalancer",
    "create_balancer",
]
