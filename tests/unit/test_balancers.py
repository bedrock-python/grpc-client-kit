"""Unit tests for the client-side load balancers.

A balancer only ever answers one question — which target the next call goes to — so every test here
asks it that question and checks the answer, health filtering included.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from grpc_client_kit.balancers import (
    LoadBalancerConfig,
    LoadBalancingStrategy,
    NoHealthyTargetsError,
    RandomLoadBalancer,
    RoundRobinLoadBalancer,
    WeightedLoadBalancer,
    create_balancer,
)

from .conftest import make_health_probe

pytestmark = pytest.mark.unit


def test__create_balancer__single_target__returns_a_round_robin_balancer() -> None:
    """One target still goes through a balancer, so the client has a single code path."""
    # Act
    balancer = create_balancer(["localhost:50051"])

    # Assert
    assert isinstance(balancer, RoundRobinLoadBalancer)


async def test__create_balancer__no_strategy_configured__rotates_the_targets_in_order() -> None:
    """Round-robin is the default, and it walks the list in the order it was configured."""
    # Arrange
    balancer = create_balancer(["host1:50051", "host2:50051"])

    # Act
    selected = [await balancer.select_target() for _ in range(3)]

    # Assert
    assert isinstance(balancer, RoundRobinLoadBalancer)
    assert selected == ["host1:50051", "host2:50051", "host1:50051"]


async def test__create_balancer__random_strategy__selects_one_of_the_targets() -> None:
    """The random balancer picks from the configured list and from nowhere else."""
    # Arrange
    targets = ["host1:50051", "host2:50051"]
    config = LoadBalancerConfig(strategy=LoadBalancingStrategy.RANDOM)
    balancer = create_balancer(targets, config)

    # Act
    target = await balancer.select_target()

    # Assert
    assert isinstance(balancer, RandomLoadBalancer)
    assert target in targets


async def test__create_balancer__weighted_strategy__respects_the_configured_weights() -> None:
    """A weight of zero means the target is configured but never picked."""
    # Arrange
    targets = ["host1:50051", "host2:50051"]
    weights = {"host1:50051": 100.0, "host2:50051": 0.0}
    config = LoadBalancerConfig(strategy=LoadBalancingStrategy.WEIGHTED, weights=weights)
    balancer = create_balancer(targets, config)

    # Act
    selected = [await balancer.select_target() for _ in range(10)]

    # Assert
    assert isinstance(balancer, WeightedLoadBalancer)
    assert selected == ["host1:50051"] * 10


def test__create_balancer__weighted_strategy_without_weights__is_refused() -> None:
    """A weighted balancer with no weights has nothing to weigh, and says so at construction."""
    # Arrange
    config = LoadBalancerConfig(strategy=LoadBalancingStrategy.WEIGHTED, weights=None)

    # Act & Assert
    with pytest.raises(ValueError, match="Weights must be provided"):
        create_balancer(["host1:50051", "host2:50051"], config)


def test__round_robin_balancer__empty_target_list__is_refused() -> None:
    """A balancer over nothing could only ever fail, one call at a time."""
    # Act & Assert
    with pytest.raises(ValueError, match="Targets list cannot be empty"):
        RoundRobinLoadBalancer([])


def test__create_balancer__empty_target_list__is_refused() -> None:
    """The factory refuses the same input as the balancer it would have built."""
    # Act & Assert
    with pytest.raises(ValueError, match="Targets list cannot be empty for load balancer"):
        create_balancer([])


async def test__round_robin_balancer__unhealthy_target__is_skipped() -> None:
    """Rotation is over the healthy targets, not over the configured ones."""
    # Arrange
    health = make_health_probe(lambda target: target == "host2:50051")
    balancer = RoundRobinLoadBalancer(["host1:50051", "host2:50051"], health_checker=health)

    # Act
    selected = [await balancer.select_target() for _ in range(2)]

    # Assert
    assert selected == ["host2:50051", "host2:50051"]


async def test__round_robin_balancer__no_healthy_target__raises() -> None:
    """Refusing loudly beats dialling a backend that is known to be down."""
    # Arrange
    health = make_health_probe(lambda target: False)
    balancer = RoundRobinLoadBalancer(["host1:50051", "host2:50051"], health_checker=health)

    # Act & Assert
    with pytest.raises(NoHealthyTargetsError, match="No healthy targets available"):
        await balancer.select_target()


async def test__weighted_balancer__configured_weights__are_handed_to_the_draw() -> None:
    """The weights reach `random.choices` in the order of the targets they belong to."""
    # Arrange
    targets = ["t1:50051", "t2:50051"]
    weights = {"t1:50051": 10.0, "t2:50051": 1.0}
    balancer = WeightedLoadBalancer(targets, weights)

    # Act
    with patch("random.choices", return_value=["t1:50051"]) as mock_choices:
        target = await balancer.select_target()

    # Assert
    assert target == "t1:50051"
    mock_choices.assert_called_once()
    assert mock_choices.call_args.kwargs["weights"] == [10.0, 1.0]


async def test__weighted_balancer__unhealthy_target__is_left_out_of_the_draw() -> None:
    """An unhealthy target keeps its weight but never enters the draw."""
    # Arrange
    targets = ["t1:50051", "t2:50051"]
    weights = {"t1:50051": 1.0, "t2:50051": 1.0}
    health = make_health_probe(lambda target: target == "t2:50051")
    balancer = WeightedLoadBalancer(targets, weights, health_checker=health)

    # Act
    with patch("random.choices", return_value=["t2:50051"]) as mock_choices:
        target = await balancer.select_target()

    # Assert
    assert target == "t2:50051"
    assert mock_choices.call_args.kwargs["weights"] == [1.0]
    assert mock_choices.call_args.args[0] == ["t2:50051"]


async def test__random_balancer__no_healthy_target__raises() -> None:
    """Every balancer reports the same way when nothing is left to dial."""
    # Arrange
    health = make_health_probe(lambda target: False)
    balancer = RandomLoadBalancer(["t1:50051"], health_checker=health)

    # Act & Assert
    with pytest.raises(NoHealthyTargetsError, match="No healthy targets available"):
        await balancer.select_target()


async def test__weighted_balancer__no_healthy_target__raises() -> None:
    """Every balancer reports the same way when nothing is left to dial."""
    # Arrange
    health = make_health_probe(lambda target: False)
    balancer = WeightedLoadBalancer(["h:1"], {"h:1": 1.0}, health_checker=health)

    # Act & Assert
    with pytest.raises(NoHealthyTargetsError, match="No healthy targets available"):
        await balancer.select_target()


async def test__round_robin_balancer__target_of_unknown_health__is_treated_as_unhealthy() -> None:
    """Nothing is known about a target the checker has not confirmed, and unknown is not healthy."""
    # Arrange
    health = make_health_probe(lambda target: target != "h1:1")
    balancer = RoundRobinLoadBalancer(["h1:1", "h2:2"], health_checker=health)

    # Act
    selected = [await balancer.select_target() for _ in range(2)]

    # Assert
    assert selected == ["h2:2", "h2:2"]
