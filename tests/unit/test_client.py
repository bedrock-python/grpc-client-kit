"""Unit tests for the gRPC client: how it resolves a target and which chain it opens a channel with."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from grpc_client_kit.balancers import RoundRobinLoadBalancer
from grpc_client_kit.client import GrpcClient
from grpc_client_kit.config import GrpcClientConfig
from grpc_client_kit.interceptors import RetryConfig, build_interceptors
from tests.helpers import make_interceptor, make_pool

from .conftest import recording_interceptor_factory

pytestmark = pytest.mark.unit


async def test__client__static_target__connects_to_it() -> None:
    """A client with one target asks the pool for exactly that address, with its own chain."""
    # Arrange
    stub_class = MagicMock()
    pool = make_pool()
    client: GrpcClient = GrpcClient(stub_class, GrpcClientConfig(target="localhost:50051"), pool=pool)

    # Act
    stub = await client.connect()

    # Assert
    # A spec'd pool passes the isinstance check, so the client takes the cached-key fast path:
    # identity is built once through make_key and the pool is asked by key, not by raw arguments.
    pool.make_key.assert_called_once_with(
        "localhost:50051",
        insecure=False,
        credentials=None,
        options=None,
        compression=None,
        interceptors=[],
    )
    pool.get_channel.assert_called_once_with("localhost:50051", interceptors=[], key=pool.make_key.return_value)
    stub_class.assert_called_once_with(pool.get_channel.return_value)
    assert stub == stub_class.return_value


async def test__client__balancer_configured__connects_to_the_target_it_selects() -> None:
    """With no static target the balancer decides where each connection goes."""
    # Arrange
    balancer = RoundRobinLoadBalancer(["host1:50051", "host2:50051"])
    pool = make_pool()
    client: GrpcClient = GrpcClient(MagicMock(), GrpcClientConfig(), pool=pool, balancer=balancer)

    # Act
    await client.connect()

    # Assert
    pool.get_channel.assert_called_with("host1:50051", interceptors=[], key=pool.make_key.return_value)


def test__client__target_and_balancer_together__is_refused() -> None:
    """Two sources of truth for the address could only disagree, so one of them must go."""
    # Arrange
    pool = make_pool()

    # Act & Assert
    with pytest.raises(ValueError, match="Ambiguous target"):
        GrpcClient(MagicMock(), GrpcClientConfig(target="t1"), pool=pool, balancer=MagicMock())


def test__client__neither_target_nor_balancer__is_refused() -> None:
    """A client that cannot name an address is a configuration error, not a runtime one."""
    # Arrange
    pool = make_pool()

    # Act & Assert
    with pytest.raises(ValueError, match="No target specified"):
        GrpcClient(MagicMock(), GrpcClientConfig(target=None), pool=pool)


def test__client__ready_chain_and_factory_together__is_refused() -> None:
    """A client cannot both share one chain and build one per target."""
    # Act & Assert
    with pytest.raises(ValueError, match="Ambiguous interceptors"):
        GrpcClient(
            MagicMock(),
            GrpcClientConfig(target="t1:1"),
            pool=make_pool(),
            interceptors=[make_interceptor()],
            interceptor_factory=lambda target: [],
        )


async def test__client__interceptor_factory__builds_one_chain_per_target() -> None:
    """Each target gets its own chain, so stateful interceptors never span backends."""
    # Arrange
    built: list[str] = []
    balancer = RoundRobinLoadBalancer(["host1:50051", "host2:50051"])
    client: GrpcClient = GrpcClient(
        MagicMock(),
        GrpcClientConfig(),
        pool=make_pool(),
        balancer=balancer,
        interceptor_factory=recording_interceptor_factory(built),
    )

    # Act
    await client.connect()
    await client.connect()

    # Assert
    assert built == ["host1:50051", "host2:50051"]
    assert client.interceptors_for("host1:50051") != client.interceptors_for("host2:50051")


async def test__client__same_target_connected_twice__reuses_its_chain() -> None:
    """The chain is built once per target: rebuilding it would open a channel per call."""
    # Arrange
    built: list[str] = []
    pool = make_pool()
    client: GrpcClient = GrpcClient(
        MagicMock(),
        GrpcClientConfig(target="localhost:50051"),
        pool=pool,
        interceptor_factory=recording_interceptor_factory(built),
    )

    # Act
    await client.connect()
    await client.connect()

    # Assert
    assert built == ["localhost:50051"]
    first_chain, second_chain = (call.kwargs["interceptors"] for call in pool.get_channel.call_args_list)
    assert first_chain is second_chain


def test__client__ready_chain__is_shared_by_every_target() -> None:
    """A ready chain is explicitly shared, unlike a factory-built one."""
    # Arrange
    interceptor = make_interceptor()
    balancer = RoundRobinLoadBalancer(["host1:50051", "host2:50051"])
    client: GrpcClient = GrpcClient(
        MagicMock(),
        GrpcClientConfig(),
        pool=make_pool(),
        balancer=balancer,
        interceptors=[interceptor],
    )

    # Act
    first = client.interceptors_for("host1:50051")
    second = client.interceptors_for("host2:50051")

    # Assert
    assert first is second
    assert first == [interceptor]


async def test__client__kit_retries_stacked_on_native_retry_policy__warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Native retryPolicy runs below the interceptors, so the two layers multiply: kit attempts
    times native attempts reach the server, invisibly to the kit's own logs and metrics."""
    # Arrange
    service_config = '{"methodConfig": [{"name": [{}], "retryPolicy": {"maxAttempts": 3}}]}'
    config = GrpcClientConfig(target="localhost:50051", options=[("grpc.service_config", service_config)])
    chain = build_interceptors(retry=RetryConfig(max_attempts=3))
    client: GrpcClient = GrpcClient(MagicMock(), config, pool=make_pool(), interceptors=chain)

    # Act
    with caplog.at_level("WARNING", logger="grpc_client_kit.client"):
        client.interceptors_for("localhost:50051")
        client.interceptors_for("localhost:50051")

    # Assert
    warnings = [record for record in caplog.records if "multiply" in record.message]
    assert len(warnings) == 1


async def test__client__native_retry_policy_without_kit_retries__passes_silently(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Native retries alone are a fully supported configuration and must not be nagged about."""
    # Arrange
    service_config = '{"methodConfig": [{"name": [{}], "retryPolicy": {"maxAttempts": 3}}]}'
    config = GrpcClientConfig(target="localhost:50051", options=[("grpc.service_config", service_config)])
    client: GrpcClient = GrpcClient(MagicMock(), config, pool=make_pool())

    # Act
    with caplog.at_level("WARNING", logger="grpc_client_kit.client"):
        client.interceptors_for("localhost:50051")

    # Assert
    assert not [record for record in caplog.records if "multiply" in record.message]


async def test__client__used_as_a_context_manager__does_not_close_the_shared_pool() -> None:
    """The pool outlives the client using it, so leaving the block must not drain it."""
    # Arrange
    stub_class = MagicMock()
    pool = make_pool()
    client: GrpcClient = GrpcClient(stub_class, GrpcClientConfig(target="localhost:50051"), pool=pool)

    # Act
    async with client as stub:
        connected = stub

    # Assert
    assert connected == stub_class.return_value
    pool.close_all.assert_not_called()
