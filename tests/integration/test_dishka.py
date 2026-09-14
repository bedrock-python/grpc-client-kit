"""The Dishka providers against real health servers: one shared pool, one checker per upstream.

Regression cover for issue #31 at the wire: two health-checked upstreams borrowing the pool of the
default component each keep a checker for their own targets, both mark the one pool, both route
through it, and closing the container stops the checkers and drains the pool once.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from dishka import make_async_container
from grpc_health.v1 import health_pb2_grpc

from grpc_client_kit import ChannelPool, GrpcClientFactory
from grpc_client_kit.dishka import AsyncChannelPoolProvider, grpc_client_providers

from .factory_bench import PoolSettings, connect_when_ready, factory_settings
from .health_bench import StartHealthServer
from .waiting import until

pytestmark = pytest.mark.usefixtures("fast_health_loop")


async def test__grpc_client_providers__two_health_checked_upstreams_on_a_shared_pool__each_mark_their_own_target_in_it(
    start_health_server: StartHealthServer,
) -> None:
    # Arrange
    users_server = await start_health_server()
    orders_server = await start_health_server()
    container = make_async_container(
        AsyncChannelPoolProvider(PoolSettings()),
        *grpc_client_providers(factory_settings([users_server.target]), component="users", shared_pool=True),
        *grpc_client_providers(factory_settings([orders_server.target]), component="orders", shared_pool=True),
    )
    pool = await container.get(ChannelPool)

    # Act
    with patch.object(pool, "update_channel_health", wraps=pool.update_channel_health) as marked:
        users = await container.get(GrpcClientFactory, component="users")
        orders = await container.get(GrpcClientFactory, component="orders")
        await until(
            lambda: {call.args[0] for call in marked.await_args_list} == {users_server.target, orders_server.target},
            message="both checkers publish into the shared pool",
        )
    await connect_when_ready(users.create_client(health_pb2_grpc.HealthStub))
    await connect_when_ready(orders.create_client(health_pb2_grpc.HealthStub))

    # Assert
    assert users.health_checker is not None and users.health_checker.is_running
    assert orders.health_checker is not None and orders.health_checker.is_running
    assert users.health_checker is not orders.health_checker
    assert {key.target for key in pool._entries} == {users_server.target, orders_server.target}

    # Act
    with patch.object(pool, "close_all", wraps=pool.close_all) as close_all:
        await container.close()

    # Assert
    assert not users.health_checker.is_running
    assert not orders.health_checker.is_running
    close_all.assert_awaited_once_with(grace=5.0)
    assert pool._entries == {}
