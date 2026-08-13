"""`GrpcClientFactory` wiring a health-checked, balanced client — and what it refuses until it can.

The factory owns the check loop, and the loop only runs inside ``async with``. These tests pin down
what the difference is worth: with the loop stopped a serving server is still not a target, and even
with it started the first pass has to have answered before anything may be routed to it.
"""

from __future__ import annotations

import logging

import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from grpc_client_kit import NoHealthyTargetsError

from .factory_bench import MakeFactory, connect_when_ready, factory_settings
from .health_bench import APP_SERVICE, SERVING, HealthServer
from .waiting import until

pytestmark = pytest.mark.usefixtures("fast_health_loop")


# Entering the factory only creates the check loop's task; it does not await the loop's first pass,
async def test__factory__used_without_async_with__warns_that_health_checks_are_not_running(
    health_server: HealthServer,
    make_factory: MakeFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrange
    caplog.set_level(logging.WARNING, logger="grpc_client_kit.factory")
    factory = make_factory(factory_settings([health_server.target]))

    # Act
    factory.create_client(health_pb2_grpc.HealthStub)

    # Assert
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any("async with" in message for message in warnings), warnings


async def test__factory__used_without_async_with__client_refuses_to_connect(
    health_server: HealthServer,
    make_factory: MakeFactory,
) -> None:
    # Arrange
    factory = make_factory(factory_settings([health_server.target]))
    client = factory.create_client(health_pb2_grpc.HealthStub)

    # Act
    with pytest.raises(NoHealthyTargetsError):
        await client.connect()

    # Assert
    # The server is up and serving; what is missing is a check loop to notice it, and an unstarted
    # checker must make that visible instead of waving the traffic through.
    assert health_server.servicer.probe_count == 0
    assert health_server.servicer.calls == []


async def test__factory__entered_with_async_with__first_connect_reaches_the_serving_target(
    health_server: HealthServer,
    make_factory: MakeFactory,
) -> None:
    # Arrange
    factory = make_factory(factory_settings([health_server.target]))

    # Act
    async with factory:
        client = factory.create_client(health_pb2_grpc.HealthStub)
        stub = await client.connect()
        response = await stub.Check(health_pb2.HealthCheckRequest(service=APP_SERVICE))

    # Assert
    assert response.status == SERVING


async def test__factory__entered_with_async_with__client_reaches_the_serving_target(
    health_server: HealthServer,
    make_factory: MakeFactory,
) -> None:
    # Arrange
    factory = make_factory(factory_settings([health_server.target]))

    # Act
    async with factory:
        client = factory.create_client(health_pb2_grpc.HealthStub)
        stub = await connect_when_ready(client)
        response = await stub.Check(health_pb2.HealthCheckRequest(service=APP_SERVICE))

    # Assert
    assert response.status == SERVING
    assert health_server.servicer.probe_count >= 1
    assert health_server.servicer.calls.count(APP_SERVICE) == 1


async def test__factory__entered_with_a_first_pass_pending__client_refuses_to_connect(
    health_server: HealthServer,
    make_factory: MakeFactory,
) -> None:
    # Arrange
    health_server.servicer.block()
    factory = make_factory(factory_settings([health_server.target]))

    # Act
    async with factory:
        client = factory.create_client(health_pb2_grpc.HealthStub)
        await until(lambda: health_server.servicer.probe_count >= 1, message="the factory never probed the server")

        # Assert
        # Entering the factory starts the loop but does not await its first pass, so a correctly
        # used factory still refuses to route until a probe has answered.
        with pytest.raises(NoHealthyTargetsError):
            await client.connect()

        health_server.servicer.unblock()
        stub = await connect_when_ready(client)
        response = await stub.Check(health_pb2.HealthCheckRequest(service=APP_SERVICE))
        assert response.status == SERVING
