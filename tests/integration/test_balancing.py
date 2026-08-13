"""Where traffic goes once health has spoken: the pool's channels and the balancers' selections.

The verdicts steering these tests are real ones, produced by probing real servers, which is what
makes the negative cases meaningful: a target nobody has managed to probe yet must be treated as
dead, and no strategy — not even a weighted one pointed straight at it — may route to it.
"""

from __future__ import annotations

import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from grpc_client_kit import (
    GrpcClient,
    GrpcClientConfig,
    LoadBalancingStrategy,
    NoHealthyTargetsError,
    create_balancer,
)

from .health_bench import (
    APP_SERVICE,
    EVERY_STRATEGY,
    NOT_SERVING,
    SERVING,
    HealthServer,
    MakeChecker,
    MakePool,
    PoolPublisher,
    StartHealthServer,
    balancer_for,
    start_checker,
    until_health,
)
from .waiting import until

pytestmark = pytest.mark.usefixtures("fast_health_loop")


async def test__channel_pool__target_turned_unhealthy__stops_handing_out_its_channel(
    health_server: HealthServer,
    make_checker: MakeChecker,
    make_pool: MakePool,
) -> None:
    # Arrange
    target = health_server.target
    publisher = PoolPublisher()
    checker = make_checker(on_status_change=publisher)
    pool = make_pool(health_checker=checker, max_channels_per_target=2)
    publisher.pool = pool
    await start_checker(checker, [target])
    first = await pool.get_channel(target, insecure=True)
    assert await pool.get_channel(target, insecure=True) is first

    # Act
    health_server.servicer.set_status(NOT_SERVING)
    await until(lambda: publisher.updates[-1:] == [(target, False)], message=f"pool was told {publisher.updates}")

    # Assert
    second = await pool.get_channel(target, insecure=True)
    assert second is not first
    # The replacement is a working channel: the server is unhealthy, not unreachable.
    response = await health_pb2_grpc.HealthStub(second).Check(health_pb2.HealthCheckRequest(service=APP_SERVICE))
    assert response.status == NOT_SERVING


async def test__channel_pool__unhealthy_target_at_the_channel_limit__hands_out_the_channel_anyway(
    health_server: HealthServer,
    make_checker: MakeChecker,
    make_pool: MakePool,
) -> None:
    # Arrange
    target = health_server.target
    publisher = PoolPublisher()
    checker = make_checker(on_status_change=publisher)
    pool = make_pool(health_checker=checker, max_channels_per_target=1)
    publisher.pool = pool
    await start_checker(checker, [target])
    first = await pool.get_channel(target, insecure=True)

    # Act
    health_server.servicer.set_status(NOT_SERVING)
    await until(lambda: publisher.updates[-1:] == [(target, False)], message=f"pool was told {publisher.updates}")

    # Assert
    # At the limit the pool serves the unhealthy channel rather than nothing; keeping a dead target
    # out of the rotation is the balancer's job, not the pool's.
    assert await pool.get_channel(target, insecure=True) is first


@pytest.mark.parametrize("strategy", EVERY_STRATEGY)
async def test__balancer__one_target_not_serving__never_selects_it(
    start_health_server: StartHealthServer,
    make_checker: MakeChecker,
    strategy: LoadBalancingStrategy,
) -> None:
    # Arrange
    live = await start_health_server()
    dead = await start_health_server()
    dead.servicer.set_status(NOT_SERVING)
    checker = make_checker()
    await start_checker(checker, [live.target, dead.target])
    await until_health(checker, dead.target, expected=False)
    # The dead target carries the weight, so only health can keep the weighted strategy off it.
    balancer = balancer_for(strategy, [live.target, dead.target], heaviest=dead.target, checker=checker)

    # Act
    selected = [await balancer.select_target() for _ in range(6)]

    # Assert
    assert selected == [live.target] * 6


@pytest.mark.parametrize("strategy", EVERY_STRATEGY)
async def test__balancer__every_target_not_serving__raises_no_healthy_targets(
    start_health_server: StartHealthServer,
    make_checker: MakeChecker,
    strategy: LoadBalancingStrategy,
) -> None:
    # Arrange
    servers = [await start_health_server(), await start_health_server()]
    targets = [server.target for server in servers]
    for server in servers:
        server.servicer.set_status(NOT_SERVING)
    checker = make_checker()
    await start_checker(checker, targets)
    for target in targets:
        await until_health(checker, target, expected=False)
    balancer = balancer_for(strategy, targets, heaviest=targets[0], checker=checker)

    # Act
    with pytest.raises(NoHealthyTargetsError) as exc_info:
        await balancer.select_target()

    # Assert
    assert exc_info.value.targets == targets


async def test__round_robin__first_pass_still_running__raises_no_healthy_targets(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    health_server.servicer.block()
    checker = make_checker()
    balancer = create_balancer([health_server.target], health_checker=checker)
    await checker.start([health_server.target])
    await until(lambda: health_server.servicer.probe_count >= 1, message="the checker never probed the server")

    # Act
    with pytest.raises(NoHealthyTargetsError):
        await balancer.select_target()

    # Assert
    # Nothing about the target changed except that a probe finally answered.
    health_server.servicer.unblock()
    await until_health(checker, health_server.target, expected=True)
    assert await balancer.select_target() == health_server.target


async def test__round_robin__target_started_serving_again__returns_it_to_the_rotation(
    start_health_server: StartHealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    live = await start_health_server()
    recovering = await start_health_server()
    recovering.servicer.set_status(NOT_SERVING)
    checker = make_checker()
    await start_checker(checker, [live.target, recovering.target])
    await until_health(checker, recovering.target, expected=False)
    assert await checker.is_healthy(live.target) is True

    # Act
    recovering.servicer.set_status(SERVING)
    await until_health(checker, recovering.target, expected=True)

    # Assert
    # No client, balancer or checker was rebuilt: the same rotation picks the target up again.
    balancer = create_balancer([live.target, recovering.target], health_checker=checker)
    selected = [await balancer.select_target() for _ in range(4)]
    assert set(selected) == {live.target, recovering.target}


async def test__balanced_client__one_target_not_serving__sends_every_call_to_the_live_target(
    start_health_server: StartHealthServer,
    make_checker: MakeChecker,
    make_pool: MakePool,
) -> None:
    # Arrange
    live = await start_health_server()
    dead = await start_health_server()
    dead.servicer.set_status(NOT_SERVING)
    checker = make_checker()
    await start_checker(checker, [live.target, dead.target])
    await until_health(checker, dead.target, expected=False)
    balancer = create_balancer([live.target, dead.target], health_checker=checker)
    client: GrpcClient[health_pb2_grpc.HealthStub] = GrpcClient(
        stub_class=health_pb2_grpc.HealthStub,
        config=GrpcClientConfig(insecure=True),
        pool=make_pool(),
        balancer=balancer,
    )

    # Act
    for _ in range(3):
        stub = await client.connect()
        await stub.Check(health_pb2.HealthCheckRequest(service=APP_SERVICE))

    # Assert
    # The checker probes both servers with the empty service name; only the client asks for APP.
    assert live.servicer.calls.count(APP_SERVICE) == 3
    assert dead.servicer.calls.count(APP_SERVICE) == 0
