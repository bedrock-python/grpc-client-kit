"""The health checker against a real ``grpc_health.v1`` service: verdicts, schedule, announcements.

Health is the one subsystem where a mock proves nothing: what matters is whether an *unprobed*
address is treated as dead, and a mock answers before the question can even be asked. Every verdict
below therefore rests on a probe that really crossed a wire, and every schedule assertion on probe
arrival times the server itself recorded.
"""

from __future__ import annotations

import pytest

from grpc_client_kit.health import HealthCheckerNotRunningError

from .health_bench import (
    CHECK_INTERVAL,
    NOT_SERVING,
    SERVING,
    HealthServer,
    MakeChecker,
    StartHealthServer,
    StatusRecorder,
    probe_gaps,
    start_checker,
    until_health,
)
from .waiting import WAIT_TIMEOUT, until

pytestmark = pytest.mark.usefixtures("fast_health_loop")

# How long a first pass that cannot finish is given to prove it did not finish anyway.
_PENDING_WINDOW = 0.2

# Backoff is scheduled, so a probe can only ever be late: gaps are asserted as lower bounds, with a
# small allowance for the monotonic clock and the loop's own granularity.
_SCHEDULE_TOLERANCE = 0.9


async def test__health_checker__server_serving__reports_the_target_healthy(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    checker = make_checker()

    # Act
    await start_checker(checker, [health_server.target])

    # Assert
    assert await checker.is_healthy(health_server.target) is True
    assert health_server.servicer.probe_count >= 1


async def test__health_checker__server_switched_to_not_serving__reports_the_target_unhealthy(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    checker = make_checker()
    await start_checker(checker, [health_server.target])
    assert await checker.is_healthy(health_server.target) is True

    # Act
    health_server.servicer.set_status(NOT_SERVING)

    # Assert
    await until_health(checker, health_server.target, expected=False)


async def test__health_checker__target_outside_the_monitored_set__is_not_reported_healthy(
    health_server: HealthServer,
    start_health_server: StartHealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    # A second server that is up and serving, but that nobody was asked to monitor.
    unmonitored = await start_health_server()
    checker = make_checker()

    # Act
    await start_checker(checker, [health_server.target])

    # Assert
    # Reachable and serving is not the same as checked: traffic must not go where nobody looked.
    assert await checker.is_healthy(unmonitored.target) is False
    assert unmonitored.servicer.probe_count == 0


async def test__health_checker__first_pass_still_running__reports_the_target_unhealthy(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    health_server.servicer.block()
    checker = make_checker()

    # Act
    await checker.start([health_server.target])
    await until(lambda: health_server.servicer.probe_count >= 1, message="the checker never probed the server")

    # Assert
    # The probe is still inside the handler, so nothing has classified the target yet.
    assert await checker.wait_until_ready(timeout=_PENDING_WINDOW) is False
    assert await checker.is_healthy(health_server.target) is False


async def test__health_checker__first_probe_answers__reports_the_target_healthy(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    health_server.servicer.block()
    checker = make_checker()
    await checker.start([health_server.target])
    await until(lambda: health_server.servicer.probe_count >= 1, message="the checker never probed the server")

    # Act
    health_server.servicer.unblock()

    # Assert
    assert await checker.wait_until_ready(timeout=WAIT_TIMEOUT) is True
    assert await checker.is_healthy(health_server.target) is True


async def test__health_checker__never_started__is_healthy_raises_not_running_error(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    checker = make_checker()

    # Act
    with pytest.raises(HealthCheckerNotRunningError) as exc_info:
        await checker.is_healthy(health_server.target)

    # Assert
    # No loop can ever produce a verdict here, so claiming health would be a fiction.
    assert exc_info.value.target == health_server.target
    assert checker.is_running is False
    assert health_server.servicer.probe_count == 0


async def test__health_checker__stopped_after_a_verdict__keeps_serving_the_stale_verdict(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    checker = make_checker()
    await start_checker(checker, [health_server.target])
    assert await checker.is_healthy(health_server.target) is True

    # Act
    await checker.stop()
    health_server.servicer.set_status(NOT_SERVING)

    # Assert
    # Only a target without any recorded verdict makes a stopped checker refuse to answer; one that
    # was checked before the loop stopped keeps its last verdict, however old it has grown.
    assert checker.is_running is False
    assert await checker.is_healthy(health_server.target) is True


async def test__health_checker__repeated_failures__grows_the_gap_between_probes(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    servicer = health_server.servicer
    servicer.set_status(NOT_SERVING)
    checker = make_checker()

    # Act
    await start_checker(checker, [health_server.target])
    await until(lambda: servicer.probe_count >= 4, message=f"only {servicer.probe_count} probes arrived")

    # Assert
    # Backoff is check_interval * 2^(failures - 1), so the first three gaps double each time.
    gaps = probe_gaps(servicer)[:3]
    expected = [CHECK_INTERVAL * 2**power for power in range(3)]
    for gap, bound in zip(gaps, expected, strict=True):
        assert gap >= bound * _SCHEDULE_TOLERANCE


async def test__health_checker__probe_succeeds_again__resets_the_gap_to_the_check_interval(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    servicer = health_server.servicer
    servicer.set_status(NOT_SERVING)
    checker = make_checker()
    await start_checker(checker, [health_server.target])
    await until(lambda: servicer.probe_count >= 3, message=f"only {servicer.probe_count} probes arrived")

    # Act
    servicer.set_status(SERVING)
    await until_health(checker, health_server.target, expected=True)
    healed_at = servicer.probe_count
    await until(lambda: servicer.probe_count > healed_at, message="no probe followed the recovery")

    # Assert
    gaps = probe_gaps(servicer)
    backed_off, recovered = gaps[healed_at - 2], gaps[healed_at - 1]
    assert recovered < backed_off / 2


async def test__health_checker__status_flips__notifies_the_callback_once_per_change(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    target = health_server.target
    recorder = StatusRecorder()
    checker = make_checker(on_status_change=recorder)
    await start_checker(checker, [target])

    # Act
    health_server.servicer.set_status(NOT_SERVING)
    await until_health(checker, target, expected=False)
    health_server.servicer.set_status(SERVING)
    await until_health(checker, target, expected=True)

    # Assert
    await until(lambda: len(recorder.changes) == 3, message=f"announced {recorder.changes}")
    assert recorder.changes == [(target, True), (target, False), (target, True)]


async def test__health_checker__status_repeats__does_not_notify_the_callback_again(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    servicer = health_server.servicer
    recorder = StatusRecorder()
    checker = make_checker(on_status_change=recorder)

    # Act
    await start_checker(checker, [health_server.target])
    await until(lambda: servicer.probe_count >= 4, message=f"only {servicer.probe_count} probes arrived")

    # Assert
    # Four identical verdicts, one announcement: the callback reports changes, not checks.
    assert recorder.changes == [(health_server.target, True)]
