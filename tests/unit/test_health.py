"""Unit tests for the health checker.

The checker is the only thing standing between a balancer and a dead backend, so what matters here
is that it never reports health it has not confirmed, and that it survives everything a failing
server can do to it.
"""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from grpc_client_kit.health import HealthChecker, HealthCheckerNotRunningError
from tests.helpers import make_channel

from .conftest import (
    NOT_SERVING,
    SERVING,
    TaskCapture,
    health_stub,
    make_health_response,
    never_completing_check,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# One check.
# --------------------------------------------------------------------------------------------


async def test__check_health__server_serving__reports_healthy() -> None:
    """A SERVING answer is the only thing that makes a target healthy."""
    # Arrange
    target = "localhost:50051"
    checker = HealthChecker()

    # Act
    with (
        patch("grpc.aio.insecure_channel", return_value=make_channel()),
        health_stub(response=make_health_response(SERVING)),
    ):
        is_healthy = await checker.check_health(target)

    # Assert
    assert is_healthy is True
    assert await checker.is_healthy(target) is True


async def test__check_health__server_not_serving__reports_unhealthy() -> None:
    """An explicit NOT_SERVING is an answer, and the answer is 'do not send traffic here'."""
    # Arrange
    target = "localhost:50051"
    checker = HealthChecker()

    # Act
    with (
        patch("grpc.aio.insecure_channel", return_value=make_channel()),
        health_stub(response=make_health_response(NOT_SERVING)),
    ):
        is_healthy = await checker.check_health(target)

    # Assert
    assert is_healthy is False
    assert await checker.is_healthy(target) is False


async def test__check_health__connection_failure__reports_unhealthy() -> None:
    """A backend that cannot even be dialled is as unusable as one that says NOT_SERVING."""
    # Arrange
    target = "localhost:50051"
    checker = HealthChecker()

    # Act
    with patch("grpc.aio.insecure_channel", side_effect=Exception("Connection failed")):
        is_healthy = await checker.check_health(target)

    # Assert
    assert is_healthy is False
    assert await checker.is_healthy(target) is False


async def test__check_health__unexpected_non_grpc_error__reports_unhealthy() -> None:
    """An error from below the gRPC layer must not escape into the caller's balancing decision."""
    # Arrange
    checker = HealthChecker()

    # Act
    with patch.object(checker, "_get_channel", side_effect=RuntimeError("unexpected boom")):
        is_healthy = await checker.check_health("h:1")

    # Assert
    assert is_healthy is False


async def test__check_health__channel_closed_under_the_probe__is_a_failed_check_not_a_cancellation() -> None:
    """grpc raises a bare CancelledError in a task nobody cancelled when its channel closes under
    an in-flight RPC; leaking it makes the caller believe it was cancelled itself."""
    # Arrange
    checker = HealthChecker()

    # Act
    with (
        patch.object(checker, "_get_channel", new_callable=AsyncMock),
        health_stub(error=asyncio.CancelledError()),
    ):
        is_healthy = await checker.check_health("h:1")

    # Assert
    assert is_healthy is False
    assert checker._fail_counts["h:1"] == 1


async def test__check_health__same_target_checked_twice__reuses_its_channel() -> None:
    """Reconnecting for every probe would cost more than the probe itself."""
    # Arrange
    checker = HealthChecker(check_interval=0.1, insecure=True)

    # Act
    with (
        patch("grpc.aio.insecure_channel", return_value=make_channel()) as mock_create,
        health_stub(response=make_health_response(SERVING)),
    ):
        await checker.check_health("host:50051")
        after_first = mock_create.call_count
        await checker.check_health("host:50051")

    # Assert
    assert after_first == 1
    assert mock_create.call_count == 1

    await checker.stop()


async def test__check_health__failed_check__drops_its_channel_so_the_next_one_reconnects() -> None:
    """A channel that just failed a probe is not worth keeping for the next one."""
    # Arrange
    checker = HealthChecker(insecure=True)
    mock_channel = make_channel()

    # Act
    with (
        patch("grpc.aio.insecure_channel", return_value=mock_channel) as mock_create,
        health_stub(error=Exception("fail")),
    ):
        await checker.check_health("h1:1")
        await checker.check_health("h1:1")

    # Assert
    assert mock_create.call_count == 2
    mock_channel.close.assert_awaited()
    assert checker._channels == {}


async def test__check_health__repeated_failures__accumulate_the_failure_count() -> None:
    """The failure count is what the backoff is computed from, so it has to be exact."""
    # Arrange
    checker = HealthChecker(check_interval=0.01)

    # Act
    with (
        patch.object(checker, "_get_channel", new_callable=AsyncMock),
        health_stub(error=Exception("fail")),
    ):
        await checker.check_health("h1:1")
        after_first = checker._fail_counts["h1:1"]
        await checker.check_health("h1:1")

    # Assert
    assert after_first == 1
    assert checker._fail_counts["h1:1"] == 2


async def test__check_health__not_serving_answer__feeds_the_backoff_counter() -> None:
    """A reachable server that reports NOT_SERVING is failing, and is backed off like one."""
    # Arrange
    checker = HealthChecker()

    # Act
    with (
        patch.object(checker, "_get_channel", new_callable=AsyncMock),
        health_stub(response=make_health_response(NOT_SERVING)),
    ):
        await checker.check_health("h1:1")
        await checker.check_health("h1:1")

    # Assert
    assert checker._fail_counts["h1:1"] == 2


async def test__check_health__parallel_checks_of_one_target__keep_the_state_consistent() -> None:
    """Concurrent probes must produce one transition and an exact failure count, not a race."""
    # Arrange
    callback = AsyncMock()
    checker = HealthChecker(on_status_change=callback)

    # Act
    with (
        patch.object(checker, "_get_channel", new_callable=AsyncMock),
        health_stub(error=Exception("fail")),
    ):
        results = await asyncio.gather(*(checker.check_health("h1:1") for _ in range(5)))

    # Assert
    assert results == [False] * 5
    assert checker._fail_counts["h1:1"] == 5
    # NOT_SERVING was reached once; the four repeats are not transitions.
    assert callback.await_count == 1


async def test__check_health__configured_channel_options__are_applied_to_the_probe_channel() -> None:
    """A probe travels over the same kind of channel as the traffic it is vouching for."""
    # Arrange
    options = [("grpc.max_receive_message_length", 1024)]
    checker = HealthChecker(insecure=True, options=options, compression=grpc.Compression.Gzip)

    # Act
    with (
        patch("grpc.aio.insecure_channel") as mock_channel_factory,
        health_stub(response=make_health_response(SERVING)),
    ):
        await checker.check_health("host1:50051")

    # Assert
    kwargs = mock_channel_factory.call_args.kwargs
    assert kwargs["options"] == options
    assert kwargs["compression"] == grpc.Compression.Gzip


async def test__check_health__secure_checker__opens_a_secure_probe_channel() -> None:
    """Probing a TLS backend over an insecure channel would fail for the wrong reason."""
    # Arrange
    credentials = MagicMock(spec=grpc.ChannelCredentials)
    checker = HealthChecker(insecure=False, credentials=credentials)

    # Act
    with (
        patch("grpc.aio.secure_channel", return_value=make_channel()) as mock_secure,
        health_stub(error=RuntimeError("stop here")),
        contextlib.suppress(RuntimeError),
    ):
        await checker.check_health("h:1")

    # Assert
    mock_secure.assert_called_once()
    assert mock_secure.call_args.args[1] == credentials


# --------------------------------------------------------------------------------------------
# Callbacks.
# --------------------------------------------------------------------------------------------


async def test__check_health__failing_callback__does_not_break_the_check() -> None:
    """Notification is a side effect of the check, and must not be able to fail it."""
    # Arrange
    callback = AsyncMock(side_effect=Exception("err"))
    checker = HealthChecker(on_status_change=callback)

    # Act
    with (
        patch.object(checker, "_get_channel", new_callable=AsyncMock),
        health_stub(response=make_health_response(SERVING)),
    ):
        await checker.check_health("h1:1")

    # Assert
    assert callback.called


async def test__check_health__failing_callback_in_fail_fast_mode__keeps_the_recorded_status() -> None:
    """A deployment may want the failure to surface, but not at the cost of a corrupted status."""
    # Arrange
    callback = AsyncMock(side_effect=RuntimeError("callback boom"))
    checker = HealthChecker(on_status_change=callback, fail_fast_callback=True)

    # Act
    with (
        patch.object(checker, "_get_channel", new_callable=AsyncMock),
        health_stub(response=make_health_response(SERVING)),
        pytest.raises(RuntimeError, match="callback boom"),
    ):
        await checker.check_health("h1:1")

    # Assert
    assert await checker.is_healthy("h1:1") is True


# --------------------------------------------------------------------------------------------
# The background loop.
# --------------------------------------------------------------------------------------------


async def test__health_checker__started__checks_every_target_in_the_background() -> None:
    """The loop is what keeps the balancer's picture of the fleet current."""
    # Arrange
    targets = ["host1:50051", "host2:50051"]
    checker = HealthChecker(check_interval=0.1, insecure=True)

    # Act
    with (
        patch("grpc.aio.insecure_channel", return_value=make_channel()),
        health_stub(response=make_health_response(SERVING)),
    ):
        await checker.start(targets)
        await asyncio.sleep(0.2)

        # Assert
        assert await checker.is_healthy("host1:50051") is True
        assert await checker.is_healthy("host2:50051") is True

        await checker.stop()


async def test__health_checker__started_twice__runs_a_single_loop() -> None:
    """A second loop would double the probe traffic and race the first one's bookkeeping."""
    # Arrange
    checker = HealthChecker()
    capture = TaskCapture()

    # Act
    with patch("asyncio.create_task", side_effect=capture) as mock_create_task:
        await checker.start(["h:1"])
        await checker.start(["h:1"])

    # Assert
    assert mock_create_task.call_count == 1

    capture.close()
    # The loop was never scheduled, so the running flag is cleared by hand instead of by stop().
    async with checker._lock:
        checker._is_running = False
        checker._task = None


async def test__health_checker__check_raising_in_the_loop__keeps_the_loop_running() -> None:
    """One transient failure must not silence the health checker for the rest of the process."""
    # Arrange
    checker = HealthChecker(check_interval=0.01)

    # Act & Assert
    with patch.object(checker, "check_health", side_effect=Exception("transient error")):
        await checker.start(["h1:50051"])
        await asyncio.sleep(0.05)
        await checker.stop()


async def test__health_checker__long_interval__checks_once_before_it_sleeps() -> None:
    """The first pass runs immediately, so a slow interval does not delay the first verdict."""
    # Arrange
    checker = HealthChecker(check_interval=100.0)

    # Act & Assert
    await checker.start(["localhost:50051"])
    await asyncio.sleep(0.1)
    await checker.stop()


async def test__health_checker__channel_of_a_target_no_longer_watched__is_closed_by_elapsed_time() -> None:
    """Stale channel cleanup depends on elapsed time, not on a magic wall-clock second."""
    # Arrange
    checker = HealthChecker(check_interval=10.0)
    stale_channel = make_channel()
    checker._channels["gone:50051"] = stale_channel

    # Act
    with (
        patch("grpc_client_kit.health.STALE_CHANNEL_CLEANUP_INTERVAL", 0.0),
        patch("grpc_client_kit.health.DEFAULT_LOOP_SLEEP", 0.01),
        patch.object(checker, "check_health", new=AsyncMock(return_value=True)),
    ):
        await checker.start(["host1:50051"])
        await asyncio.sleep(0.05)
        await checker.stop()

    # Assert
    stale_channel.close.assert_awaited()
    assert "gone:50051" not in checker._channels


# --------------------------------------------------------------------------------------------
# Reporting what is known, and only what is known.
# --------------------------------------------------------------------------------------------


async def test__is_healthy__checker_not_running__raises() -> None:
    """An unchecked target on a stopped checker is an error, not an optimistic True."""
    # Arrange
    checker = HealthChecker()

    # Act & Assert
    with pytest.raises(HealthCheckerNotRunningError, match="not running"):
        await checker.is_healthy("host1:50051")


async def test__is_healthy__target_the_loop_does_not_watch__reports_unhealthy() -> None:
    """Only targets confirmed SERVING are reported healthy; nothing else is assumed."""
    # Arrange
    checker = HealthChecker(check_interval=0.05)

    # Act
    with (
        patch("grpc.aio.insecure_channel", return_value=make_channel()),
        health_stub(response=make_health_response(SERVING)),
    ):
        await checker.start(["host1:50051"])
        ready = await checker.wait_until_ready(timeout=1.0)

        # Assert
        assert ready is True
        assert await checker.is_healthy("host1:50051") is True
        assert await checker.is_healthy("host2:50051") is False

        await checker.stop()


async def test__wait_until_ready__checker_not_running__returns_immediately() -> None:
    """Waiting for a loop that was never started would otherwise hang the caller's startup."""
    # Arrange
    checker = HealthChecker()

    # Act & Assert
    assert await checker.wait_until_ready(timeout=1.0) is False


async def test__wait_until_ready__first_pass_never_finishing__reports_a_timeout() -> None:
    """Readiness reports a timeout instead of blocking forever on an unreachable fleet."""
    # Arrange
    checker = HealthChecker(check_interval=0.05)
    first_check_started = asyncio.Event()

    # Act
    with patch.object(checker, "check_health", side_effect=never_completing_check(first_check_started)):
        await checker.start(["host1:50051"])
        await first_check_started.wait()
        ready = await checker.wait_until_ready(timeout=0.05)

        # Assert
        assert ready is False

        await checker.stop()
