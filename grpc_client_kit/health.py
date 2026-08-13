from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import grpc.aio
from grpc_health.v1 import health_pb2, health_pb2_grpc

from .channel import ChannelPool
from .errors import HealthCheckerNotRunningError
from .protocols import (
    HealthCheckerProtocol,
    HealthStatusCallbackProtocol,
)
from .utils import create_aio_channel
from .validation import validate_target

logger = logging.getLogger(__name__)

DEFAULT_CHECK_INTERVAL = 30.0
DEFAULT_TIMEOUT = 5.0
MAX_BACKOFF = 300.0  # 5 minutes
DEFAULT_LOOP_SLEEP = 1.0
STALE_CHANNEL_CLEANUP_INTERVAL = 300.0  # 5 minutes


class HealthChecker(HealthCheckerProtocol):
    """Monitors target health via the gRPC health protocol.

    Features:
    - Concurrent health checks for multiple targets.
    - Persistent gRPC channels for efficiency.
    - Exponential backoff for failed targets.
    - Status change callbacks.

    Cold start:
        A target is healthy only once a check has said so. Targets that have never been
        checked are reported as unhealthy, so a load balancer cannot fan traffic out to
        addresses nobody has probed yet, and asking a checker that was never started
        raises :class:`HealthCheckerNotRunningError` instead of silently claiming health.
        Call :meth:`wait_until_ready` after :meth:`start` to await the first pass.
    """

    @property
    def is_running(self) -> bool:
        """Check if health checker is currently running."""
        return self._is_running

    def __init__(
        self,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        timeout: float = DEFAULT_TIMEOUT,
        max_backoff: float = MAX_BACKOFF,
        on_status_change: HealthStatusCallbackProtocol | None = None,
        insecure: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        pool: ChannelPool | None = None,
        fail_fast_callback: bool = False,
        options: list[tuple[str, Any]] | None = None,
        compression: grpc.Compression | None = None,
        service: str = "",
    ) -> None:
        """Initialize the health checker.

        Args:
            check_interval: Seconds between checks for healthy targets.
            timeout: Timeout in seconds for each health check RPC.
            max_backoff: Maximum seconds to wait between checks for a failed target.
            on_status_change: Optional callback for status changes.
            insecure: Whether to use insecure channels for health checks.
            credentials: Optional TLS credentials for secure health checks.
            pool: Optional ChannelPool to automatically update channel health.
            fail_fast_callback: Whether to raise if the callback fails.
            options: gRPC channel options for health check channels. Pass the same options
                the application channels use, so both negotiate HTTP/2 identically.
            compression: Compression for health check channels.
            service: Service name probed via the health protocol. The default ``""`` asks about
                the server as a whole; naming a service asks about that service specifically,
                which is half the point of the standard health protocol.
        """
        self._check_interval = check_interval
        self._timeout = timeout
        self._max_backoff = max_backoff
        self._on_status_change = on_status_change
        self._insecure = insecure
        self._credentials = credentials
        self._pool = pool
        self._fail_fast_callback = fail_fast_callback
        self._options = options
        self._compression = compression
        self._service = service

        self._health_status: dict[str, health_pb2.HealthCheckResponse.ServingStatus] = {}
        self._last_checked: dict[str, float] = {}
        self._fail_counts: dict[str, int] = {}
        self._channels: dict[str, grpc.aio.Channel] = {}

        self._lock = asyncio.Lock()
        # Separate leaf lock for the status maps: checks run concurrently within a tick, and
        # "read previous status, write new one, decide whether to notify" must be one step.
        # Never acquire self._lock while holding it.
        self._status_lock = asyncio.Lock()
        self._first_pass_done = asyncio.Event()
        self._warned_not_running = False
        self._is_running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self, targets: list[str]) -> None:
        """Start background health checking for the given targets."""
        for t in targets:
            validate_target(t)

        async with self._lock:
            if self._is_running:
                logger.warning("HealthChecker is already running. Ignoring new start() call.")
                return

            self._first_pass_done.clear()
            self._is_running = True
            self._task = asyncio.create_task(self._health_check_loop(list(targets)))

    async def stop(self, timeout: float = 5.0) -> None:
        """Stop background health checking and cleanup resources.

        Args:
            timeout: Maximum time to wait for graceful shutdown (default 5s)
        """
        async with self._lock:
            if not self._is_running:
                return
            self._is_running = False
            task = self._task

        if task:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except (asyncio.CancelledError, TimeoutError):
                logger.debug("Health checker task stopped")
            except Exception:
                logger.exception("Error stopping health checker task")

            async with self._lock:
                self._task = None

        # Close all health check channels
        async with self._lock:
            if self._channels:
                await asyncio.gather(*(ch.close() for ch in self._channels.values()), return_exceptions=True)
                self._channels.clear()

    async def wait_until_ready(self, timeout: float | None = None) -> bool:
        """Wait until the first check pass has classified every monitored target.

        Until that pass completes, every target is reported unhealthy, so callers that
        route traffic immediately after :meth:`start` should await this first.

        Args:
            timeout: Maximum seconds to wait. None waits indefinitely.

        Returns:
            True if the first pass completed, False if it timed out or the loop is not running.
        """
        if not self._is_running:
            return False

        try:
            await asyncio.wait_for(self._first_pass_done.wait(), timeout=timeout)
        except TimeoutError:
            logger.warning("Timed out after %ss waiting for the first health check pass", timeout)
            return False

        return True

    async def _get_channel(self, target: str) -> grpc.aio.Channel:
        """Get or create a persistent channel for health checks.

        Probes run on their own channels instead of borrowing one from the ChannelPool:
        pooled channels carry the client interceptor chain (retries, circuit breaker,
        metrics), so probing through them would pollute client metrics and trip breakers;
        they are evicted once idle, while monitoring must survive idle periods; and a failed
        probe closes its channel to force a reconnect, which must never happen to a channel
        that application RPCs are holding. Channel options and compression are taken from
        the settings so both connections negotiate HTTP/2 the same way.

        This method is async-safe and ensures only one channel is created per target.
        """
        async with self._lock:
            if target in self._channels:
                return self._channels[target]

            # Channel creation in grpc.aio is lightweight and does not perform I/O
            # so we can safely do it inside the lock to simplify the implementation
            # and avoid double-check complexity.
            channel = create_aio_channel(
                target,
                insecure=self._insecure,
                credentials=self._credentials,
                options=self._options,
                compression=self._compression,
            )

            self._channels[target] = channel
            return channel

    async def _drop_channel(self, target: str) -> None:
        """Close and forget the health check channel for a target."""
        async with self._lock:
            channel = self._channels.pop(target, None)

        if channel:
            try:
                await channel.close()
            except Exception:
                logger.exception("Failed to close health check channel for %s", target)

    async def _close_stale_channels(self, targets: list[str]) -> None:
        """Close channels held for targets that are no longer monitored."""
        monitored = set(targets)

        async with self._lock:
            stale = [t for t in self._channels if t not in monitored]
            channels = [self._channels.pop(t) for t in stale]

        for target, channel in zip(stale, channels, strict=True):
            try:
                await channel.close()
            except Exception:
                logger.exception("Failed to close stale health check channel for %s", target)

    async def _health_check_loop(self, targets: list[str]) -> None:
        """Concurrent health check loop with individual backoffs."""
        # Scheduling runs on the monotonic clock: wall-clock jumps (NTP, DST) would otherwise
        # stall every target's next check or stampede them all at once.
        next_check: dict[str, float] = dict.fromkeys(targets, 0.0)
        next_cleanup = time.monotonic() + STALE_CHANNEL_CLEANUP_INTERVAL

        try:
            while self._is_running:
                now = time.monotonic()
                to_check = [t for t in targets if now >= next_check[t]]

                if to_check:
                    # Perform checks concurrently
                    results = await asyncio.gather(*(self.check_health(t) for t in to_check), return_exceptions=True)

                    async with self._status_lock:
                        fail_counts = dict(self._fail_counts)

                    completed_at = time.monotonic()
                    for target, result in zip(to_check, results, strict=True):
                        if result is True:
                            # Success: use standard interval
                            next_check[target] = completed_at + self._check_interval
                            continue

                        # Failure: apply exponential backoff
                        if isinstance(result, BaseException):
                            logger.error("Unexpected error checking %s: %s", target, result)

                        fails = fail_counts.get(target, 0)
                        # Exponential backoff: check_interval * 2^fails (capped at _max_backoff)
                        backoff = min(self._max_backoff, self._check_interval * (2 ** max(0, fails - 1)))
                        next_check[target] = completed_at + backoff
                        logger.debug("Health check failed for %s, next check in %.1fs", target, backoff)

                # Every target now has a verdict, so is_healthy() answers from evidence.
                self._first_pass_done.set()

                if time.monotonic() >= next_cleanup:
                    next_cleanup = time.monotonic() + STALE_CHANNEL_CLEANUP_INTERVAL
                    await self._close_stale_channels(targets)

                # Wait for the next scheduling round. The loop tick is the floor of the effective
                # probe period: an interval shorter than the tick would silently stretch to it,
                # so the tick follows the interval down instead.
                await asyncio.sleep(min(DEFAULT_LOOP_SLEEP, self._check_interval))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Fatal error in health check loop")
        finally:
            self._is_running = False

    async def check_health(self, target: str) -> bool:
        """Perform an active health check RPC for the target.

        Args:
            target: The target address (host:port).

        Returns:
            True if the target is SERVING, False otherwise.

        Raises:
            Exception: Whatever the status change callback raised, if the checker was
                built with `fail_fast_callback=True`.
        """
        try:
            channel = await self._get_channel(target)
            stub = health_pb2_grpc.HealthStub(channel)

            response = await stub.Check(
                health_pb2.HealthCheckRequest(service=self._service),
                timeout=self._timeout,
            )
        except asyncio.CancelledError:
            # A channel closed under an in-flight probe — a concurrent stop(), typically — makes
            # grpc raise a bare CancelledError in a task nobody cancelled. Swallowing it for a task
            # that *was* cancelled would break cancellation, so only the phantom form is treated as
            # a failed check.
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise
            logger.debug("Health check for %s was cancelled by its channel closing", target)
            await self._drop_channel(target)
            await self._publish(target, health_pb2.HealthCheckResponse.NOT_SERVING)
            return False
        except Exception as e:
            if isinstance(e, grpc.aio.AioRpcError):
                logger.debug("Health check failed for %s: %s", target, e.code())
            else:
                logger.warning("Health check failed for %s with unexpected error: %s", target, e)

            # The channel may be stuck half-open; drop it so the next check reconnects.
            # Done before publishing, which may re-raise a failing callback.
            await self._drop_channel(target)
            await self._publish(target, health_pb2.HealthCheckResponse.NOT_SERVING)
            return False

        await self._publish(target, response.status)
        return bool(response.status == health_pb2.HealthCheckResponse.SERVING)

    async def _publish(self, target: str, status: health_pb2.HealthCheckResponse.ServingStatus) -> None:
        """Record a check result and fan it out to the pool and the status callback."""
        is_healthy = bool(status == health_pb2.HealthCheckResponse.SERVING)

        async with self._status_lock:
            changed = self._health_status.get(target) != status
            self._health_status[target] = status
            # Wall clock: this timestamp is only ever read by humans inspecting the checker.
            self._last_checked[target] = time.time()
            self._fail_counts[target] = 0 if is_healthy else self._fail_counts.get(target, 0) + 1

        if self._pool:
            await self._pool.update_channel_health(target, is_healthy)

        if changed and self._on_status_change:
            try:
                await self._on_status_change(target, is_healthy)
            except Exception:
                logger.exception("Health status callback failed for %s", target)
                if self._fail_fast_callback:
                    raise

    async def is_healthy(self, target: str) -> bool:
        """Check if the target is healthy from cache.

        Only evidence counts: a target is healthy after a check saw it SERVING, and
        unhealthy until then, so an unchecked target never receives traffic.

        Args:
            target: The target address (host:port).

        Returns:
            True if the last check reported SERVING, False otherwise.

        Raises:
            HealthCheckerNotRunningError: If the target has no recorded status and the
                check loop is not running, so no status will ever be recorded.
        """
        async with self._status_lock:
            status = self._health_status.get(target)

        if status is not None:
            return bool(status == health_pb2.HealthCheckResponse.SERVING)

        if not self._is_running:
            self._warn_not_running()
            raise HealthCheckerNotRunningError(target)

        return False

    def _warn_not_running(self) -> None:
        """Log the missing check loop once, since callers may swallow the error.

        Load balancers collect health with `return_exceptions=True`, which would otherwise
        turn a checker that was never started into a silent "no healthy targets".
        """
        if self._warned_not_running:
            return

        self._warned_not_running = True
        logger.warning(
            "HealthChecker.is_healthy() was called while the check loop is not running: "
            "no target can be confirmed healthy. Call start() to begin monitoring."
        )


__all__ = [
    "DEFAULT_CHECK_INTERVAL",
    "DEFAULT_TIMEOUT",
    "MAX_BACKOFF",
    "STALE_CHANNEL_CLEANUP_INTERVAL",
    "HealthChecker",
    "HealthCheckerNotRunningError",
]
