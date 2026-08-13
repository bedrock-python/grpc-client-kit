from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4
from weakref import WeakKeyDictionary

import grpc.aio

from .protocols import (
    ChannelProviderProtocol,
    GrpcClientMetricsProtocol,
    HealthCheckerProtocol,
)
from .utils import create_aio_channel
from .validation import validate_target

logger = logging.getLogger(__name__)

DEFAULT_MAX_CHANNELS_PER_TARGET = 1
DEFAULT_IDLE_TIMEOUT = 300.0  # 5 minutes

# Idle management is delegated to gRPC core: past this timeout without active RPCs the channel
# parks its connection (goes IDLE) and transparently reconnects on the next call. The pool must
# never close idle channels itself — a channel object handed to a caller stays valid for life,
# and an in-flight RPC can never be killed by idleness (core only idles when there are none).
_IDLE_OPTION = "grpc.client_idle_timeout_ms"

# Each interceptor instance is tagged once and keeps its tag for life. A pooled channel holds its
# interceptors alive, so a tag can never be reassigned to a different object the way id() is after
# garbage collection — and the pool outlives the clients that filled it.
_INTERCEPTOR_TOKENS: WeakKeyDictionary[grpc.aio.ClientInterceptor, str] = WeakKeyDictionary()


def _interceptor_token(interceptor: grpc.aio.ClientInterceptor) -> str:
    """Return the stable token of a single interceptor, minting it on first use.

    Args:
        interceptor: The interceptor instance to identify.

    Returns:
        A token that lives exactly as long as the interceptor instance.
    """
    token = _INTERCEPTOR_TOKENS.get(interceptor)
    if token is None:
        token = uuid4().hex
        _INTERCEPTOR_TOKENS[interceptor] = token
    return token


def chain_token(interceptors: list[grpc.aio.ClientInterceptor] | None) -> str | None:
    """Return a stable identity for an interceptor chain.

    The token changes only when the chain's members or their order change, so a client that reuses
    its chain keeps hitting the same pooled channel, while a client with a different chain never
    inherits someone else's.

    Args:
        interceptors: The chain to identify, in call order.

    Returns:
        A token identifying the chain, or None when the chain is empty (a channel built without
        interceptors is interchangeable with any other such channel).
    """
    if not interceptors:
        return None
    return "|".join(_interceptor_token(interceptor) for interceptor in interceptors)


@dataclass(frozen=True, slots=True)
class ChannelKey:
    """Full identity of a pooled channel.

    Two callers may share a channel only when every field matches: gRPC binds credentials, options,
    compression and interceptors to a channel at creation time and none of them can be changed or
    added afterwards, so a channel built for one combination cannot serve another.

    Attributes:
        target: The target address (host:port).
        insecure: Whether the channel is insecure.
        credentials: TLS credentials, compared by identity because gRPC credentials define no
            equality — reuse one credentials object instead of rebuilding it per call.
        options: gRPC channel options, normalized to a tuple and compared by value.
        compression: The channel compression setting.
        interceptors_token: Stable identity of the interceptor chain, or None when there is none.
    """

    target: str
    insecure: bool
    credentials: grpc.ChannelCredentials | None = None
    options: tuple[tuple[str, Any], ...] | None = None
    compression: grpc.Compression | None = None
    interceptors_token: str | None = None

    @classmethod
    def build(
        cls,
        target: str,
        insecure: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        options: list[tuple[str, Any]] | None = None,
        compression: grpc.Compression | None = None,
        interceptors: list[grpc.aio.ClientInterceptor] | None = None,
    ) -> ChannelKey:
        """Build a key from the arguments of a channel request.

        Args:
            target: The target address (host:port).
            insecure: Whether to use an insecure channel.
            credentials: Optional TLS credentials for a secure channel.
            options: Optional gRPC channel options.
            compression: Optional gRPC compression setting.
            interceptors: Optional interceptor chain to bind to the channel.

        Returns:
            The hashable identity of the requested channel.
        """
        return cls(
            target=target,
            insecure=insecure,
            credentials=credentials,
            options=tuple(options) if options is not None else None,
            compression=compression,
            interceptors_token=chain_token(interceptors),
        )


@dataclass(slots=True)
class ChannelWrapper:
    """Wrapper for a gRPC channel with health status."""

    channel: grpc.aio.Channel
    target: str
    is_healthy: bool = True


@dataclass(slots=True)
class _PoolEntry:
    """Everything the pool keeps for one channel identity.

    Attributes:
        lock: Guards ``channels`` and ``index``.
        channels: Channels created for this identity.
        index: Cursor for round-robin selection among healthy channels.
        users: Number of coroutines that checked the entry out. An entry is only ever dropped at
            zero, so a coroutine can never end up holding a lock the pool has already replaced.
        active: Cleared when the entry leaves the pool, so a late waiter does not resurrect it.
    """

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    channels: list[ChannelWrapper] = field(default_factory=list)
    index: int = 0
    users: int = 0
    active: bool = True


class ChannelPool(ChannelProviderProtocol):
    """Advanced channel pool for gRPC channels.

    Channels are pooled by their full identity (see :class:`ChannelKey`), not by target alone:
    asking for ``host:1`` over TLS never returns the insecure channel someone else opened for the
    same address, and a client never inherits another client's interceptor chain.

    Async-safe through per-identity locking to minimize contention and prevent deadlocks.
    """

    def __init__(
        self,
        max_channels_per_target: int = DEFAULT_MAX_CHANNELS_PER_TARGET,
        idle_timeout: float = DEFAULT_IDLE_TIMEOUT,
        health_checker: HealthCheckerProtocol | None = None,
        metrics: GrpcClientMetricsProtocol | None = None,
    ) -> None:
        """Initialize the channel pool.

        Args:
            max_channels_per_target: Maximum number of concurrent channels to keep per identity.
            idle_timeout: Seconds without active RPCs after which a channel parks its connection.
                Enforced by gRPC core (``grpc.client_idle_timeout_ms``), never by the pool closing
                channels: a parked channel reconnects transparently on the next call, so held stubs
                and long streams survive any idle period. ``0`` or less disables idling.
            health_checker: Optional health checker for monitoring target health.
            metrics: Optional metrics registry for pool statistics.

        Raises:
            ValueError: If max_channels_per_target is not positive.
        """
        if max_channels_per_target <= 0:
            raise ValueError("max_channels_per_target must be positive")

        self._max_channels_per_target = max_channels_per_target
        self._idle_timeout = idle_timeout
        self._health_checker = health_checker
        self._metrics = metrics

        self._entries: dict[ChannelKey, _PoolEntry] = {}
        self._pool_lock = asyncio.Lock()
        self._closing = False

    async def _checkout(self, key: ChannelKey) -> _PoolEntry:
        """Get or create the entry for a key and mark it as in use.

        Args:
            key: The channel identity.

        Returns:
            The entry, whose lock the caller must take before touching its channels.
        """
        async with self._pool_lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _PoolEntry()
                self._entries[key] = entry
            entry.users += 1
            return entry

    async def _checkin(self, key: ChannelKey, entry: _PoolEntry) -> None:
        """Release an entry and drop it if it became empty and unused.

        Args:
            key: The channel identity the entry was checked out under.
            entry: The entry to release.
        """
        async with self._pool_lock:
            entry.users -= 1
            # Nobody holds the entry at zero users, so reading `channels` without its lock is safe
            # and no coroutine can be waiting on the lock we are about to discard.
            if entry.users == 0 and not entry.channels and self._entries.get(key) is entry:
                entry.active = False
                del self._entries[key]

    def _update_pool_metrics(self) -> None:
        """Update pool usage metrics if available."""
        if not self._metrics:
            return

        total_channels = sum(len(entry.channels) for entry in self._entries.values())

        try:
            self._metrics.record_pool_stats(
                active_channels=total_channels,
                # One target can back several identities, so report distinct addresses.
                idle_targets=len({key.target for key in self._entries}),
            )
        except Exception:
            logger.exception("Failed to record pool metrics")

    def make_key(
        self,
        target: str,
        insecure: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        options: list[tuple[str, Any]] | None = None,
        compression: grpc.Compression | None = None,
        interceptors: list[grpc.aio.ClientInterceptor] | None = None,
    ) -> ChannelKey:
        """Validate a channel request and build its pool identity.

        The result is safe to cache and hand back through ``get_channel(key=...)``: a client whose
        target, configuration and chain never change should pay for validation and identity
        hashing once, not on every call.

        Args:
            target: The target address (host:port).
            insecure: Whether to use an insecure channel.
            credentials: Optional TLS credentials for a secure channel.
            options: Optional gRPC channel options.
            compression: Optional gRPC compression setting.
            interceptors: Optional interceptor chain to bind to the channel.

        Returns:
            The hashable identity the pool files this request under.
        """
        validate_target(target)
        return ChannelKey.build(
            target,
            insecure=insecure,
            credentials=credentials,
            options=self._with_idle_option(options),
            compression=compression,
            interceptors=interceptors,
        )

    def _with_idle_option(self, options: list[tuple[str, Any]] | None) -> list[tuple[str, Any]] | None:
        """Merge the pool's idle timeout into channel options, letting an explicit value win."""
        if self._idle_timeout <= 0:
            return options
        if options is not None and any(name == _IDLE_OPTION for name, _ in options):
            return options
        merged: list[tuple[str, Any]] = list(options) if options is not None else []
        merged.append((_IDLE_OPTION, int(self._idle_timeout * 1000)))
        return merged

    async def get_channel(
        self,
        target: str,
        insecure: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        options: list[tuple[str, Any]] | None = None,
        compression: grpc.Compression | None = None,
        interceptors: list[grpc.aio.ClientInterceptor] | None = None,
        key: ChannelKey | None = None,
    ) -> grpc.aio.Channel:
        """Get or create a gRPC channel for the requested channel identity.

        Implements round-robin selection among healthy channels of that identity. If all channels
        are unhealthy or the limit is reached, it will either create a new channel or return an
        existing one as a fallback.

        Args:
            target: The target address (host:port).
            insecure: Whether to use an insecure channel.
            credentials: Optional TLS credentials for a secure channel.
            options: Optional gRPC channel options.
            compression: Optional gRPC compression setting.
            interceptors: Optional list of interceptors to bind to the channel.
            key: Precomputed identity from :meth:`make_key`. When given, the target is not
                re-validated and the identity is not rebuilt; the other arguments must be the ones
                the key was built from.

        Returns:
            An async gRPC channel.

        Raises:
            RuntimeError: If the pool is closing.
        """
        if self._closing:
            raise RuntimeError("ChannelPool is closing")

        if key is None:
            key = self.make_key(
                target,
                insecure=insecure,
                credentials=credentials,
                options=options,
                compression=compression,
                interceptors=interceptors,
            )

        entry = await self._checkout(key)
        try:
            async with entry.lock:
                # close_all() may have detached this entry while we waited for the lock; handing
                # out a channel the pool no longer tracks would leak it past shutdown.
                if self._closing or not entry.active:
                    raise RuntimeError("ChannelPool is closing")

                wrapper = self._select_channel(entry)
                if wrapper is None:
                    wrapper = self._add_channel(entry, key, interceptors)
        finally:
            await self._checkin(key, entry)

        return wrapper.channel

    def _select_channel(self, entry: _PoolEntry) -> ChannelWrapper | None:
        """Pick the next healthy channel of an entry in round-robin order.

        Args:
            entry: The entry to select from; its lock must be held.

        Returns:
            The selected channel wrapper, or None if the entry has no healthy channel.
        """
        # The flag is written through the public update_channel_health seam, so it is honoured
        # regardless of whether this pool owns a checker — an external checker counts too.
        healthy = [w for w in entry.channels if w.is_healthy]
        if not healthy:
            return None

        position = entry.index % len(healthy)
        entry.index = (position + 1) % len(healthy)
        return healthy[position]

    def _add_channel(
        self,
        entry: _PoolEntry,
        key: ChannelKey,
        interceptors: list[grpc.aio.ClientInterceptor] | None,
    ) -> ChannelWrapper:
        """Grow an entry by one channel, or fall back to an existing one at the limit.

        The channel is built from the key it is filed under, so what the pool promises and what
        gRPC was asked for cannot drift apart.

        Args:
            entry: The entry to grow; its lock must be held.
            key: The identity of the channel to create.
            interceptors: The chain the key's token stands for.

        Returns:
            The channel wrapper to serve this request with.
        """
        if len(entry.channels) >= self._max_channels_per_target:
            # Every channel is unhealthy and the limit is reached: serve the oldest one anyway, it
            # may have recovered since the last health check and is better than no channel at all.
            return entry.channels[0]

        logger.info("Creating new gRPC channel for target: %s", key.target)
        channel = self._create_aio_channel(
            key.target,
            insecure=key.insecure,
            credentials=key.credentials,
            options=list(key.options) if key.options is not None else None,
            compression=key.compression,
            interceptors=interceptors,
        )
        wrapper = ChannelWrapper(channel=channel, target=key.target)
        entry.channels.append(wrapper)
        self._update_pool_metrics()
        return wrapper

    def _create_aio_channel(
        self,
        target: str,
        insecure: bool = False,
        credentials: grpc.ChannelCredentials | None = None,
        options: list[tuple[str, Any]] | None = None,
        compression: grpc.Compression | None = None,
        interceptors: list[grpc.aio.ClientInterceptor] | None = None,
    ) -> grpc.aio.Channel:
        """Internal helper to create a new async gRPC channel."""
        return create_aio_channel(
            target,
            insecure=insecure,
            credentials=credentials,
            options=options,
            compression=compression,
            interceptors=interceptors,
        )

    async def close_all(self, grace: float | None = None) -> None:
        """Close all pooled channels.

        The pool stays usable afterwards: this drains it rather than retiring it, so a shared pool
        can outlive one shutdown and an ``async with`` block can be entered again.

        Args:
            grace: Optional time to wait for active RPCs to finish.
        """
        self._closing = True
        try:
            async with self._pool_lock:
                entries = list(self._entries.values())
                self._entries.clear()

            wrappers: list[ChannelWrapper] = []
            for entry in entries:
                entry.active = False
                wrappers.extend(entry.channels)
                entry.channels.clear()

            if wrappers:
                logger.info("Closing %d pooled gRPC channels", len(wrappers))
                await asyncio.gather(*(w.channel.close(grace=grace) for w in wrappers), return_exceptions=True)

            self._update_pool_metrics()
        finally:
            self._closing = False

    async def health_check(self, target: str | None = None) -> bool:
        """Check if pool is healthy (ready to accept requests).

        Args:
            target: Optional target to check for reachability instead of the pool itself.

        Returns:
            True if the pool (or the given target) can serve requests.
        """
        if self._closing:
            return False

        if target and self._health_checker:
            return await self._health_checker.check_health(target)

        return True

    async def update_channel_health(self, target: str, is_healthy: bool) -> None:
        """Update health status for every pooled channel of a target.

        All channel identities sharing the address are updated, since health is a property of the
        server rather than of the channel configuration.

        Args:
            target: The target address (host:port).
            is_healthy: Whether the target is healthy.
        """
        async with self._pool_lock:
            checked_out = [(key, entry) for key, entry in self._entries.items() if key.target == target]
            for _, entry in checked_out:
                entry.users += 1

        try:
            for _, entry in checked_out:
                async with entry.lock:
                    for wrapper in entry.channels:
                        wrapper.is_healthy = is_healthy
        finally:
            for key, entry in checked_out:
                await self._checkin(key, entry)

    async def __aenter__(self) -> ChannelPool:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close_all()


__all__ = ["ChannelKey", "ChannelPool", "ChannelWrapper", "chain_token"]
