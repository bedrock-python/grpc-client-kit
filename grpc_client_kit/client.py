from __future__ import annotations

import logging
from collections.abc import Callable
from typing import (
    Any,
)

import grpc.aio

from .balancers import LoadBalancer
from .channel import ChannelKey, ChannelPool
from .config import GrpcClientConfig
from .protocols import ChannelProviderProtocol

logger = logging.getLogger(__name__)


class GrpcClient[T]:
    """Factory for gRPC stubs. Manages target selection and channel acquisition from pool.

    Interceptor Chains:
        A chain is built per target and cached, because gRPC binds interceptors to a channel when
        the channel is created. Stateful interceptors — the circuit breaker above all — therefore
        track one backend each: a single failing member of a load-balanced set can no longer trip
        the breaker for its healthy peers. Pass ``interceptor_factory`` to get that isolation;
        passing a ready ``interceptors`` list instead shares one chain (and one breaker) across
        every target of this client.

    Ownership Semantics:
        This client DOES NOT own the `ChannelPool` it uses. It is a lightweight wrapper that
        acquires channels from a shared pool. Closing the client (via __aexit__) does NOT
        close the underlying channels or the pool.
    """

    def __init__(
        self,
        stub_class: type[T],
        config: GrpcClientConfig,
        pool: ChannelProviderProtocol,
        balancer: LoadBalancer | None = None,
        interceptors: list[grpc.aio.ClientInterceptor] | None = None,
        interceptor_factory: Callable[[str], list[grpc.aio.ClientInterceptor]] | None = None,
    ) -> None:
        """Initialize the gRPC client.

        Args:
            stub_class: The gRPC stub class to instantiate (e.g., MyServiceStub).
            config: Configuration for the client (target, security, etc.).
            pool: The channel pool to acquire channels from.
            balancer: Optional load balancer for multiple targets.
            interceptors: Optional ready chain, shared by every target of this client.
            interceptor_factory: Optional builder called once per target to create a dedicated
                chain for it.

        Raises:
            ValueError: If target configuration is ambiguous or missing, or if both an interceptor
                chain and an interceptor factory are given.
        """
        self._stub_class = stub_class
        self._config = config
        self._pool = pool
        self._balancer = balancer

        if interceptors is not None and interceptor_factory is not None:
            raise ValueError(
                "Ambiguous interceptors: provide either 'interceptors' (one shared chain) "
                "or 'interceptor_factory' (a chain per target), not both."
            )

        self._static_interceptors = list(interceptors) if interceptors is not None else None
        self._interceptor_factory = interceptor_factory
        self._chains: dict[str, list[grpc.aio.ClientInterceptor]] = {}
        self._warned_native_retry = False
        # Target, config and chain are fixed per target, so the pool identity is too: computing it
        # (validation, option merging, chain tokens) once per target instead of once per call keeps
        # that work off the hot path.
        self._channel_keys: dict[str, ChannelKey] = {}

        self._validate_target_configuration()

    def _validate_target_configuration(self) -> None:
        """Ensure that either a single target or a balancer is provided, but not both."""
        if self._balancer and self._config.target:
            raise ValueError(
                "Ambiguous target: both 'balancer' and 'config.target' provided. "
                "Use 'balancer' for multiple targets or 'config.target' for a single target."
            )
        if not self._balancer and not self._config.target:
            raise ValueError("No target specified: provide either 'balancer' or 'config.target'")

    def interceptors_for(self, target: str) -> list[grpc.aio.ClientInterceptor]:
        """Return this client's interceptor chain for one target.

        The chain is built at most once per target and then reused. That caching is what keeps the
        pool key stable: rebuilding the chain on every call would mint a new channel identity each
        time and the pool would open a channel per RPC.

        Args:
            target: The target address (host:port) the chain will be bound to.

        Returns:
            The interceptor chain for the target, empty when the client has no interceptors.
        """
        chain = self._chains.get(target)
        if chain is None:
            if self._static_interceptors is not None:
                chain = self._static_interceptors
            elif self._interceptor_factory is not None:
                chain = self._interceptor_factory(target)
            else:
                chain = []
            self._chains[target] = chain
            self._warn_on_native_retry_overlap(chain)
        return chain

    def _warn_on_native_retry_overlap(self, chain: list[grpc.aio.ClientInterceptor]) -> None:
        """Warn once when kit retries are stacked on top of native service-config retries.

        Native ``retryPolicy`` runs inside the channel, below every interceptor, so the two layers
        multiply: kit attempts times native attempts reach the server — a retry storm the kit's own
        logs and metrics cannot see. The channel options pass through untouched on purpose (native
        retries *without* kit retries are a fully supported configuration); only the combination is
        worth a warning.
        """
        if self._warned_native_retry:
            return

        options = self._config.channel_options() or []
        service_config = next((value for name, value in options if name == "grpc.service_config"), None)
        if service_config is None or "retryPolicy" not in str(service_config):
            return

        from .interceptors.base import logical_interceptor  # noqa: PLC0415 - avoids import cycle at module load
        from .interceptors.retry import AsyncRetryInterceptor  # noqa: PLC0415

        if any(isinstance(logical_interceptor(entry), AsyncRetryInterceptor) for entry in chain):
            self._warned_native_retry = True
            logger.warning(
                "Both kit retries and a native retryPolicy are configured for %s: the two layers "
                "multiply (kit attempts x native attempts reach the server). Configure one source "
                "of retries — drop the kit RetryConfig, or remove retryPolicy from grpc.service_config.",
                self._config.target or "the balanced targets",
            )

    async def circuit_breaker_states(self) -> dict[str, dict[str, Any]]:
        """Snapshot the circuit breakers of every chain this client has built.

        During an incident this is the question an operator asks first — "is the breaker open, or
        is the backend down?" — and it must be answerable without spelunking through the chain.

        Returns:
            Mapping of target to that target's breaker snapshot (method to status). Targets whose
            chain has no breaker, or no chain built yet, are absent.
        """
        from .interceptors.base import logical_interceptor  # noqa: PLC0415 - avoids import cycle at module load
        from .interceptors.circuit_breaker import AsyncCircuitBreakerInterceptor  # noqa: PLC0415

        snapshot: dict[str, dict[str, Any]] = {}
        for target, chain in self._chains.items():
            for entry in chain:
                owner = logical_interceptor(entry)
                if isinstance(owner, AsyncCircuitBreakerInterceptor):
                    snapshot[target] = dict(await owner.get_states())
                    break
        return snapshot

    async def connect(self) -> T:
        """Acquires a channel from the pool and returns a new stub instance.

        Returns:
            An instance of the stub_class configured with a pooled channel.

        Raises:
            NoHealthyTargetsError: If balancer is used and no healthy targets are available.
            ValueError: If target is not specified.
        """
        target: str | None
        if self._balancer:
            target = await self._balancer.select_target()
        else:
            target = self._config.target

        if target is None:
            # This should be caught by _validate_target_configuration but adding check for mypy
            raise ValueError("Target is not specified")

        interceptors = self.interceptors_for(target)
        if isinstance(self._pool, ChannelPool):
            key = self._channel_keys.get(target)
            if key is None:
                key = self._pool.make_key(
                    target,
                    insecure=self._config.insecure,
                    credentials=self._config.credentials,
                    # Explicit options plus whatever the connectivity tuning adds; built the same
                    # way every time, so the pool keeps recognising the channel it already has.
                    options=self._config.channel_options(),
                    compression=self._config.compression,
                    interceptors=interceptors,
                )
                self._channel_keys[target] = key
            channel = await self._pool.get_channel(target, interceptors=interceptors, key=key)
        else:
            # A custom provider speaks only the protocol surface, which has no key parameter.
            channel = await self._pool.get_channel(
                target,
                insecure=self._config.insecure,
                credentials=self._config.credentials,
                options=self._config.channel_options(),
                compression=self._config.compression,
                interceptors=interceptors,
            )

        # Mypy might complain about calling a generic type as a class
        return self._stub_class(channel)  # type: ignore[call-arg]

    async def __aenter__(self) -> T:
        """Async context manager entry. Calls connect()."""
        return await self.connect()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        """Context manager exit.

        NOTE: This does NOT close the underlying channel, as channels are managed by the ChannelPool.
        The pool owns channel lifetimes; idle connections are parked by gRPC core, not closed.
        """
        return None


__all__ = ["GrpcClient"]
