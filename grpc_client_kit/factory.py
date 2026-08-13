from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import grpc.aio

from .balancers import LoadBalancer, LoadBalancerConfig, LoadBalancingStrategy, create_balancer
from .channel import ChannelPool
from .client import GrpcClient
from .config import GrpcClientConfig
from .interceptors import (
    CircuitBreakerConfig,
    DeadlineBudgetConfig,
    InterceptorChainBuilder,
    ObservabilityConfig,
    RetryConfig,
    TimeoutConfig,
    WaitForReadyConfig,
)
from .protocols import (
    ChannelProviderProtocol,
    CircuitBreakerSettingsProtocol,
    GrpcClientMetricsProtocol,
    GrpcClientSettingsProtocol,
    RetryMetricsProtocol,
    RetrySettingsProtocol,
    TimeoutSettingsProtocol,
)

if TYPE_CHECKING:
    from .health import HealthChecker

logger = logging.getLogger(__name__)

_MISSING_MESSAGE = "Install grpc-client-kit[health] (grpcio-health-checking) to use gRPC health checking"


def _validate_settings(settings: object | None) -> None:
    """Refuse a settings object that cannot satisfy the factory, while the mistake is visible.

    Chains are built lazily, per target, so a settings object missing a required field would
    otherwise be accepted here and explode with a bare ``AttributeError`` on the first RPC.
    Optional blocks stay optional — this checks the required protocol surface only.

    Args:
        settings: The object handed to the factory, or None.

    Raises:
        TypeError: If the object does not satisfy `GrpcClientSettingsProtocol`, naming exactly
            the fields it is missing.
    """
    if settings is None or isinstance(settings, GrpcClientSettingsProtocol):
        return

    required = [name for name in dir(GrpcClientSettingsProtocol) if not name.startswith("_")]
    missing = sorted(name for name in required if not hasattr(settings, name))
    raise TypeError(
        f"settings object {type(settings).__name__!r} does not satisfy GrpcClientSettingsProtocol: "
        f"missing {', '.join(missing) if missing else 'field types differ from the protocol'}"
    )


def _load_health_checker() -> type[HealthChecker]:
    """Import the health checker on demand.

    Health checking is an optional extra, so ``grpc_health`` must stay out of import time: the
    package has to install and import without it.

    Returns:
        The HealthChecker class.

    Raises:
        ImportError: If the [health] extra is not installed.
    """
    try:
        from .health import HealthChecker  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(_MISSING_MESSAGE) from exc

    return HealthChecker


class GrpcClientFactory:
    """High-level factory for creating configured GrpcClient instances.

    Encapsulates mapping from settings objects to kit-specific configuration objects.
    """

    def __init__(
        self,
        settings: GrpcClientSettingsProtocol | None = None,
        pool: ChannelProviderProtocol | None = None,
        shutdown_grace: float | None = 5.0,
        ready_timeout: float | None = 10.0,
    ) -> None:
        """Initialize the factory.

        Ownership Semantics:
            If `pool` is provided, this factory uses it but does NOT own it. Closing the factory
            will not close the provided pool.
            If `pool` is NOT provided, the factory creates and OWNS its own `ChannelPool`.
            Closing the factory will close this internal pool.

        Args:
            settings: Base settings object (optional)
            pool: Shared channel provider (if not provided, creates a ChannelPool based on settings)
            shutdown_grace: Seconds in-flight RPCs get to finish when the ``async with`` block is
                left. ``None`` cancels them immediately — a deliberate choice, not the default:
                a k8s SIGTERM lands here, and cutting every in-flight dependency call short turns
                a rolling deploy into an error spike.
            ready_timeout: Seconds ``__aenter__`` waits for the first health check pass. Until
                that pass every target reads unhealthy, so entering the context without waiting
                would make the first call of every freshly started pod fail deterministically.
                ``None`` waits indefinitely; only relevant when health checking is configured.

        Raises:
            TypeError: If `settings` does not satisfy `GrpcClientSettingsProtocol`. Validated
                eagerly: a missing required field otherwise surfaces as a bare ``AttributeError``
                on the first RPC, in production, far from the mistake.
            ImportError: If health checking is configured but the [health] extra is missing.
        """
        _validate_settings(settings)
        self._settings = settings
        self._shutdown_grace = shutdown_grace
        self._ready_timeout = ready_timeout
        self._owns_pool = False
        self._health_checker: HealthChecker | None = None
        # Chains cached per (service_name, target): recreating a client for the same stub must
        # reuse the same interceptor instances, or every create_client would mint a fresh channel
        # identity and a per-request client pattern would open a connection per request.
        self._chains: dict[tuple[str, str], list[grpc.aio.ClientInterceptor]] = {}

        if pool:
            self._pool = pool
        else:
            self._owns_pool = True
            metrics = getattr(settings, "metrics_registry", None) if settings else None

            if settings and settings.pool:
                self._pool = ChannelPool(
                    max_channels_per_target=settings.pool.max_channels_per_target,
                    idle_timeout=settings.pool.idle_timeout,
                    metrics=metrics,
                )
            else:
                self._pool = ChannelPool(metrics=metrics)

        # Build and start HealthChecker if configured and we have targets
        if settings and settings.health_checker and settings.targets:
            health_checker_class = _load_health_checker()
            self._health_checker = health_checker_class(
                check_interval=settings.health_checker.check_interval,
                timeout=settings.health_checker.timeout,
                service=getattr(settings.health_checker, "service", ""),
                insecure=settings.insecure,
                credentials=getattr(settings, "credentials", None),
                # Same options and compression as the application channels, so the health probes
                # negotiate HTTP/2 the same way the real traffic does.
                options=getattr(settings, "options", None),
                compression=getattr(settings, "compression", None),
                # Pass pool if it supports updating health status
                pool=self._pool if isinstance(self._pool, ChannelPool) else None,
            )

    async def circuit_breaker_states(self) -> dict[str, dict[str, Any]]:
        """Snapshot the circuit breakers of every chain this factory has built.

        Returns:
            Mapping of ``"service -> target"`` to that chain's breaker snapshot (method to
            status). Chains without a breaker are absent.
        """
        from .interceptors.base import logical_interceptor  # noqa: PLC0415 - avoids import cycle at module load
        from .interceptors.circuit_breaker import AsyncCircuitBreakerInterceptor  # noqa: PLC0415

        snapshot: dict[str, dict[str, Any]] = {}
        for (service_name, target), chain in self._chains.items():
            for entry in chain:
                owner = logical_interceptor(entry)
                if isinstance(owner, AsyncCircuitBreakerInterceptor):
                    snapshot[f"{service_name} -> {target}"] = dict(await owner.get_states())
                    break
        return snapshot

    async def close(self, grace: float | None = None) -> None:
        """Close the underlying pool and stop the health checker.

        Args:
            grace: Optional time in seconds to allow active RPCs to complete.
        """
        if self._health_checker:
            await self._health_checker.stop()

        if self._owns_pool:
            await self._pool.close_all(grace=grace)

    @property
    def health_checker(self) -> HealthChecker | None:
        """The checker this factory built from settings, or None when none is configured."""
        return self._health_checker

    async def wait_until_ready(self, timeout: float | None = None) -> bool:
        """Wait until the first health check pass has classified every target.

        Args:
            timeout: Maximum seconds to wait. None waits indefinitely.

        Returns:
            True once the first pass completed — and trivially when no checker is configured,
            since there is then nothing to wait for. False if the wait timed out or the checker
            is not running.
        """
        if self._health_checker is None:
            return True
        return await self._health_checker.wait_until_ready(timeout=timeout)

    async def __aenter__(self) -> GrpcClientFactory:
        """Start the health checker and wait for its first verdicts.

        Waiting is the point: until the first pass every target reads unhealthy by design, so a
        factory that returned before it would hand out clients whose very first call fails with
        `NoHealthyTargetsError` on every fresh start — a deterministic error spike per deploy.
        """
        if self._health_checker and self._settings and self._settings.targets:
            await self._health_checker.start(self._settings.targets)
            await self._health_checker.wait_until_ready(timeout=self._ready_timeout)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit. Calls close() with the configured shutdown grace."""
        await self.close(grace=self._shutdown_grace)

    def create_client[T](
        self,
        stub_class: type[T],
        target: str | None = None,
        service_name: str | None = None,
        metrics: GrpcClientMetricsProtocol | None = None,
        interceptors: list[grpc.aio.ClientInterceptor] | None = None,
    ) -> GrpcClient[T]:
        """Create a fully configured gRPC client instance.

        This method applies resilience (retries, timeouts, circuit breakers)
         and observability (metrics, logging, tracing) based on factory settings.

        Args:
            stub_class: The gRPC stub class to instantiate.
            target: Optional target override. If not provided, uses target from settings.
            service_name: Name of the service for observability. Defaults to stub class name.
            metrics: Optional metrics registry, overriding `settings.metrics_registry`.
            interceptors: Optional list of additional custom interceptors. These instances are
                shared by every target, unlike the chain the factory builds around them.

        Returns:
            A configured GrpcClient instance.

        Raises:
            ValueError: If neither a target nor a list of targets is configured.
        """
        config = self._build_config(target)
        balancer = self._build_balancer()

        # Validation: either target or balancer must be available
        if not config.target and not balancer:
            raise ValueError(
                "No target configured. Provide either 'target' in settings/overrides or 'targets' for load balancing."
            )

        # Lifecycle warning
        if self._health_checker and not self._health_checker.is_running:
            logger.warning(
                "GrpcClientFactory used without 'async with'. "
                "Health checks are NOT running. Traffic may be sent to unhealthy targets."
            )

        actual_service_name = service_name or stub_class.__name__
        metrics_registry = self._resolve_metrics_registry(metrics)
        custom_interceptors = list(interceptors) if interceptors else []

        def build_chain(chain_target: str) -> list[grpc.aio.ClientInterceptor]:
            """Build a chain dedicated to one target so its stateful interceptors stay isolated.

            Chains are cached per (service_name, target): recreating a client for the same stub —
            a per-request DI scope, say — must land on the same interceptor instances and thereby
            the same pooled channel, not open a new connection per client. Custom interceptors are
            caller-owned instances the factory cannot prove reusable, so chains around them stay
            per-client.
            """
            cache_key = (actual_service_name, chain_target)
            if not custom_interceptors and metrics is None:
                cached = self._chains.get(cache_key)
                if cached is not None:
                    return cached

            logger.debug("Building gRPC interceptor chain for %s -> %s", actual_service_name, chain_target)
            builder = InterceptorChainBuilder()

            if self._settings:
                builder.with_observability(
                    ObservabilityConfig(
                        tracing=self._settings.tracing_enabled,
                        # A registry-less metrics interceptor records nothing; keep it out of the
                        # chain so "no metrics" is visible in the chain instead of silent.
                        metrics=self._settings.metrics_enabled and metrics_registry is not None,
                        logging=self._settings.logging_enabled,
                        metrics_registry=metrics_registry,
                        service_name=actual_service_name,
                        sensitive_headers=getattr(self._settings, "sensitive_headers", None),
                        enable_method_label=getattr(self._settings, "enable_method_label", True),
                        success_log_level=getattr(self._settings, "success_log_level", logging.INFO),
                    )
                )
                builder.with_resilience(
                    timeout=self._build_timeout_config(self._settings.timeout),
                    retry=self._build_retry_config(self._settings.retry, metrics=metrics_registry),
                    circuit_breaker=self._build_cb_config(self._settings.circuit_breaker, metrics=metrics_registry),
                    deadline_budget=self._build_deadline_budget_config(
                        getattr(self._settings, "deadline_budget", None)
                    ),
                    wait_for_ready=self._build_wait_for_ready_config(getattr(self._settings, "wait_for_ready", None)),
                )

            builder.with_custom(custom_interceptors)
            if balancer is not None:
                # Passive outlier detection, innermost: a call that just got UNAVAILABLE is
                # fresher evidence than any probe, and quarantining the target immediately closes
                # the window in which an active checker would keep routing into a dead backend.
                from .interceptors.outlier import AsyncPassiveOutlierInterceptor  # noqa: PLC0415

                builder.with_extra_inner([AsyncPassiveOutlierInterceptor(balancer, chain_target)])
            chain = builder.build()
            if not custom_interceptors and metrics is None:
                self._chains[cache_key] = chain
            return chain

        return GrpcClient(
            stub_class=stub_class,
            config=config,
            pool=self._pool,
            balancer=balancer,
            interceptor_factory=build_chain,
        )

    def _resolve_metrics_registry(self, metrics: GrpcClientMetricsProtocol | None) -> GrpcClientMetricsProtocol | None:
        """Pick the registry RPC metrics are recorded into.

        An explicit argument wins over `settings.metrics_registry`, which otherwise only fed pool
        statistics and left `metrics_enabled` clients recording nothing at all.

        Args:
            metrics: The registry passed to `create_client`, if any.

        Returns:
            The registry to use, or None if metrics cannot be recorded.
        """
        settings_registry = getattr(self._settings, "metrics_registry", None) if self._settings else None
        registry: GrpcClientMetricsProtocol | None = metrics if metrics is not None else settings_registry

        if registry is None and self._settings and self._settings.metrics_enabled:
            logger.warning(
                "metrics_enabled is set but no metrics registry is available. "
                "Pass 'metrics=' to create_client() or set 'metrics_registry' in settings; "
                "gRPC client metrics are disabled."
            )

        return registry

    def _build_config(self, target: str | None) -> GrpcClientConfig:
        """Build GrpcClientConfig from settings or overrides.

        Channel construction settings are optional blocks (see `protocols.GrpcChannelExtrasProtocol`)
        and are read with `getattr`, so a settings model that declares none of them still satisfies
        the factory. ``connectivity`` joins them on the same terms: a settings object that carries a
        `config.ConnectivityConfig` under that name has its channels tuned by it, and one that does
        not is left with gRPC's defaults.
        """
        if self._settings:
            actual_target = target or self._settings.target
            insecure = self._settings.insecure
            credentials = getattr(self._settings, "credentials", None)
            options = getattr(self._settings, "options", None)
            compression = getattr(self._settings, "compression", None)
            connectivity = getattr(self._settings, "connectivity", None)
        else:
            actual_target = target
            insecure = False
            credentials = None
            options = None
            compression = None
            connectivity = None

        return GrpcClientConfig(
            target=actual_target,
            insecure=insecure,
            credentials=credentials,
            options=options,
            compression=compression,
            connectivity=connectivity,
        )

    def _build_balancer(self) -> LoadBalancer | None:
        """Build LoadBalancer from settings targets and balancer strategy."""
        if not self._settings or not self._settings.targets:
            return None

        balancer_config = None
        if self._settings.balancer:
            s = self._settings.balancer
            try:
                strategy = LoadBalancingStrategy(s.strategy)
            except ValueError as e:
                raise ValueError(f"Invalid load balancing strategy: {s.strategy}") from e

            balancer_config = LoadBalancerConfig(
                strategy=strategy,
                weights=s.weights,
            )

        return create_balancer(
            targets=self._settings.targets,
            config=balancer_config,
            health_checker=self._health_checker,
        )

    def _build_timeout_config(self, s: TimeoutSettingsProtocol | None) -> TimeoutConfig | None:
        """Build TimeoutConfig from settings protocol."""
        if not s:
            return None
        return TimeoutConfig(default=s.default)

    def _build_retry_config(
        self, s: RetrySettingsProtocol | None, metrics: GrpcClientMetricsProtocol | None = None
    ) -> RetryConfig | None:
        """Build RetryConfig from settings protocol."""
        if not s:
            return None
        return RetryConfig(
            max_attempts=s.max_attempts,
            initial_backoff=s.initial_backoff,
            max_backoff=s.max_backoff,
            backoff_multiplier=s.backoff_multiplier,
            jitter=getattr(s, "jitter", 0.1),
            retryable_codes=getattr(s, "retryable_codes", None),
            retry_streaming=getattr(s, "retry_streaming", False),
            idempotent_methods=getattr(s, "idempotent_methods", None),
            on_retry=getattr(s, "on_retry", None),
            # The registry opts into retry visibility by implementing the extension protocol.
            metrics=metrics if isinstance(metrics, RetryMetricsProtocol) else None,
        )

    def _build_wait_for_ready_config(self, s: Any | None) -> WaitForReadyConfig | None:
        """Build WaitForReadyConfig from an optional settings block (duck-typed)."""
        if not s:
            return None
        return WaitForReadyConfig(
            default=getattr(s, "default", True),
            per_method=getattr(s, "per_method", None) or {},
            require_deadline=getattr(s, "require_deadline", True),
        )

    def _build_deadline_budget_config(self, s: Any | None) -> DeadlineBudgetConfig | None:
        """Build DeadlineBudgetConfig from an optional settings block (duck-typed)."""
        if not s:
            return None
        return DeadlineBudgetConfig(reserve_for_next=getattr(s, "reserve_for_next", 0.0))

    def _build_cb_config(
        self, s: CircuitBreakerSettingsProtocol | None, metrics: GrpcClientMetricsProtocol | None = None
    ) -> CircuitBreakerConfig | None:
        """Build CircuitBreakerConfig from settings protocol."""
        if not s:
            return None
        return CircuitBreakerConfig(
            fail_threshold=s.fail_threshold,
            recovery_timeout=s.recovery_timeout,
            half_open_max_calls=s.half_open_max_calls,
            max_methods=getattr(s, "max_methods", 1000),
            metrics=metrics,
        )


__all__ = ["GrpcClientFactory"]
