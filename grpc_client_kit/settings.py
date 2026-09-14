"""The settings shape the factory reads, as pydantic models, written down once.

`factory.GrpcClientFactory` reads a settings object structurally — `protocols.GrpcClientSettingsProtocol`
and the optional blocks around it — so every service used to write its own models for that shape and
its own mapping onto the kit's dataclasses, and every copy drifted in its own way. These models are
that shape with the kit's own defaults and bounds; each one that mirrors a dataclass knows how to
become it (``to_config()``), and the kit's own tests pin the two field sets to each other.

Every class here is a plain :class:`pydantic.BaseModel`, deliberately. A section that were a
``BaseSettings`` would read the environment on its own, with no prefix, and a bare ``TARGET`` or
``TIMEOUT`` in a pod would land in it. Nest :class:`BaseGrpcClientSettings` under the service's own
settings instead, once per upstream, and let that class own the environment::

    from pydantic import Field
    from pydantic_settings import BaseSettings, SettingsConfigDict

    from grpc_client_kit import GrpcClientFactory
    from grpc_client_kit.settings import BaseGrpcClientSettings


    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter="__")

        users_grpc: BaseGrpcClientSettings = Field(default_factory=BaseGrpcClientSettings)


    # USERS_GRPC__TARGET=users.internal:50051 USERS_GRPC__RETRY__MAX_ATTEMPTS=5 python main.py
    settings = Settings()
    factory = GrpcClientFactory(settings=settings.users_grpc)

Runtime objects are not settings and have no field here: channel credentials are handed to
:meth:`BaseGrpcClientSettings.to_config`, metrics registries and retry callbacks to the factory or to
the chain built from the sections. Unknown fields are refused, so a misspelled variable fails at load
instead of quietly yielding a default.

This module needs the ``settings`` extra (``grpc-client-kit[settings]``), which pulls in pydantic;
importing it without raises an ``ImportError`` naming the extra.
"""

from __future__ import annotations

import logging
from typing import Any

import grpc

from .balancers import LoadBalancerConfig, LoadBalancingStrategy
from .config import ConnectivityConfig, GrpcClientConfig
from .interceptors import (
    CircuitBreakerConfig,
    DeadlineBudgetConfig,
    RetryConfig,
    TimeoutConfig,
    WaitForReadyConfig,
)

try:
    from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
except ImportError as exc:
    raise ImportError("Install grpc-client-kit[settings] (pydantic) to use the settings models") from exc

# The names an operator writes for `grpc.Compression`. "none" is gRPC's explicit no-compression
# setting, not the absence of one — that is ``None``, which leaves the gRPC default alone.
_COMPRESSION_BY_NAME: dict[str, grpc.Compression] = {
    "none": grpc.Compression.NoCompression,
    "deflate": grpc.Compression.Deflate,
    "gzip": grpc.Compression.Gzip,
}


def _status_code_by_name(value: object) -> object:
    """Resolve a status code written by name (``UNAVAILABLE``); anything else is left to pydantic."""
    if not isinstance(value, str):
        return value
    try:
        return grpc.StatusCode[value.upper()]
    except KeyError:
        names = ", ".join(code.name for code in grpc.StatusCode)
        raise ValueError(f"unknown gRPC status code {value!r}; expected one of {names}") from None


class ConnectivitySettings(BaseModel):
    """``connectivity``: keepalive and reconnect backoff in seconds, `config.ConnectivityConfig` by environment.

    Opt-in like the dataclass: an absent block leaves every channel argument at gRPC's own default,
    and ``None`` in a field does the same for that argument alone.
    """

    model_config = ConfigDict(extra="forbid")

    keepalive_time: float | None = Field(
        default=30.0, gt=0, description="Seconds of inactivity before a keepalive ping (None: never ping)"
    )
    keepalive_timeout: float | None = Field(default=10.0, gt=0, description="Seconds to wait for the ping's answer")
    permit_without_calls: bool = Field(default=False, description="Keep pinging while no call is in flight")
    max_pings_without_data: int | None = Field(
        default=2, ge=0, description="Pings allowed on a connection carrying no data (0: no limit)"
    )
    initial_reconnect_backoff: float | None = Field(
        default=1.0, gt=0, description="Seconds before the first reconnect attempt"
    )
    min_reconnect_backoff: float | None = Field(
        default=None, gt=0, description="Lower bound between reconnect attempts (None: gRPC's default)"
    )
    max_reconnect_backoff: float | None = Field(
        default=30.0, gt=0, description="Upper bound between reconnect attempts"
    )

    def to_config(self) -> ConnectivityConfig:
        """Build the `config.ConnectivityConfig` these fields describe.

        Raises:
            ValueError: If the reconnect bounds contradict each other; the dataclass keeps that check.
        """
        return ConnectivityConfig(**self.model_dump())


class ChannelPoolSettings(BaseModel):
    """``pool``: what `channel.ChannelPool` is sized with, read by the factory."""

    model_config = ConfigDict(extra="forbid")

    max_channels_per_target: int = Field(default=1, ge=1, description="Channels per channel identity")
    idle_timeout: float = Field(
        default=300.0, description="Seconds without active RPCs before gRPC parks a connection (0: never)"
    )


class TimeoutSettings(BaseModel):
    """``timeout``: the budget of a whole call, retries included; `interceptors.TimeoutConfig` by environment."""

    model_config = ConfigDict(extra="forbid")

    default: float = Field(
        default=10.0, ge=0, description="Budget in seconds for methods not listed in per_method (0: no deadline)"
    )
    per_method: dict[str, float | None] = Field(
        default_factory=dict,
        description='Budgets by full method name, as JSON: {"/pkg.Service/Method": 60} (null: no deadline)',
    )

    def to_config(self) -> TimeoutConfig:
        """Build the `interceptors.TimeoutConfig` these fields describe."""
        return TimeoutConfig(**self.model_dump())


class RetrySettings(BaseModel):
    """``retry``: the retry policy, `interceptors.RetryConfig` by environment.

    Status codes are written by name (``UNAVAILABLE``). ``on_retry`` and ``metrics`` are runtime
    objects and have no field here; the factory injects the registry, a hand-built chain sets both on
    the config `to_config` returns.
    """

    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=3, ge=1, description="Total attempts, the first one included")
    initial_backoff: float = Field(default=0.1, ge=0, description="Seconds before the first retry")
    max_backoff: float = Field(default=10.0, ge=0, description="Upper bound for the wait between retries")
    backoff_multiplier: float = Field(default=2.0, ge=1, description="Growth factor of the backoff per attempt")
    jitter: float = Field(default=0.1, ge=0, le=1, description="Random variation applied to the backoff")
    retryable_codes: set[grpc.StatusCode] | None = Field(
        default=None, description="Status codes that trigger a retry, by name (None: the kit's default set)"
    )
    retry_streaming: bool = Field(default=False, description="Allow retrying unary-stream calls at all")
    idempotent_methods: set[str] | None = Field(
        default=None, description="Full method names that may be retried; a whitelist once given"
    )

    @field_validator("retryable_codes", mode="before")
    @classmethod
    def _codes_by_name(cls, value: object) -> object:
        if isinstance(value, (list, tuple, set, frozenset)):
            return {_status_code_by_name(item) for item in value}
        return value

    def to_config(self) -> RetryConfig:
        """Build the `interceptors.RetryConfig` these fields describe, with no callback and no registry."""
        return RetryConfig(**self.model_dump())


class CircuitBreakerSettings(BaseModel):
    """``circuit_breaker``: per-method, per-target breaking; `interceptors.CircuitBreakerConfig` by environment.

    ``metrics`` is a runtime object and has no field here; the factory injects the registry.
    """

    model_config = ConfigDict(extra="forbid")

    fail_threshold: int = Field(default=5, ge=1, description="Consecutive failed attempts that open a circuit")
    recovery_timeout: float = Field(default=60.0, ge=0, description="Seconds an open circuit waits before a trial")
    half_open_max_calls: int = Field(default=1, ge=1, description="Trial calls admitted while half-open")
    max_methods: int = Field(default=1000, ge=1, description="Methods tracked per breaker before the LRU evicts")

    def to_config(self) -> CircuitBreakerConfig:
        """Build the `interceptors.CircuitBreakerConfig` these fields describe, with no registry."""
        return CircuitBreakerConfig(**self.model_dump())


class WaitForReadySettings(BaseModel):
    """``wait_for_ready``: wait for a connection instead of failing fast; `interceptors.WaitForReadyConfig`."""

    model_config = ConfigDict(extra="forbid")

    default: bool | None = Field(
        default=True, description="Value for methods not listed in per_method (None: untouched)"
    )
    per_method: dict[str, bool | None] = Field(
        default_factory=dict, description='Values by full method name, as JSON: {"/pkg.Service/Method": false}'
    )
    require_deadline: bool = Field(default=True, description="Wait only on calls that carry a deadline")

    def to_config(self) -> WaitForReadyConfig:
        """Build the `interceptors.WaitForReadyConfig` these fields describe."""
        return WaitForReadyConfig(**self.model_dump())


class DeadlineBudgetSettings(BaseModel):
    """``deadline_budget``: request budget propagation; `interceptors.DeadlineBudgetConfig` by environment.

    Needs the ``deadline`` extra to take effect; without it the layer is skipped with a warning.
    """

    model_config = ConfigDict(extra="forbid")

    reserve_for_next: float = Field(
        default=0.0, ge=0, description="Seconds every call keeps back for the work that follows it"
    )

    def to_config(self) -> DeadlineBudgetConfig:
        """Build the `interceptors.DeadlineBudgetConfig` these fields describe."""
        return DeadlineBudgetConfig(**self.model_dump())


class LoadBalancerSettings(BaseModel):
    """``balancer``: how a target is picked from ``targets``; `balancers.LoadBalancerConfig` by environment."""

    model_config = ConfigDict(extra="forbid")

    strategy: LoadBalancingStrategy = Field(
        default=LoadBalancingStrategy.ROUND_ROBIN, description="round_robin, random or weighted"
    )
    weights: dict[str, float] | None = Field(
        default=None, description='Weights by target for the weighted strategy, as JSON: {"host:50051": 2.0}'
    )

    def to_config(self) -> LoadBalancerConfig:
        """Build the `balancers.LoadBalancerConfig` these fields describe."""
        return LoadBalancerConfig(**self.model_dump())


class HealthCheckerSettings(BaseModel):
    """``health_checker``: active ``grpc.health.v1`` probing of ``targets``, read by the factory.

    Needs the ``health`` extra: the factory raises ``ImportError`` naming it when this block is set.
    """

    model_config = ConfigDict(extra="forbid")

    check_interval: float = Field(default=30.0, gt=0, description="Seconds between probes of a healthy target")
    timeout: float = Field(default=5.0, gt=0, description="Seconds one probe may take")
    service: str = Field(default="", description="Service name probed; empty asks about the server as a whole")


class BaseGrpcClientSettings(BaseModel):
    """One upstream's client settings: what the factory reads, and `config.GrpcClientConfig` by `to_config`.

    Satisfies `protocols.GrpcClientSettingsProtocol` and carries every optional block the factory
    reads, so an instance is handed to `factory.GrpcClientFactory` as it is. The observability flags
    are flat because that is how the protocol spells them; the rest of
    `interceptors.ObservabilityConfig` sits next to them under the dataclass's own names, minus
    ``service_name`` (the factory names each client after its stub) and the registry.

    Two blocks are present by default: ``pool``, at the pool's own defaults, and ``timeout``, because
    a client without a deadline is the one default this kit will not ship — ``TIMEOUT__DEFAULT=0``
    switches it off. Everything that adds a layer or needs an extra — retries, the breaker,
    wait-for-ready, budget propagation, balancing, health checks, connectivity tuning — is ``None``
    until configured; with pydantic-settings a nested block is created the moment one of its
    variables is set. Subclass it to change a default or add fields of your own::

        class UsersGrpcSettings(BaseGrpcClientSettings):
            retry: RetrySettings | None = Field(default_factory=RetrySettings)
    """

    model_config = ConfigDict(extra="forbid")

    # Channel
    target: str | None = Field(
        default=None, description="Single target, host:port or a gRPC resolver URI; exclusive with targets"
    )
    targets: list[str] | None = Field(default=None, description="Targets to balance across; exclusive with target")
    insecure: bool = Field(
        default=False,
        description="Plaintext channel (False: TLS, from the system trust store or to_config's credentials)",
    )
    options: list[tuple[str, Any]] | None = Field(
        default=None, description='Raw channel arguments, as JSON pairs: [["grpc.max_receive_message_length", 8388608]]'
    )
    compression: grpc.Compression | None = Field(
        default=None, description="Channel compression by name: none, deflate or gzip (None: the gRPC default)"
    )
    connectivity: ConnectivitySettings | None = Field(
        default=None, description="Keepalive and reconnect tuning (None: gRPC's defaults)"
    )

    # Observability, flat: this is how GrpcClientSettingsProtocol spells the flags
    tracing_enabled: bool = Field(default=False, description="Add the tracing layer (needs the tracing extra)")
    metrics_enabled: bool = Field(default=False, description="Add the metrics layer (needs a registry)")
    logging_enabled: bool = Field(default=True, description="Add the structured logging layer")
    sensitive_headers: set[str] | None = Field(
        default=None, description="Header names redacted in logged metadata (None: the kit's default set)"
    )
    sensitive_methods: set[str] | None = Field(
        default=None, description="Full method names whose payloads and errors are kept out of the logs"
    )
    sensitive_patterns: list[str] | None = Field(
        default=None, description="Regex patterns marking methods as sensitive"
    )
    log_request_payload: bool = Field(default=False, description="Log request payloads, truncated")
    log_response_payload: bool = Field(default=False, description="Log response payloads, truncated")
    enable_method_label: bool = Field(
        default=True, description="Label metrics by method (off for services with thousands of methods)"
    )
    success_log_level: int = Field(
        default=logging.INFO, description="Level of the record a successful call emits, by name or number"
    )

    # The blocks the factory maps onto the pool, the chain, the balancer and the health checker
    pool: ChannelPoolSettings | None = Field(default_factory=ChannelPoolSettings, description="Channel pool sizing")
    timeout: TimeoutSettings | None = Field(
        default_factory=TimeoutSettings, description="Call budgets (None: no timeout layer, so no deadline at all)"
    )
    retry: RetrySettings | None = Field(default=None, description="Retry policy (None: no retry layer)")
    circuit_breaker: CircuitBreakerSettings | None = Field(
        default=None, description="Circuit breaker (None: no breaker layer)"
    )
    wait_for_ready: WaitForReadySettings | None = Field(
        default=None, description="Wait for a connection instead of failing fast (None: gRPC's fail-fast)"
    )
    deadline_budget: DeadlineBudgetSettings | None = Field(
        default=None, description="Request budget propagation (None: none; needs the deadline extra)"
    )
    balancer: LoadBalancerSettings | None = Field(default=None, description="Strategy over targets (None: round-robin)")
    health_checker: HealthCheckerSettings | None = Field(
        default=None, description="Active probing of targets (None: none; needs the health extra)"
    )

    @field_validator("compression", mode="before")
    @classmethod
    def _compression_by_name(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        try:
            return _COMPRESSION_BY_NAME[value.lower()]
        except KeyError:
            names = ", ".join(_COMPRESSION_BY_NAME)
            raise ValueError(f"unknown compression {value!r}; expected one of {names}") from None

    @field_validator("success_log_level", mode="before")
    @classmethod
    def _level_by_name(cls, value: object) -> object:
        # An environment hands over strings, so a numeric one is left to pydantic's int coercion.
        if not isinstance(value, str) or value.isdecimal():
            return value
        try:
            return logging.getLevelNamesMapping()[value.upper()]
        except KeyError:
            raise ValueError(f"unknown log level {value!r}") from None

    @model_validator(mode="after")
    def _one_way_to_name_the_backend(self) -> BaseGrpcClientSettings:
        if self.target is not None and self.targets:
            raise ValueError("target and targets are mutually exclusive: one address, or a list to balance across")
        return self

    def to_config(self, *, credentials: grpc.ChannelCredentials | None = None) -> GrpcClientConfig:
        """Build the `config.GrpcClientConfig` these fields describe.

        The chain is built from the sections (``settings.retry.to_config()`` and so on, into
        `interceptors.build_interceptors`); the factory does both from the same instance.

        Args:
            credentials: TLS credentials for the channel. A credentials object is built in code, not
                read from an environment, so it is handed in here — and ``insecure=True`` together with
                credentials is refused by the dataclass, exactly as it always was.

        Returns:
            The channel configuration.

        Raises:
            ValueError: If the configuration contradicts itself; the dataclasses keep those checks.
        """
        return GrpcClientConfig(
            target=self.target,
            insecure=self.insecure,
            credentials=credentials,
            options=self.options,
            compression=self.compression,
            connectivity=self.connectivity.to_config() if self.connectivity is not None else None,
        )


__all__ = [
    "BaseGrpcClientSettings",
    "ChannelPoolSettings",
    "CircuitBreakerSettings",
    "ConnectivitySettings",
    "DeadlineBudgetSettings",
    "HealthCheckerSettings",
    "LoadBalancerSettings",
    "RetrySettings",
    "TimeoutSettings",
    "WaitForReadySettings",
]
