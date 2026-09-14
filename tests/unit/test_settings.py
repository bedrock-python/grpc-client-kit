"""Unit tests for the shipped settings models: the factory's settings shape, written down once.

Regression cover for issue #23: every consumer wrote the same pydantic models over again and mapped
them onto the kit's dataclasses by hand, which is where drift lives. These pin that the shipped
models carry the dataclasses' own field sets and defaults, load from a nested environment the
documented way, and cannot pick up an unprefixed variable.
"""

from __future__ import annotations

import importlib
import logging
import re
import subprocess
import sys
from dataclasses import MISSING, fields
from dataclasses import Field as DataclassField
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import grpc
import pytest
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from grpc_client_kit import (
    ConnectivityConfig,
    GrpcClientConfig,
    GrpcClientFactory,
    GrpcClientSettingsProtocol,
    LoadBalancerConfig,
    LoadBalancingStrategy,
    ObservabilityConfig,
    RetryConfig,
    TimeoutConfig,
)
from grpc_client_kit import settings as settings_module
from grpc_client_kit.interceptors import (
    AsyncCircuitBreakerInterceptor,
    AsyncLoggingInterceptor,
    AsyncRetryInterceptor,
    AsyncTimeoutInterceptor,
    CircuitBreakerConfig,
    DeadlineBudgetConfig,
    WaitForReadyConfig,
)
from grpc_client_kit.settings import (
    BaseChannelPoolSettings,
    BaseCircuitBreakerSettings,
    BaseConnectivitySettings,
    BaseDeadlineBudgetSettings,
    BaseGrpcClientSettings,
    BaseHealthCheckerSettings,
    BaseLoadBalancerSettings,
    BaseRetrySettings,
    BaseTimeoutSettings,
    BaseWaitForReadySettings,
)

from .conftest import layers_of, make_stub_class

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]

# A prefix no real environment sets, so the process environment cannot leak into these tests.
ENV_PREFIX = "GCK_UNIT_TEST_"

# Each shipped section against the dataclass it becomes, and the fields of that dataclass which hold
# runtime objects — callbacks, registries — and therefore cannot come from an environment.
MIRRORS = [
    (BaseConnectivitySettings, ConnectivityConfig, set()),
    (BaseTimeoutSettings, TimeoutConfig, set()),
    (BaseRetrySettings, RetryConfig, {"on_retry", "metrics"}),
    (BaseCircuitBreakerSettings, CircuitBreakerConfig, {"metrics"}),
    (BaseWaitForReadySettings, WaitForReadyConfig, set()),
    (BaseDeadlineBudgetSettings, DeadlineBudgetConfig, set()),
    (BaseLoadBalancerSettings, LoadBalancerConfig, set()),
]

# The observability flags the protocol spells differently from the dataclass.
OBSERVABILITY_FLAGS = {"tracing": "tracing_enabled", "metrics": "metrics_enabled", "logging": "logging_enabled"}


class _AppSettings(BaseSettings):
    """The reporter's service settings: one client section per upstream, nested, prefixed."""

    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX, env_nested_delimiter="__")

    users_grpc: BaseGrpcClientSettings = Field(default_factory=BaseGrpcClientSettings)


def _dataclass_default(field: DataclassField[Any]) -> Any:
    return field.default if field.default is not MISSING else field.default_factory()  # type: ignore[misc]


def _model_defaults(model: type[Any]) -> dict[str, Any]:
    return {name: info.get_default(call_default_factory=True) for name, info in model.model_fields.items()}


# --------------------------------------------------------------------------------------------
# The reporter's scenario: environment -> nested model -> the factory, and the dataclasses by hand.
# --------------------------------------------------------------------------------------------


def test__client_settings__nested_under_the_service_settings__load_every_section_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One prefix per upstream, one variable per knob, and the sections come out typed and bounded."""
    # Arrange
    for name, value in {
        "USERS_GRPC__TARGET": "users.internal:50051",
        "USERS_GRPC__INSECURE": "true",
        "USERS_GRPC__COMPRESSION": "gzip",
        "USERS_GRPC__OPTIONS": '[["grpc.max_receive_message_length", 8388608]]',
        "USERS_GRPC__CONNECTIVITY__KEEPALIVE_TIME": "15",
        "USERS_GRPC__SUCCESS_LOG_LEVEL": "DEBUG",
        "USERS_GRPC__TIMEOUT__DEFAULT": "5",
        "USERS_GRPC__TIMEOUT__PER_METHOD": '{"/users.v1.Users/Export": 60}',
        "USERS_GRPC__RETRY__MAX_ATTEMPTS": "5",
        "USERS_GRPC__RETRY__RETRYABLE_CODES": '["UNAVAILABLE", "aborted"]',
        "USERS_GRPC__CIRCUIT_BREAKER__FAIL_THRESHOLD": "7",
        "USERS_GRPC__BALANCER__STRATEGY": "random",
    }.items():
        monkeypatch.setenv(ENV_PREFIX + name, value)

    # Act
    users = _AppSettings().users_grpc

    # Assert
    assert users.to_config() == GrpcClientConfig(
        target="users.internal:50051",
        insecure=True,
        options=[("grpc.max_receive_message_length", 8388608)],
        compression=grpc.Compression.Gzip,
        connectivity=ConnectivityConfig(keepalive_time=15.0),
    )
    assert users.success_log_level == logging.DEBUG
    assert users.timeout is not None
    assert users.timeout.to_config() == TimeoutConfig(default=5.0, per_method={"/users.v1.Users/Export": 60.0})
    assert users.retry is not None
    assert users.retry.to_config() == RetryConfig(
        max_attempts=5, retryable_codes={grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.ABORTED}
    )
    assert users.circuit_breaker is not None
    assert users.circuit_breaker.to_config() == CircuitBreakerConfig(fail_threshold=7)
    assert users.balancer is not None
    assert users.balancer.to_config() == LoadBalancerConfig(strategy=LoadBalancingStrategy.RANDOM)


def test__client_settings__handed_to_the_factory__every_section_reaches_the_client_it_builds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The blessed path: the model is the settings object, and nothing set on it is dropped on the way."""
    # Arrange
    for name, value in {
        "USERS_GRPC__TARGET": "users.internal:50051",
        "USERS_GRPC__INSECURE": "true",
        "USERS_GRPC__OPTIONS": '[["grpc.max_receive_message_length", 8388608]]',
        "USERS_GRPC__CONNECTIVITY__KEEPALIVE_TIME": "15",
        "USERS_GRPC__SUCCESS_LOG_LEVEL": "DEBUG",
        "USERS_GRPC__LOG_REQUEST_PAYLOAD": "true",
        "USERS_GRPC__TIMEOUT__DEFAULT": "5",
        "USERS_GRPC__TIMEOUT__PER_METHOD": '{"/users.v1.Users/Export": 60}',
        "USERS_GRPC__RETRY__MAX_ATTEMPTS": "5",
        "USERS_GRPC__RETRY__RETRYABLE_CODES": '["UNAVAILABLE", "ABORTED"]',
        "USERS_GRPC__CIRCUIT_BREAKER__FAIL_THRESHOLD": "7",
    }.items():
        monkeypatch.setenv(ENV_PREFIX + name, value)
    users = _AppSettings().users_grpc

    # Act
    client = GrpcClientFactory(settings=users).create_client(make_stub_class())

    # Assert
    options = client._config.channel_options()
    assert options is not None
    assert ("grpc.max_receive_message_length", 8388608) in options
    assert ("grpc.keepalive_time_ms", 15000) in options
    target = "users.internal:50051"
    (timeout,) = layers_of(client, target, AsyncTimeoutInterceptor)
    assert (timeout._default_timeout, timeout._per_method_timeouts) == (5.0, {"/users.v1.Users/Export": 60.0})
    (retry,) = layers_of(client, target, AsyncRetryInterceptor)
    assert retry._max_attempts == 5
    assert retry._retryable_codes == frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.ABORTED})
    (breaker,) = layers_of(client, target, AsyncCircuitBreakerInterceptor)
    assert breaker._fail_threshold == 7
    (logging_layer,) = layers_of(client, target, AsyncLoggingInterceptor)
    assert (logging_layer._success_log_level, logging_layer._log_request_payload) == (logging.DEBUG, True)


def test__client_settings__defaults__are_a_pooled_client_with_a_deadline_and_nothing_else() -> None:
    """A deadline is the one layer shipped on by default; everything that needs an extra or an opt-in is off."""
    # Act
    settings = BaseGrpcClientSettings()

    # Assert
    assert settings.to_config() == GrpcClientConfig()
    assert settings.pool == BaseChannelPoolSettings(max_channels_per_target=1, idle_timeout=300.0)
    assert settings.timeout == BaseTimeoutSettings(default=10.0, per_method={})
    assert (settings.tracing_enabled, settings.metrics_enabled, settings.logging_enabled) == (False, False, True)
    assert settings.retry is None
    assert settings.circuit_breaker is None
    assert settings.wait_for_ready is None
    assert settings.deadline_budget is None
    assert settings.balancer is None
    assert settings.health_checker is None
    assert settings.connectivity is None


# --------------------------------------------------------------------------------------------
# The hazard: a nested section cannot pick up an unprefixed variable.
# --------------------------------------------------------------------------------------------


def test__client_settings__bare_variables_in_the_environment__do_not_reach_a_nested_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bare TARGET, DEFAULT or MAX_ATTEMPTS in a pod must not become anyone's client configuration."""
    # Arrange
    for name, value in {"TARGET": "evil:1", "DEFAULT": "0", "MAX_ATTEMPTS": "99", "FAIL_THRESHOLD": "1"}.items():
        monkeypatch.setenv(name, value)
    # A variable of its own, so that the retry block exists to be reached.
    monkeypatch.setenv(ENV_PREFIX + "USERS_GRPC__RETRY__INITIAL_BACKOFF", "0.5")

    # Act
    users = _AppSettings().users_grpc

    # Assert
    assert users.target is None
    assert users.timeout is not None
    assert users.timeout.default == 10.0
    assert users.retry is not None
    assert (users.retry.max_attempts, users.retry.initial_backoff) == (3, 0.5)
    assert users.circuit_breaker is None


@pytest.mark.parametrize("name", settings_module.__all__)
def test__shipped_models__none_of_them__reads_the_environment_on_its_own(name: str) -> None:
    """Every class is a plain BaseModel: the environment is read by the service's settings, or not at all."""
    # Act & Assert
    assert not issubclass(getattr(settings_module, name), BaseSettings)


def test__shipped_models__unknown_field__is_refused_rather_than_ignored() -> None:
    """A typo in a variable name fails at load; a default silently taken instead is the drift being fixed."""
    # Act & Assert
    with pytest.raises(ValidationError, match="max_attemps"):
        BaseGrpcClientSettings(retry={"max_attemps": 1})  # type: ignore[arg-type]


# --------------------------------------------------------------------------------------------
# The drift: each model's field set and defaults are the dataclass's, minus the runtime objects.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("model", "dataclass", "runtime_only"), MIRRORS)
def test__section_model__fields_and_defaults__are_the_dataclass_s(
    model: type[Any], dataclass: type[Any], runtime_only: set[str]
) -> None:
    """A knob added to a dataclass fails here until the model carries it; so does a changed default."""
    # Act
    expected = {field.name: _dataclass_default(field) for field in fields(dataclass) if field.name not in runtime_only}

    # Assert
    assert _model_defaults(model) == expected


@pytest.mark.parametrize(("model", "dataclass", "runtime_only"), MIRRORS)
def test__section_model__built_with_nothing__becomes_the_dataclass_built_with_nothing(
    model: type[Any], dataclass: type[Any], runtime_only: set[str]
) -> None:
    """The translation is the identity on defaults, runtime objects left at None."""
    # Act & Assert
    assert model().to_config() == dataclass()


def test__client_settings__fields_and_defaults__cover_the_channel_config_and_the_observability_config() -> None:
    """The top-level model mirrors GrpcClientConfig minus credentials, and ObservabilityConfig minus the runtime."""
    # Arrange
    channel = {
        field.name: _dataclass_default(field) for field in fields(GrpcClientConfig) if field.name != "credentials"
    }
    observability = {
        OBSERVABILITY_FLAGS.get(field.name, field.name): _dataclass_default(field)
        for field in fields(ObservabilityConfig)
        if field.name not in {"service_name", "metrics_registry"}
    }

    # Act
    defaults = _model_defaults(BaseGrpcClientSettings)

    # Assert
    assert {name: defaults[name] for name in channel} == channel
    assert {name: defaults[name] for name in observability} == observability


def test__client_settings__pool_and_health_checker_blocks__carry_what_the_factory_reads() -> None:
    """Neither block becomes a dataclass, so their field sets are pinned to the factory's reads directly."""
    # Act & Assert
    assert set(BaseChannelPoolSettings.model_fields) == {"max_channels_per_target", "idle_timeout"}
    assert set(BaseHealthCheckerSettings.model_fields) == {"check_interval", "timeout", "service"}


# --------------------------------------------------------------------------------------------
# What the factory requires of a settings object, the model provides.
# --------------------------------------------------------------------------------------------


def test__client_settings__instance__satisfies_the_settings_protocol() -> None:
    """The factory validates its settings against the protocol at construction; the model must pass."""
    # Act & Assert
    assert isinstance(BaseGrpcClientSettings(), GrpcClientSettingsProtocol)
    GrpcClientFactory(settings=BaseGrpcClientSettings(target="localhost:50051"))


def test__client_settings__type_checked_as_the_protocol__passes_mypy(tmp_path: Path) -> None:
    """Runtime conformance is by attribute names; a field typed wider than the protocol only mypy can see."""
    # Arrange
    snippet = tmp_path / "settings_snippet.py"
    snippet.write_text(
        "from grpc_client_kit import GrpcClientFactory, GrpcClientSettingsProtocol\n"
        "from grpc_client_kit.settings import BaseGrpcClientSettings, BaseRetrySettings\n"
        "\n"
        "\n"
        "class Settings(BaseGrpcClientSettings):\n"
        "    retry: BaseRetrySettings | None = BaseRetrySettings(max_attempts=5)\n"
        "\n"
        "\n"
        "def takes_protocol(settings: GrpcClientSettingsProtocol) -> None: ...\n"
        "\n"
        "\n"
        "takes_protocol(BaseGrpcClientSettings())\n"
        "GrpcClientFactory(settings=Settings())\n"
        "reveal_type(Settings().to_config())\n"
    )

    # Act
    result = subprocess.run(  # noqa: S603 - fixed argv, only the snippet path varies
        [sys.executable, "-m", "mypy", "--config-file", str(ROOT / "pyproject.toml"), str(snippet)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    # Assert
    assert result.returncode == 0, result.stdout
    # ``ignore_missing_imports`` would type an unresolved module as Any and pass vacuously; the
    # revealed type proves the model really was resolved.
    assert re.search(r'Revealed type is ".*GrpcClientConfig"', result.stdout), result.stdout


# --------------------------------------------------------------------------------------------
# Values an operator writes, and values the kit refuses.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("given", ["gzip", "Gzip", grpc.Compression.Gzip, 2])
def test__client_settings__compression__accepts_the_name_the_member_and_the_number(given: object) -> None:
    """An operator writes ``gzip``; code writes the enum; both end as what the channel takes."""
    # Act & Assert
    assert BaseGrpcClientSettings(compression=given).compression is grpc.Compression.Gzip  # type: ignore[arg-type]


def test__client_settings__unknown_compression__is_refused_naming_the_choices() -> None:
    """A misspelled algorithm must not silently leave the gRPC default alone."""
    # Act & Assert
    with pytest.raises(ValidationError, match="none, deflate, gzip"):
        BaseGrpcClientSettings(compression="bzip2")  # type: ignore[arg-type]


@pytest.mark.parametrize(("given", "level"), [("debug", logging.DEBUG), ("WARNING", logging.WARNING), ("10", 10)])
def test__client_settings__success_log_level__accepts_a_name_or_a_number(given: str, level: int) -> None:
    """An environment hands over strings: a level name, or the number logging uses."""
    # Act & Assert
    assert BaseGrpcClientSettings(success_log_level=given).success_log_level == level  # type: ignore[arg-type]


def test__client_settings__unknown_log_level__is_refused() -> None:
    # Act & Assert
    with pytest.raises(ValidationError, match="unknown log level"):
        BaseGrpcClientSettings(success_log_level="verbose")  # type: ignore[arg-type]


def test__retry_settings__unknown_status_code__is_refused_naming_the_choices() -> None:
    """A code that does not exist would otherwise be a retry policy that never fires."""
    # Act & Assert
    with pytest.raises(ValidationError, match=r"unknown gRPC status code 'BOGUS'.*UNAVAILABLE"):
        BaseRetrySettings(retryable_codes=["BOGUS"])  # type: ignore[list-item]


def test__retry_settings__status_codes_as_members__are_kept() -> None:
    """Code, unlike an environment, writes the enum itself."""
    # Act & Assert
    assert BaseRetrySettings(retryable_codes={grpc.StatusCode.UNAVAILABLE}).retryable_codes == {
        grpc.StatusCode.UNAVAILABLE
    }


@pytest.mark.parametrize(
    ("model", "field", "value"),
    [
        (BaseConnectivitySettings, "keepalive_time", 0.0),
        (BaseConnectivitySettings, "max_pings_without_data", -1),
        (BaseChannelPoolSettings, "max_channels_per_target", 0),
        (BaseTimeoutSettings, "default", -1.0),
        (BaseRetrySettings, "max_attempts", 0),
        (BaseRetrySettings, "backoff_multiplier", 0.5),
        (BaseRetrySettings, "jitter", 1.5),
        (BaseCircuitBreakerSettings, "fail_threshold", 0),
        (BaseCircuitBreakerSettings, "half_open_max_calls", 0),
        (BaseDeadlineBudgetSettings, "reserve_for_next", -1.0),
        (BaseHealthCheckerSettings, "check_interval", 0.0),
    ],
)
def test__section_model__value_the_layer_would_refuse__is_refused_at_load(
    model: type[Any], field: str, value: float
) -> None:
    """The interceptors validate at construction; the models validate at load, naming the field."""
    # Act & Assert
    with pytest.raises(ValidationError, match=field):
        model(**{field: value})


def test__load_balancer_settings__unknown_strategy__is_refused() -> None:
    # Act & Assert
    with pytest.raises(ValidationError, match="strategy"):
        BaseLoadBalancerSettings(strategy="sticky")  # type: ignore[arg-type]


def test__client_settings__target_and_targets_together__are_refused() -> None:
    """The client would refuse a balancer next to a fixed target anyway; the model says so at load."""
    # Act & Assert
    with pytest.raises(ValidationError, match="mutually exclusive"):
        BaseGrpcClientSettings(target="a:1", targets=["a:1", "b:1"])


# --------------------------------------------------------------------------------------------
# Runtime objects stay out of the model and go where the config is built.
# --------------------------------------------------------------------------------------------


def test__client_settings__to_config_with_credentials__carries_them_into_the_channel_config() -> None:
    """A credentials object is built in code, so it is handed in at translation time."""
    # Arrange
    credentials = MagicMock(spec=grpc.ChannelCredentials)

    # Act
    config = BaseGrpcClientSettings(target="secure:443").to_config(credentials=credentials)

    # Assert
    assert config.credentials is credentials
    assert config.insecure is False


def test__client_settings__to_config_with_credentials_on_an_insecure_channel__is_refused_by_the_dataclass() -> None:
    """The contradiction is still checked where it always was, not re-implemented in the model."""
    # Act & Assert
    with pytest.raises(ValueError, match="Cannot provide credentials for an insecure channel"):
        BaseGrpcClientSettings(target="a:1", insecure=True).to_config(
            credentials=MagicMock(spec=grpc.ChannelCredentials)
        )


def test__connectivity_settings__contradictory_reconnect_bounds__are_refused_by_the_dataclass() -> None:
    """The cross-field check belongs to the dataclass; the model does not repeat it."""
    # Act & Assert
    with pytest.raises(ValueError, match="min_reconnect_backoff must not exceed"):
        BaseConnectivitySettings(min_reconnect_backoff=10.0, max_reconnect_backoff=1.0).to_config()


def test__section_models__to_config__hand_out_copies_of_their_containers() -> None:
    """A chain must not share a dict with the settings object it was built from."""
    # Arrange
    settings = BaseTimeoutSettings(per_method={"/pkg.Svc/M": 1.0})

    # Act
    config = settings.to_config()

    # Assert
    assert config.per_method == settings.per_method
    assert config.per_method is not settings.per_method


# --------------------------------------------------------------------------------------------
# Packaging.
# --------------------------------------------------------------------------------------------


def test__settings_module__all__is_sorted_and_complete() -> None:
    # Act & Assert
    assert settings_module.__all__ == sorted(settings_module.__all__)
    assert set(settings_module.__all__) == {
        "BaseGrpcClientSettings",
        "BaseChannelPoolSettings",
        "BaseCircuitBreakerSettings",
        "BaseConnectivitySettings",
        "BaseDeadlineBudgetSettings",
        "BaseHealthCheckerSettings",
        "BaseLoadBalancerSettings",
        "BaseRetrySettings",
        "BaseTimeoutSettings",
        "BaseWaitForReadySettings",
    }


def test__settings_module__extra_missing__raises_the_install_hint() -> None:
    """Without pydantic the import fails naming the extra, not with a bare ModuleNotFoundError."""
    # Arrange
    with patch.dict("sys.modules", {"pydantic": None}):
        sys.modules.pop("grpc_client_kit.settings", None)

        # Act & Assert
        with pytest.raises(ImportError, match=r"grpc-client-kit\[settings\]"):
            importlib.import_module("grpc_client_kit.settings")
