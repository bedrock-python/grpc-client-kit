"""Unit tests for the client configuration object and the channel options it assembles."""

from __future__ import annotations

import grpc
import pytest

from grpc_client_kit import ConnectivityConfig, GrpcClientConfig

pytestmark = pytest.mark.unit

# The keepalive interval used throughout, and the channel argument it is spelled as.
KEEPALIVE_SECONDS = 30.0
KEEPALIVE_KEY = "grpc.keepalive_time_ms"


def test__client_config__built_without_arguments__is_an_unconfigured_secure_client() -> None:
    """Defaulting to a secure channel means a forgotten flag cannot silently downgrade a client."""
    # Act
    config = GrpcClientConfig()

    # Assert
    assert config.target is None
    assert config.insecure is False
    assert config.credentials is None
    assert config.options is None
    assert config.compression is None


def test__client_config__built_with_arguments__keeps_them() -> None:
    """The config is a plain carrier: what goes in is what the pool later opens a channel with."""
    # Act
    config = GrpcClientConfig(
        target="localhost:50051", insecure=True, options=[("grpc.max_receive_message_length", 1024)]
    )

    # Assert
    assert config.target == "localhost:50051"
    assert config.insecure is True
    assert config.options == [("grpc.max_receive_message_length", 1024)]


def test__channel_options__no_connectivity_config__hands_back_the_callers_own_list() -> None:
    """An untuned client must keep the exact channel identity it had before tuning existed."""
    # Arrange
    options = [("grpc.max_receive_message_length", 1024)]
    config = GrpcClientConfig(target="localhost:50051", options=options)

    # Act
    built = config.channel_options()

    # Assert
    assert built is options


def test__channel_options__no_options_at_all__stays_none() -> None:
    """None and [] are different channel identities to the pool, so the distinction is kept."""
    # Act
    built = GrpcClientConfig(target="localhost:50051").channel_options()

    # Assert
    assert built is None


def test__channel_options__connectivity_configured__adds_the_grpc_arguments_it_stands_for() -> None:
    """The point of the config: the caller writes seconds, gRPC gets the arguments it understands."""
    # Arrange
    config = GrpcClientConfig(
        target="localhost:50051",
        connectivity=ConnectivityConfig(keepalive_time=KEEPALIVE_SECONDS, keepalive_timeout=10.0),
    )

    # Act
    built = config.channel_options()

    # Assert
    assert built is not None
    assert (KEEPALIVE_KEY, 30000) in built
    assert ("grpc.keepalive_timeout_ms", 10000) in built


def test__channel_options__connectivity_and_explicit_options__keeps_both() -> None:
    """Tuning the connection must not cost a caller the unrelated options they already passed."""
    # Arrange
    config = GrpcClientConfig(
        target="localhost:50051",
        options=[("grpc.max_receive_message_length", 1024)],
        connectivity=ConnectivityConfig(),
    )

    # Act
    built = config.channel_options()

    # Assert
    assert built is not None
    assert built[0] == ("grpc.max_receive_message_length", 1024)
    assert (KEEPALIVE_KEY, 30000) in built


def test__channel_options__explicit_option_for_a_tuned_key__wins_and_is_not_duplicated() -> None:
    """gRPC is handed one value per argument, so "explicit wins" is settled before it ever sees it."""
    # Arrange
    config = GrpcClientConfig(
        target="localhost:50051",
        options=[(KEEPALIVE_KEY, 5000)],
        connectivity=ConnectivityConfig(keepalive_time=KEEPALIVE_SECONDS),
    )

    # Act
    built = config.channel_options()

    # Assert
    assert built is not None
    assert [value for key, value in built if key == KEEPALIVE_KEY] == [5000]


def test__channel_options__connectivity_that_derives_nothing__leaves_the_options_untouched() -> None:
    """A connectivity config with everything left to gRPC must not change the channel's identity."""
    # Arrange
    options = [("grpc.max_receive_message_length", 1024)]
    nothing_set = ConnectivityConfig(
        keepalive_time=None,
        keepalive_timeout=None,
        max_pings_without_data=None,
        initial_reconnect_backoff=None,
        max_reconnect_backoff=None,
    )
    config = GrpcClientConfig(target="localhost:50051", options=options, connectivity=nothing_set)

    # Act
    built = config.channel_options()

    # Assert
    # The permit-without-calls flag is always spelled out, so "nothing derived" is the case where
    # the caller set that key themselves.
    assert built is not None
    assert [key for key, _ in built] == [
        "grpc.max_receive_message_length",
        "grpc.keepalive_permit_without_calls",
    ]


def test__channel_options__connectivity_adding_nothing_new__hands_back_the_callers_own_list() -> None:
    """When the caller has already set every key, the pool must see the very list it saw before."""
    # Arrange
    options = [("grpc.keepalive_permit_without_calls", 1)]
    nothing_left = ConnectivityConfig(
        keepalive_time=None,
        keepalive_timeout=None,
        max_pings_without_data=None,
        initial_reconnect_backoff=None,
        max_reconnect_backoff=None,
    )
    config = GrpcClientConfig(target="localhost:50051", options=options, connectivity=nothing_left)

    # Act
    built = config.channel_options()

    # Assert
    assert built is options


def test__client_config__credentials_on_an_insecure_channel__is_refused() -> None:
    """The two contradict each other, and silently ignoring one would decide security by accident."""
    # Act & Assert
    with pytest.raises(ValueError, match="Cannot provide credentials for an insecure channel"):
        GrpcClientConfig(target="localhost:50051", insecure=True, credentials=grpc.ssl_channel_credentials())


def test__channel_options__asked_twice__builds_the_same_list_in_the_same_order() -> None:
    """Options are part of the pool key: a list that varied would open a channel per connect."""
    # Arrange
    config = GrpcClientConfig(target="localhost:50051", connectivity=ConnectivityConfig())

    # Act
    first = config.channel_options()
    second = config.channel_options()

    # Assert
    assert first == second


def test__channel_options__two_configs_tuned_alike__build_equal_lists() -> None:
    """Two clients configured the same way must be able to share one channel."""
    # Arrange
    first = GrpcClientConfig(target="localhost:50051", connectivity=ConnectivityConfig(keepalive_time=15.0))
    second = GrpcClientConfig(target="localhost:50051", connectivity=ConnectivityConfig(keepalive_time=15.0))

    # Act & Assert
    assert first.channel_options() == second.channel_options()


def test__connectivity_config__default_options__are_the_documented_keepalive_and_backoff_set() -> None:
    """The whole point is that a caller no longer has to know these names; so they are pinned here."""
    # Act
    options = ConnectivityConfig().to_options()

    # Assert
    assert options == [
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", 0),
        ("grpc.http2.max_pings_without_data", 2),
        ("grpc.initial_reconnect_backoff_ms", 1000),
        ("grpc.max_reconnect_backoff_ms", 30000),
    ]


def test__connectivity_config__pinging_without_calls__is_spelled_as_the_flag_grpc_reads() -> None:
    """gRPC takes an int here, and a server reads it as permission to be pinged while idle."""
    # Act
    options = ConnectivityConfig(permit_without_calls=True).to_options()

    # Assert
    assert ("grpc.keepalive_permit_without_calls", 1) in options


def test__connectivity_config__reconnect_bounds__are_converted_to_milliseconds() -> None:
    """A backend that restarts in seconds should not be waited on for gRPC's default two minutes."""
    # Act
    options = ConnectivityConfig(
        initial_reconnect_backoff=0.5,
        min_reconnect_backoff=0.5,
        max_reconnect_backoff=5.0,
    ).to_options()

    # Assert
    assert ("grpc.initial_reconnect_backoff_ms", 500) in options
    assert ("grpc.min_reconnect_backoff_ms", 500) in options
    assert ("grpc.max_reconnect_backoff_ms", 5000) in options


@pytest.mark.parametrize(
    "field_name",
    [
        "keepalive_time",
        "keepalive_timeout",
        "initial_reconnect_backoff",
        "min_reconnect_backoff",
        "max_reconnect_backoff",
    ],
)
def test__connectivity_config__non_positive_duration__is_refused(field_name: str) -> None:
    """Zero is not "off" for any of these arguments, so it can only be a mistake; None is off."""
    # Act & Assert
    with pytest.raises(ValueError, match=f"{field_name} must be positive"):
        ConnectivityConfig(**{field_name: 0.0})


def test__connectivity_config__negative_ping_allowance__is_refused() -> None:
    """0 already means "no limit" here, so a negative number has nothing left to mean."""
    # Act & Assert
    with pytest.raises(ValueError, match="max_pings_without_data must be non-negative"):
        ConnectivityConfig(max_pings_without_data=-1)


def test__connectivity_config__reconnect_bounds_in_the_wrong_order__are_refused() -> None:
    """A floor above the ceiling cannot be honoured, and gRPC would resolve it silently."""
    # Act & Assert
    with pytest.raises(ValueError, match="min_reconnect_backoff must not exceed"):
        ConnectivityConfig(min_reconnect_backoff=10.0, max_reconnect_backoff=1.0)
