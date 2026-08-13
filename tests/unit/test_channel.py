"""Unit tests for the channel pool.

A pooled channel is shared by every caller that asks for the same identity, so the two things worth
pinning down are what makes two requests the same identity and what happens to a channel that is
unhealthy or caught in a shutdown. Idleness is deliberately absent from that list: the pool
delegates it to gRPC core (``grpc.client_idle_timeout_ms``), which parks connections without ever
invalidating a channel object someone may still hold.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from grpc_client_kit.channel import ChannelKey, ChannelPool, ChannelWrapper, chain_token
from grpc_client_kit.config import ConnectivityConfig, GrpcClientConfig
from tests.helpers import make_channel, make_interceptor, make_metrics

from .conftest import blocking_close

pytestmark = pytest.mark.unit

_IDLE_OPTION = "grpc.client_idle_timeout_ms"


def _idle_options_of(mock_factory: MagicMock) -> dict[str, object]:
    """Return the options the channel factory was called with, as a dict."""
    options = mock_factory.call_args.kwargs["options"]
    return dict(options) if options else {}


# --------------------------------------------------------------------------------------------
# Channel identity: what makes two requests share a channel.
# --------------------------------------------------------------------------------------------


def test__chain_token__interceptor_chain__identifies_its_members_and_their_order() -> None:
    """Token identifies the exact chain members and their order."""
    # Arrange
    first = make_interceptor()
    second = make_interceptor()

    # Act & Assert
    assert chain_token(None) is None
    assert chain_token([]) is None
    assert chain_token([first, second]) == chain_token([first, second])
    assert chain_token([first, second]) != chain_token([second, first])
    assert chain_token([first]) != chain_token([second])


def test__channel_key__any_creation_argument_differing__makes_a_different_key() -> None:
    """Every channel-creation argument takes part in the pool key."""
    # Arrange
    base = ChannelKey.build("h:1")

    # Act & Assert
    assert base == ChannelKey.build("h:1")
    assert hash(base) == hash(ChannelKey.build("h:1"))
    assert base != ChannelKey.build("h:1", insecure=True)
    assert base != ChannelKey.build("h:1", compression=grpc.Compression.Gzip)
    assert base != ChannelKey.build("h:1", options=[("grpc.max_receive_message_length", 1024)])
    assert base != ChannelKey.build("h:1", interceptors=[make_interceptor()])
    assert base != ChannelKey.build("h:2")


def test__channel_key__equal_option_lists__produce_equal_keys() -> None:
    """Equal option lists must not produce different keys just because they are different objects."""
    # Act
    first = ChannelKey.build("h:1", options=[("grpc.enable_retries", 1)])
    second = ChannelKey.build("h:1", options=[("grpc.enable_retries", 1)])

    # Assert
    assert first == second
    assert hash(first) == hash(second)
    assert first != ChannelKey.build("h:1", options=[("grpc.enable_retries", 0)])


def test__channel_key__options_built_from_equal_connectivity_configs__produce_equal_keys() -> None:
    """Connectivity tuning goes into the pool key, so building it twice must key the same channel."""
    # Arrange
    tuned = GrpcClientConfig(target="h:1", connectivity=ConnectivityConfig(keepalive_time=15.0))
    same = GrpcClientConfig(target="h:1", connectivity=ConnectivityConfig(keepalive_time=15.0))

    # Act
    first = ChannelKey.build("h:1", options=tuned.channel_options())
    second = ChannelKey.build("h:1", options=same.channel_options())

    # Assert
    assert first == second
    assert hash(first) == hash(second)
    # And every field of the tuning takes part: a client that pings differently needs its own
    # channel, because gRPC binds these arguments when the channel is created.
    other = GrpcClientConfig(target="h:1", connectivity=ConnectivityConfig(keepalive_time=16.0))
    assert first != ChannelKey.build("h:1", options=other.channel_options())


def test__channel_key__untuned_client__keys_the_channel_it_always_did() -> None:
    """Adding the field must not move an existing client onto a different channel identity."""
    # Arrange
    options = [("grpc.enable_retries", 1)]
    untuned = GrpcClientConfig(target="h:1", options=options)

    # Act
    key = ChannelKey.build("h:1", options=untuned.channel_options())

    # Assert
    assert key == ChannelKey.build("h:1", options=options)


def test__make_key__same_request_twice__yields_equal_cacheable_keys() -> None:
    """make_key is the cacheable form of a request: equal requests must produce equal keys."""
    # Arrange
    pool = ChannelPool()

    # Act
    first = pool.make_key("h:1", insecure=True)
    second = pool.make_key("h:1", insecure=True)

    # Assert
    assert first == second
    assert hash(first) == hash(second)


def test__make_key__invalid_target__is_refused() -> None:
    """Validation happens where the key is built, so a cached key is a validated one."""
    # Act & Assert
    with pytest.raises(ValueError, match="Target address cannot be empty"):
        ChannelPool().make_key("")


# --------------------------------------------------------------------------------------------
# Idle management: delegated to gRPC core, never enforced by closing channels.
# --------------------------------------------------------------------------------------------


async def test__channel_pool__idle_timeout__is_delegated_to_grpc_core_as_a_channel_option() -> None:
    """The pool must never close an idle channel itself: a held stub or a long stream would die.

    Instead the idle timeout is handed to gRPC core, which parks the connection only when no RPC
    is active and transparently reconnects on the next call.
    """
    # Arrange
    pool = ChannelPool(idle_timeout=120.0)

    # Act
    async with pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_factory.return_value = make_channel()
            await pool.get_channel("localhost:50051", insecure=True)

    # Assert
    assert _idle_options_of(mock_factory)[_IDLE_OPTION] == 120_000


async def test__channel_pool__explicit_idle_option__wins_over_the_pool_default() -> None:
    """A caller that tuned core idling explicitly must not have the pool second-guess it."""
    # Arrange
    pool = ChannelPool(idle_timeout=120.0)

    # Act
    async with pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_factory.return_value = make_channel()
            await pool.get_channel("localhost:50051", insecure=True, options=[(_IDLE_OPTION, 5_000)])

    # Assert
    options = mock_factory.call_args.kwargs["options"]
    assert options == [(_IDLE_OPTION, 5_000)]


async def test__channel_pool__idle_disabled__adds_no_idle_option() -> None:
    """idle_timeout of zero or less means never idle, so no option is injected."""
    # Arrange
    pool = ChannelPool(idle_timeout=0)

    # Act
    async with pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_factory.return_value = make_channel()
            await pool.get_channel("localhost:50051", insecure=True)

    # Assert
    assert mock_factory.call_args.kwargs["options"] is None


async def test__channel_pool__channel_handed_out__is_never_closed_behind_the_caller() -> None:
    """The only thing that closes pooled channels is close_all: held stubs stay valid for life."""
    # Arrange
    pool = ChannelPool(idle_timeout=0.01)
    mock_channel = make_channel()

    # Act
    with patch.object(pool, "_create_aio_channel", return_value=mock_channel):
        await pool.get_channel("localhost:50051", insecure=True)
        # Far longer than the idle timeout; under the old sweeper this killed the channel.
        await asyncio.sleep(0.05)

    # Assert
    mock_channel.close.assert_not_called()

    await pool.close_all()
    mock_channel.close.assert_called_once()


# --------------------------------------------------------------------------------------------
# Acquiring a channel.
# --------------------------------------------------------------------------------------------


async def test__channel_pool__unknown_target__creates_a_channel_and_keeps_it() -> None:
    """The first request for an identity opens the channel every later one will reuse."""
    # Arrange
    target = "localhost:50051"

    # Act
    async with ChannelPool() as pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_channel = make_channel()
            mock_factory.return_value = mock_channel
            channel = await pool.get_channel(target, insecure=True)

            # Assert
            assert channel == mock_channel
            mock_factory.assert_called_once()
            entry = pool._entries[pool.make_key(target, insecure=True)]
            assert len(entry.channels) == 1
            assert entry.channels[0].channel == mock_channel


async def test__channel_pool__same_identity_requested_twice__reuses_the_healthy_channel() -> None:
    """Pooling only pays off if the second request does not open a second connection."""
    # Arrange
    target = "localhost:50051"

    # Act
    async with ChannelPool() as pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_channel = make_channel()
            mock_factory.return_value = mock_channel
            await pool.get_channel(target, insecure=True)
            channel = await pool.get_channel(target, insecure=True)

            # Assert
            assert channel == mock_channel
            assert mock_factory.call_count == 1


async def test__channel_pool__precomputed_key__skips_rebuilding_and_lands_on_the_same_channel() -> None:
    """A cached key from make_key must reach the very channel the plain-argument path pooled."""
    # Arrange
    target = "localhost:50051"

    # Act
    async with ChannelPool() as pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_factory.return_value = make_channel()
            key = pool.make_key(target, insecure=True)
            first = await pool.get_channel(target, insecure=True)
            second = await pool.get_channel(target, interceptors=None, key=key)

            # Assert
            assert first is second
            assert mock_factory.call_count == 1


async def test__channel_pool__secure_request_after_an_insecure_one__opens_its_own_channel() -> None:
    """A secure request must not inherit the insecure channel opened for the same address."""
    # Arrange
    credentials = MagicMock(spec=grpc.ChannelCredentials)
    insecure_channel = make_channel()
    secure_channel = make_channel()

    # Act
    async with ChannelPool() as pool:
        with (
            patch("grpc.aio.insecure_channel", return_value=insecure_channel),
            patch("grpc.aio.secure_channel", return_value=secure_channel),
        ):
            first = await pool.get_channel("localhost:50051", insecure=True)
            second = await pool.get_channel("localhost:50051", insecure=False, credentials=credentials)

    # Assert
    assert first is insecure_channel
    assert second is secure_channel


async def test__channel_pool__different_options_or_compression__open_their_own_channels() -> None:
    """Options and compression are bound at creation, so they must split the pool."""
    # Arrange
    channels = [make_channel(), make_channel(), make_channel()]

    # Act
    async with ChannelPool() as pool:
        with patch.object(pool, "_create_aio_channel", side_effect=channels):
            plain = await pool.get_channel("localhost:50051", insecure=True)
            with_options = await pool.get_channel(
                "localhost:50051", insecure=True, options=[("grpc.enable_retries", 1)]
            )
            compressed = await pool.get_channel("localhost:50051", insecure=True, compression=grpc.Compression.Gzip)

    # Assert
    assert [plain, with_options, compressed] == channels
    assert len({plain, with_options, compressed}) == 3


async def test__channel_pool__different_interceptor_chains__open_their_own_channels() -> None:
    """Two clients on one address must not inherit each other's interceptor chain."""
    # Arrange
    first_chain = [make_interceptor()]
    second_chain = [make_interceptor()]
    first_channel = make_channel()
    second_channel = make_channel()

    # Act
    async with ChannelPool() as pool:
        with patch.object(pool, "_create_aio_channel", side_effect=[first_channel, second_channel]):
            first = await pool.get_channel("localhost:50051", insecure=True, interceptors=first_chain)
            first_again = await pool.get_channel("localhost:50051", insecure=True, interceptors=first_chain)
            second = await pool.get_channel("localhost:50051", insecure=True, interceptors=second_chain)

    # Assert
    assert first is first_channel
    assert first_again is first_channel
    assert second is second_channel


async def test__channel_pool__unhealthy_channel_below_the_limit__is_replaced() -> None:
    """An unhealthy channel is replaced, and the healthy replacement is then reused."""
    # Arrange
    target = "localhost:50051"
    first_channel = make_channel()
    second_channel = make_channel()

    # Act & Assert
    async with ChannelPool(max_channels_per_target=2, health_checker=MagicMock()) as pool:
        with patch.object(pool, "_create_aio_channel", side_effect=[first_channel, second_channel]):
            assert await pool.get_channel(target, insecure=True) is first_channel

            await pool.update_channel_health(target, False)

            # The only channel is unhealthy and the limit allows one more.
            assert await pool.get_channel(target, insecure=True) is second_channel
            # The healthy one is now preferred over the unhealthy one.
            assert await pool.get_channel(target, insecure=True) is second_channel


async def test__channel_pool__external_health_verdict__is_honoured_without_an_own_checker() -> None:
    """update_channel_health is a public seam: an external checker's verdict counts even when the
    pool was built without one, otherwise the factory wiring marks channels nobody ever avoids."""
    # Arrange
    target = "localhost:50051"
    first_channel = make_channel()
    second_channel = make_channel()

    # Act & Assert
    async with ChannelPool(max_channels_per_target=2) as pool:
        with patch.object(pool, "_create_aio_channel", side_effect=[first_channel, second_channel]):
            assert await pool.get_channel(target, insecure=True) is first_channel

            await pool.update_channel_health(target, False)

            assert await pool.get_channel(target, insecure=True) is second_channel


async def test__channel_pool__every_channel_unhealthy_at_the_limit__hands_one_out_anyway() -> None:
    """At the per-identity limit an unhealthy channel is better than no channel."""
    # Arrange
    target = "localhost:50051"
    channels = [make_channel(), make_channel()]

    # Act
    async with ChannelPool(max_channels_per_target=2, health_checker=MagicMock()) as pool:
        with patch.object(pool, "_create_aio_channel", side_effect=channels):
            await pool.get_channel(target, insecure=True)
            await pool.update_channel_health(target, False)
            await pool.get_channel(target, insecure=True)
            await pool.update_channel_health(target, False)

            # Assert
            assert await pool.get_channel(target, insecure=True) in channels


async def test__channel_pool__invalid_configuration_or_target__is_refused() -> None:
    """Guard rails sit at construction and at acquisition, where the mistake is still visible."""
    # Act & Assert
    with pytest.raises(ValueError, match="max_channels_per_target must be positive"):
        ChannelPool(max_channels_per_target=0)

    async with ChannelPool() as pool:
        with pytest.raises(ValueError, match="Target address cannot be empty"):
            await pool.get_channel("")


# --------------------------------------------------------------------------------------------
# Shutdown.
# --------------------------------------------------------------------------------------------


async def test__channel_pool__close_all__drains_the_pool_and_leaves_it_reusable() -> None:
    """close_all() drains the pool instead of retiring it."""
    # Arrange
    pool = ChannelPool()
    target = "localhost:50051"
    first_channel = make_channel()
    second_channel = make_channel()

    with patch.object(pool, "_create_aio_channel", return_value=first_channel):
        await pool.get_channel(target, insecure=True)

    # Act
    await pool.close_all()

    # Assert
    first_channel.close.assert_called_once()
    assert pool._entries == {}
    assert pool._closing is False
    assert await pool.health_check() is True

    # A shared pool has to survive one shutdown: acquiring again must work, not raise.
    with patch.object(pool, "_create_aio_channel", return_value=second_channel):
        assert await pool.get_channel(target, insecure=True) is second_channel

    await pool.close_all()


async def test__channel_pool__acquire_while_closing__is_refused() -> None:
    """No channel may escape a shutdown that is already in progress."""
    # Arrange
    pool = ChannelPool()
    target = "localhost:50051"
    closing_started = asyncio.Event()
    finish_closing = asyncio.Event()
    mock_channel = make_channel()
    mock_channel.close = AsyncMock(side_effect=blocking_close(closing_started, finish_closing))

    # Act & Assert
    with patch.object(pool, "_create_aio_channel", return_value=mock_channel):
        await pool.get_channel(target, insecure=True)

        closing = asyncio.create_task(pool.close_all())
        await closing_started.wait()

        with pytest.raises(RuntimeError, match="ChannelPool is closing"):
            await pool.get_channel(target, insecure=True)

        finish_closing.set()
        await closing


async def test__channel_pool__caller_waiting_across_a_shutdown__does_not_get_an_orphan_channel() -> None:
    """A caller that waited for the lock across a whole shutdown must not get an orphan channel."""
    # Arrange
    pool = ChannelPool()
    target = "localhost:50051"
    key = pool.make_key(target, insecure=True)

    with patch.object(pool, "_create_aio_channel", return_value=make_channel()):
        await pool.get_channel(target, insecure=True)

        entry = pool._entries[key]
        await entry.lock.acquire()

        # Act
        acquiring = asyncio.create_task(pool.get_channel(target, insecure=True))
        for _ in range(10):
            # The task checks the entry out before blocking on its lock.
            if entry.users == 1:
                break
            await asyncio.sleep(0)
        assert entry.users == 1, "the task should be waiting on the entry lock"

        # The pool is drained and reopened while the caller still waits for the lock.
        await pool.close_all()
        entry.lock.release()

        # Assert
        with pytest.raises(RuntimeError, match="ChannelPool is closing"):
            await acquiring

    assert pool._entries == {}


async def test__channel_pool__used_as_a_context_manager__is_drained_on_exit() -> None:
    """Leaving the block closes what the pool holds without retiring the pool itself."""
    # Arrange
    target = "localhost:50051"

    # Act
    async with ChannelPool() as pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_factory.return_value = make_channel()
            await pool.get_channel(target, insecure=True)
            assert len(pool._entries) == 1

    # Assert
    assert pool._entries == {}
    assert pool._closing is False


async def test__channel_pool__entry_released_empty__is_retired_instead_of_leaking() -> None:
    """An entry that ends up with no channels and no users must leave the registry."""
    # Arrange
    pool = ChannelPool()
    key = pool.make_key("localhost:50051", insecure=True)

    # Act
    entry = await pool._checkout(key)
    assert pool._entries[key] is entry
    await pool._checkin(key, entry)

    # Assert
    assert key not in pool._entries
    assert entry.active is False


# --------------------------------------------------------------------------------------------
# Health and metrics.
# --------------------------------------------------------------------------------------------


async def test__channel_pool__health_check_while_closing__reports_unavailable() -> None:
    """Health check reports unavailability only while a shutdown is running."""
    # Arrange
    pool = ChannelPool()
    pool._closing = True

    # Act & Assert
    assert await pool.health_check() is False


async def test__channel_pool__update_channel_health__flips_the_flag_both_ways() -> None:
    """Health is a property of the server, and it can recover as well as degrade."""
    # Arrange
    target = "localhost:50051"

    # Act & Assert
    async with ChannelPool() as pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_factory.return_value = make_channel()
            await pool.get_channel(target, insecure=True)

            wrapper = pool._entries[pool.make_key(target, insecure=True)].channels[0]
            assert wrapper.is_healthy is True

            await pool.update_channel_health(target, False)
            assert wrapper.is_healthy is False

            await pool.update_channel_health(target, True)
            assert wrapper.is_healthy is True


async def test__channel_pool__update_channel_health__covers_every_identity_of_the_address() -> None:
    """Health belongs to the server, so every channel identity of that address is updated."""
    # Arrange
    target = "localhost:50051"

    # Act
    async with ChannelPool() as pool:
        with patch.object(pool, "_create_aio_channel", side_effect=[make_channel(), make_channel()]):
            await pool.get_channel(target, insecure=True)
            await pool.get_channel(target, insecure=True, options=[("grpc.enable_retries", 1)])

        await pool.update_channel_health(target, False)

        # Assert
        wrappers = [wrapper for entry in pool._entries.values() for wrapper in entry.channels]
        assert len(wrappers) == 2
        assert all(not wrapper.is_healthy for wrapper in wrappers)


async def test__channel_pool__update_channel_health_of_an_unknown_target__is_a_no_op() -> None:
    """Updating an address the pool never opened is a no-op."""
    # Act & Assert
    async with ChannelPool() as pool:
        await pool.update_channel_health("unknown:1", False)
        assert pool._entries == {}


async def test__channel_pool__channel_acquired__records_the_pool_stats() -> None:
    """The gauges have to move when the pool does, or they describe a pool nobody is using."""
    # Arrange
    metrics = make_metrics()
    target = "localhost:50051"

    # Act
    async with ChannelPool(metrics=metrics) as pool:
        with patch("grpc.aio.insecure_channel") as mock_factory:
            mock_factory.return_value = make_channel()
            await pool.get_channel(target, insecure=True)

            # Assert
            metrics.record_pool_stats.assert_called()
            kwargs = metrics.record_pool_stats.call_args.kwargs
            assert kwargs["active_channels"] == 1
            assert kwargs["idle_targets"] == 1


async def test__channel_pool__two_identities_of_one_address__count_as_one_target() -> None:
    """Two identities of one address are one target as far as pool stats go."""
    # Arrange
    metrics = make_metrics()

    # Act
    async with ChannelPool(metrics=metrics) as pool:
        with patch.object(pool, "_create_aio_channel", side_effect=[make_channel(), make_channel()]):
            await pool.get_channel("localhost:50051", insecure=True)
            await pool.get_channel("localhost:50051", insecure=True, compression=grpc.Compression.Gzip)

        # Assert
        kwargs = metrics.record_pool_stats.call_args.kwargs
        assert kwargs["active_channels"] == 2
        assert kwargs["idle_targets"] == 1


async def test__channel_pool__health_check_of_a_target__delegates_to_the_health_checker() -> None:
    """The pool has no opinion about a server's health; it asks whoever is configured to know."""
    # Arrange
    health_checker = AsyncMock()
    health_checker.check_health.return_value = True
    target = "localhost:50051"

    # Act
    async with ChannelPool(health_checker=health_checker) as pool:
        healthy = await pool.health_check(target)

    # Assert
    assert healthy is True
    health_checker.check_health.assert_called_once_with(target)


async def test__channel_pool__secure_request__opens_a_secure_channel_with_the_credentials() -> None:
    """The secure path reaches grpc's own factory with the credentials it was given."""
    # Arrange
    credentials = MagicMock(spec=grpc.ChannelCredentials)
    target = "secure:443"

    # Act
    async with ChannelPool() as pool:
        with patch("grpc.aio.secure_channel") as mock_secure:
            mock_channel = make_channel()
            mock_secure.return_value = mock_channel
            channel = await pool.get_channel(target, insecure=False, credentials=credentials)

    # Assert
    assert channel == mock_channel
    mock_secure.assert_called_once()
    assert mock_secure.call_args.args[0] == target
    assert mock_secure.call_args.args[1] == credentials


async def test__channel_pool__request_without_credentials__defaults_to_tls() -> None:
    """Security is the default: not asking for insecure must not silently produce plaintext."""
    # Act
    async with ChannelPool() as pool:
        with (
            patch("grpc.aio.secure_channel") as mock_secure,
            patch("grpc.aio.insecure_channel") as mock_insecure,
        ):
            mock_secure.return_value = make_channel()
            await pool.get_channel("secure:443")

    # Assert
    mock_secure.assert_called_once()
    mock_insecure.assert_not_called()


def test__channel_wrapper__built__starts_healthy() -> None:
    """A fresh channel is healthy until something says otherwise."""
    # Act
    wrapper = ChannelWrapper(channel=make_channel(), target="localhost:50051")

    # Assert
    assert wrapper.is_healthy is True
