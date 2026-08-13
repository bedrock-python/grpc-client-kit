"""Keepalive and reconnect tuning on real channels: does gRPC take it, and does the pool survive it.

Two things have to hold for this configuration to be worth having. The arguments must actually reach
gRPC core and change what a channel does — asserted here by timing a call against an address nothing
serves, which is governed by the reconnect backoff and by nothing else the kit sets. And the options
must not disturb the channel pool: they are part of a channel's identity, so a list assembled in a
different order, or twice over, would quietly open a second connection per client.
"""

from __future__ import annotations

from grpc_client_kit import ChannelPool, ConnectivityConfig

from .calls import seconds_until_unavailable
from .chains import canonical_chain
from .echo_bench import RunningServer, client_for
from .recording import RecordingMetrics

# Reconnect tuning tight enough that a channel gives up on an unreachable address in a fraction of
# the time gRPC's own defaults take. `min_reconnect_backoff` is the one that decides it: it also
# bounds how long a single connect attempt may sit there before it is written off.
_FAST_RECONNECT = ConnectivityConfig(
    initial_reconnect_backoff=0.1,
    min_reconnect_backoff=0.1,
    max_reconnect_backoff=0.1,
)

# How much of the untuned time the tuned channel has to beat to count as tuned at all. The measured
# gap is far wider than this — around twenty times — so the bound holds on a slow machine, where
# both measurements grow together.
_SPEEDUP = 0.5

# Deadline of the calls that are expected to fail: long enough that they report a real connection
# failure rather than running out of time, which would measure the deadline instead of the backoff.
_CALL_TIMEOUT = 10.0


async def test__connectivity__reconnect_backoff__decides_how_long_a_dead_address_is_dialed(
    pool: ChannelPool,
) -> None:
    # Arrange
    # Both measurements come from the same pool, the same chain and the same kind of address, so the
    # tuning is the only difference between them — and comparing them to each other rather than to a
    # fixed number keeps the assertion honest on any machine.

    # Act
    untuned = await seconds_until_unavailable(pool, None, timeout=_CALL_TIMEOUT)
    tuned = await seconds_until_unavailable(pool, _FAST_RECONNECT, timeout=_CALL_TIMEOUT)

    # Assert
    # A client that waits out gRPC's default backoff keeps failing long after a backend has come
    # back; this is that window, made configurable and shown to move.
    assert tuned < untuned * _SPEEDUP, f"tuned {tuned:.3f}s vs untuned {untuned:.3f}s"


async def test__connectivity__tuned_channel__serves_calls_normally(
    pool: ChannelPool,
    echo_server: RunningServer,
) -> None:
    # Arrange
    # Every argument the config emits at once, on a channel that then has to work: gRPC validates
    # channel arguments when the channel is created, so a malformed one shows up here.
    everything = ConnectivityConfig(
        keepalive_time=1.0,
        keepalive_timeout=0.5,
        permit_without_calls=True,
        max_pings_without_data=0,
        initial_reconnect_backoff=0.1,
        min_reconnect_backoff=0.1,
        max_reconnect_backoff=1.0,
    )
    client = client_for(pool, echo_server, canonical_chain(), connectivity=everything)

    # Act
    stub = await client.connect()

    # Assert
    assert await stub.echo(b"hello") == b"hello"
    assert echo_server.service.unary_unary.calls == 1


async def test__connectivity__two_clients_tuned_alike__share_one_pooled_channel(
    metrics: RecordingMetrics,
    pool: ChannelPool,
    echo_server: RunningServer,
) -> None:
    # Arrange
    # One chain shared by both clients, so the only thing that could tell their channels apart is
    # the options list each of them builds from its own config object.
    chain = canonical_chain(metrics)
    first_client = client_for(pool, echo_server, chain, connectivity=ConnectivityConfig(keepalive_time=15.0))
    second_client = client_for(pool, echo_server, chain, connectivity=ConnectivityConfig(keepalive_time=15.0))

    # Act
    first = await first_client.connect()
    second = await second_client.connect()

    # Assert
    assert first.channel is second.channel
    assert metrics.pool_stats[-1].active_channels == 1
    assert await first.echo(b"a") == b"a"
    assert await second.echo(b"b") == b"b"


async def test__connectivity__clients_tuned_differently__get_their_own_channels(
    metrics: RecordingMetrics,
    pool: ChannelPool,
    echo_server: RunningServer,
) -> None:
    # Arrange
    # gRPC binds these arguments when it creates the channel and they cannot be changed afterwards,
    # so a client that pings on a different schedule genuinely needs a channel of its own.
    chain = canonical_chain(metrics)
    first_client = client_for(pool, echo_server, chain, connectivity=ConnectivityConfig(keepalive_time=15.0))
    second_client = client_for(pool, echo_server, chain, connectivity=ConnectivityConfig(keepalive_time=25.0))

    # Act
    first = await first_client.connect()
    second = await second_client.connect()

    # Assert
    assert first.channel is not second.channel
    assert metrics.pool_stats[-1].active_channels == 2
    assert await first.echo(b"a") == b"a"
    assert await second.echo(b"b") == b"b"


async def test__connectivity__one_client_connecting_twice__reuses_its_channel(
    metrics: RecordingMetrics,
    pool: ChannelPool,
    echo_server: RunningServer,
) -> None:
    # Arrange
    # The options are rebuilt on every connect, so this is where a list that varied between two
    # identical calls would show up: as a second channel for a client that asked for one.
    client = client_for(pool, echo_server, canonical_chain(metrics), connectivity=ConnectivityConfig())

    # Act
    first = await client.connect()
    second = await client.connect()

    # Assert
    assert first.channel is second.channel
    assert metrics.pool_stats[-1].active_channels == 1
