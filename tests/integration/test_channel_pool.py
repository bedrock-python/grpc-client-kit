"""Channel reuse on a live pool: who shares a channel, who gets their own, and what a drain leaves.

gRPC binds interceptors to a channel at creation time and a channel cannot be re-bound, so the
identity of a pooled channel is also the identity of the interceptor chain running on it — which is
what makes these tests about isolation rather than about bookkeeping.
"""

from __future__ import annotations

import grpc.aio
import pytest

from grpc_client_kit import ChannelPool

from .chains import canonical_chain
from .echo_bench import ClientFactory, RunningServer
from .recording import RecordingMetrics


async def test__channel_pool__two_connects_of_one_client__reuse_one_real_channel(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))

    # Act
    first = await client.connect()
    second = await client.connect()

    # Assert
    assert first.channel is second.channel
    assert await first.echo(b"a") == b"a"
    assert await second.echo(b"b") == b"b"
    assert metrics.pool_stats[-1].active_channels == 1


async def test__channel_pool__clients_with_distinct_chains__get_distinct_channels(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # Two chains of two separate interceptor instances. Sharing a channel would mean sharing their
    # circuit breakers, and gRPC binds interceptors at creation time: a channel cannot be re-bound.
    first_client = make_client(canonical_chain(metrics))
    second_client = make_client(canonical_chain(metrics))

    # Act
    first = await first_client.connect()
    second = await second_client.connect()

    # Assert
    assert first.channel is not second.channel
    assert await first.echo(b"a") == b"a"
    assert await second.echo(b"b") == b"b"
    assert metrics.pool_stats[-1].active_channels == 2


async def test__channel_pool__clients_sharing_one_chain__share_one_channel(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    chain = canonical_chain(metrics)
    first_client = make_client(chain)
    second_client = make_client(chain)

    # Act
    first = await first_client.connect()
    second = await second_client.connect()

    # Assert
    assert first.channel is second.channel
    assert await first.echo(b"a") == b"a"
    assert await second.echo(b"b") == b"b"
    assert metrics.pool_stats[-1].active_channels == 1


async def test__channel_pool__close_all__closes_live_channels_and_leaves_the_pool_usable(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    pool: ChannelPool,
    make_client: ClientFactory,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()
    assert await stub.echo(b"ping") == b"ping"
    closed_channel = stub.channel

    # Act
    await pool.close_all()

    # Assert
    with pytest.raises(grpc.aio.UsageError):
        await stub.echo(b"after-close")

    # Draining the pool must not retire it: the next client gets a fresh, working channel.
    fresh = await client.connect()
    assert fresh.channel is not closed_channel
    assert await fresh.echo(b"pong") == b"pong"
    assert echo_server.service.unary_unary.calls == 2
