"""Passive outlier detection against live servers: one failed call closes the failover window.

An active checker learns about a dead backend one probe interval late; during that window a
balancer keeps routing a full share of traffic into an address nothing answers at. The audit
measured that window at 50% loss for up to ~31s on default settings. Passive marking closes it:
the first call that gets UNAVAILABLE takes the target out of the rotation immediately.
"""

from __future__ import annotations

import grpc
import grpc.aio

from grpc_client_kit import ChannelPool, GrpcClient, GrpcClientConfig, create_balancer
from grpc_client_kit.interceptors.outlier import AsyncPassiveOutlierInterceptor

from .chains import canonical_chain
from .echo_bench import EchoStub, RunningServer


async def test__balanced_client__backend_dies__loses_at_most_one_call_to_it(
    echo_server: RunningServer,
    spare_echo_server: RunningServer,
) -> None:
    # Arrange: a factory-shaped client over two live backends, no active health checker at all.

    pool = ChannelPool()
    balancer = create_balancer(targets=[echo_server.target, spare_echo_server.target])

    def build_chain(target: str) -> list[grpc.aio.ClientInterceptor]:
        chain = canonical_chain(max_attempts=1, fail_threshold=10_000, timeout=1.0)
        outlier = AsyncPassiveOutlierInterceptor(balancer, target, quarantine=30.0)
        return [*chain, *outlier.adapters]

    client: GrpcClient[EchoStub] = GrpcClient(
        stub_class=EchoStub,
        config=GrpcClientConfig(insecure=True),
        pool=pool,
        balancer=balancer,
        interceptor_factory=build_chain,
    )

    # Warm both targets so each has a channel.
    for _ in range(4):
        stub = await client.connect()
        await stub.echo(b"warm")

    # Act: kill one backend, then run a burst of connect-per-call traffic.
    await spare_echo_server.stop()
    failures = 0
    for _ in range(20):
        stub = await client.connect()
        try:
            await stub.echo(b"payload")
        except grpc.aio.AioRpcError:
            failures += 1

    # Assert: the dead backend cost at most a couple of calls — the ones that discovered it —
    # not its full 50% share of the burst. Without passive marking this is ~10 of 20.
    assert failures <= 2, f"passive quarantine did not close the failover window: {failures} failures of 20"

    await pool.close_all()
