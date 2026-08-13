"""Concurrency invariants under storm conditions, pinned as regression tests.

The audit's storm probes found these invariants HOLD — and hold for a delicate reason: every
critical section of the pool and the breaker is await-free, hence atomic on the event loop. That
is exactly the kind of property an innocent refactor (one ``await`` added under a lock) breaks
without failing any ordinary test, so the storms run in CI.
"""

from __future__ import annotations

import asyncio
import random

import grpc

from grpc_client_kit import ChannelPool

from .calls import outcome
from .chains import canonical_chain
from .echo_bench import ClientFactory, RunningServer
from .recording import RecordingMetrics
from .waiting import until

_STORM_CALLS = 120
_STORM_CANCELS = 40


async def test__storm__concurrent_calls_with_random_cancellations__leave_no_state_behind(
    echo_server: RunningServer,
    make_client: ClientFactory,
    metrics: RecordingMetrics,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics, fail_threshold=10_000))
    stub = await client.connect()
    rng = random.Random(20260814)  # noqa: S311 - reproducibility, not cryptography

    async def one_call(index: int) -> None:
        task = asyncio.ensure_future(stub.echo(b"payload"))
        if index < _STORM_CANCELS:
            # Cancel at a random point of the call's life, including before it even started.
            await asyncio.sleep(rng.uniform(0, 0.005))
            task.cancel()
        try:
            await task
        except (asyncio.CancelledError, grpc.aio.AioRpcError):
            pass

    # Act
    await asyncio.gather(*(one_call(index) for index in range(_STORM_CALLS)))

    # Assert
    # The in-flight gauge must balance for every call that was created, cancelled ones included.
    await until(lambda: metrics.total_inflight() == 0, message=f"in-flight stuck at {metrics.total_inflight()}")
    # The breaker took no damage from cancellations: they are nobody's failure.
    states = await client.circuit_breaker_states()
    for snapshot in states.values():
        for status in snapshot.values():
            assert status["state"] == "closed"
    # And the pool is still fully serviceable.
    assert await outcome(stub) == "ok"


async def test__storm__close_all_racing_get_channel__leaks_no_channel(
    echo_server: RunningServer,
) -> None:
    # Arrange
    pool = ChannelPool()
    target = echo_server.target

    # Act: hammer the pool with concurrent acquisitions while draining it, repeatedly.
    for _ in range(10):
        results = await asyncio.gather(
            *(pool.get_channel(target, insecure=True) for _ in range(8)),
            pool.close_all(),
            *(pool.get_channel(target, insecure=True) for _ in range(8)),
            return_exceptions=True,
        )
        # Some acquisitions were refused because the drain was in progress: that is the contract.
        refused = [r for r in results if isinstance(r, RuntimeError)]
        for error in refused:
            assert "ChannelPool is closing" in str(error)

    await pool.close_all()

    # Assert: no entry survived the final drain, and the pool still works afterwards.
    assert pool._entries == {}
    channel = await pool.get_channel(target, insecure=True)
    assert channel is not None
    await pool.close_all()
