"""A sub-second check_interval must actually probe sub-second.

The scheduling loop used to sleep a fixed second per round, which silently stretched any shorter
interval to ~1s — a user asking for 0.2s failover latency got 1s and no warning. Deliberately run
WITHOUT the ``fast_health_loop`` fixture: the point is that the floor follows the interval down on
the production constant, not on a patched one.
"""

from __future__ import annotations

import asyncio

from .health_bench import HealthServer, MakeChecker, start_checker


async def test__health_checker__sub_second_interval__probes_sub_second(
    health_server: HealthServer,
    make_checker: MakeChecker,
) -> None:
    # Arrange
    checker = make_checker(check_interval=0.2)

    # Act
    await start_checker(checker, [health_server.target])
    first_pass = health_server.servicer.probe_count
    await asyncio.sleep(1.2)

    # Assert: with the old 1s loop floor at most one more probe fits into the window; the interval
    # being honoured means several do. Threshold is deliberately loose for slow CI runners.
    assert health_server.servicer.probe_count - first_pass >= 3
