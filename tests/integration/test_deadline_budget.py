"""The caller's deadline, as the server sees it: what a request budget does to a real call.

The measurement is taken where it cannot be faked. `MethodControl.deadlines` is what the *server*
read off each call as it arrived, so a budget that never left the client, or one applied after the
retry layer had already divided the untrimmed deadline, shows up as the wrong number on the wire
rather than as a passing assertion about an interceptor's internals.
"""

from __future__ import annotations

import asyncio
import time

import grpc
import grpc.aio
import pytest

from grpc_client_kit import DeadlineBudgetConfig, use_budget
from grpc_client_kit.deadline import HAS_DEADLINE_BUDGET

from .budgets import request_budget
from .chains import resilience_chain
from .echo_bench import ECHO, ClientFactory, RunningServer

pytestmark = pytest.mark.skipif(not HAS_DEADLINE_BUDGET, reason="needs grpc-client-kit[deadline]")

# The deadline the chain is configured with: far larger than any budget below, so a call carrying it
# is unmistakably a call the budget never touched.
_CONFIGURED_TIMEOUT = 30.0

# The budget of the request under test, and the floor a call issued under it must still clear. The
# gap between them is what a localhost round trip plus the client-side layers may spend.
_BUDGET = 0.5
_BUDGET_FLOOR = 0.2

# How far above the truth the server's own reading of a deadline may sit. `time_remaining()` is the
# distance to the deadline measured against the wall clock, whose tick is 15.6 ms on Windows, so a
# call issued with exactly the budget can be reported as a shade more than it. Small enough that it
# cannot be confused with the 30 s the untrimmed deadline would have shown.
_CLOCK_TICK = 0.05

# A budget already spent by the time the call is made.
_SPENT_BUDGET = 0.05


async def test__deadline_budget__call_under_a_budget__reaches_the_server_with_the_trimmed_deadline(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    chain = resilience_chain(timeout=_CONFIGURED_TIMEOUT, deadline_budget=DeadlineBudgetConfig())
    stub = await make_client(chain.interceptors).connect()

    # Act
    # The first call runs without a budget, which is what the second one is compared against: same
    # client, same chain, same configured deadline, and the only difference is the budget.
    assert await stub.echo(b"unbudgeted") == b"unbudgeted"
    with use_budget(request_budget(_BUDGET)):
        assert await stub.echo(b"budgeted") == b"budgeted"

    # Assert
    unbudgeted, budgeted = control.deadlines
    assert unbudgeted is not None and budgeted is not None
    # Without a budget the server sees the configured deadline; with one it sees what the request
    # had left, which is the whole chain closing.
    assert unbudgeted > _BUDGET * 2
    assert _BUDGET_FLOOR < budgeted <= _BUDGET + _CLOCK_TICK


async def test__deadline_budget__budget_wider_than_the_configured_deadline__keeps_the_deadline(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    chain = resilience_chain(timeout=_BUDGET, deadline_budget=DeadlineBudgetConfig())
    stub = await make_client(chain.interceptors).connect()

    # Act
    with use_budget(request_budget(_CONFIGURED_TIMEOUT)):
        assert await stub.echo(b"hello") == b"hello"

    # Assert
    # A budget may only tighten a deadline. A request with 30s left does not entitle this call to
    # more than the 0.5s its own configuration allows it.
    left = control.deadlines[0]
    assert left is not None
    assert left <= _BUDGET + _CLOCK_TICK


async def test__deadline_budget__spent_budget__refuses_the_call_without_reaching_the_server(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    chain = resilience_chain(timeout=_CONFIGURED_TIMEOUT, deadline_budget=DeadlineBudgetConfig())
    stub = await make_client(chain.interceptors).connect()
    budget = request_budget(_SPENT_BUDGET)
    await asyncio.sleep(_SPENT_BUDGET * 2)

    # Act
    with use_budget(budget), pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.echo(b"too late")

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    # Neither the server nor the innermost client layer saw anything: the call was never created.
    assert control.calls == 0
    assert chain.attempts == 0


async def test__deadline_budget__retried_call__divides_the_trimmed_budget_between_attempts(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(
        timeout=_CONFIGURED_TIMEOUT,
        max_attempts=3,
        deadline_budget=DeadlineBudgetConfig(),
    )
    stub = await make_client(chain.interceptors).connect()

    # Act
    started = time.perf_counter()
    with use_budget(request_budget(_BUDGET)), pytest.raises(grpc.aio.AioRpcError):
        await stub.echo(b"hello")
    elapsed = time.perf_counter() - started

    # Assert
    # The budget layer runs above the retry layer, so every attempt is cut from the trimmed
    # deadline. Were it below, each attempt would have been handed the configured 30s instead.
    assert control.calls == 3
    assert [left for left in control.deadlines if left is None or left > _BUDGET + _CLOCK_TICK] == []
    # Read below the retry layer on the client side, where no clock but ours is involved: every
    # attempt was issued out of the one trimmed budget.
    assert [budget for budget in chain.budgets if budget > _BUDGET] == []
    assert elapsed < _BUDGET


async def test__deadline_budget__per_call_cap__is_keyed_by_the_full_method_name(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    chain = resilience_chain(timeout=_CONFIGURED_TIMEOUT, deadline_budget=DeadlineBudgetConfig())
    stub = await make_client(chain.interceptors).connect()
    capped = _BUDGET / 2

    # Act
    with use_budget(request_budget(_CONFIGURED_TIMEOUT, call_caps={ECHO: capped})):
        assert await stub.echo(b"hello") == b"hello"

    # Assert
    # The cap is stored under the RPC's own path, so it only applies because the kit asks for a
    # timeout under that same name.
    left = control.deadlines[0]
    assert left is not None
    assert left <= capped + _CLOCK_TICK


async def test__deadline_budget__no_budget_installed__leaves_the_configured_deadline_alone(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    chain = resilience_chain(timeout=_CONFIGURED_TIMEOUT, deadline_budget=DeadlineBudgetConfig())
    stub = await make_client(chain.interceptors).connect()

    # Act
    assert await stub.echo(b"hello") == b"hello"

    # Assert
    # The kit ships a mechanism, not a policy: a caller who installs no budget gets what they had.
    left = control.deadlines[0]
    assert left is not None
    assert left > _BUDGET * 2
