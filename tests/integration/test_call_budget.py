"""One deadline for the whole call: how the retry layer divides it among the attempts.

The budget is read where it cannot be faked. `MethodControl.deadlines` is what the *server* saw of
each attempt's deadline as it crossed the wire, and `InnermostProbe` reads the same figure from
below the retry layer on the client side; a retry layer handing every attempt a fresh budget would
show up in both, and in the wall clock the calls take.
"""

from __future__ import annotations

import itertools
import time

import grpc
import grpc.aio
import pytest

from .chains import resilience_chain
from .echo_bench import ClientFactory, RunningServer

# Budget of a whole call in the deadline tests. Four attempts of their own budget would take four
# times as long, which is the regression the bound below is there to catch.
_BUDGET = 0.4
_BUDGET_TOLERANCE = 2.0

# Budget of the test that reads the deadline off the wire. Long enough that four attempts and their
# backoffs fit inside it, so every attempt reaches the server and can report what it was given.
_WIRE_BUDGET = 1.0

# How far above the truth the server's reading of a deadline may sit. `time_remaining()` is
# ``deadline - time.time()``, and the wall clock ticks every 15.6 ms on Windows, so the first
# attempt — issued with the whole budget — can be reported as a shade more than the whole budget.
# Four attempts each given a fresh budget would still show four times _WIRE_BUDGET, so the bound
# below keeps all of its meaning.
_WIRE_TOLERANCE = 0.05

# Backoff for the tests that watch the budget shrink. Well clear of the 15.6 ms tick of the Windows
# monotonic clock, so two consecutive readings of the remaining budget cannot collapse into one.
_SHRINKING_BACKOFF = 0.05


async def test__retry__attempts_on_the_wire__each_carries_a_slice_of_one_deadline(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # The budget is read where it cannot be faked: on the server, which reports how much of the
    # deadline was left when each attempt arrived.
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(timeout=_WIRE_BUDGET, max_attempts=4, initial_backoff=_SHRINKING_BACKOFF)
    stub = await make_client(chain.interceptors).connect()

    # Act
    started = time.perf_counter()
    with pytest.raises(grpc.aio.AioRpcError):
        await stub.echo(b"hello")
    elapsed = time.perf_counter() - started

    # Assert
    deadlines = control.deadlines
    assert control.calls == 4
    assert len(deadlines) == 4
    assert [left for left in deadlines if left is None or left > _WIRE_BUDGET + _WIRE_TOLERANCE] == []
    # Four attempts of their own budget would leave the same time on every one of them, and the call
    # would last 4 * _WIRE_BUDGET. One shared deadline shows up as both assertions below.
    assert all(later < earlier for earlier, later in itertools.pairwise(deadlines))
    assert elapsed < _WIRE_BUDGET


async def test__retry__unresponsive_server__the_whole_call_fits_in_one_budget(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.hang = True
    chain = resilience_chain(
        timeout=_BUDGET,
        max_attempts=4,
        # DEADLINE_EXCEEDED is not retryable by default. Opting in is what puts the budget under
        # test: without it the first deadline ends the call and nothing divides anything.
        retryable_codes={grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED},
    )
    stub = await make_client(chain.interceptors).connect()

    # Act
    started = time.perf_counter()
    with pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await stub.echo(b"hello")
    elapsed = time.perf_counter() - started

    # Assert
    assert exc_info.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    # Four attempts issued with a fresh budget each would take 4 * _BUDGET. The retry layer hands
    # out slices of a single deadline instead, so the hung first attempt spends the lot and there is
    # nothing left to retry with.
    assert elapsed < _BUDGET * _BUDGET_TOLERANCE
    assert control.calls == 1
    assert chain.attempts == 1


async def test__retry__successive_attempts__each_starts_with_less_budget_than_the_last(
    echo_server: RunningServer,
    make_client: ClientFactory,
) -> None:
    # Arrange
    control = echo_server.service.unary_unary
    control.abort_code = grpc.StatusCode.UNAVAILABLE
    chain = resilience_chain(timeout=5.0, max_attempts=4, initial_backoff=_SHRINKING_BACKOFF)
    stub = await make_client(chain.interceptors).connect()

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await stub.echo(b"hello")

    # Assert
    budgets = chain.budgets
    assert control.calls == 4
    assert len(budgets) == 4
    assert budgets[0] == pytest.approx(5.0)
    assert all(later < earlier for earlier, later in itertools.pairwise(budgets))
