"""Unit tests for the ambient deadline budget: who can see it, and for how long.

The contextvar is the whole mechanism the kit adds on top of `deadline-budget`, so what is asserted
here is the part a budget library deliberately leaves out: a budget must be visible to code that was
never handed one, must disappear when its block ends, and must never leak between concurrent
requests being served by the same process.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from grpc_client_kit.deadline import (
    HAS_DEADLINE_BUDGET,
    DeadlineBudgetProtocol,
    current_budget,
    use_budget,
)
from tests.helpers import FakeBudget

from .conftest import NO_DEADLINE_EXTRA_PROBE

pytestmark = pytest.mark.unit


def test__current_budget__nothing_installed__is_none() -> None:
    """The default is "no budget", which is what makes the interceptor a pass-through by default."""
    # Act
    budget = current_budget()

    # Assert
    assert budget is None


def test__use_budget__inside_the_block__is_what_the_context_reports() -> None:
    """Code that was never handed the budget still finds it; that is the point of the contextvar."""
    # Arrange
    installed = FakeBudget()

    # Act
    with use_budget(installed) as yielded:
        seen = current_budget()

    # Assert
    assert seen is installed
    assert yielded is installed


def test__use_budget__block_left__restores_what_was_there_before() -> None:
    """A budget belongs to one request, so it must not outlive the block that installed it."""
    # Arrange
    installed = FakeBudget()

    # Act
    with use_budget(installed):
        pass

    # Assert
    assert current_budget() is None


def test__use_budget__block_left_by_exception__still_restores_the_previous_budget() -> None:
    """A failing request must not leave its deadline behind for the next one to inherit."""
    # Arrange
    installed = FakeBudget()

    # Act
    with pytest.raises(RuntimeError), use_budget(installed):
        raise RuntimeError("request failed")

    # Assert
    assert current_budget() is None


def test__use_budget__nested_blocks__restore_the_outer_budget() -> None:
    """A tighter budget for one step must give way to the request's own when that step is over."""
    # Arrange
    outer = FakeBudget(granted=5.0)
    inner = FakeBudget(granted=1.0)

    # Act
    with use_budget(outer):
        with use_budget(inner):
            during = current_budget()
        after = current_budget()

    # Assert
    assert during is inner
    assert after is outer


def test__use_budget__none_inside_a_block__detaches_the_inherited_budget() -> None:
    """Background work spawned by a request must be able to outlive that request's deadline."""
    # Arrange
    outer = FakeBudget()

    # Act
    with use_budget(outer):
        with use_budget(None):
            detached = current_budget()
        restored = current_budget()

    # Assert
    assert detached is None
    assert restored is outer


async def test__use_budget__concurrent_tasks__each_task_sees_only_its_own_budget() -> None:
    """One process serves many requests at once; their deadlines must not bleed into each other."""
    # Arrange
    first = FakeBudget(granted=1.0)
    second = FakeBudget(granted=2.0)
    started = asyncio.Event()

    async def serve(budget: FakeBudget, wait_for_the_other: bool) -> DeadlineBudgetProtocol | None:
        with use_budget(budget):
            if wait_for_the_other:
                started.set()
                await asyncio.sleep(0)
            else:
                await started.wait()
            return current_budget()

    # Act
    seen = await asyncio.gather(serve(first, True), serve(second, False))

    # Assert
    assert seen == [first, second]
    assert current_budget() is None


async def test__use_budget__task_started_under_a_budget__inherits_it() -> None:
    """A fan-out shares the deadline of the request that spawned it, rather than running unbounded."""
    # Arrange
    installed = FakeBudget()

    async def fan_out() -> DeadlineBudgetProtocol | None:
        return current_budget()

    # Act
    with use_budget(installed):
        inherited = await asyncio.create_task(fan_out())

    # Assert
    assert inherited is installed


def test__package__install_without_the_deadline_extra__works_as_it_did_before() -> None:
    """The extra adds a mechanism; without it the kit must import, build chains and run unchanged."""
    # Act & Assert
    subprocess.run(  # noqa: S603
        [sys.executable, "-c", NO_DEADLINE_EXTRA_PROBE],
        cwd=Path(__file__).parents[2],
        check=True,
        capture_output=True,
    )


@pytest.mark.skipif(not HAS_DEADLINE_BUDGET, reason="needs grpc-client-kit[deadline]")
def test__budget_protocol__real_budget_context__satisfies_it() -> None:
    """The protocol exists to keep the library optional, so it has to describe the real class."""
    # Arrange
    from deadline_budget import BudgetContext  # noqa: PLC0415

    # Act
    budget = BudgetContext.create(total_seconds=5.0)

    # Assert
    assert isinstance(budget, DeadlineBudgetProtocol)
    with use_budget(budget):
        assert current_budget() is budget
