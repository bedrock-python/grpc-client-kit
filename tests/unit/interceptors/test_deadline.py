"""Unit tests for the deadline budget interceptor.

The budget is read off the ambient context, so every test here installs one with `use_budget` and
then asserts on the deadline that reached the wire — the same place the timeout tests read, because
that is the only value the server will ever see.
"""

from __future__ import annotations

import grpc
import grpc.aio
import pytest

from grpc_client_kit.deadline import use_budget
from grpc_client_kit.interceptors.deadline import (
    AsyncDeadlineBudgetInterceptor,
    DeadlineBudgetExhaustedError,
)
from tests.helpers import (
    METHOD,
    RPC_KINDS,
    STREAMING_RESPONSE,
    FakeBudget,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    collect,
    make_call_details,
)

from .conftest import run_call, start_call

pytestmark = pytest.mark.unit


async def test__deadline_interceptor__no_budget_installed__leaves_the_call_untouched() -> None:
    """A service that installs no budget must keep the behaviour it had before the kit grew one."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    wire = Wire()
    details = make_call_details(timeout=7.0)

    # Act
    response = await run_call(interceptor, wire, details=details)

    # Assert
    assert response == "response"
    assert wire.details is details


async def test__deadline_interceptor__budget_installed__issues_the_call_with_what_is_left() -> None:
    """The point of the layer: a call inherits the remainder of the request it belongs to."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget(granted=1.5)
    wire = Wire()

    # Act
    with use_budget(budget):
        await run_call(interceptor, wire, details=make_call_details())

    # Assert
    assert wire.timeout == 1.5


async def test__deadline_interceptor__budget_larger_than_the_call_deadline__keeps_the_deadline() -> None:
    """A budget may only tighten a deadline; a call configured for 2s must not be granted 10."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget(granted=10.0)
    wire = Wire()

    # Act
    with use_budget(budget):
        await run_call(interceptor, wire, details=make_call_details(timeout=2.0))

    # Assert
    assert wire.timeout == 2.0


async def test__deadline_interceptor__budget_smaller_than_the_call_deadline__trims_it() -> None:
    """The static per-method budget is a ceiling; what the request has left is the real limit."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget(granted=0.5)
    wire = Wire()

    # Act
    with use_budget(budget):
        await run_call(interceptor, wire, details=make_call_details(timeout=10.0))

    # Assert
    assert wire.timeout == 0.5


async def test__deadline_interceptor__exhausted_budget__refuses_the_call_without_issuing_it() -> None:
    """Dialing out with nothing left can only fail a round trip later, so it is refused here."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget(left=0.0)
    wire = Wire()

    # Act
    with use_budget(budget), pytest.raises(DeadlineBudgetExhaustedError) as exc_info:
        await run_call(interceptor, wire, details=make_call_details())

    # Assert
    assert wire.attempts == 0
    assert exc_info.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    assert METHOD in exc_info.value.details()


async def test__deadline_interceptor__exhausted_budget__reports_as_a_grpc_failure() -> None:
    """Callers handle `AioRpcError`; a refusal that is not one would escape every existing handler."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget(left=-1.0)
    wire = Wire()

    # Act
    with use_budget(budget), pytest.raises(grpc.aio.AioRpcError) as exc_info:
        await run_call(interceptor, wire, details=make_call_details())

    # Assert
    # The budget library's own error is kept underneath, for anyone mapping it to a status.
    assert exc_info.value.__cause__ is not None
    assert type(exc_info.value.__cause__).__name__ == "DeadlineExceededError"


async def test__deadline_interceptor__budget_with_per_call_caps__asks_under_the_full_method_name() -> None:
    """Per-call caps are keyed by name, so the kit has to ask under the one callers configure."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget()
    wire = Wire()

    # Act
    with use_budget(budget):
        await run_call(interceptor, wire, details=make_call_details("/pkg.Service/Capped"))

    # Assert
    assert budget.call_names == ["/pkg.Service/Capped"]


async def test__deadline_interceptor__reserve_configured__hands_it_to_the_budget() -> None:
    """The reserve is the budget's own knob; the kit forwards it instead of reimplementing it."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor(reserve_for_next=0.25)
    budget = FakeBudget()
    wire = Wire()

    # Act
    with use_budget(budget):
        await run_call(interceptor, wire, details=make_call_details())

    # Assert
    assert budget.asked_for == [(METHOD, 0.25)]


def test__deadline_interceptor__negative_reserve__is_refused() -> None:
    """A negative reserve would hand out more time than the budget has, so it is a config error."""
    # Act & Assert
    with pytest.raises(ValueError, match="reserve_for_next must be non-negative"):
        AsyncDeadlineBudgetInterceptor(reserve_for_next=-0.1)


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__deadline_interceptor__every_rpc_kind__is_trimmed_to_the_budget(rpc_type: str) -> None:
    """A channel files an interceptor by class, so all four kinds need their own adapter."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget(granted=0.75)
    streaming = rpc_type in STREAMING_RESPONSE
    wire = Wire(FakeStreamCall("item") if streaming else FakeUnaryCall())

    # Act
    with use_budget(budget):
        result = await start_call(interceptor, wire, rpc_type)
        if streaming:
            assert await collect(result) == ["item"]

    # Assert
    assert wire.timeout == 0.75


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__deadline_interceptor__exhausted_budget__refuses_every_rpc_kind(rpc_type: str) -> None:
    """A stream started without budget would run unbounded, so the refusal cannot be unary-only."""
    # Arrange
    interceptor = AsyncDeadlineBudgetInterceptor()
    budget = FakeBudget(left=0.0)
    wire = Wire(FakeStreamCall("item"))

    # Act
    with use_budget(budget), pytest.raises(DeadlineBudgetExhaustedError):
        await start_call(interceptor, wire, rpc_type)

    # Assert
    assert wire.attempts == 0
