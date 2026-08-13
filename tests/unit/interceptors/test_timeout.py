"""Unit tests for the timeout interceptor.

Every test drives the interceptor through the adapters a channel files it under, with a continuation
that behaves like grpc's: it resolves to a `Call` object and never raises. The budget is asserted on
the details that actually reached the wire.
"""

from __future__ import annotations

import pytest

from grpc_client_kit.interceptors.timeout import AsyncTimeoutInterceptor
from tests.helpers import (
    METHOD,
    RPC_KINDS,
    STREAMING_RESPONSE,
    DeadlineCallDetails,
    FakeStreamCall,
    FakeUnaryCall,
    UnsettableCallDetails,
    Wire,
    collect,
    make_call_details,
)

from .conftest import run_call, start_call

pytestmark = pytest.mark.unit


async def test__timeout_interceptor__call_without_a_deadline__gets_the_default_budget() -> None:
    """A call that carries no deadline of its own is given the configured one."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=5.0)
    wire = Wire()

    # Act
    response = await run_call(interceptor, wire, details=make_call_details("/Default"))

    # Assert
    assert response == "response"
    assert wire.timeout == 5.0


async def test__timeout_interceptor__method_with_an_override__gets_the_per_method_budget() -> None:
    """A per-method budget beats the default, which is the point of configuring one."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=10.0, per_method_timeouts={"/Method1": 1.0})
    wire = Wire()

    # Act
    await run_call(interceptor, wire, details=make_call_details("/Method1"))

    # Assert
    assert wire.timeout == 1.0


async def test__timeout_interceptor__caller_deadline_smaller_than_configured__keeps_the_callers() -> None:
    """An explicit per-call deadline may tighten the configured budget."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=100.0)
    wire = Wire()

    # Act
    await run_call(interceptor, wire, details=make_call_details(timeout=10.0))

    # Assert
    assert wire.timeout == 10.0


async def test__timeout_interceptor__caller_deadline_larger_than_configured__keeps_the_configured() -> None:
    """An explicit per-call deadline can only tighten the configured budget, never loosen it."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=5.0)
    wire = Wire()

    # Act
    await run_call(interceptor, wire, details=make_call_details(timeout=100.0))

    # Assert
    assert wire.timeout == 5.0


async def test__timeout_interceptor__default_of_zero__leaves_the_call_deadline_free() -> None:
    """0 is the 'disabled' spelling of settings models bounded by ge=0 and must not reach gRPC."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=0)
    wire = Wire()
    details = make_call_details()

    # Act
    await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details is details
    assert wire.timeout is None


async def test__timeout_interceptor__default_of_none__leaves_the_call_deadline_free() -> None:
    """A default of None means 'no deadline', it must not fall back to a built-in value."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=None)
    wire = Wire()
    details = make_call_details()

    # Act
    await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details is details


@pytest.mark.parametrize("disabled", [None, 0])
async def test__timeout_interceptor__per_method_override_disabling_it__exempts_that_method(
    disabled: float | None,
) -> None:
    """A per-method override of None/0 exempts one method from an otherwise active default."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=5.0, per_method_timeouts={"/Free": disabled})
    wire = Wire()
    details = make_call_details("/Free")

    # Act
    await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details is details


def test__timeout_interceptor__negative_default__is_refused() -> None:
    """Negative timeouts are a configuration error, unlike 0 which disables the deadline."""
    # Act & Assert
    with pytest.raises(ValueError, match="default_timeout must be non-negative"):
        AsyncTimeoutInterceptor(default_timeout=-1.0)


def test__timeout_interceptor__negative_per_method_override__is_refused() -> None:
    """The per-method budgets are validated as strictly as the default, and name the method."""
    # Act & Assert
    with pytest.raises(ValueError, match="timeout for method /Method1 must be non-negative"):
        AsyncTimeoutInterceptor(default_timeout=5.0, per_method_timeouts={"/Method1": -0.5})


def test__timeout_interceptor__built__offers_one_adapter_per_rpc_kind() -> None:
    """One object per gRPC base class, or the channel registers the layer for unary-unary only."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=5.0)

    # Act
    adapters = interceptor.adapters

    # Assert
    for abc_class in RPC_KINDS.values():
        matching = [entry for entry in adapters if isinstance(entry, abc_class)]
        assert len(matching) == 1, f"expected exactly one adapter for {abc_class.__name__}, got {matching}"

    assert [sum(isinstance(entry, abc) for abc in RPC_KINDS.values()) for entry in adapters] == [1, 1, 1, 1]


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__timeout_interceptor__every_rpc_kind__is_given_the_budget(rpc_type: str) -> None:
    """Streaming calls used to run with no deadline at all: a channel files an interceptor by class."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=5.0)
    streaming = rpc_type in STREAMING_RESPONSE
    wire = Wire(FakeStreamCall("item") if streaming else FakeUnaryCall())

    # Act
    result = await start_call(interceptor, wire, rpc_type)
    if streaming:
        assert await collect(result) == ["item"]

    # Assert
    assert wire.timeout == 5.0


async def test__timeout_interceptor__details_carrying_an_absolute_deadline__keeps_the_earlier_one() -> None:
    """Call details that carry a deadline instead of a timeout keep the earlier of the two."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=100.0)
    wire = Wire()
    near = 1.0

    # Act
    await run_call(interceptor, wire, details=DeadlineCallDetails(method=METHOD, deadline=near))

    # Assert
    assert wire.details.deadline == near


async def test__timeout_interceptor__details_refusing_a_timeout__falls_back_to_a_deadline() -> None:
    """Custom call details may not accept a timeout at all; the budget then becomes a deadline."""
    # Arrange
    interceptor = AsyncTimeoutInterceptor(default_timeout=5.0)
    wire = Wire()

    # Act
    await run_call(interceptor, wire, details=UnsettableCallDetails(METHOD))

    # Assert
    assert wire.details.deadline is not None
