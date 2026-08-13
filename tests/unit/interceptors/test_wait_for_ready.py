"""Unit tests for the wait-for-ready interceptor.

The flag is asserted on the details that actually reached the wire, because that is the only place
it means anything: `wait_for_ready` is read by gRPC when the call is created, from the call details
the innermost layer hands it.
"""

from __future__ import annotations

import logging

import pytest

from grpc_client_kit.interceptors.wait_for_ready import AsyncWaitForReadyInterceptor
from tests.helpers import (
    METHOD,
    OTHER_METHOD,
    RPC_KINDS,
    STREAMING_RESPONSE,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    collect,
    make_call_details,
)

from .conftest import run_call, start_call

pytestmark = pytest.mark.unit

# A deadline, so that the calls under test are bounded and waiting is allowed at all.
BOUNDED = 5.0

# What the warning about an unbounded call says, as a caller reading the log would see it.
NO_DEADLINE = "carries no deadline"


async def test__wait_for_ready__bounded_call__is_issued_willing_to_wait() -> None:
    """The point of the layer: a call on a cold channel waits for it instead of failing at once."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor()
    wire = Wire()

    # Act
    response = await run_call(interceptor, wire, details=make_call_details(timeout=BOUNDED))

    # Assert
    assert response == "response"
    assert wire.details.wait_for_ready is True


async def test__wait_for_ready__call_without_a_deadline__is_left_fail_fast(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Waiting with nothing to end the wait is a hang, so an unbounded call keeps failing fast."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor()
    wire = Wire()
    details = make_call_details()

    # Act
    with caplog.at_level(logging.WARNING):
        await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details is details
    assert NO_DEADLINE in caplog.text


async def test__wait_for_ready__repeated_unbounded_calls__are_reported_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The condition is a property of the configuration, so one warning says all there is to say."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor()
    wire = Wire()

    # Act
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            await run_call(interceptor, wire, details=make_call_details())

    # Assert
    assert len([record for record in caplog.records if NO_DEADLINE in record.getMessage()]) == 1
    assert wire.attempts == 3


async def test__wait_for_ready__deadline_not_required__waits_on_an_unbounded_call_too() -> None:
    """A caller who knows what bounds their call may opt out of the interlock."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor(require_deadline=False)
    wire = Wire()

    # Act
    await run_call(interceptor, wire, details=make_call_details())

    # Assert
    assert wire.details.wait_for_ready is True


async def test__wait_for_ready__caller_already_decided__keeps_the_callers_choice() -> None:
    """An explicit decision at the call site is the only way to opt one call out of the policy."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor()
    wire = Wire()
    details = make_call_details(timeout=BOUNDED)._replace(wait_for_ready=False)

    # Act
    await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details.wait_for_ready is False


async def test__wait_for_ready__default_of_none__leaves_the_call_untouched() -> None:
    """None switches the layer off without taking it out of the chain, and out of the pool key."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor(default=None)
    wire = Wire()
    details = make_call_details(timeout=BOUNDED)

    # Act
    await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details is details


async def test__wait_for_ready__method_with_an_override__uses_it_instead_of_the_default() -> None:
    """A method that must fail fast — a health probe, a fast path — can say so on its own."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor(default=True, per_method={METHOD: False})
    wire = Wire()

    # Act
    await run_call(interceptor, wire, details=make_call_details(METHOD, timeout=BOUNDED))
    await run_call(interceptor, wire, details=make_call_details(OTHER_METHOD, timeout=BOUNDED))

    # Assert
    assert wire.wait_flags == [False, True]


async def test__wait_for_ready__method_override_of_none__exempts_that_method() -> None:
    """An explicit None override must exempt the method, not fall back to the default."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor(default=True, per_method={METHOD: None})
    wire = Wire()
    details = make_call_details(METHOD, timeout=BOUNDED)

    # Act
    await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details is details


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__wait_for_ready__every_rpc_kind__is_issued_willing_to_wait(rpc_type: str) -> None:
    """A channel files an interceptor by class, so all four kinds need their own adapter."""
    # Arrange
    interceptor = AsyncWaitForReadyInterceptor()
    streaming = rpc_type in STREAMING_RESPONSE
    wire = Wire(FakeStreamCall("item") if streaming else FakeUnaryCall())

    # Act
    result = await start_call(interceptor, wire, rpc_type, details=make_call_details(timeout=BOUNDED))
    if streaming:
        assert await collect(result) == ["item"]

    # Assert
    assert wire.details.wait_for_ready is True
