"""Unit tests for the retry interceptor.

Every test drives the interceptor the way a channel does: through the adapters in
`AsyncClientInterceptor.adapters`, with a continuation that behaves like grpc's — it resolves to a
`Call` object without raising, and the status only surfaces when that Call is awaited or iterated.
Feeding the interceptor a continuation that raises by itself is what let the old suite pass while
retries did nothing on a live connection.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock

import grpc.aio
import pytest

from grpc_client_kit.interceptors.circuit_breaker import CircuitBreakerOpenError
from grpc_client_kit.interceptors.retry import DEFAULT_RETRYABLE_CODES, AsyncRetryInterceptor
from grpc_client_kit.interceptors.timeout import AsyncTimeoutInterceptor
from tests.helpers import (
    METHOD,
    RPC_KINDS,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    collect,
    make_call_details,
    make_rpc_error,
    refusing_wire,
    requests,
)

from .conftest import depth_recording_stream, nested_wire, run_call, start_call

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# Dispatch: one logical interceptor has to reach all four of a channel's lists.
# --------------------------------------------------------------------------------------------


def test__retry_interceptor__built__offers_one_adapter_per_rpc_kind() -> None:
    """A channel files interceptors by class, so each kind needs an object of exactly its own ABC."""
    # Arrange
    interceptor = AsyncRetryInterceptor()

    # Act
    adapters = interceptor.adapters

    # Assert
    for abc_class in RPC_KINDS.values():
        matching = [entry for entry in adapters if isinstance(entry, abc_class)]
        assert len(matching) == 1, f"expected exactly one adapter for {abc_class.__name__}, got {matching}"

    # An adapter that inherited more than one ABC would be filed under the first match and lost for
    # the rest, which is the bug the four adapters exist to avoid.
    assert [sum(isinstance(entry, abc) for abc in RPC_KINDS.values()) for entry in adapters] == [1, 1, 1, 1]


def test__retry_interceptor__adapters_read_twice__hands_out_the_same_objects() -> None:
    """A chain's identity is the identity of its entries: the pool keys its channels on it."""
    # Arrange
    interceptor = AsyncRetryInterceptor()

    # Act & Assert
    assert interceptor.adapters is interceptor.adapters


def test__retry_interceptor__on_its_own__is_not_a_grpc_interceptor() -> None:
    """Handing the logical object to a channel must fail loudly, not register it for unary only."""
    # Act & Assert
    assert not isinstance(AsyncRetryInterceptor(), grpc.aio.ClientInterceptor)


async def test__retry__stream_unary_call__hands_back_the_call_not_the_bare_response() -> None:
    """stream-unary passes an interceptor's return value straight to the caller, which awaits it."""
    # Arrange
    interceptor = AsyncRetryInterceptor()
    wire = Wire(FakeUnaryCall("collected"))

    # Act
    result = await start_call(interceptor, wire, "stream_unary", request=requests("a"))

    # Assert
    assert hasattr(result, "__await__"), "a bare response here breaks the caller and the call finalizer"
    assert await result == "collected"


# --------------------------------------------------------------------------------------------
# Reading the outcome: the continuation resolves to a Call and never raises.
# --------------------------------------------------------------------------------------------


async def test__retry__failure_surfacing_only_on_awaiting_the_call__still_retries() -> None:
    """The whole point: a continuation that resolves happily still hides a failed RPC."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0)
    wire = Wire(
        FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)),
        FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)),
        FakeUnaryCall("recovered"),
    )

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "recovered"
    assert wire.attempts == 3


async def test__retry__max_attempts_reached__raises_the_last_error() -> None:
    """Retries stop at max_attempts, and the caller sees the last error."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=2, initial_backoff=0.0)
    wire = Wire(lambda: FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire)

    # Assert
    assert wire.attempts == 2


async def test__retry__call_that_works__is_issued_exactly_once() -> None:
    """A call that works is issued exactly once."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0)
    wire = Wire(FakeUnaryCall("first-try"))

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "first-try"
    assert wire.attempts == 1


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ],
)
async def test__retry__ambiguous_status_code__is_not_retried_by_default(code: grpc.StatusCode) -> None:
    """Codes that may mean 'the server already applied the write' must not be retried by default."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0)
    wire = Wire(lambda: FakeUnaryCall(error=make_rpc_error(code)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire)

    # Assert
    assert code not in DEFAULT_RETRYABLE_CODES
    assert wire.attempts == 1


async def test__retry__ambiguous_code_opted_into__is_retried() -> None:
    """A caller who knows the method is safe can widen the code set."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=2, initial_backoff=0.0, retryable_codes={grpc.StatusCode.INTERNAL})
    wire = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.INTERNAL)), FakeUnaryCall("ok"))

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "ok"
    assert wire.attempts == 2


async def test__retry__empty_retryable_code_set__never_retries() -> None:
    """An empty set is a deliberate 'never retry', not an absent value."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0, retryable_codes=set())
    wire = Wire(lambda: FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire)

    # Assert
    assert wire.attempts == 1


async def test__retry__circuit_breaker_open__is_not_retried() -> None:
    """A call the breaker refused never reached the network; repeating it only burns the budget."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0)
    wire = Wire(lambda: FakeUnaryCall(error=CircuitBreakerOpenError(METHOD)))

    # Act
    with pytest.raises(CircuitBreakerOpenError):
        await run_call(interceptor, wire)

    # Assert
    assert wire.attempts == 1


async def test__retry__continuation_refusing_the_call__lets_the_error_reach_the_caller() -> None:
    """An interceptor further in may raise instead of resolving; that error must reach the caller."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0)

    # Act & Assert
    with pytest.raises(CircuitBreakerOpenError):
        await start_call(interceptor, refusing_wire(CircuitBreakerOpenError(METHOD)))


async def test__retry__idempotent_whitelist__narrows_unary_retries_to_whitelisted_methods() -> None:
    """A whitelist narrows unary retries too, not only streaming ones."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0, idempotent_methods={"/pkg.Service/Get"})
    unsafe = Wire(lambda: FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))
    safe = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)), FakeUnaryCall("ok"))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, unsafe, details=make_call_details("/pkg.Service/Pay"))
    response = await run_call(interceptor, safe, details=make_call_details("/pkg.Service/Get"))

    # Assert
    assert unsafe.attempts == 1
    assert response == "ok"
    assert safe.attempts == 2


async def test__retry__streaming_request__is_not_retried() -> None:
    """A request iterator is consumed by the first attempt and cannot be replayed."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0)
    wire = Wire(lambda: FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire, "stream_unary", request=requests("req"))

    # Assert
    assert wire.attempts == 1


async def test__retry__stream_stream_call__is_not_retried_even_when_whitelisted() -> None:
    """Same for a bidirectional call, even with streaming retries switched on for the method."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        max_attempts=3, initial_backoff=0.0, retry_streaming=True, idempotent_methods={METHOD}
    )
    wire = Wire(lambda: FakeStreamCall(b"a", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    stream = await start_call(interceptor, wire, "stream_stream", request=requests("a"))
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    assert wire.attempts == 1


# --------------------------------------------------------------------------------------------
# Streaming responses: the failure surfaces while the stream is being consumed.
# --------------------------------------------------------------------------------------------


async def test__retry__stream_failing_midway__is_restarted_from_the_beginning() -> None:
    """A restarted stream replays what the consumer has already seen; that is the documented cost."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        max_attempts=2, initial_backoff=0.0, retry_streaming=True, idempotent_methods={METHOD}
    )
    wire = Wire(
        FakeStreamCall("item1", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)),
        FakeStreamCall("item1", "item2"),
    )

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    delivered = await collect(stream)

    # Assert
    assert delivered == ["item1", "item1", "item2"]
    assert wire.attempts == 2


async def test__retry__stream_failing_every_attempt__raises_once_the_attempts_are_spent() -> None:
    """The last failure of a stream reaches the consumer once the attempts are spent."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        max_attempts=2, initial_backoff=0.0, retry_streaming=True, idempotent_methods={METHOD}
    )
    wire = Wire(lambda: FakeStreamCall("item1", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    assert wire.attempts == 2


async def test__retry__streaming_method_not_whitelisted__is_not_restarted() -> None:
    """Replaying delivered items is only safe where the caller said so, method by method."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        max_attempts=2, initial_backoff=0.0, retry_streaming=True, idempotent_methods={"/pkg.Service/Safe"}
    )
    wire = Wire(lambda: FakeStreamCall("item1", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream", details=make_call_details("/pkg.Service/Unsafe"))
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    assert wire.attempts == 1


async def test__retry__streaming_retries_disabled__hands_the_stream_through_unwrapped() -> None:
    """With retry_streaming off the stream is handed through with no wrapper at all."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=2, retry_streaming=False)
    wire = Wire(lambda: FakeStreamCall("item1", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    assert wire.attempts == 1


async def test__retry__streaming_response__creates_the_rpc_before_returning_its_iterator() -> None:
    """grpc binds the Call an interceptor made to the iterator it returns; a lazy one binds None."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        max_attempts=2, initial_backoff=0.0, retry_streaming=True, idempotent_methods={METHOD}
    )
    wire = Wire(FakeStreamCall("item1"))

    # Act
    await start_call(interceptor, wire, "unary_stream")

    # Assert
    assert wire.attempts == 1, "the RPC must exist by the time the iterator reaches grpc"


async def test__retry__stream_restart_the_budget_cannot_cover__gives_up_early() -> None:
    """A stream restart obeys the same budget rule as a unary retry: no backoff it cannot outlive."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        max_attempts=10,
        initial_backoff=0.1,
        backoff_multiplier=1.0,
        jitter=0.0,
        retry_streaming=True,
        idempotent_methods={METHOD},
    )
    wire = Wire(lambda: FakeStreamCall("item", error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    started = time.monotonic()
    stream = await start_call(interceptor, wire, "unary_stream", details=make_call_details(timeout=0.3))
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)
    elapsed = time.monotonic() - started

    # Assert
    assert wire.attempts == 3
    assert elapsed < 0.6


async def test__retry__repeated_stream_restarts__do_not_nest_generators() -> None:
    """Stream restarts iterate: nesting one wrapper per attempt would deepen the stack every retry."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        max_attempts=5,
        initial_backoff=0.05,
        backoff_multiplier=1.0,
        jitter=0.0,
        retry_streaming=True,
        idempotent_methods={METHOD},
    )
    depths: list[int] = []
    wire = Wire(depth_recording_stream(depths, make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream", details=make_call_details(timeout=5.0))
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    assert len(depths) == 5
    assert len(set(depths)) == 1
    # Restarted streams are issued with the remaining budget, like unary retries are.
    assert wire.timeouts[0] == 5.0
    assert wire.timeouts[-1] < 5.0 - 0.1


# --------------------------------------------------------------------------------------------
# The call budget: retries divide it, they never reset it.
# --------------------------------------------------------------------------------------------


async def test__retry__call_with_a_budget__issues_every_attempt_with_what_is_left_of_it() -> None:
    """Every attempt is issued with what is left of the call budget, never with a fresh one."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.05, backoff_multiplier=1.0, jitter=0.0)
    wire = Wire(lambda: FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire, details=make_call_details(timeout=5.0))

    # Assert
    seen = wire.timeouts
    assert len(seen) == 3
    # The first attempt inherits the budget as given; each retry is trimmed by the elapsed time.
    assert seen[0] == 5.0
    assert seen[1] < seen[0]
    assert seen[2] < seen[1]
    assert seen[2] <= 5.0 - 2 * 0.05


async def test__retry__budget_too_small_for_the_backoff__gives_up_with_the_original_error() -> None:
    """A retry that could not outlive its own backoff is pointless: fail with the original error."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=10, initial_backoff=0.1, backoff_multiplier=1.0, jitter=0.0)
    wire = Wire(lambda: FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    started = time.monotonic()
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire, details=make_call_details(timeout=0.3))
    elapsed = time.monotonic() - started

    # Assert
    assert wire.attempts == 3
    assert elapsed < 0.6


async def test__retry__below_a_timeout_interceptor__shares_its_single_budget() -> None:
    """The timeout interceptor bounds the whole call: retries divide its budget instead of resetting it."""
    # Arrange
    timeout_interceptor = AsyncTimeoutInterceptor(default_timeout=0.3)
    retry_interceptor = AsyncRetryInterceptor(max_attempts=10, initial_backoff=0.1, backoff_multiplier=1.0, jitter=0.0)
    wire = Wire(lambda: FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    started = time.monotonic()
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(timeout_interceptor, nested_wire(retry_interceptor, wire))
    elapsed = time.monotonic() - started

    # Assert
    seen = wire.timeouts
    assert seen[0] == 0.3
    assert seen == sorted(seen, reverse=True)
    # Without a shared budget this would run max_attempts * 0.3s of deadline and 10 attempts.
    assert len(seen) == 3
    assert elapsed < 0.6


async def test__retry__call_without_a_budget__leaves_the_timeout_alone() -> None:
    """A call with no deadline must not acquire one just because it was retried."""
    # Arrange
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0)
    wire = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)), FakeUnaryCall("ok"))

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "ok"
    assert wire.timeouts == [None, None]


# --------------------------------------------------------------------------------------------
# Backoff and callbacks.
# --------------------------------------------------------------------------------------------


def test__calculate_backoff__without_jitter__grows_exponentially_up_to_the_cap() -> None:
    """Exponential backoff doubles per attempt and then stops at max_backoff."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        initial_backoff=1.0,
        backoff_multiplier=2.0,
        max_backoff=5.0,
        jitter=0,
    )

    # Act
    backoffs = [interceptor._calculate_backoff(attempt) for attempt in (1, 2, 3, 4)]

    # Assert
    # attempts 1..3: 1.0 * 2^0, 2^1, 2^2; attempt 4 would be 8.0 and is capped at max_backoff.
    assert backoffs == [1.0, 2.0, 4.0, 5.0]


def test__calculate_backoff__with_jitter__stays_within_the_jitter_band() -> None:
    """Jitter spreads the wait around the nominal backoff instead of replacing it."""
    # Arrange
    interceptor = AsyncRetryInterceptor(
        initial_backoff=10.0,
        jitter=0.5,
    )

    # Act
    backoffs = [interceptor._calculate_backoff(1) for _ in range(100)]

    # Assert
    # The nominal backoff of attempt 1 is 10.0, so 0.5 jitter allows 5.0 to 15.0.
    assert all(5.0 <= value <= 15.0 for value in backoffs)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_attempts": 0}, "max_attempts must be at least 1"),
        ({"initial_backoff": -1.0}, "initial_backoff must be non-negative"),
        ({"max_backoff": -1.0}, "max_backoff must be non-negative"),
        ({"backoff_multiplier": 0.5}, "backoff_multiplier must be at least 1"),
        ({"jitter": 1.5}, "jitter must be between 0.0 and 1.0"),
    ],
)
def test__retry_interceptor__impossible_configuration__is_refused(kwargs: dict[str, Any], message: str) -> None:
    """Configuration that could only misbehave is refused where it is written, not in production."""
    # Act & Assert
    with pytest.raises(ValueError, match=message):
        AsyncRetryInterceptor(**kwargs)


async def test__retry__every_retry__is_reported_to_the_callback() -> None:
    """`on_retry` sees the attempt number, the status that caused it and the backoff it waited."""
    # Arrange
    on_retry = AsyncMock()
    interceptor = AsyncRetryInterceptor(max_attempts=3, initial_backoff=0.0, jitter=0.0, on_retry=on_retry)
    wire = Wire(
        FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)),
        FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED)),
        FakeUnaryCall("ok"),
    )

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "ok"
    assert [call.args[:3] for call in on_retry.await_args_list] == [
        (METHOD, 1, grpc.StatusCode.UNAVAILABLE),
        (METHOD, 2, grpc.StatusCode.RESOURCE_EXHAUSTED),
    ]


async def test__retry__failing_on_retry_callback__does_not_decide_the_fate_of_the_call() -> None:
    """Observability must never decide the fate of the call it observes."""
    # Arrange
    on_retry = AsyncMock(side_effect=RuntimeError("callback exploded"))
    interceptor = AsyncRetryInterceptor(max_attempts=2, initial_backoff=0.0, on_retry=on_retry)
    wire = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)), FakeUnaryCall("ok"))

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "ok"
    on_retry.assert_awaited_once()
