"""Unit tests for the circuit breaker interceptor.

Every test drives the breaker the way a channel does: through the adapters in
`AsyncClientInterceptor.adapters`, with a continuation shaped like grpc's — it resolves to a `Call`
object without raising, and the status only surfaces when that Call is awaited or iterated. A
continuation that raises the failure by itself is what let the old suite pass while the live breaker
recorded a success for every failed call.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import grpc.aio
import pytest

from grpc_client_kit.interceptors.circuit_breaker import (
    AsyncCircuitBreakerInterceptor,
    CircuitBreakerOpenError,
    CircuitState,
    MethodCircuitState,
    _error_code,
)
from tests.helpers import (
    METHOD,
    OTHER_METHOD,
    RPC_KINDS,
    BlockingUnaryCall,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    collect,
    make_call_details,
    make_metrics,
    make_rpc_error,
    raises,
    requests,
)

from .conftest import BEYOND_RECOVERY, RECOVERY_TIMEOUT, fail_unary, open_circuit, run_call, start_call

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------------------------
# Dispatch: one logical interceptor has to reach all four of a channel's lists.
# --------------------------------------------------------------------------------------------


def test__circuit_breaker__built__offers_one_adapter_per_rpc_kind() -> None:
    """A channel files interceptors by class, so each kind needs an object of exactly its own ABC."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor()

    # Act
    adapters = interceptor.adapters

    # Assert
    for abc_class in RPC_KINDS.values():
        matching = [entry for entry in adapters if isinstance(entry, abc_class)]
        assert len(matching) == 1, f"expected exactly one adapter for {abc_class.__name__}, got {matching}"

    # An adapter that inherited more than one ABC would be filed under the first match and lost for
    # the rest, which is the bug that left every streaming call unguarded.
    assert [sum(isinstance(entry, abc) for abc in RPC_KINDS.values()) for entry in adapters] == [1, 1, 1, 1]


def test__circuit_breaker__adapters_read_twice__hands_out_the_same_objects() -> None:
    """A chain's identity is the identity of its entries: the pool keys its channels on it."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor()

    # Act & Assert
    assert interceptor.adapters is interceptor.adapters


def test__circuit_breaker__on_its_own__is_not_a_grpc_interceptor() -> None:
    """Handing the logical object to a channel must fail loudly, not register it for unary only."""
    # Act & Assert
    assert not isinstance(AsyncCircuitBreakerInterceptor(), grpc.aio.ClientInterceptor)


async def test__circuit_breaker__stream_unary_call__hands_back_the_call_not_the_bare_response() -> None:
    """stream-unary passes an interceptor's return value straight to the caller, which awaits it."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor()
    wire = Wire(FakeUnaryCall("collected"))

    # Act
    result = await start_call(interceptor, wire, "stream_unary", request=requests("a"))

    # Assert
    assert hasattr(result, "__await__"), "a bare response here breaks the caller and the call finalizer"
    assert await result == "collected"


async def test__circuit_breaker__open_circuit__refuses_every_rpc_kind() -> None:
    """An open circuit refuses all four kinds; before the fix only unary-unary was even installed."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    await open_circuit(interceptor)
    wire = Wire(FakeStreamCall(b"item"))

    # Act & Assert
    for rpc_type in RPC_KINDS:
        with pytest.raises(CircuitBreakerOpenError):
            await start_call(interceptor, wire, rpc_type)

    assert wire.attempts == 0


async def test__circuit_breaker__binary_method_path__files_the_state_under_the_decoded_name() -> None:
    """Live call details carry the method as bytes; the breaker used to decode it by hand."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    wire = Wire(FakeUnaryCall(error=make_rpc_error(grpc.StatusCode.UNAVAILABLE)))

    # Act
    with pytest.raises(grpc.aio.AioRpcError):
        await run_call(interceptor, wire, details=make_call_details(METHOD.encode()))

    # Assert
    states = await interceptor.get_states()
    assert list(states) == [METHOD]


# --------------------------------------------------------------------------------------------
# Reading the outcome: the continuation resolves to a Call and never raises.
# --------------------------------------------------------------------------------------------


async def test__circuit_breaker__failure_surfacing_only_on_awaiting_the_call__is_recorded() -> None:
    """The whole point: a continuation that resolves happily still hides a failed RPC."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=2)

    # Act
    await fail_unary(interceptor)

    # Assert
    state = await interceptor._get_method_state(METHOD)
    assert state.failure_count == 1
    assert state.state == CircuitState.CLOSED


async def test__circuit_breaker__successful_unary_call__keeps_the_circuit_closed() -> None:
    """A call that resolves to a Call whose await returns keeps the circuit closed."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=2)
    wire = Wire(FakeUnaryCall("response"))

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "response"
    assert wire.attempts == 1
    state = await interceptor._get_method_state(METHOD)
    assert state.state == CircuitState.CLOSED
    assert state.failure_count == 0


async def test__circuit_breaker__failures_reaching_the_threshold__stop_the_next_call() -> None:
    """Failures reaching the threshold open the circuit, and the next call never reaches the wire."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=2, recovery_timeout=60.0)
    rejected = Wire(FakeUnaryCall("response"))

    # Act
    await fail_unary(interceptor)
    state = await interceptor._get_method_state(METHOD)
    after_first = (state.state, state.failure_count)

    await fail_unary(interceptor)
    with pytest.raises(CircuitBreakerOpenError, match=f"Circuit breaker for {METHOD} is open"):
        await run_call(interceptor, rejected)

    # Assert
    assert after_first == (CircuitState.CLOSED, 1)
    assert state.state == CircuitState.OPEN
    # An open circuit is only worth having if it stops the traffic instead of merely relabelling it.
    assert rejected.attempts == 0


async def test__circuit_breaker__fully_delivered_stream__counts_as_a_success() -> None:
    """A stream counts as a success once it has been fully delivered, not when it was created."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=2)
    wire = Wire(FakeStreamCall(b"one", b"two"))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    state = await interceptor._get_method_state(METHOD)
    state.failure_count = 1
    delivered = await collect(stream)

    # Assert
    assert delivered == [b"one", b"two"]
    assert state.state == CircuitState.CLOSED
    assert state.failure_count == 0


async def test__circuit_breaker__stream_dying_halfway__is_recorded_as_a_failure() -> None:
    """A stream that dies halfway through has still failed, and the breaker has to see it."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    wire = Wire(FakeStreamCall(b"one", error=make_rpc_error(grpc.StatusCode.INTERNAL)))
    delivered = []

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    with pytest.raises(grpc.aio.AioRpcError):
        async for item in stream:
            delivered.append(item)

    # Assert
    assert delivered == [b"one"]
    state = await interceptor._get_method_state(METHOD)
    assert state.state == CircuitState.OPEN


async def test__circuit_breaker__stream_stream_call__is_guarded_like_any_other() -> None:
    """A stream-stream call is guarded like any other, request iterator and all."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor()
    wire = Wire(FakeStreamCall(b"one"))

    # Act
    stream = await start_call(interceptor, wire, "stream_stream", request=requests(b"a"))
    delivered = await collect(stream)

    # Assert
    assert delivered == [b"one"]
    state = await interceptor._get_method_state(METHOD)
    assert state.state == CircuitState.CLOSED


# --------------------------------------------------------------------------------------------
# What counts as a failure.
# --------------------------------------------------------------------------------------------


def test__should_record_failure__status_code__tells_a_broken_server_from_a_bad_request() -> None:
    """Only codes that say something about the server's health may count towards a trip."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)

    # Act & Assert
    assert not interceptor._should_record_failure(grpc.StatusCode.INVALID_ARGUMENT)
    # An error without a usable code says nothing about the server.
    assert not interceptor._should_record_failure(None)
    assert interceptor._should_record_failure(grpc.StatusCode.UNAVAILABLE)
    assert interceptor._should_record_failure(grpc.StatusCode.INTERNAL)
    assert interceptor._should_record_failure(grpc.StatusCode.RESOURCE_EXHAUSTED)
    assert interceptor._should_record_failure(grpc.StatusCode.DEADLINE_EXCEEDED)
    assert interceptor._should_record_failure(grpc.StatusCode.ABORTED)
    assert interceptor._should_record_failure(grpc.StatusCode.UNKNOWN)
    assert interceptor._should_record_failure(grpc.StatusCode.DATA_LOSS)


def test__error_code__error_without_a_usable_code__degrades_to_none() -> None:
    """Errors without a callable or usable code() must degrade to None, not crash."""
    # Arrange
    codeless = MagicMock(spec=Exception)
    codeless.code = "not-callable"
    wrong_type = MagicMock(spec=Exception)
    wrong_type.code = MagicMock(return_value="UNAVAILABLE")

    # Act & Assert
    assert _error_code(grpc.aio.AioRpcError(grpc.StatusCode.INTERNAL, None, None, "boom")) is grpc.StatusCode.INTERNAL
    assert _error_code(RuntimeError("boom")) is None
    assert _error_code(codeless) is None
    assert _error_code(wrong_type) is None


async def test__circuit_breaker__rejected_request__cannot_trip_the_circuit() -> None:
    """A rejected request is the caller's fault, so it must not be able to trip a circuit."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=2)

    # Act
    await fail_unary(interceptor, grpc.StatusCode.INVALID_ARGUMENT)

    # Assert
    states = await interceptor.get_states()
    assert states[METHOD]["failure_count"] == 0
    assert states[METHOD]["state"] == CircuitState.CLOSED.value


async def test__circuit_breaker__error_without_a_status__is_still_a_failed_call() -> None:
    """An error carrying no status at all is still a failed call."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    wire = Wire(FakeUnaryCall(error=RuntimeError("generic")))

    # Act
    with pytest.raises(RuntimeError):
        await run_call(interceptor, wire)

    # Assert
    state = await interceptor._get_method_state(METHOD)
    assert state.failure_count == 1
    assert state.state == CircuitState.OPEN


async def test__circuit_breaker__continuation_blowing_up__is_recorded_as_a_failure() -> None:
    """A layer below that fails before the RPC exists is a failure too, not a swallowed one."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    wire = Wire(raises(RuntimeError("layer below is broken")))

    # Act
    with pytest.raises(RuntimeError):
        await run_call(interceptor, wire)

    # Assert
    state = await interceptor._get_method_state(METHOD)
    assert state.state == CircuitState.OPEN


async def test__circuit_breaker__cancelled_call__is_not_a_failure() -> None:
    """A client that walked away has said nothing about the health of the server."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    wire = Wire(FakeUnaryCall(error=asyncio.CancelledError()))

    # Act
    with pytest.raises(asyncio.CancelledError):
        await run_call(interceptor, wire)

    # Assert
    state = await interceptor._get_method_state(METHOD)
    assert state.failure_count == 0
    assert state.state == CircuitState.CLOSED


async def test__circuit_breaker__success_after_failures__wipes_the_failure_count() -> None:
    """A success wipes the failures accumulated so far, so old noise cannot add up to a trip."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=5)
    for _ in range(3):
        await fail_unary(interceptor)
    state = await interceptor._get_method_state(METHOD)
    accumulated = state.failure_count

    # Act
    await run_call(interceptor, Wire(FakeUnaryCall("ok")))

    # Assert
    assert accumulated == 3
    assert state.failure_count == 0


async def test__circuit_breaker__one_broken_method__leaves_its_neighbour_callable() -> None:
    """One broken method must not stop the calls to its healthy neighbour."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    await open_circuit(interceptor)
    healthy = Wire(FakeUnaryCall("response"))

    # Act
    response = await run_call(interceptor, healthy, details=make_call_details(OTHER_METHOD))

    # Assert
    assert response == "response"
    states = await interceptor.get_states()
    assert states[METHOD]["state"] == CircuitState.OPEN.value
    assert states[OTHER_METHOD]["state"] == CircuitState.CLOSED.value


# --------------------------------------------------------------------------------------------
# Recovery: OPEN -> HALF-OPEN -> CLOSED.
# --------------------------------------------------------------------------------------------


async def test__circuit_breaker__successful_trial_after_the_recovery_window__closes_the_circuit() -> None:
    """Once the recovery window has passed, one trial call decides whether the circuit closes."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1, recovery_timeout=RECOVERY_TIMEOUT)
    state = await open_circuit(interceptor)
    trial = Wire(FakeUnaryCall("response"))

    # Act
    await asyncio.sleep(BEYOND_RECOVERY)
    response = await run_call(interceptor, trial)

    # Assert
    assert response == "response"
    assert trial.attempts == 1
    assert state.state == CircuitState.CLOSED
    assert state.half_open_calls == 0


async def test__circuit_breaker__failing_trial__reopens_the_circuit() -> None:
    """A trial that fails sends the circuit straight back to OPEN, threshold or no threshold."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=100, recovery_timeout=RECOVERY_TIMEOUT)
    await fail_unary(interceptor)
    state = await interceptor._get_method_state(METHOD)
    state.state = CircuitState.OPEN

    # Act
    await asyncio.sleep(BEYOND_RECOVERY)
    await fail_unary(interceptor)

    # Assert
    assert state.state == CircuitState.OPEN
    assert state.half_open_calls == 0


async def test__circuit_breaker__half_open_circuit__admits_one_trial_at_a_time() -> None:
    """A half-open circuit admits one trial at a time; the RPC, not its creation, holds the slot."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(
        fail_threshold=1,
        half_open_max_calls=1,
        recovery_timeout=RECOVERY_TIMEOUT,
    )
    state = await open_circuit(interceptor)
    await asyncio.sleep(BEYOND_RECOVERY)
    blocking = BlockingUnaryCall()
    second = Wire(FakeUnaryCall("response"))

    # Act
    trial = asyncio.create_task(run_call(interceptor, Wire(blocking)))
    await blocking.started.wait()
    taken = state.half_open_calls

    # The slot is still taken, because the first trial is still in flight rather than merely created.
    with pytest.raises(CircuitBreakerOpenError):
        await run_call(interceptor, second)

    blocking.release.set()
    response = await trial

    # Assert
    assert taken == 1
    assert second.attempts == 0
    assert response == "response"
    assert state.state == CircuitState.CLOSED
    assert state.half_open_calls == 0


async def test__circuit_breaker__half_open_trial_stream__holds_its_slot_until_the_last_item() -> None:
    """A half-open trial that streams holds its slot until the last item has been delivered."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(
        fail_threshold=1,
        half_open_max_calls=1,
        recovery_timeout=RECOVERY_TIMEOUT,
    )
    state = await open_circuit(interceptor)
    await asyncio.sleep(BEYOND_RECOVERY)

    # Act
    stream = await start_call(interceptor, Wire(FakeStreamCall(b"one")), "unary_stream")
    in_flight = (state.half_open_calls, state.state)
    delivered = await collect(stream)

    # Assert
    assert in_flight == (1, CircuitState.HALF_OPEN)
    assert delivered == [b"one"]
    assert state.half_open_calls == 0
    assert state.state == CircuitState.CLOSED


async def test__circuit_breaker__half_open_trial_stream_failing__reopens_and_frees_the_slot() -> None:
    """A trial stream that fails mid-flight reopens the circuit and gives its slot back."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(
        fail_threshold=1,
        half_open_max_calls=1,
        recovery_timeout=RECOVERY_TIMEOUT,
    )
    state = await open_circuit(interceptor)
    await asyncio.sleep(BEYOND_RECOVERY)
    wire = Wire(FakeStreamCall(b"one", error=make_rpc_error(grpc.StatusCode.INTERNAL)))

    # Act
    stream = await start_call(interceptor, wire, "unary_stream")
    in_flight = state.half_open_calls
    with pytest.raises(grpc.aio.AioRpcError):
        await collect(stream)

    # Assert
    assert in_flight == 1
    assert state.half_open_calls == 0
    assert state.state == CircuitState.OPEN


async def test__circuit_breaker__cancelled_trial__releases_its_slot() -> None:
    """A cancellation records nothing, so only the explicit release keeps the circuit usable."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(
        fail_threshold=1,
        half_open_max_calls=1,
        recovery_timeout=RECOVERY_TIMEOUT,
    )
    state = await open_circuit(interceptor)
    await asyncio.sleep(BEYOND_RECOVERY)

    # Act
    with pytest.raises(asyncio.CancelledError):
        await run_call(interceptor, Wire(FakeUnaryCall(error=asyncio.CancelledError())))
    after_cancel = (state.state, state.half_open_calls)
    response = await run_call(interceptor, Wire(FakeUnaryCall("response")))

    # Assert
    # A leaked slot would leave the circuit half-open and refusing every trial for good.
    assert after_cancel == (CircuitState.HALF_OPEN, 0)
    assert response == "response"
    assert state.state == CircuitState.CLOSED


# --------------------------------------------------------------------------------------------
# Bookkeeping: state storage, snapshots, metrics and configuration.
# --------------------------------------------------------------------------------------------


async def test__method_state__more_methods_than_the_cap__evicts_the_least_recently_used() -> None:
    """The state map is bounded, so a service with unbounded method names cannot grow it forever."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(max_methods=2)

    # Act & Assert
    await interceptor._get_method_state("method1")
    await interceptor._get_method_state("method2")
    assert "method1" in interceptor._states
    assert "method2" in interceptor._states

    await interceptor._get_method_state("method3")
    assert "method1" not in interceptor._states
    assert "method2" in interceptor._states
    assert "method3" in interceptor._states

    # Reading method2 moves it back to the end, so method3 becomes the next one to go.
    await interceptor._get_method_state("method2")
    await interceptor._get_method_state("method4")
    assert "method3" not in interceptor._states
    assert "method2" in interceptor._states
    assert "method4" in interceptor._states


async def test__method_state__eviction_under_churn__never_discards_an_open_circuit() -> None:
    """Evicting an OPEN state silently re-closes the breaker: the next call to the "protected"
    method goes to a backend the breaker had declared down, and it takes another full threshold of
    real failures to open it again."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1, recovery_timeout=3600.0, max_methods=2)
    await open_circuit(interceptor, method=METHOD)

    # Act: churn of unrelated methods pushes the map past its cap several times over.
    for index in range(5):
        await interceptor._get_method_state(f"/churn.Service/Method{index}")

    # Assert
    state = interceptor._states[METHOD]
    assert state.state is CircuitState.OPEN
    # And the protection is live, not just bookkeeping: the method is still refused.
    with pytest.raises(CircuitBreakerOpenError):
        await run_call(interceptor, Wire(FakeUnaryCall("ok")), details=make_call_details(METHOD))


async def test__method_state__every_state_protecting__grows_past_the_cap_instead_of_evicting() -> None:
    """When every state is OPEN there is no safe victim; memory yields to correctness."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1, recovery_timeout=3600.0, max_methods=2)
    await open_circuit(interceptor, method=METHOD)
    await open_circuit(interceptor, method=OTHER_METHOD)

    # Act
    await interceptor._get_method_state("/extra.Service/Method")

    # Assert
    assert len(interceptor._states) == 3
    assert interceptor._states[METHOD].state is CircuitState.OPEN
    assert interceptor._states[OTHER_METHOD].state is CircuitState.OPEN


async def test__method_state__unseen_method__is_a_typed_object_with_closed_defaults() -> None:
    """Method state is a typed object, not a bag of stringly-keyed values."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor()

    # Act
    state = await interceptor._get_method_state("method1")

    # Assert
    assert isinstance(state, MethodCircuitState)
    assert state.state is CircuitState.CLOSED
    assert state.failure_count == 0
    assert state.last_failure_time == 0.0
    assert state.half_open_calls == 0


async def test__get_states__tripped_circuit__returns_a_serializable_snapshot() -> None:
    """get_states() exposes a serializable snapshot without the internal lock."""
    # Arrange
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1)
    await fail_unary(interceptor)

    # Act
    states = await interceptor.get_states()

    # Assert
    assert states[METHOD]["state"] == CircuitState.OPEN.value
    assert states[METHOD]["failure_count"] == 1
    assert set(states[METHOD]) == {
        "state",
        "failure_count",
        "last_failure_time",
        "half_open_calls",
    }


def test__circuit_breaker_open_error__raised__carries_an_unavailable_status() -> None:
    """The refusal is shaped like a gRPC failure, so the layers above need no special case."""
    # Arrange
    error = CircuitBreakerOpenError("method")

    # Act & Assert
    assert error.code() == grpc.StatusCode.UNAVAILABLE
    assert "Circuit breaker for method is open" in error.details()


async def test__circuit_breaker__state_transitions__reach_a_registry_that_opted_in() -> None:
    """A registry implementing `CircuitBreakerMetricsProtocol` sees every transition by name.

    The old surface was an undeclared ``circuit_breaker_state`` gauge probed with hasattr — a
    contract that existed only in tests, so no real registry ever received a transition.
    """
    # Arrange
    metrics = make_metrics(with_circuit=True)
    interceptor = AsyncCircuitBreakerInterceptor(
        fail_threshold=1,
        recovery_timeout=RECOVERY_TIMEOUT,
        metrics=metrics,
    )

    # Act
    await fail_unary(interceptor)
    await asyncio.sleep(BEYOND_RECOVERY)
    await run_call(interceptor, Wire(FakeUnaryCall("response")))

    # Assert
    recorded = [call.args for call in metrics.record_circuit_state.call_args_list]
    assert recorded == [
        (METHOD, "closed"),
        (METHOD, "open"),
        (METHOD, "half-open"),
        (METHOD, "closed"),
    ]


async def test__circuit_breaker__rejection__reaches_a_registry_that_opted_in() -> None:
    """Locally refused calls are counted apart from real failures for whoever opted in."""
    # Arrange
    # Recovery far beyond the test's lifetime: a suite under load must not slip into HALF-OPEN.
    metrics = make_metrics(with_circuit=True)
    interceptor = AsyncCircuitBreakerInterceptor(
        fail_threshold=1,
        recovery_timeout=3600.0,
        metrics=metrics,
    )
    await fail_unary(interceptor)

    # Act
    with pytest.raises(CircuitBreakerOpenError):
        await run_call(interceptor, Wire(FakeUnaryCall("response")))

    # Assert
    metrics.record_circuit_rejection.assert_called_once_with(METHOD)


async def test__circuit_breaker__plain_registry__receives_no_breaker_calls() -> None:
    """A registry that only implements the base protocol is never poked with extension methods."""
    # Arrange
    metrics = make_metrics()
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1, metrics=metrics)

    # Act
    await fail_unary(interceptor)

    # Assert
    assert interceptor._state_metrics is None


async def test__circuit_breaker__metrics_registry_raising__does_not_break_the_call() -> None:
    """A registry that raises must not break the call it is only observing."""
    # Arrange
    metrics = make_metrics(with_circuit=True)
    metrics.record_circuit_state.side_effect = RuntimeError("registry down")
    interceptor = AsyncCircuitBreakerInterceptor(fail_threshold=1, metrics=metrics)

    # Act
    response = await run_call(interceptor, Wire(FakeUnaryCall("ok")))

    # Assert
    assert response == "ok"


def test__circuit_breaker__invalid_configuration__is_refused() -> None:
    """Configuration that could only misbehave is refused where it is written, not in production."""
    # Act & Assert
    with pytest.raises(ValueError, match="fail_threshold must be positive"):
        AsyncCircuitBreakerInterceptor(fail_threshold=0)
    with pytest.raises(ValueError, match="recovery_timeout must be non-negative"):
        AsyncCircuitBreakerInterceptor(recovery_timeout=-1.0)
    with pytest.raises(ValueError, match="half_open_max_calls must be positive"):
        AsyncCircuitBreakerInterceptor(half_open_max_calls=0)
    with pytest.raises(ValueError, match="max_methods must be positive"):
        AsyncCircuitBreakerInterceptor(max_methods=0)
