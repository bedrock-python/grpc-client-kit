"""Test doubles for the gRPC vocabulary every suite in this repository speaks.

Two properties of `grpc.aio` shape all of them, and both are reproduced faithfully rather than
simplified away:

* Call details are a real `grpc.aio.ClientCallDetails`, so ``_replace`` behaves as it does in a
  live chain instead of accepting whatever a hand-rolled namedtuple would.
* A continuation resolves to a `Call` object and never raises. The outcome of an RPC only surfaces
  when that Call is awaited (unary response) or iterated (streaming response). A double that
  raises from the continuation itself is what let a whole suite pass while, on a live connection,
  every failed call was observed as an instant success.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Generator
from typing import Any, NamedTuple
from unittest.mock import AsyncMock, MagicMock

import grpc
import grpc.aio

from grpc_client_kit.channel import ChannelPool
from grpc_client_kit.deadline import DeadlineExceededError

# The method path calls are issued against unless a test needs to tell two methods apart.
METHOD = "/pkg.Service/Method"

# A second method path, for the tests asserting that one method's state leaves its neighbour alone.
OTHER_METHOD = "/pkg.Service/Other"

# The gRPC base class a channel files an adapter under, keyed by the RPC kind that adapter serves.
RPC_KINDS: dict[str, type] = {
    "unary_unary": grpc.aio.UnaryUnaryClientInterceptor,
    "unary_stream": grpc.aio.UnaryStreamClientInterceptor,
    "stream_unary": grpc.aio.StreamUnaryClientInterceptor,
    "stream_stream": grpc.aio.StreamStreamClientInterceptor,
}

# The RPC kinds whose response side is a stream, and which are therefore only over on the last item.
STREAMING_RESPONSE: tuple[str, ...] = ("unary_stream", "stream_stream")


# --------------------------------------------------------------------------------------------
# Call details.
# --------------------------------------------------------------------------------------------


def make_call_details(
    method: Any = METHOD,
    timeout: float | None = None,
    metadata: Any = None,
) -> grpc.aio.ClientCallDetails:
    """Build real call details, the shape a live chain hands an interceptor."""
    return grpc.aio.ClientCallDetails(
        method=method,
        timeout=timeout,
        metadata=metadata,
        credentials=None,
        wait_for_ready=None,
    )


class DeadlineCallDetails(NamedTuple):
    """Call details of the shape older interceptors pass on: an absolute deadline, no timeout.

    Attributes:
        method: Full method name of the call.
        deadline: The absolute deadline the call is bounded by.
    """

    method: str
    deadline: float | None = None


class UnsettableCallDetails:
    """Call details whose ``_replace`` refuses a timeout, as a hand-written shim might.

    Attributes:
        method: Full method name of the call.
        deadline: The absolute deadline an interceptor fell back to setting.
    """

    def __init__(self, method: str = METHOD) -> None:
        """Start with no deadline at all."""
        self.method = method
        self.deadline: float | None = None

    def _replace(self, **kwargs: Any) -> UnsettableCallDetails:
        if "timeout" in kwargs:
            raise TypeError("this call details object has no timeout")
        replaced = UnsettableCallDetails(self.method)
        replaced.deadline = kwargs["deadline"]
        return replaced


# --------------------------------------------------------------------------------------------
# Deadline budgets, as the kit reads them off the ambient context.
# --------------------------------------------------------------------------------------------


class FakeBudget:
    """A request budget granting a fixed timeout, with the shape `BudgetContext` has.

    The real budget measures a monotonic clock, which makes "how much was this call granted" a
    moving target; this one answers with a number the test chose. What it does reproduce faithfully
    is the refusal: an exhausted budget raises `DeadlineExceededError` out of ``timeout_for_call``
    rather than returning a non-positive timeout.

    Attributes:
        granted: The timeout, in seconds, every call is granted.
        left: The seconds reported as remaining; at or below zero the budget is exhausted.
        asked_for: The call name and reserve of every timeout request, in order.
    """

    def __init__(self, granted: float = 1.0, left: float = 1.0) -> None:
        """Fix what this budget grants and how much of it is left."""
        self.granted = granted
        self.left = left
        self.asked_for: list[tuple[str, float]] = []

    @property
    def call_names(self) -> list[str]:
        """The name every timeout was requested under, which is what per-call caps are keyed by."""
        return [name for name, _ in self.asked_for]

    def timeout_for_call(self, call_name: str, reserve_for_next: float = 0.0) -> float:
        self.asked_for.append((call_name, reserve_for_next))
        if self.expired():
            raise DeadlineExceededError(1.0, 2.0)
        return self.granted

    def remaining(self) -> float:
        return self.left

    def expired(self) -> bool:
        return self.left <= 0


# --------------------------------------------------------------------------------------------
# Errors.
# --------------------------------------------------------------------------------------------


def make_rpc_error(code: grpc.StatusCode, details: str = "boom") -> grpc.aio.AioRpcError:
    """Build an AioRpcError carrying the given status code."""
    return grpc.aio.AioRpcError(code, grpc.aio.Metadata(), grpc.aio.Metadata(), details)


class CodelessRpcError(grpc.aio.AioRpcError):
    """A gRPC-shaped error whose ``code`` is a plain attribute, as a custom inner layer may raise."""

    def __init__(self) -> None:
        """Build the error and hide the ``code()`` method behind a non-callable attribute."""
        super().__init__(grpc.StatusCode.UNKNOWN, grpc.aio.Metadata(), grpc.aio.Metadata(), "no code")
        self.code = None  # type: ignore[assignment]


# --------------------------------------------------------------------------------------------
# Calls, as grpc.aio hands them out.
# --------------------------------------------------------------------------------------------


class FakeUnaryCall:
    """A unary `Call` as grpc.aio hands one out: created without failing, resolved when awaited."""

    def __init__(self, response: Any = None, error: BaseException | None = None, delay: float = 0.0) -> None:
        """Fix the outcome this call produces, and how long the RPC takes to produce it."""
        self._response = response
        self._error = error
        self._delay = delay

    def __await__(self) -> Generator[Any, None, Any]:
        async def resolve() -> Any:
            await asyncio.sleep(self._delay)
            if self._error is not None:
                raise self._error
            return self._response

        return resolve().__await__()


class BlockingUnaryCall:
    """A unary `Call` that stays in flight until the test releases it.

    Attributes:
        started: Set once the RPC has begun, which is when a test may inspect in-flight state.
        release: Awaited by the RPC; setting it lets the call resolve.
    """

    def __init__(self, response: Any = "response") -> None:
        """Create the call together with the two events a test drives it by."""
        self._response = response
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def __await__(self) -> Generator[Any, None, Any]:
        async def resolve() -> Any:
            self.started.set()
            await self.release.wait()
            return self._response

        return resolve().__await__()


class FakeStreamCall:
    """A streaming `Call`: items first, then optionally the status the server ended with."""

    def __init__(self, *items: Any, error: BaseException | None = None, delay: float = 0.0) -> None:
        """Fix the items this stream yields, how long each takes and how it ends."""
        self._items = items
        self._error = error
        self._delay = delay

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Any]:
        for item in self._items:
            await asyncio.sleep(self._delay)
            yield item
        if self._error is not None:
            raise self._error


# --------------------------------------------------------------------------------------------
# The continuation an interceptor is handed.
# --------------------------------------------------------------------------------------------


class Wire:
    """A continuation that records every attempt and answers with a scripted `Call`.

    Attributes:
        calls: The call details of every attempt that reached the wire, in order.
        requests: The request handed to every attempt, in the same order.
    """

    def __init__(self, *outcomes: Any) -> None:
        """Script one outcome per attempt; the last one repeats once the script runs out."""
        self._outcomes: list[Any] = list(outcomes) or [FakeUnaryCall("response")]
        self.calls: list[Any] = []
        self.requests: list[Any] = []

    @property
    def attempts(self) -> int:
        """How many attempts reached the wire."""
        return len(self.calls)

    @property
    def details(self) -> Any:
        """The call details of the last attempt."""
        return self.calls[-1]

    @property
    def timeout(self) -> float | None:
        """The relative timeout the last attempt was issued with."""
        return getattr(self.details, "timeout", None)

    @property
    def timeouts(self) -> list[float | None]:
        """The relative timeout every attempt was issued with."""
        return [getattr(details, "timeout", None) for details in self.calls]

    @property
    def wait_flags(self) -> list[bool | None]:
        """The wait-for-ready flag every attempt was issued with."""
        return [getattr(details, "wait_for_ready", None) for details in self.calls]

    @property
    def metadata_pairs(self) -> list[tuple[str, Any]]:
        """The metadata of the last attempt, as the ordered pairs that go on the wire."""
        return list(self.details.metadata or [])

    @property
    def metadata(self) -> dict[str, Any]:
        """The metadata of the last attempt, as a mapping."""
        return dict(self.metadata_pairs)

    async def __call__(self, details: Any, request: Any) -> Any:
        self.calls.append(details)
        self.requests.append(request)
        outcome = self._outcomes[min(len(self.calls) - 1, len(self._outcomes) - 1)]
        return outcome() if callable(outcome) else outcome


def raises(error: BaseException) -> Callable[[], Any]:
    """An outcome for `Wire` where the continuation itself blows up, as a broken layer below would."""

    def _raise() -> Any:
        raise error

    return _raise


def refusing_wire(error: BaseException) -> Callable[[Any, Any], Any]:
    """A continuation that refuses the call outright instead of resolving to a `Call`."""

    async def _refuse(details: Any, request: Any) -> Any:
        raise error

    return _refuse


# --------------------------------------------------------------------------------------------
# Driving an interceptor the way a channel does.
# --------------------------------------------------------------------------------------------


def adapter_for(interceptor: Any, abc_class: type) -> Any:
    """Pick the one adapter of `interceptor` that a channel would file under `abc_class`."""
    return next(entry for entry in interceptor.adapters if isinstance(entry, abc_class))


async def await_result(result: Any) -> Any:
    """Await what an interceptor handed back, the way grpc's caller does."""
    return await result if hasattr(result, "__await__") else result


async def collect(stream: Any) -> list[Any]:
    """Drain a response stream into a list."""
    return [item async for item in stream]


async def requests(*items: Any) -> AsyncIterator[Any]:
    """A streaming request, as gRPC delivers one to a stream-request interceptor."""
    for item in items:
        yield item


# --------------------------------------------------------------------------------------------
# Channels, pools and recorders.
# --------------------------------------------------------------------------------------------


def make_channel() -> AsyncMock:
    """Create a stand-in for an async gRPC channel."""
    return AsyncMock(spec=grpc.aio.Channel)


def make_interceptor() -> MagicMock:
    """Create a stand-in for a client interceptor a channel would accept."""
    return MagicMock(spec=grpc.aio.UnaryUnaryClientInterceptor)


def make_pool() -> AsyncMock:
    """Create a channel pool stub that hands out a fresh channel mock."""
    pool = AsyncMock(spec=ChannelPool)
    pool.get_channel.return_value = make_channel()
    return pool


class _MetricsSurface:
    """Spec of the base metrics protocol, so a double answers for exactly these methods."""

    def record_request(
        self, service: str, method: str, rpc_type: str, status: str, grpc_code: str, duration: float
    ) -> None: ...

    def record_inflight_delta(self, service: str, method: str, rpc_type: str, delta: int) -> None: ...

    def record_pool_stats(self, active_channels: int, idle_targets: int) -> None: ...


class _CircuitAwareMetricsSurface(_MetricsSurface):
    """Spec of a registry that also opted into the circuit breaker extension protocol."""

    def record_circuit_state(self, method: str, state: str) -> None: ...

    def record_circuit_rejection(self, method: str) -> None: ...


def make_metrics(*, with_circuit: bool = False) -> MagicMock:
    """Create a metrics recorder double; every ``record_*`` call is captured for assertions.

    Spec'd on purpose: a bare MagicMock answers ``isinstance`` for every runtime-checkable
    protocol, which is precisely how a state gauge that no real registry implemented once looked
    covered by tests.

    Args:
        with_circuit: Whether the double also implements the circuit breaker extension protocol.
    """
    return MagicMock(spec=_CircuitAwareMetricsSurface() if with_circuit else _MetricsSurface())
