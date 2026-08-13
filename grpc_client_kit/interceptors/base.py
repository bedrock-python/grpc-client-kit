"""One logical client interceptor, four channel adapters, one seam to write against.

Two properties of `grpc.aio` — both measured against a live server, not inferred — shape everything
in this module.

**A channel files interceptors by class, not by capability.** `grpc.aio.Channel.__init__` walks the
interceptor list once and sorts each entry into one of four lists with an ``if / elif / elif /
elif`` chain over the four interceptor ABCs. An interceptor that inherits all four therefore lands
in the *first* matching list and nowhere else: the channel registers it as unary-unary only, and
every streaming call runs completely unintercepted. The fix is to give the channel four thin
adapters per logical interceptor, each inheriting exactly one ABC and delegating to the same
implementation — see `AsyncClientInterceptor.adapters` and `flatten_interceptors`.

**A continuation resolves to a Call, not to a response.** It resolves the moment the RPC is
*created*, identically for a call that will succeed and one that will fail, and it never raises. The
outcome only becomes visible later: awaiting the `Call` raises `grpc.aio.AioRpcError` for a unary
response, and iterating it raises for a streaming one. An interceptor that treats the continuation's
result as the response therefore observes every call as an instant success. `ClientCall.invoke_unary`
and `ClientCall.invoke_stream` are the two places that get this right, so no interceptor has to.

Writing one::

    class TimingInterceptor(AsyncAroundClientInterceptor):
        async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
            started = time.perf_counter()
            try:
                yield
            except grpc.aio.AioRpcError as error:
                record(call.method, error.code(), time.perf_counter() - started)
                raise
            else:
                record(call.method, grpc.StatusCode.OK, time.perf_counter() - started)

That one generator covers all four RPC kinds: for a streaming response the base class holds it open
until the last item has been delivered, so the ``except`` branch sees a mid-stream failure too.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import functools
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Literal

import grpc.aio

# Outcome observers for deferred calls; referenced here so the loop cannot collect them mid-flight.
_BACKGROUND_TASKS: set[asyncio.Task[None]] = set()


def _spawn_background(coro: Awaitable[None]) -> None:
    """Run an outcome observer as a task the event loop cannot garbage-collect early."""
    task = asyncio.ensure_future(coro)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)


type RpcType = Literal["unary_unary", "unary_stream", "stream_unary", "stream_stream"]

# What a logical interceptor is handed as a continuation: ``(call_details, request) -> Call``.
type Continuation = Callable[[Any, Any], Awaitable[Any]]

# A chain entry during the migration: either an already-adapted grpc interceptor or a logical one.
type ClientInterceptorLike = grpc.aio.ClientInterceptor | AsyncClientInterceptor

_REQUEST_STREAMING: frozenset[str] = frozenset({"stream_unary", "stream_stream"})
_RESPONSE_STREAMING: frozenset[str] = frozenset({"unary_stream", "stream_stream"})


@dataclass(slots=True)
class ClientCall:
    """One outgoing RPC as an interceptor sees it, whichever of the four kinds it is.

    Attributes:
        method: Full method name (``/package.Service/Method``), already decoded — grpc hands the
            raw call details a `bytes` method, and every interceptor used to decode it by hand.
        rpc_type: Which of the four RPC kinds this call is.
        details: The `grpc.aio.ClientCallDetails` the next attempt will be issued with. Rebind it
            (``call.details = call.details._replace(timeout=...)``) before invoking to change the
            deadline, the metadata or the credentials; `invoke_unary` and `invoke_stream` read it
            at the moment they issue the call.
        request: The request message, or the request iterator for a streaming-request call.
    """

    method: str
    rpc_type: RpcType
    details: Any
    request: Any
    _continuation: Continuation
    _underlying: Any = field(default=None, init=False, repr=False)
    _response: Any = field(default=None, init=False, repr=False)

    @property
    def request_streaming(self) -> bool:
        """Whether the caller streams the request (stream-unary and stream-stream)."""
        return self.rpc_type in _REQUEST_STREAMING

    @property
    def response_streaming(self) -> bool:
        """Whether the server streams the response (unary-stream and stream-stream)."""
        return self.rpc_type in _RESPONSE_STREAMING

    @property
    def underlying_call(self) -> Any:
        """The `grpc.aio.Call` of the attempt last issued, or None while none has been."""
        return self._underlying

    @property
    def response(self) -> Any:
        """The unary response once it has arrived; None for streams and for calls that failed."""
        return self._response

    async def invoke_unary(self) -> Any:
        """Issue the call and, for a unary request, wait for its single response.

        Returns:
            The response message — or, for a streaming request, the `Call` itself, promptly.

        Raises:
            grpc.aio.AioRpcError: If the RPC failed. The continuation never raises — it resolves to
                a `Call` as soon as the RPC is created — so the status is only reached by awaiting
                that Call, which is what this does.
        """
        call = await self._continuation(self.details, self.request)
        self._underlying = call
        if self.request_streaming:
            # Awaiting the response here would deadlock the write()-style API: grpc parks both
            # ``call.write()`` and the request proxy until the interceptors task has finished, and
            # the response cannot arrive before the requests are written. The Call is handed back
            # promptly instead; whoever needs the outcome observes it from a separate task.
            return call
        # An interceptor further in is allowed to short-circuit with a plain response; only a real
        # Call has to be awaited a second time for its status to surface.
        response = await call if hasattr(call, "__await__") else call
        self._response = response
        return response

    async def invoke_stream(self) -> AsyncIterator[Any]:
        """Issue the call now and return the iterator over its responses.

        The RPC is created before this returns, deliberately. grpc.aio binds the `Call` an
        interceptor created to whatever iterator that interceptor hands back, and an iterator that
        only creates its call on the first ``__anext__`` leaves that binding empty — ``code()``,
        ``cancel()`` and the call's own finalizer then fail on ``None``.

        Returns:
            The response iterator. A failed RPC raises `grpc.aio.AioRpcError` while it is iterated,
            which for a mid-stream failure happens after some items have already been delivered.
        """
        call = await self._continuation(self.details, self.request)
        self._underlying = call
        stream: AsyncIterator[Any] = call
        return stream


class AsyncClientInterceptor(abc.ABC):
    """A logical client interceptor: one implementation covering all four RPC kinds.

    Subclasses implement `intercept`, which is handed a `ClientCall` and owns the call from there:
    it decides whether to issue it at all, how many times, and with which details. That is the hook
    for layers that re-issue calls — retries — or refuse them outright. Everything that only needs
    to *wrap* a call should subclass `AsyncAroundClientInterceptor` instead and write one generator.

    Deliberately not a `grpc.aio.ClientInterceptor`: passing one straight to a channel raises
    ``ValueError`` from grpc instead of silently registering it for unary-unary calls only. Chains
    reach a channel through `flatten_interceptors`, which turns each logical interceptor into the
    four adapters a channel can file correctly.
    """

    @abc.abstractmethod
    async def intercept(self, call: ClientCall) -> Any:
        """Run one RPC and return its outcome.

        Args:
            call: The call to run, and the handle used to issue it.

        Returns:
            For a unary response, the response message (`ClientCall.invoke_unary` returns it). For a
            streaming response, the response iterator — which must be built over an *already issued*
            call, i.e. over the result of `ClientCall.invoke_stream`.

        Raises:
            grpc.aio.AioRpcError: To fail the call, whether the error came off the wire or was
                raised by this interceptor without issuing anything.
        """

    @functools.cached_property
    def adapters(self) -> tuple[grpc.aio.ClientInterceptor, ...]:
        """The four single-ABC objects through which a channel registers this interceptor.

        Built once per instance and kept: a chain's identity — and with it the channel pool's idea
        of which channels are interchangeable — is the identity of the objects in it.
        """
        return (
            _UnaryUnaryAdapter(self),
            _UnaryStreamAdapter(self),
            _StreamUnaryAdapter(self),
            _StreamStreamAdapter(self),
        )

    async def _run_rpc(self, rpc_type: RpcType, continuation: Continuation, details: Any, request: Any) -> Any:
        """Turn one grpc ``intercept_*`` invocation into an `intercept` call (adapter plumbing)."""
        method = details.method
        call = ClientCall(
            method=method.decode("utf-8", errors="replace") if isinstance(method, bytes) else method,
            rpc_type=rpc_type,
            details=details,
            request=request,
            _continuation=continuation,
        )
        result = await self.intercept(call)

        if call.response_streaming:
            return result
        # grpc.aio wraps a bare response into a Call for unary-unary only; stream-unary passes it
        # to the caller untouched, where a message without ``__await__`` breaks the call and its
        # finalizer. Handing back the Call is correct for both, and it keeps the status and the
        # trailing metadata that a bare response drops.
        return call.underlying_call if call.underlying_call is not None else result


class AsyncAroundClientInterceptor(AsyncClientInterceptor, abc.ABC):
    """A logical interceptor that wraps calls instead of re-issuing them.

    Subclasses implement `around_call` — an async generator that yields exactly once:

    - code before ``yield`` runs before the RPC is created, which is where the call details may
      still be rewritten;
    - the RPC happens at the ``yield``, including full consumption of a streaming response;
    - code after ``yield``, or in ``except`` / ``finally``, runs once the call has finished.

    A failure reaches the ``yield`` as `grpc.aio.AioRpcError`, mid-stream failures included, so an
    ordinary ``try/except`` around it covers the whole call. Refusing a call is a matter of raising
    before the ``yield``: the RPC is then never created. Swallowing an exception is not supported —
    the call has already failed by then, and there is no response to return in its place.
    """

    @abc.abstractmethod
    def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Wrap one RPC (async generator: setup before ``yield``, teardown after)."""

    @functools.cached_property
    def _around(self) -> Callable[[ClientCall], contextlib.AbstractAsyncContextManager[None]]:
        # Built once per instance; asynccontextmanager itself makes a fresh manager per call.
        return contextlib.asynccontextmanager(self.around_call)

    async def intercept(self, call: ClientCall) -> Any:
        """Issue the call inside `around_call` (base plumbing; subclasses override the generator)."""
        around = self._around(call)
        await around.__aenter__()

        if call.response_streaming:
            try:
                stream = await call.invoke_stream()
            except BaseException as error:
                await around.__aexit__(type(error), error, error.__traceback__)
                raise
            # The teardown has to outlive this method: the call is not over until the last item is.
            # It must also not depend on the consumer finishing the iteration — a cancelled or
            # abandoned call fires its done callback, and that closes the teardown deterministically
            # instead of waiting for garbage collection.
            finalizer = _AroundFinalizer(around)
            _finalize_when_done(call.underlying_call, finalizer)
            return _closing_stream(finalizer, stream)

        if call.request_streaming:
            try:
                deferred = await call.invoke_unary()
            except BaseException as error:
                await around.__aexit__(type(error), error, error.__traceback__)
                raise
            # The outcome of a streaming-request call arrives after this task has returned (see
            # `invoke_unary`), so the teardown runs from an observer instead of from here.
            finalizer = _AroundFinalizer(around)
            _spawn_background(_finalize_unary_outcome(call, deferred, finalizer))
            return deferred

        try:
            response = await call.invoke_unary()
        except BaseException as error:
            await around.__aexit__(type(error), error, error.__traceback__)
            raise

        await around.__aexit__(None, None, None)
        return response


class _AroundFinalizer:
    """Closes one around context exactly once, from whichever side finishes first.

    The iterator path and the call's done callback both race to report the outcome; whichever
    arrives second must find the work already done rather than throw a second exception into a
    generator that has already stopped.
    """

    __slots__ = ("_around", "_closed")

    def __init__(self, around: contextlib.AbstractAsyncContextManager[None]) -> None:
        self._around = around
        self._closed = False

    @property
    def closed(self) -> bool:
        """Whether the context has already been closed."""
        return self._closed

    async def close(self, error: BaseException | None) -> None:
        """Close the context with the call's outcome, once."""
        if self._closed:
            return
        self._closed = True
        if error is None:
            await self._around.__aexit__(None, None, None)
        else:
            await self._around.__aexit__(type(error), error, error.__traceback__)


async def _finalize_unary_outcome(call: ClientCall, deferred: Any, finalizer: _AroundFinalizer) -> None:
    """Observe a deferred unary outcome and close the around context with it."""
    try:
        response = await deferred if hasattr(deferred, "__await__") else deferred
    except BaseException as error:
        await finalizer.close(error)
    else:
        call._response = response
        await finalizer.close(None)


def _finalize_when_done(underlying: Any, finalizer: _AroundFinalizer) -> None:
    """Arrange for the around context to close when the call finishes, however it finishes."""
    add_done_callback = getattr(underlying, "add_done_callback", None)
    if add_done_callback is None:
        return

    def _on_done(done_call: Any) -> None:
        if not finalizer.closed:
            _spawn_background(_finalize_stream_outcome(done_call, finalizer))

    add_done_callback(_on_done)


async def _finalize_stream_outcome(done_call: Any, finalizer: _AroundFinalizer) -> None:
    """Close the around context with the status of an already finished streaming call."""
    if finalizer.closed:
        return
    try:
        code = await done_call.code()
    except BaseException as error:
        await finalizer.close(error)
        return

    if code == grpc.StatusCode.OK:
        await finalizer.close(None)
    elif done_call.cancelled():
        await finalizer.close(asyncio.CancelledError())
    else:
        failure = grpc.aio.AioRpcError(
            code,
            await done_call.initial_metadata(),
            await done_call.trailing_metadata(),
            await done_call.details(),
        )
        await finalizer.close(failure)


async def _closing_stream(
    finalizer: _AroundFinalizer,
    stream: AsyncIterator[Any],
) -> AsyncIterator[Any]:
    """Yield a whole response stream, then close the around context with however it ended."""
    try:
        async for item in stream:
            yield item
    except GeneratorExit:
        # The consumer closed the iterator without draining it. From the interceptors' point of
        # view the call did not finish; closing with a cancellation says exactly that, while
        # throwing GeneratorExit into the around generators would only produce "generator didn't
        # stop after athrow" noise. The done callback of the underlying call, when there is one,
        # may report the real outcome first — the finalizer keeps whichever arrived first.
        await finalizer.close(asyncio.CancelledError())
        raise
    except BaseException as error:
        await finalizer.close(error)
        raise

    await finalizer.close(None)


class _Adapter:
    """Half of the answer to grpc's ``if / elif`` registration: one ABC per object.

    Attributes:
        interceptor: The logical interceptor this adapter stands for on the channel.
    """

    def __init__(self, interceptor: AsyncClientInterceptor) -> None:
        """Bind the adapter to the interceptor it delegates to."""
        self.interceptor = interceptor


class _UnaryUnaryAdapter(_Adapter, grpc.aio.UnaryUnaryClientInterceptor):
    """Registers its interceptor in the channel's unary-unary list."""

    async def intercept_unary_unary(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        return await self.interceptor._run_rpc("unary_unary", continuation, client_call_details, request)


class _UnaryStreamAdapter(_Adapter, grpc.aio.UnaryStreamClientInterceptor):
    """Registers its interceptor in the channel's unary-stream list."""

    async def intercept_unary_stream(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        return await self.interceptor._run_rpc("unary_stream", continuation, client_call_details, request)


class _StreamUnaryAdapter(_Adapter, grpc.aio.StreamUnaryClientInterceptor):
    """Registers its interceptor in the channel's stream-unary list."""

    async def intercept_stream_unary(self, continuation: Any, client_call_details: Any, request_iterator: Any) -> Any:
        return await self.interceptor._run_rpc("stream_unary", continuation, client_call_details, request_iterator)


class _StreamStreamAdapter(_Adapter, grpc.aio.StreamStreamClientInterceptor):
    """Registers its interceptor in the channel's stream-stream list."""

    async def intercept_stream_stream(self, continuation: Any, client_call_details: Any, request_iterator: Any) -> Any:
        return await self.interceptor._run_rpc("stream_stream", continuation, client_call_details, request_iterator)


def flatten_interceptors(interceptors: Iterable[ClientInterceptorLike]) -> list[grpc.aio.ClientInterceptor]:
    """Expand every logical interceptor into the four adapters a channel can file correctly.

    Order is preserved, and that is what makes the expansion safe: a channel appends each entry to
    the list of its own kind in the order it reads them, so four adapters of A ahead of four
    adapters of B put A ahead of B in all four lists at once.

    Interceptors written against grpc's ``intercept_*`` methods directly are passed through
    untouched, so a chain may mix both kinds while the kit is being migrated.

    Args:
        interceptors: The chain, outermost first.

    Returns:
        A flat list a channel accepts, outermost first.
    """
    flat: list[grpc.aio.ClientInterceptor] = []
    for interceptor in interceptors:
        if isinstance(interceptor, AsyncClientInterceptor):
            flat.extend(interceptor.adapters)
        else:
            flat.append(interceptor)
    return flat


def logical_interceptor(interceptor: ClientInterceptorLike) -> ClientInterceptorLike:
    """Return the logical interceptor behind a channel adapter, or the argument unchanged.

    Lets callers question a flattened chain (``isinstance`` checks in tests, a handle onto the
    circuit breaker) without knowing whether an entry is an adapter or an old-style interceptor.
    """
    owner = getattr(interceptor, "interceptor", None)
    return owner if isinstance(owner, AsyncClientInterceptor) else interceptor


__all__ = [
    "AsyncAroundClientInterceptor",
    "AsyncClientInterceptor",
    "ClientCall",
    "ClientInterceptorLike",
    "Continuation",
    "RpcType",
    "flatten_interceptors",
    "logical_interceptor",
]
