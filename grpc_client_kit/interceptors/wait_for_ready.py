"""Wait-for-ready interceptor: lets a call wait for its connection instead of failing on a cold channel.

A `grpc.aio` channel connects lazily, and a call made before the connection is up fails immediately
with ``UNAVAILABLE``. That is the burst of errors every pod produces for the first second or two of
its life, and every client produces again after a backend restart — failures that describe the
channel's age rather than the service's health. gRPC's answer is the ``wait_for_ready`` flag on the
call details, and this layer is where it gets configured, globally or per method, the way deadlines
are.
"""

from __future__ import annotations

import logging
from typing import Any

from .base import AsyncClientInterceptor, ClientCall

logger = logging.getLogger(__name__)


class AsyncWaitForReadyInterceptor(AsyncClientInterceptor):
    """Marks outgoing calls as willing to wait for the channel to be ready.

    Behavior:
    - A call whose method resolves to ``True`` is issued with ``wait_for_ready=True``: instead of
      failing while the channel is still connecting, it waits and runs as soon as it is up.
    - A call whose method resolves to ``False`` is issued fail-fast, explicitly.
    - ``None`` — globally or for one method — leaves the call exactly as it arrived.
    - A call whose caller already set ``wait_for_ready`` keeps that value. An explicit decision at
      the call site is more specific than a configured default, and it is the only way to opt one
      call out of a policy set for the whole client.

    Default:
        ``True``, and the pairing with a deadline is what makes that defensible. On its own the flag
        trades one failure mode for another: the call no longer fails fast, it waits — and a wait
        for a backend that never comes back is a hang, which is worse than the error it replaced.
        Bounded by a deadline the trade is one-sided: the call either connects and runs, or ends in
        ``DEADLINE_EXCEEDED`` after exactly the time it was allowed, which is what the caller asked
        for either way. So waiting is enabled only for calls that carry a deadline
        (see `require_deadline`), and a chain built by this kit carries one by default.

    Note:
        The flag is written onto the call details before the RPC exists, so this layer implements
        `base.AsyncClientInterceptor.intercept` and hands the call straight on instead of using the
        ``around_call`` seam — for the reasons `timeout.AsyncTimeoutInterceptor` documents: a
        wrapper around a response stream would cost a generator hop per item and turns a caller
        abandoning that stream into noise at loop shutdown.
    """

    def __init__(
        self,
        default: bool | None = True,
        per_method: dict[str, bool | None] | None = None,
        require_deadline: bool = True,
    ) -> None:
        """Initialize the wait-for-ready interceptor.

        Args:
            default: Value for methods without an entry in `per_method`. ``None`` leaves calls
                untouched, which switches the layer off without removing it from the chain.
            per_method: Values for individual methods, keyed by full method name
                (``/package.Service/Method``). An entry of ``None`` exempts that method from the
                default.
            require_deadline: Whether waiting is limited to calls that carry a deadline. Leave it on
                unless something else bounds the call, because an unbounded wait never ends by
                itself: the call sits there for as long as the backend stays unreachable.
        """
        self._default = default
        self._per_method: dict[str, bool | None] = dict(per_method or {})
        self._require_deadline = require_deadline
        self._warned_about_deadline = False

    def _warn_once(self, method: str) -> None:
        """Report the first call that was left fail-fast for want of a deadline.

        Once per interceptor, not once per call: the condition is a property of the configuration,
        so repeating it for every RPC would drown the log without adding anything.

        Args:
            method: The method whose call was left alone.
        """
        if self._warned_about_deadline:
            return

        self._warned_about_deadline = True
        logger.warning(
            "wait_for_ready is enabled but %s carries no deadline: the call is left fail-fast, "
            "because waiting for a channel with nothing to bound the wait never ends. "
            "Configure a timeout for it, or set require_deadline=False to wait anyway.",
            method,
        )

    def _with_wait_for_ready(self, client_call_details: Any, method: str) -> Any:
        """Return call details carrying this method's wait-for-ready setting.

        Args:
            client_call_details: Details of the call being intercepted.
            method: Full method name, already decoded.

        Returns:
            The details to issue the call with; the argument itself when nothing applies.
        """
        # Looking the method up with a default keeps an explicit ``None`` override meaningful:
        # "method present with no value" must exempt it, not fall back to the default.
        configured = self._per_method.get(method, self._default)

        if configured is None or getattr(client_call_details, "wait_for_ready", None) is not None:
            return client_call_details

        if configured and self._require_deadline and not self._has_deadline(client_call_details):
            self._warn_once(method)
            return client_call_details

        return client_call_details._replace(wait_for_ready=configured)

    def _has_deadline(self, client_call_details: Any) -> bool:
        """Whether the call is bounded, which is what makes waiting for a connection safe."""
        return isinstance(getattr(client_call_details, "timeout", None), (int, float))

    async def intercept(self, call: ClientCall) -> Any:
        """Apply this method's wait-for-ready setting and issue the call.

        Args:
            call: The call to issue; its details are rewritten before the RPC exists.

        Returns:
            The response of a unary call, or the iterator of a streaming one, untouched.
        """
        call.details = self._with_wait_for_ready(call.details, call.method)

        if call.response_streaming:
            return await call.invoke_stream()

        return await call.invoke_unary()


__all__ = [
    "AsyncWaitForReadyInterceptor",
]
