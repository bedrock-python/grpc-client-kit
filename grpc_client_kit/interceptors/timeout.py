"""Timeout interceptor: installs the deadline that bounds a whole call, retries included."""

from __future__ import annotations

import logging
import time
from typing import Any

from .base import AsyncClientInterceptor, ClientCall

logger = logging.getLogger(__name__)


def _normalize_timeout(value: float | None, name: str) -> float | None:
    """Normalize a configured timeout into "positive seconds" or "disabled".

    Both ``None`` and ``0`` spell "no timeout". Accepting ``0`` keeps this interceptor usable with
    settings models that constrain the field with ``ge=0``; forwarding a literal ``0`` to gRPC would
    instead fail every call with ``DEADLINE_EXCEEDED`` before it leaves the process.

    Args:
        value: Configured timeout in seconds, or None.
        name: Name of the setting, used in the error message.

    Returns:
        A positive timeout in seconds, or None when the timeout is disabled.

    Raises:
        ValueError: If the timeout is negative.
    """
    if value is None:
        return None

    numeric = float(value)
    if numeric < 0:
        raise ValueError(f"{name} must be non-negative (0 or None disables the timeout)")

    return numeric or None


class AsyncTimeoutInterceptor(AsyncClientInterceptor):
    """Async timeout interceptor that sets the budget for a whole gRPC call.

    The timeout set here is the budget for the **entire logical call**, not for a single network
    attempt. As the outermost resilience interceptor it runs once per call, so everything nested
    below it — retries in particular — has to fit into the budget it installs. `AsyncRetryInterceptor`
    honours this by converting the timeout into a deadline and shrinking it before every attempt.

    Behavior:
    - If a call already carries a timeout, the smaller of the two wins: an explicit per-call
      deadline set by the caller can only tighten the configured one, never loosen it.
    - If no timeout is present, the configured one is applied.
    - A timeout of ``None`` or ``0`` means "no deadline"; calls of that method are left untouched.
    - The budget applies to all four RPC kinds. It used to reach unary-unary calls only: a channel
      files an interceptor by class, and an interceptor deriving from all four gRPC base classes
      lands in the first list alone (see `base`), which left every stream deadline-free.

    Note:
        The budget is written onto the call details before the RPC exists, so this layer implements
        `base.AsyncClientInterceptor.intercept` and hands the call straight on instead of using the
        ``around_call`` seam. Wrapping a response stream to observe an outcome it has no use for
        would cost a generator hop per item, and that wrapper is not free in another way either:
        when a caller abandons a stream, closing it drives the ``GeneratorExit`` back into the
        wrapper, which the event loop reports as an error if it has closed the generator first
        (measured at loop shutdown as ``RuntimeError: generator didn't stop after athrow()``).
    """

    def __init__(
        self,
        default_timeout: float | None = 10.0,
        per_method_timeouts: dict[str, float | None] | None = None,
    ) -> None:
        """Initialize the timeout interceptor.

        Args:
            default_timeout: Total call budget in seconds for methods without a specific value.
                             ``None`` or ``0`` disables the default timeout.
            per_method_timeouts: Mapping of full method names to specific budgets. ``None`` or ``0``
                                 disables the timeout for that method, overriding `default_timeout`.

        Raises:
            ValueError: If any configured timeout is negative.
        """
        self._default_timeout = _normalize_timeout(default_timeout, "default_timeout")
        self._per_method_timeouts: dict[str, float | None] = {
            method: _normalize_timeout(timeout, f"timeout for method {method}")
            for method, timeout in (per_method_timeouts or {}).items()
        }

    def _with_budget(self, client_call_details: Any, method: str) -> Any:
        """Return call details carrying this method's call budget.

        Args:
            client_call_details: Details of the call being intercepted.
            method: Full method name, already decoded.

        Returns:
            The details to issue the call with; the argument itself when no budget applies.
        """
        # Looking the method up with a default keeps an explicit ``None`` override meaningful:
        # "method present with no timeout" must disable the budget, not fall back to the default.
        timeout = self._per_method_timeouts.get(method, self._default_timeout)

        if timeout is None:
            return client_call_details

        # Use the smaller of existing timeout and our configured timeout
        if getattr(client_call_details, "timeout", None) is not None:
            return client_call_details._replace(timeout=min(client_call_details.timeout, timeout))

        if getattr(client_call_details, "deadline", None) is not None:
            # Fallback for old/custom ClientCallDetails that might have deadline
            return client_call_details._replace(deadline=min(client_call_details.deadline, time.time() + timeout))

        # Try setting timeout first, then deadline if timeout doesn't exist
        try:
            return client_call_details._replace(timeout=timeout)
        except (AttributeError, TypeError):
            # For compatibility with older sync interceptors or custom ones
            return client_call_details._replace(deadline=time.time() + timeout)

    async def intercept(self, call: ClientCall) -> Any:
        """Give the call its budget and issue it.

        Args:
            call: The call to issue; its details are rewritten before the RPC exists.

        Returns:
            The response of a unary call, or the iterator of a streaming one, untouched.
        """
        call.details = self._with_budget(call.details, call.method)

        if call.response_streaming:
            return await call.invoke_stream()

        return await call.invoke_unary()


__all__ = [
    "AsyncTimeoutInterceptor",
]
