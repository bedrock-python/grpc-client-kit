"""Deadline budget interceptor: trims every call to the time the request actually has left.

The layer above installs a *static* deadline — the budget configured for this method, the same on
every call. This one applies the *dynamic* half: how much of the caller's request budget is still
unspent by the time this particular call is made. Five hops into a request that started with five
seconds, the last hop is issued with what those four predecessors left over instead of with its own
fresh ten.

What closes the chain is the caller, not this interceptor: the budget is read from the ambient
context (see `grpc_client_kit.deadline`), and a service that never calls `use_budget` propagates
nothing. Without a budget installed this layer is a pass-through that touches no call details.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

import grpc.aio

from ..deadline import (
    HAS_DEADLINE_BUDGET,
    DeadlineBudgetProtocol,
    DeadlineExceededError,
    current_budget,
)
from ..errors import GrpcClientKitError
from .base import AsyncAroundClientInterceptor, ClientCall

logger = logging.getLogger(__name__)


class DeadlineBudgetExhaustedError(grpc.aio.AioRpcError, GrpcClientKitError):
    """Raised instead of issuing a call the request has no time left for.

    A `grpc.aio.AioRpcError` rather than the budget library's own exception, and carrying
    ``DEADLINE_EXCEEDED``, because that is what the caller of an RPC is written to handle: the
    layers above (logging, tracing, metrics) record it like any other failed call. The original
    `deadline_budget.DeadlineExceededError` is kept as the ``__cause__`` for anyone who maps it.
    """

    def __init__(self, method: str, reason: str) -> None:
        """Build the error naming the call that was refused and why."""
        # Explicit rather than super(): with GrpcClientKitError also in the bases, super() would
        # resolve the keyword arguments against Exception, which takes none of them.
        grpc.aio.AioRpcError.__init__(
            self,
            code=grpc.StatusCode.DEADLINE_EXCEEDED,
            initial_metadata=grpc.aio.Metadata(),
            trailing_metadata=grpc.aio.Metadata(),
            details=f"Deadline budget exhausted before {method}: {reason}",
        )


class AsyncDeadlineBudgetInterceptor(AsyncAroundClientInterceptor):
    """Cuts each call's deadline down to the remaining budget of the request being served.

    Behavior:
    - No budget installed in the current context: the call is issued exactly as it arrived.
    - A budget with time left: the call is issued with the timeout the budget grants it. If the call
      already carries a deadline, the smaller of the two wins — a budget may only tighten a
      deadline, never extend one that a caller or the timeout layer asked for.
    - An exhausted budget: no RPC is created at all. Dialing out with nothing left can only produce
      a `DEADLINE_EXCEEDED` a round trip later, so the call is refused here with
      `DeadlineBudgetExhaustedError` before it touches the network.

    Per-call caps:
        The budget is asked for a timeout under the RPC's own full method name
        (``/package.Service/Method``), which is how `deadline_budget.BudgetContext` keys its
        ``call_caps``: a cap stored under that name applies to that method and nothing else.

    Floor:
        `BudgetContext` refuses to hand out a timeout below its own ``min_timeout``, so a budget
        down to its last milliseconds still yields that floor. The kit does not second-guess it —
        the floor exists to stop calls being issued with a deadline too short to ever answer.

    Position in the chain:
        Outside the retry layer, so the trimmed deadline is the one retry divides between attempts,
        and inside the observability layers, so a refused call is logged, traced and measured like
        any other failure. See the `interceptors` module docstring.

    Note:
        Only the setup half of ``around_call`` is used — the deadline is written before the RPC
        exists and there is nothing to observe afterwards — but the seam is what makes refusing a
        call a matter of raising before the ``yield``, on all four RPC kinds at once.
    """

    def __init__(self, reserve_for_next: float = 0.0) -> None:
        """Initialize the deadline budget interceptor.

        Args:
            reserve_for_next: Seconds held back from every call for the work that follows it — the
                caller's own response handling, or a compensating call after a failure. Passed
                straight to the budget, which subtracts it from what it grants.

        Raises:
            ValueError: If `reserve_for_next` is negative.
        """
        if reserve_for_next < 0:
            raise ValueError("reserve_for_next must be non-negative")

        self._reserve_for_next = reserve_for_next

    def _trimmed(self, client_call_details: Any, budget: DeadlineBudgetProtocol, method: str) -> Any:
        """Return call details bounded by whatever the budget still grants this method.

        Args:
            client_call_details: Details of the call being intercepted.
            budget: The budget of the request in flight.
            method: Full method name, both the cap key and the name in the error.

        Returns:
            The details to issue the call with.

        Raises:
            DeadlineBudgetExhaustedError: If the budget is spent.
        """
        try:
            granted = budget.timeout_for_call(method, reserve_for_next=self._reserve_for_next)
        except DeadlineExceededError as error:
            raise DeadlineBudgetExhaustedError(method, str(error)) from error

        current = getattr(client_call_details, "timeout", None)
        # Anything that is not a real number means "no deadline yet", so the budget becomes one.
        timeout = min(float(current), granted) if isinstance(current, (int, float)) else granted

        return client_call_details._replace(timeout=timeout)

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Give the call the deadline the request can still afford, then let it run.

        Args:
            call: The call being issued; its details are rewritten before the RPC exists.

        Yields:
            Once, with the RPC in flight.

        Raises:
            DeadlineBudgetExhaustedError: If the request budget is spent. Raised before the
                ``yield``, so the RPC is never created.
        """
        budget = current_budget()
        if budget is not None:
            call.details = self._trimmed(call.details, budget, call.method)

        yield


__all__ = [
    "HAS_DEADLINE_BUDGET",
    "AsyncDeadlineBudgetInterceptor",
    "DeadlineBudgetExhaustedError",
]
