"""The deadline budget of the request in flight, and the ambient context that carries it.

`deadline-budget` deliberately has **no implicit context**: a `BudgetContext` is created by whoever
starts the request and handed from one call site to the next by argument. That is the right default
for a budget library and the wrong shape for a client kit — an interceptor sits far below the code
that knows what the budget is, and none of the layers in between would carry it. Nothing propagates
a deadline between services today; this module supplies the one piece that was missing, a contextvar
holding the current budget, plus the two functions that write and read it.

Installing a budget is the caller's job, and the chain only closes when they do it::

    from deadline_budget import BudgetContext
    from grpc_client_kit import use_budget

    with use_budget(BudgetContext.create(total_seconds=5.0)):
        await client.fetch_user(user_id)   # issued with what is left of those 5 seconds

Without that block every call behaves exactly as it did before: the interceptor finds no budget and
touches nothing. The kit provides the mechanism, not the magic — a service that never installs a
budget propagates nothing, however many interceptors it runs.

A `ContextVar` is per-task, which gives the semantics a deadline needs: a task started while a
budget is installed inherits it, so a fan-out shares one deadline, while a budget installed inside
one task is invisible to its siblings and cannot leak out of it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Protocol, runtime_checkable

# HAS_DEADLINE_BUDGET tells whether `deadline-budget` (the `deadline` extra) is importable. When it
# is False the deadline interceptor is left out of the chain and everything here still works: the
# contextvar is typed structurally, so it never needs the library to describe what it holds.
try:
    from deadline_budget import DeadlineExceededError

    HAS_DEADLINE_BUDGET = True
except ImportError:  # pragma: no cover - only reachable without the "deadline" extra
    HAS_DEADLINE_BUDGET = False

    class DeadlineExceededError(Exception):  # type: ignore[no-redef]
        """Stand-in that keeps this module importable, and its ``except`` clauses evaluable."""


@runtime_checkable
class DeadlineBudgetProtocol(Protocol):
    """What the kit needs of a request budget: the shape of `deadline_budget.BudgetContext`.

    Typing against the shape rather than against the class is what keeps the library optional — the
    kit never imports `deadline-budget` to describe a budget — and it leaves the door open for a
    caller who tracks deadlines their own way.
    """

    def timeout_for_call(self, call_name: str, reserve_for_next: float = 0.0) -> float:
        """Return the timeout the named call may use, bounded by what is left of the budget.

        Args:
            call_name: Name the per-call caps are keyed by.
            reserve_for_next: Seconds to keep back for the steps that follow this call.

        Returns:
            The timeout in seconds.

        Raises:
            DeadlineExceededError: If the budget is already exhausted.
        """
        ...

    def remaining(self) -> float:
        """Return the seconds left, which is negative once the deadline has passed."""
        ...

    def expired(self) -> bool:
        """Return whether the budget is exhausted."""
        ...


# The budget of the request being served, or None when nobody installed one. Read on every outgoing
# call by `interceptors.deadline.AsyncDeadlineBudgetInterceptor`.
_CURRENT_BUDGET: ContextVar[DeadlineBudgetProtocol | None] = ContextVar("grpc_client_kit_budget", default=None)


def current_budget() -> DeadlineBudgetProtocol | None:
    """Return the budget installed for the current task, or None if there is none.

    Returns:
        The budget `use_budget` last installed in this context, or None.
    """
    return _CURRENT_BUDGET.get()


@contextmanager
def use_budget(budget: DeadlineBudgetProtocol | None) -> Iterator[DeadlineBudgetProtocol | None]:
    """Install `budget` as the current one for the duration of the block.

    The previous value is restored on the way out, whether the block ends normally or by exception,
    so nesting works and an inner budget cannot outlive its block. Passing None is the way to detach
    an inherited budget — for background work that must not die with the request that spawned it.

    Args:
        budget: The budget to install, or None to run this block without one.

    Yields:
        The budget that was installed, for convenience at the call site.
    """
    token: Token[DeadlineBudgetProtocol | None] = _CURRENT_BUDGET.set(budget)
    try:
        yield budget
    finally:
        _CURRENT_BUDGET.reset(token)


__all__ = [
    "HAS_DEADLINE_BUDGET",
    "DeadlineBudgetProtocol",
    "DeadlineExceededError",
    "current_budget",
    "use_budget",
]
