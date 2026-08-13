"""Real request budgets for the deadline tests.

`deadline-budget` is an optional extra, so the import sits inside the factory rather than at module
level: this package has to stay importable on an install that never had the extra, while the tests
that need a budget skip themselves on `HAS_DEADLINE_BUDGET`. What the factory hands back is the
library's own `BudgetContext` — the tests measure the mechanism against the real thing, not against
a stand-in that would agree with the kit by construction.
"""

from __future__ import annotations

from grpc_client_kit.deadline import DeadlineBudgetProtocol


def request_budget(total_seconds: float, call_caps: dict[str, float] | None = None) -> DeadlineBudgetProtocol:
    """Start a real deadline budget for one request.

    Args:
        total_seconds: Total time the request may spend, from now.
        call_caps: Per-call ceilings, keyed by full method name (``/package.Service/Method``).

    Returns:
        The budget, already running: its clock starts here.
    """
    from deadline_budget import BudgetContext  # noqa: PLC0415

    return BudgetContext.create(total_seconds=total_seconds, call_caps=call_caps)
