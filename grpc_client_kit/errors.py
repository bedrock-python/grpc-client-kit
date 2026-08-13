"""The common ancestor of every failure the kit raises on its own authority.

A caller talking to a gRPC service sees two very different families of failure: the server said no
(an ordinary `grpc.aio.AioRpcError` carrying the server's status), and the *kit* said no before or
instead of the network — an open circuit breaker, an exhausted deadline budget, a balancer with no
healthy target left, a health checker that was never started. Telling them apart matters: the
first is an answer, the second is local state, and retrying or alerting on them differs.
``except GrpcClientKitError`` is the one handler that catches exactly the second family.

Kit failures that stand in for an RPC outcome — the breaker's rejection, the exhausted budget —
additionally derive from `grpc.aio.AioRpcError`, so an existing ``except AioRpcError`` (and every
logging, metrics and tracing layer) keeps seeing them as the call failures they are.
"""

from __future__ import annotations


class GrpcClientKitError(Exception):
    """Base class of every failure raised by the kit itself rather than by a server."""


class HealthCheckerNotRunningError(GrpcClientKitError, RuntimeError):
    """Raised when health state is requested but no check loop can ever produce it.

    Lives here rather than in `health` so that catching it does not require the ``[health]``
    extra: the caller of a balancer meets this error, and a balancer works on a bare install.
    Also a `RuntimeError` — being asked before ``start()`` is a lifecycle mistake in the calling
    code, and existing ``except RuntimeError`` handlers keep working.

    Attributes:
        target: The target whose health was requested.
    """

    def __init__(self, target: str) -> None:
        """Initialize the error.

        Args:
            target: The target whose health was requested.
        """
        self.target = target
        super().__init__(
            f"HealthChecker is not running, so the health of '{target}' is unknown. "
            "Call start() before routing traffic (a factory that owns the checker must be "
            "entered with 'async with')."
        )


__all__ = [
    "GrpcClientKitError",
    "HealthCheckerNotRunningError",
]
