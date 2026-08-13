"""Passive outlier detection: a failed call marks its target down without waiting for a probe.

An active health checker learns about a dead backend one probe interval late — with the default
interval that is up to thirty seconds during which the balancer keeps routing a full share of
traffic into an address nothing answers at, each call burning its entire deadline. The call that
just failed is fresher evidence than any probe will ever be; this layer forwards that evidence to
the balancer, which quarantines the target immediately.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import grpc.aio

from .base import AsyncAroundClientInterceptor, ClientCall
from .circuit_breaker import CircuitBreakerOpenError

if TYPE_CHECKING:
    from ..balancers import LoadBalancer

logger = logging.getLogger(__name__)

# Codes that condemn the *address* rather than the request. UNAVAILABLE is the transport saying
# so outright. DEADLINE_EXCEEDED is ambiguous — a slow-but-alive server produces it too — but a
# freshly dead backend often burns whole deadlines before the transport settles on UNAVAILABLE,
# and that is precisely the failure mode passive marking exists to stop. The trade is deliberate:
# a healthy-but-slow target sits out one short quarantine, a dead one stops eating deadlines
# immediately. Envoy's outlier detection makes the same call. Application-level codes never
# quarantine: they say nothing about the address.
_OUTLIER_CODES = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})

DEFAULT_QUARANTINE_SECONDS = 5.0


class AsyncPassiveOutlierInterceptor(AsyncAroundClientInterceptor):
    """Reports transport-level failures of one target to the balancer that routed to it.

    Innermost by design — below the retry layer — so it sees every individual wire attempt: a
    retry that fails over to another target still quarantines the one that failed.
    """

    def __init__(
        self,
        balancer: LoadBalancer,
        target: str,
        quarantine: float = DEFAULT_QUARANTINE_SECONDS,
    ) -> None:
        """Bind the reporter to the target its chain dials.

        Args:
            balancer: The balancer routing this client's calls.
            target: The address this chain (and hence this reporter) is bound to.
            quarantine: Seconds the balancer keeps the target out of the rotation per report.
        """
        self._balancer = balancer
        self._target = target
        self._quarantine = quarantine

    async def around_call(self, call: ClientCall) -> AsyncIterator[None]:
        """Pass the call through, reporting a transport-level failure to the balancer."""
        try:
            yield
        except CircuitBreakerOpenError:
            # A local rejection never dialed the target; it is no evidence about the address.
            raise
        except grpc.aio.AioRpcError as error:
            if error.code() in _OUTLIER_CODES:
                self._balancer.report_failure(self._target, self._quarantine)
            raise


__all__ = [
    "DEFAULT_QUARANTINE_SECONDS",
    "AsyncPassiveOutlierInterceptor",
]
