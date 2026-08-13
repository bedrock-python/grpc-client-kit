"""The in-memory metrics registry the client interceptors and the channel pool report into.

It implements the same protocol the shipped Prometheus registry does, so what the tests read back is
what a production collector would have been handed.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class RequestRecord:
    """One completed RPC, as the metrics interceptor reported it."""

    service: str
    method: str
    rpc_type: str
    status: str
    grpc_code: str
    duration: float


@dataclass(slots=True)
class PoolStats:
    """One snapshot of the channel pool, as it reported itself."""

    active_channels: int
    idle_targets: int


@dataclass(slots=True)
class RecordingMetrics:
    """In-memory GrpcClientMetricsProtocol implementation for assertions."""

    requests: list[RequestRecord] = field(default_factory=list)
    inflight: dict[tuple[str, str, str], int] = field(default_factory=dict)
    pool_stats: list[PoolStats] = field(default_factory=list)

    def record_request(
        self,
        service: str,
        method: str,
        rpc_type: str,
        status: str,
        grpc_code: str,
        duration: float,
    ) -> None:
        self.requests.append(
            RequestRecord(
                service=service,
                method=method,
                rpc_type=rpc_type,
                status=status,
                grpc_code=grpc_code,
                duration=duration,
            )
        )

    def record_inflight_delta(self, service: str, method: str, rpc_type: str, delta: int) -> None:
        key = (service, method, rpc_type)
        self.inflight[key] = self.inflight.get(key, 0) + delta

    def record_pool_stats(self, active_channels: int, idle_targets: int) -> None:
        self.pool_stats.append(PoolStats(active_channels=active_channels, idle_targets=idle_targets))

    def total_inflight(self) -> int:
        """Sum of every in-flight gauge, which has to be zero once nothing is running."""
        return sum(self.inflight.values())

    def only_request(self, method: str) -> RequestRecord:
        """Return the single record of `method`, asserting there is exactly one."""
        matching = [record for record in self.requests if record.method == method]
        assert len(matching) == 1, f"expected exactly one metric record for {method}, got {matching}"
        return matching[0]
