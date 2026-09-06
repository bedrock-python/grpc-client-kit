"""Batteries-optional async gRPC client toolkit.

Channel pooling with health monitoring, client-side load balancing
(round-robin, random, weighted), resilience (retries, timeouts, circuit
breakers) and observability (logging, OpenTelemetry tracing, metrics) — with
every integration behind an extra.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .__version__ import __version__
from .balancers import (
    LoadBalancer,
    LoadBalancerConfig,
    LoadBalancingStrategy,
    NoHealthyTargetsError,
    RandomLoadBalancer,
    RoundRobinLoadBalancer,
    WeightedLoadBalancer,
    create_balancer,
)
from .channel import ChannelKey, ChannelPool
from .client import GrpcClient
from .config import ConnectivityConfig, GrpcClientConfig
from .deadline import DeadlineBudgetProtocol, current_budget, use_budget
from .errors import GrpcClientKitError, HealthCheckerNotRunningError
from .factory import GrpcClientFactory
from .interceptors import (
    AsyncCircuitBreakerInterceptor,
    AsyncClientContextInterceptor,
    AsyncLoggingInterceptor,
    AsyncRetryInterceptor,
    AsyncTimeoutInterceptor,
    AsyncWaitForReadyInterceptor,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    DeadlineBudgetConfig,
    InterceptorChainBuilder,
    ObservabilityConfig,
    RetryConfig,
    TimeoutConfig,
    WaitForReadyConfig,
    build_interceptors,
)
from .interceptors.base import (
    AsyncAroundClientInterceptor,
    AsyncClientInterceptor,
    ClientCall,
    flatten_interceptors,
    logical_interceptor,
)
from .interceptors.circuit_breaker import CircuitBreakerStatus, CircuitState
from .interceptors.deadline import DeadlineBudgetExhaustedError
from .protocols import (
    ChannelPoolSettingsProtocol,
    ChannelProviderProtocol,
    CircuitBreakerMetricsProtocol,
    CircuitBreakerSettingsProtocol,
    FullGrpcClientSettingsProtocol,
    GrpcChannelExtrasProtocol,
    GrpcClientMetricsProtocol,
    GrpcClientSettingsProtocol,
    GrpcObservabilityExtrasProtocol,
    HealthCheckerProtocol,
    HealthCheckerSettingsProtocol,
    HealthStatusCallbackProtocol,
    LoadBalancerSettingsProtocol,
    RetryMetricsProtocol,
    RetrySettingsProtocol,
    TimeoutSettingsProtocol,
)
from .utils import metadata_to_dict

if TYPE_CHECKING:
    from .health import HealthChecker

logger = logging.getLogger(__name__)

# The public surface, deliberately curated. The extension seam (AsyncAroundClientInterceptor,
# ClientCall, flatten_interceptors) is first-class: it is what custom interceptors are written
# against. Every protocol in `protocols.__all__` is re-exported here, so a settings object can be
# written against the package alone. Internals — the pool's ChannelWrapper and chain_token, the
# circuit breaker's MethodCircuitState — are deliberately NOT here: exporting them would freeze
# those implementations into the compatibility contract.
__all__ = [
    "AsyncAroundClientInterceptor",
    "AsyncCircuitBreakerInterceptor",
    "AsyncClientContextInterceptor",
    "AsyncClientInterceptor",
    "AsyncLoggingInterceptor",
    "AsyncRetryInterceptor",
    "AsyncTimeoutInterceptor",
    "AsyncWaitForReadyInterceptor",
    "ChannelKey",
    "ChannelPool",
    "ChannelPoolSettingsProtocol",
    "ChannelProviderProtocol",
    "CircuitBreakerConfig",
    "CircuitBreakerMetricsProtocol",
    "CircuitBreakerOpenError",
    "CircuitBreakerSettingsProtocol",
    "CircuitBreakerStatus",
    "CircuitState",
    "ClientCall",
    "ConnectivityConfig",
    "DeadlineBudgetConfig",
    "DeadlineBudgetExhaustedError",
    "DeadlineBudgetProtocol",
    "FullGrpcClientSettingsProtocol",
    "GrpcChannelExtrasProtocol",
    "GrpcClient",
    "GrpcClientConfig",
    "GrpcClientFactory",
    "GrpcClientKitError",
    "GrpcClientMetricsProtocol",
    "GrpcClientSettingsProtocol",
    "GrpcObservabilityExtrasProtocol",
    "HealthChecker",
    "HealthCheckerNotRunningError",
    "HealthCheckerProtocol",
    "HealthCheckerSettingsProtocol",
    "HealthStatusCallbackProtocol",
    "InterceptorChainBuilder",
    "LoadBalancer",
    "LoadBalancerConfig",
    "LoadBalancerSettingsProtocol",
    "LoadBalancingStrategy",
    "NoHealthyTargetsError",
    "ObservabilityConfig",
    "RandomLoadBalancer",
    "RetryConfig",
    "RetryMetricsProtocol",
    "RetrySettingsProtocol",
    "RoundRobinLoadBalancer",
    "TimeoutConfig",
    "TimeoutSettingsProtocol",
    "WaitForReadyConfig",
    "WeightedLoadBalancer",
    "__version__",
    "build_interceptors",
    "create_balancer",
    "current_budget",
    "flatten_interceptors",
    "logical_interceptor",
    "metadata_to_dict",
    "use_budget",
]


def __getattr__(name: str) -> Any:
    """Resolve extras-gated exports on first access.

    ``HealthChecker`` needs the [health] extra, so importing this package must not import it: a
    module-level import would make ``import grpc_client_kit`` fail on a bare install.

    A missing extra raises ``ImportError``, not ``AttributeError``, so that a broken install
    says so instead of looking like a name that never existed. The cost is that ``hasattr`` and
    ``getattr`` with a default do not swallow it — they propagate the ImportError — so probe for
    the extra with ``importlib.util.find_spec("grpc_health")`` or catch the ImportError, not
    with ``hasattr(grpc_client_kit, "HealthChecker")``.

    Args:
        name: The attribute being looked up.

    Returns:
        The resolved attribute.

    Raises:
        AttributeError: If the package has no such attribute.
        ImportError: If the attribute needs an extra that is not installed. Deliberately not an
            AttributeError: see above.
    """
    if name == "HealthChecker":
        from .factory import _load_health_checker  # noqa: PLC0415 - lazy: needs the [health] extra

        return _load_health_checker()

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
