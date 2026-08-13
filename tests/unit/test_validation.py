"""Unit tests for target validation.

A target that gRPC cannot resolve fails at connection time, far from the configuration that spelled
it. These tests pin the shapes that are accepted and the ones that are refused where they are typed.
"""

from __future__ import annotations

import pytest

from grpc_client_kit.validation import validate_target

pytestmark = pytest.mark.unit


def test__validate_target__empty_string__is_refused() -> None:
    """An unset setting reaches this function as an empty string, not as None."""
    # Act & Assert
    with pytest.raises(ValueError, match="Target address cannot be empty"):
        validate_target("")


def test__validate_target__host_without_a_port__is_refused() -> None:
    """gRPC has no default port, so a bare host name can only fail later."""
    # Act & Assert
    with pytest.raises(ValueError, match="Expected 'host:port'"):
        validate_target("localhost")


def test__validate_target__port_out_of_range__is_refused() -> None:
    """Port 0 and anything above 65535 cannot be dialled."""
    # Act & Assert
    with pytest.raises(ValueError, match="Invalid port number"):
        validate_target("localhost:70000")
    with pytest.raises(ValueError, match="Invalid port number"):
        validate_target("localhost:0")
    with pytest.raises(ValueError, match="Invalid port number"):
        validate_target("[::1]:65536")


def test__validate_target__host_and_port__is_accepted() -> None:
    """The everyday shapes, including the Kubernetes and underscore ones, must all pass."""
    # Act & Assert
    validate_target("localhost:50051")
    validate_target("127.0.0.1:8080")
    validate_target("api.example.com:443")
    validate_target("user-service.default.svc.cluster.local:9090")
    validate_target("example.com.:443")
    validate_target("user_service:50051")


def test__validate_target__malformed_host_name__is_refused() -> None:
    """A host name that no resolver would accept is a typo, and is reported as one."""
    # Act & Assert
    with pytest.raises(ValueError, match="Expected an IP address or a DNS host name"):
        validate_target("-bad-host:50051")
    with pytest.raises(ValueError, match="Expected an IP address or a DNS host name"):
        validate_target("bad!host:50051")
    with pytest.raises(ValueError, match="Host names are limited"):
        validate_target(f"{'a' * 300}:50051")


def test__validate_target__whitespace_in_the_target__is_refused() -> None:
    """Whitespace usually means a comma-separated list was pasted into a single-target setting."""
    # Act & Assert
    with pytest.raises(ValueError, match="cannot contain whitespace"):
        validate_target(" localhost:50051")
    with pytest.raises(ValueError, match="cannot contain whitespace"):
        validate_target("localhost:50051, localhost:50052")


def test__validate_target__bracketed_ipv6__is_accepted() -> None:
    """IPv6 literals, zone index included, are valid targets when bracketed."""
    # Act & Assert
    validate_target("[::1]:50051")
    validate_target("[2001:db8::1]:80")
    validate_target("[fe80::1%eth0]:50051")


def test__validate_target__ambiguous_ipv6__is_refused() -> None:
    """Without brackets there is no telling the address apart from the port."""
    # Act & Assert
    with pytest.raises(ValueError, match="must be bracketed"):
        validate_target("::1:50051")
    with pytest.raises(ValueError, match=r"Expected '\[ipv6\]:port'"):
        validate_target("[fe80::1%eth0]")
    with pytest.raises(ValueError, match="Unclosed"):
        validate_target("[::1:50051")
    with pytest.raises(ValueError, match="Invalid IPv6 address"):
        validate_target("[not-an-address]:50051")


def test__validate_target__resolver_uri__is_accepted() -> None:
    """Every scheme a gRPC resolver understands has to survive validation untouched."""
    # Act & Assert
    validate_target("dns:localhost:50051")
    validate_target("dns:///localhost:50051")
    validate_target("dns://8.8.8.8/service:50051")
    validate_target("unix:/var/run/grpc.sock")
    validate_target("unix:///var/run/grpc.sock")
    validate_target("unix:relative.sock")
    validate_target("unix-abstract:my-service")
    validate_target("ipv4:127.0.0.1:50051,127.0.0.2:50052")
    validate_target("ipv6:[::1]:50051,[2001:db8::1]:50052")
    validate_target("xds:///my-service")
    validate_target("xds://xds.authority/my-service")
    validate_target("vsock:2:50051")


def test__validate_target__unsupported_resolver_scheme__is_refused() -> None:
    """A URI no gRPC resolver understands would be read as a host name and fail obscurely."""
    # Act & Assert
    with pytest.raises(ValueError, match="Unsupported resolver scheme 'http'"):
        validate_target("http://localhost:50051")
    with pytest.raises(ValueError, match="Unsupported resolver scheme 'grpc'"):
        validate_target("grpc://localhost:50051")


def test__validate_target__resolver_uri_with_a_malformed_endpoint__is_refused() -> None:
    """The scheme being right does not make the endpoint behind it dialable."""
    # Act & Assert
    with pytest.raises(ValueError, match="Expected 'dns:host:port'"):
        validate_target("dns:")
    with pytest.raises(ValueError, match="Expected 'dns:host:port'"):
        validate_target("dns://8.8.8.8")
    with pytest.raises(ValueError, match="Expected 'host:port'"):
        validate_target("dns:///localhost")
    with pytest.raises(ValueError, match="Expected a socket path"):
        validate_target("unix:")
    with pytest.raises(ValueError, match="Expected a socket path"):
        validate_target("unix://")
    with pytest.raises(ValueError, match="cannot be empty"):
        validate_target("unix-abstract:")
    with pytest.raises(ValueError, match="Invalid IPv4 address"):
        validate_target("ipv4:localhost:50051")
    with pytest.raises(ValueError, match="must be bracketed"):
        validate_target("ipv6:::1:50051")
    with pytest.raises(ValueError, match="Expected at least one"):
        validate_target("ipv4:")


def test__validate_target__non_numeric_port__is_refused() -> None:
    """A service name or a URL path where the port belongs must not pass through silently."""
    # Act & Assert
    with pytest.raises(ValueError, match="Invalid port 'service'"):
        validate_target("localhost:service")
    with pytest.raises(ValueError, match="Invalid port ''"):
        validate_target("localhost:")
    with pytest.raises(ValueError, match="Invalid port"):
        validate_target("localhost:50051/health")
