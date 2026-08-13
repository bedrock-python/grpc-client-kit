"""Validation of gRPC target strings.

The kit validates targets before a channel is created, because a malformed target
is otherwise reported only as an opaque ``UNAVAILABLE`` at the first RPC, long after
the misconfiguration was introduced.

Validation is deliberately stricter than gRPC itself in one respect: a port is always
required. gRPC silently falls back to port 443 for a portless target, which for a
plaintext service target turns a typo into a connection to the wrong port.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Callable

MIN_PORT = 1
MAX_PORT = 65535
MAX_HOSTNAME_LENGTH = 253

# Underscores are not valid DNS host names per RFC 1123, but they are common in
# container/compose service names that gRPC resolves through the system resolver.
_LABEL_RE = re.compile(r"^[A-Za-z0-9_]([A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?$")
_SCHEME_RE = re.compile(r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*):(?P<rest>.*)$", re.DOTALL)


def validate_target(target: str) -> None:
    """Validate a gRPC target address.

    Accepted forms:
        - ``host:port`` (host name or IPv4 literal), e.g. ``api.example.com:443``.
        - ``[ipv6]:port`` with the address in brackets, e.g. ``[::1]:50051``.
        - Resolver URIs: ``dns:``, ``ipv4:``, ``ipv6:``, ``unix:``, ``unix-abstract:``,
          ``vsock:``, ``xds:``, ``google-c2p:`` — with or without the ``//authority/`` part.

    Args:
        target: The target address to validate.

    Raises:
        ValueError: If the target is empty, uses an unsupported resolver scheme, or is
            not a well-formed endpoint for its scheme.
    """
    if not target:
        raise ValueError("Target address cannot be empty")

    if any(char.isspace() for char in target):
        raise ValueError(f"Invalid target format: '{target}'. Target addresses cannot contain whitespace.")

    scheme, rest = _split_scheme(target)
    if scheme is None:
        _validate_endpoint(target, target)
        return

    _SCHEME_VALIDATORS[scheme](rest, target)


def _split_scheme(target: str) -> tuple[str | None, str]:
    """Split a known resolver scheme off the target.

    Returns:
        The lower-cased scheme and the part after it, or ``(None, target)`` when the
        target is a plain endpoint.

    Raises:
        ValueError: If the target carries an explicit ``scheme://`` that no gRPC
            resolver understands (typically ``http://`` copied from a REST config).
    """
    match = _SCHEME_RE.match(target)
    if not match:
        return None, target

    scheme = match.group("scheme").lower()
    if scheme in _SCHEME_VALIDATORS:
        return scheme, match.group("rest")

    # `host:port` also matches the scheme grammar, so an unknown scheme is only an error
    # when the authority marker makes the URI intent explicit; otherwise it is a host name.
    if "://" in target:
        supported = ", ".join(sorted(_SCHEME_VALIDATORS))
        raise ValueError(
            f"Unsupported resolver scheme '{scheme}' in target '{target}'. Supported schemes: {supported}."
        )

    return None, target


def _strip_authority(rest: str) -> str:
    """Return the endpoint part of a ``//authority/endpoint`` remainder."""
    if not rest.startswith("//"):
        return rest

    _, separator, endpoint = rest[2:].partition("/")
    return endpoint if separator else ""


def _validate_dns_uri(rest: str, target: str) -> None:
    """Validate ``dns:endpoint``, ``dns:///endpoint`` or ``dns://authority/endpoint``."""
    endpoint = _strip_authority(rest)
    if not endpoint:
        raise ValueError(f"Invalid target format: '{target}'. Expected 'dns:host:port' or 'dns:///host:port'.")

    _validate_endpoint(endpoint, target)


def _validate_ipv4_uri(rest: str, target: str) -> None:
    """Validate ``ipv4:address:port[,address:port...]``."""
    for endpoint in _split_addresses(rest, target):
        host, port = _split_host_port(endpoint, target)
        try:
            ipaddress.IPv4Address(host)
        except ValueError as e:
            raise ValueError(f"Invalid IPv4 address '{host}' in target '{target}'.") from e
        _validate_port(port, target)


def _validate_ipv6_uri(rest: str, target: str) -> None:
    """Validate ``ipv6:[address]:port[,[address]:port...]``."""
    for endpoint in _split_addresses(rest, target):
        if not endpoint.startswith("["):
            raise ValueError(
                f"Invalid target format: '{target}'. IPv6 addresses must be bracketed, e.g. 'ipv6:[::1]:50051'."
            )
        _validate_bracketed_endpoint(endpoint, target)


def _validate_unix_uri(rest: str, target: str) -> None:
    """Validate ``unix:path`` and ``unix://absolute_path``."""
    path = rest[2:] if rest.startswith("//") else rest
    if not path.strip("/"):
        raise ValueError(f"Invalid target format: '{target}'. Expected a socket path, e.g. 'unix:/var/run/grpc.sock'.")


def _validate_opaque_uri(rest: str, target: str) -> None:
    """Validate schemes whose endpoint is an opaque name (``unix-abstract``, ``xds``, ...)."""
    endpoint = _strip_authority(rest) if rest.startswith("//") else rest
    if not endpoint:
        raise ValueError(f"Invalid target format: '{target}'. The name after the scheme cannot be empty.")


_SCHEME_VALIDATORS: dict[str, Callable[[str, str], None]] = {
    "dns": _validate_dns_uri,
    "google-c2p": _validate_opaque_uri,
    "ipv4": _validate_ipv4_uri,
    "ipv6": _validate_ipv6_uri,
    "unix": _validate_unix_uri,
    "unix-abstract": _validate_opaque_uri,
    "vsock": _validate_opaque_uri,
    "xds": _validate_opaque_uri,
}


def _split_addresses(rest: str, target: str) -> list[str]:
    """Split the comma-separated address list accepted by the ``ipv4``/``ipv6`` schemes."""
    endpoints = [endpoint for endpoint in _strip_authority(rest).split(",") if endpoint]
    if not endpoints:
        raise ValueError(f"Invalid target format: '{target}'. Expected at least one 'address:port' pair.")

    return endpoints


def _validate_endpoint(endpoint: str, target: str) -> None:
    """Validate a single ``host:port`` endpoint, where the host may be a bracketed IPv6 literal."""
    if endpoint.startswith("["):
        _validate_bracketed_endpoint(endpoint, target)
        return

    host, port = _split_host_port(endpoint, target)
    if ":" in host:
        raise ValueError(f"Invalid target format: '{target}'. IPv6 addresses must be bracketed, e.g. '[::1]:50051'.")
    if not host:
        raise ValueError(f"Invalid target format: '{target}'. Host cannot be empty.")

    _validate_host(host, target)
    _validate_port(port, target)


def _validate_bracketed_endpoint(endpoint: str, target: str) -> None:
    """Validate a ``[ipv6]:port`` endpoint."""
    address, closed, tail = endpoint[1:].partition("]")
    if not closed:
        raise ValueError(f"Invalid target format: '{target}'. Unclosed '[' in IPv6 address.")

    try:
        ipaddress.IPv6Address(address)
    except ValueError as e:
        raise ValueError(f"Invalid IPv6 address '{address}' in target '{target}'.") from e

    if not tail.startswith(":"):
        raise ValueError(f"Invalid target format: '{target}'. Expected '[ipv6]:port', e.g. '[::1]:50051'.")

    _validate_port(tail[1:], target)


def _split_host_port(endpoint: str, target: str) -> tuple[str, str]:
    """Split an endpoint on its last colon."""
    host, separator, port = endpoint.rpartition(":")
    if not separator:
        raise ValueError(f"Invalid target format: '{target}'. Expected 'host:port' or a gRPC resolver URI.")

    return host, port


def _validate_host(host: str, target: str) -> None:
    """Validate an IPv4 literal or a DNS host name."""
    try:
        ipaddress.IPv4Address(host)
    except ValueError:
        pass
    else:
        return

    if len(host) > MAX_HOSTNAME_LENGTH:
        raise ValueError(f"Invalid host '{host}' in target '{target}'. Host names are limited to 253 characters.")

    # A trailing dot marks a fully-qualified name and carries no label of its own.
    labels = host.removesuffix(".").split(".")
    if not all(_LABEL_RE.match(label) for label in labels):
        raise ValueError(f"Invalid host '{host}' in target '{target}'. Expected an IP address or a DNS host name.")


def _validate_port(port: str, target: str) -> None:
    """Validate the numeric port of an endpoint."""
    if not (port.isascii() and port.isdigit()):
        raise ValueError(f"Invalid port '{port}' in target '{target}'. Expected a decimal port number.")

    port_num = int(port)
    if not (MIN_PORT <= port_num <= MAX_PORT):
        raise ValueError(f"Invalid port number: {port_num}. Must be between {MIN_PORT} and {MAX_PORT}.")


__all__ = ["MAX_PORT", "MIN_PORT", "validate_target"]
