"""Unit tests for the context interceptor.

Every test drives the interceptor through the adapters a channel files it under, with a continuation
that behaves like grpc's: it resolves to a `Call` object and never raises. Injection is asserted on
the details that actually reached the wire.
"""

from __future__ import annotations

import pytest

from grpc_client_kit.interceptors.context import AsyncClientContextInterceptor
from tests.helpers import (
    RPC_KINDS,
    STREAMING_RESPONSE,
    FakeStreamCall,
    FakeUnaryCall,
    Wire,
    collect,
    make_call_details,
)

from .conftest import failing_metadata_provider, fresh_token_provider, run_call, start_call

pytestmark = pytest.mark.unit


async def test__context_interceptor__provider_with_metadata__injects_it() -> None:
    """The whole purpose of the layer: what the provider returns travels with the call."""
    # Arrange
    interceptor = AsyncClientContextInterceptor(lambda: {"k": "v"})
    wire = Wire()

    # Act
    response = await run_call(interceptor, wire)

    # Assert
    assert response == "response"
    assert wire.metadata["k"] == "v"


async def test__context_interceptor__caller_set_metadata__is_kept_alongside_the_injected_one() -> None:
    """Enrichment adds to what the caller sent; it never replaces it."""
    # Arrange
    interceptor = AsyncClientContextInterceptor(lambda: {"k": "v"})
    wire = Wire()

    # Act
    await run_call(interceptor, wire, details=make_call_details(metadata=[("x-request-id", "req-1")]))

    # Assert
    assert wire.metadata == {"x-request-id": "req-1", "k": "v"}


async def test__context_interceptor__binary_metadata_value__travels_as_bytes() -> None:
    """A ``-bin`` key carries bytes, which must reach the wire unencoded."""
    # Arrange
    interceptor = AsyncClientContextInterceptor(lambda: {"key-bin": b"\xff\xfe"})
    wire = Wire()

    # Act
    await run_call(interceptor, wire)

    # Assert
    assert wire.metadata["key-bin"] == b"\xff\xfe"


async def test__context_interceptor__mixed_case_key__is_normalized_to_lowercase() -> None:
    """gRPC only carries lowercase ASCII keys."""
    # Arrange
    interceptor = AsyncClientContextInterceptor(lambda: {"X-Tenant": "acme"})
    wire = Wire()

    # Act
    await run_call(interceptor, wire)

    # Assert
    assert wire.metadata == {"x-tenant": "acme"}


async def test__context_interceptor__provider_returning_nothing__leaves_the_details_untouched() -> None:
    """Nothing to inject: the details reach the wire as they came, without a rebuilt metadata list."""
    # Arrange
    interceptor = AsyncClientContextInterceptor(lambda: {})
    wire = Wire()
    details = make_call_details()

    # Act
    await run_call(interceptor, wire, details=details)

    # Assert
    assert wire.details is details


async def test__context_interceptor__async_provider__is_awaited() -> None:
    """Providers that have to await something (a token refresh) are supported."""
    # Arrange
    wire = Wire()

    # Act
    await run_call(AsyncClientContextInterceptor(fresh_token_provider), wire)

    # Assert
    assert wire.metadata["authorization"] == "bearer fresh"


async def test__context_interceptor__failing_provider__lets_the_call_through(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Enrichment must never decide the fate of the call it was supposed to describe."""
    # Arrange
    wire = Wire()

    # Act
    response = await run_call(AsyncClientContextInterceptor(failing_metadata_provider), wire)

    # Assert
    assert response == "response"
    assert "Metadata provider failed" in caplog.text


@pytest.mark.parametrize("rpc_type", list(RPC_KINDS))
async def test__context_interceptor__every_rpc_kind__injects_the_metadata(rpc_type: str) -> None:
    """Streaming calls used to be left out entirely: a channel files an interceptor by class."""
    # Arrange
    interceptor = AsyncClientContextInterceptor(lambda: {"k": "v"})
    streaming = rpc_type in STREAMING_RESPONSE
    wire = Wire(FakeStreamCall("item") if streaming else FakeUnaryCall())

    # Act
    result = await start_call(interceptor, wire, rpc_type)
    if streaming:
        assert await collect(result) == ["item"]

    # Assert
    assert wire.metadata["k"] == "v"


async def test__context_interceptor__metadata_grpc_would_reject__fails_before_the_call_is_issued() -> None:
    """Keys and values are validated where the mistake is, not inside the C core mid-flight."""
    # Arrange
    wire = Wire()

    # Act & Assert
    with pytest.raises(ValueError, match="Metadata key must be ASCII"):
        await run_call(AsyncClientContextInterceptor(lambda: {"Ключ": "value"}), wire)

    with pytest.raises(ValueError, match="Non-binary metadata value must be ASCII"):
        await run_call(AsyncClientContextInterceptor(lambda: {"key": "значение"}), wire)

    assert wire.calls == [], "metadata gRPC would reject must never reach the wire"

    # A binary key is exempt: it carries whatever bytes the value encodes to.
    await run_call(AsyncClientContextInterceptor(lambda: {"key-bin": "значение"}), wire)
    assert wire.metadata["key-bin"] == "значение".encode()
