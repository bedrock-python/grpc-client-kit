"""Context interceptor: puts provider-supplied metadata on every outgoing call."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .base import AsyncClientInterceptor, ClientCall

logger = logging.getLogger(__name__)


class AsyncClientContextInterceptor(AsyncClientInterceptor):
    """Async client interceptor that injects metadata from a provider.

    This interceptor allows for dynamic metadata injection (e.g., authorization
    tokens, request IDs, tenant IDs) into every outgoing gRPC call.

    Features:
    - Support for both synchronous and asynchronous metadata providers.
    - Automatic validation and normalization of gRPC metadata keys.
    - Automatic encoding of binary metadata values.
    - The same injection on all four RPC kinds, streams included. Injection used to reach
      unary-unary calls only: a channel files an interceptor by class, and an interceptor deriving
      from all four gRPC base classes lands in the first list alone (see `base`).

    Note:
        The metadata is written onto the call details before the RPC exists, so this layer
        implements `base.AsyncClientInterceptor.intercept` and hands the call straight on instead of
        using the ``around_call`` seam. Wrapping a response stream to observe an outcome it has no
        use for would cost a generator hop per item, and that wrapper is not free in another way
        either: when a caller abandons a stream, closing it drives the ``GeneratorExit`` back into
        the wrapper, which the event loop reports as an error if it has closed the generator first
        (measured at loop shutdown as ``RuntimeError: generator didn't stop after athrow()``).
    """

    def __init__(
        self, metadata_provider: Callable[[], dict[str, str | bytes] | Awaitable[dict[str, str | bytes]]]
    ) -> None:
        """Initialize the context interceptor.

        Args:
            metadata_provider: A callable that returns a dictionary of metadata.
                               Can be a regular function or an async function.
        """
        self._provider = metadata_provider

    def _validate_metadata_key(self, key: str) -> str:
        """Validate and normalize metadata key.

        gRPC metadata keys must be:
        - ASCII lowercase
        - No uppercase, no unicode
        - Binary keys must end with '-bin'
        """
        key_lower = key.lower()
        try:
            key_lower.encode("ascii")
        except UnicodeEncodeError as e:
            raise ValueError(f"Metadata key must be ASCII: {key}") from e
        return key_lower

    def _encode_metadata_value(self, key: str, value: str | bytes) -> tuple[str, str | bytes]:
        """Encode metadata value based on key suffix.

        Binary metadata (keys ending with '-bin') can contain arbitrary bytes.
        Text metadata must be ASCII-only.
        """
        if key.endswith("-bin"):
            # Binary metadata - encode as bytes
            if isinstance(value, bytes):
                return (key, value)
            return (key, value.encode("utf-8"))
        else:
            # Text metadata - must be ASCII
            str_value = str(value)
            try:
                str_value.encode("ascii")
            except UnicodeEncodeError as e:
                raise ValueError(f"Non-binary metadata value must be ASCII (key={key}): {str_value}") from e
            return (key, str_value)

    async def _context_metadata(self) -> dict[str, str | bytes]:
        """Ask the provider for this call's metadata, tolerating a provider that fails.

        Returns:
            The provider's metadata, or an empty mapping if it raised. Injection is an enrichment:
            a broken provider must not decide the fate of the call it was supposed to describe.
        """
        try:
            provided = self._provider()
            metadata: dict[str, str | bytes] = await provided if inspect.isawaitable(provided) else provided
        except Exception:
            logger.exception("Metadata provider failed")
            return {}
        else:
            return metadata

    async def intercept(self, call: ClientCall) -> Any:
        """Attach the provider's metadata to the call and issue it.

        Args:
            call: The call to issue; its details are rewritten before the RPC exists.

        Returns:
            The response of a unary call, or the iterator of a streaming one, untouched.

        Raises:
            ValueError: If the provider returned a key or a value gRPC cannot carry. Raising before
                anything is issued means no call ever leaves with metadata that would be rejected
                further down.
        """
        context_metadata = await self._context_metadata()

        if context_metadata:
            metadata = list(call.details.metadata or [])
            for key, value in context_metadata.items():
                if value is not None:
                    key_normalized = self._validate_metadata_key(key)
                    encoded_key, encoded_value = self._encode_metadata_value(key_normalized, value)
                    metadata.append((encoded_key, encoded_value))

            call.details = call.details._replace(metadata=metadata)

        if call.response_streaming:
            return await call.invoke_stream()

        return await call.invoke_unary()


__all__ = [
    "AsyncClientContextInterceptor",
]
