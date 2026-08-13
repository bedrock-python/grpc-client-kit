from __future__ import annotations

import base64
from typing import Any

import grpc
import grpc.aio

# Type for gRPC metadata: list of tuples or the Metadata object
type MetadataType = list[tuple[str | bytes, str | bytes]] | grpc.aio.Metadata


def metadata_to_dict(metadata: MetadataType | None) -> dict[str, str]:
    """Convert gRPC metadata to a flat dictionary.

    Handles various metadata formats (list of tuples, grpc.aio.Metadata object).
    Performs the following transformations:
    - Decodes bytes keys and values to UTF-8 strings.
    - If a value is binary and not valid UTF-8, it is base64 encoded with a 'base64:' prefix.
    - Joins multiple values for the same key with commas (RFC 2616 style).

    Args:
        metadata: The gRPC metadata to convert.

    Returns:
        A dictionary mapping metadata keys to their string values.
    """
    if not metadata:
        return {}

    # Convert to list if it's a Metadata object or other iterable
    metadata_list: list[tuple[str | bytes, str | bytes]] = (
        metadata if isinstance(metadata, list) else list(metadata)  # type: ignore[arg-type, assignment]
    )

    result: dict[str, str] = {}
    for key, value in metadata_list:
        str_key = key if isinstance(key, str) else key.decode("utf-8")

        if isinstance(value, bytes):
            try:
                str_value = value.decode("utf-8")
            except UnicodeDecodeError:
                # Fallback for truly binary data if it's not UTF-8
                str_value = f"base64:{base64.b64encode(value).decode('ascii')}"
        else:
            str_value = str(value)

        # Handle duplicate keys by joining values with commas (similar to HTTP headers)
        if str_key in result:
            result[str_key] = f"{result[str_key]},{str_value}"
        else:
            result[str_key] = str_value

    return result


def create_aio_channel(
    target: str,
    insecure: bool = False,
    credentials: grpc.ChannelCredentials | None = None,
    options: list[tuple[str, Any]] | None = None,
    compression: grpc.Compression | None = None,
    interceptors: list[grpc.aio.ClientInterceptor] | None = None,
) -> grpc.aio.Channel:
    """Create a new async gRPC channel with standard configuration.

    Args:
        target: The target address (host:port).
        insecure: Whether to use an insecure channel.
        credentials: Optional TLS credentials for a secure channel.
        options: Optional gRPC channel options.
        compression: Optional gRPC compression setting.
        interceptors: Optional list of interceptors to apply.

    Returns:
        An async gRPC channel.
    """
    if insecure:
        return grpc.aio.insecure_channel(
            target,
            options=options,
            compression=compression,
            interceptors=interceptors,
        )

    if not credentials:
        credentials = grpc.ssl_channel_credentials()

    return grpc.aio.secure_channel(
        target,
        credentials,
        options=options,
        compression=compression,
        interceptors=interceptors,
    )


__all__ = ["create_aio_channel", "metadata_to_dict"]
