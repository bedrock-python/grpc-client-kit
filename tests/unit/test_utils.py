"""Unit tests for the shared helpers: metadata normalization and channel creation."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import grpc
import grpc.aio
import pytest

from grpc_client_kit.utils import create_aio_channel, metadata_to_dict

pytestmark = pytest.mark.unit


def test__metadata_to_dict__string_pairs__returns_them_as_a_mapping() -> None:
    """The everyday case: text metadata becomes a mapping a log record can carry."""
    # Arrange
    metadata = [("key1", "val1"), ("key2", "val2")]

    # Act & Assert
    assert metadata_to_dict(metadata) == {"key1": "val1", "key2": "val2"}


def test__metadata_to_dict__byte_keys_and_values__decodes_them() -> None:
    """gRPC hands keys and values over as bytes, which no log handler should have to decode."""
    # Arrange
    metadata = [(b"key1", b"val1"), ("key2", b"val2")]

    # Act & Assert
    assert metadata_to_dict(metadata) == {"key1": "val1", "key2": "val2"}


def test__metadata_to_dict__value_that_is_not_utf8__encodes_it_as_base64() -> None:
    """A binary value has no text form, so it is labelled rather than mangled."""
    # Arrange
    binary_value = b"\xff\xfe\xfd"
    expected = f"base64:{base64.b64encode(binary_value).decode('ascii')}"

    # Act & Assert
    assert metadata_to_dict([("key", binary_value)]) == {"key": expected}


def test__metadata_to_dict__repeated_key__joins_its_values() -> None:
    """gRPC metadata is a multimap, and dropping the repeats would lose information."""
    # Arrange
    metadata = [("key", "val1"), ("key", "val2")]

    # Act & Assert
    assert metadata_to_dict(metadata) == {"key": "val1,val2"}


def test__metadata_to_dict__grpc_metadata_object__is_accepted() -> None:
    """A live chain passes a `grpc.aio.Metadata`, not a list of pairs."""
    # Arrange
    metadata = grpc.aio.Metadata(("key1", "val1"), ("key2", "val2"))

    # Act & Assert
    assert metadata_to_dict(metadata) == {"key1": "val1", "key2": "val2"}


def test__create_aio_channel__insecure__opens_an_insecure_channel() -> None:
    """The insecure path must reach grpc's own factory, options and all."""
    # Arrange
    target = "localhost:50051"

    # Act
    with patch("grpc.aio.insecure_channel") as mock_insecure:
        create_aio_channel(target, insecure=True)

    # Assert
    mock_insecure.assert_called_once_with(target, options=None, compression=None, interceptors=None)


def test__create_aio_channel__secure_without_credentials__uses_the_default_ssl_credentials() -> None:
    """A secure channel with no credentials falls back to the system trust store, not to insecure."""
    # Arrange
    target = "localhost:50051"

    # Act
    with (
        patch("grpc.aio.secure_channel") as mock_secure,
        patch("grpc.ssl_channel_credentials", return_value=MagicMock()) as mock_credentials,
    ):
        create_aio_channel(target, insecure=False)

    # Assert
    mock_secure.assert_called_once_with(
        target, mock_credentials.return_value, options=None, compression=None, interceptors=None
    )


def test__create_aio_channel__secure_with_credentials__uses_the_given_ones() -> None:
    """Credentials the caller built are passed through untouched."""
    # Arrange
    target = "localhost:50051"
    credentials = MagicMock()

    # Act
    with patch("grpc.aio.secure_channel") as mock_secure:
        create_aio_channel(target, insecure=False, credentials=credentials)

    # Assert
    mock_secure.assert_called_once_with(target, credentials, options=None, compression=None, interceptors=None)
