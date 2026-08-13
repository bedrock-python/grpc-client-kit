"""Unit tests for the package metadata."""

from __future__ import annotations

import pytest

import grpc_client_kit

pytestmark = pytest.mark.unit


def test__package__imported__exposes_non_empty_version() -> None:
    # Act
    version = grpc_client_kit.__version__

    # Assert
    assert isinstance(version, str)
    assert version
