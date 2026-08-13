"""Shared fixtures for grpc-client-kit tests."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def pytest_configure(config: pytest.Config) -> None:
    """Relax coverage threshold when running integration tests only."""
    markexpr = getattr(config.option, "markexpr", "") or ""
    if "integration" in markexpr and "unit" not in markexpr:
        cov_plugin = config.pluginmanager.getplugin("_cov")
        if cov_plugin is not None and hasattr(cov_plugin, "options") and hasattr(cov_plugin.options, "cov_fail_under"):
            cov_plugin.options.cov_fail_under = 0


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-mark tests as unit or integration based on directory."""
    has_integration = False
    has_unit = False

    for item in items:
        path = Path(str(item.fspath)).as_posix()
        if "/tests/unit/" in path:
            item.add_marker("unit")
            has_unit = True
        elif "/tests/integration/" in path:
            item.add_marker("integration")
            has_integration = True

    if has_integration and not has_unit:
        cov_plugin = config.pluginmanager.getplugin("_cov")
        if cov_plugin is not None and hasattr(cov_plugin, "options") and hasattr(cov_plugin.options, "cov_fail_under"):
            cov_plugin.options.cov_fail_under = 0
