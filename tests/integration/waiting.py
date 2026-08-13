"""Waiters that drive the event loop until a condition holds, instead of sleeping a guessed time.

Nothing here polls a mock: every condition these functions wait for is produced by a real server, a
real check loop or a real backoff, so the only alternative would be sleeping long enough to make the
slowest machine pass — and short enough to keep the suite quick.
"""

from __future__ import annotations

import asyncio
import gc
import time
from collections.abc import Awaitable, Callable

# Gap between two polls: short enough that a wait costs about as much as the event it waits for.
POLL_INTERVAL = 0.005

# Upper bound for something a real server, check loop or backoff has to produce first.
WAIT_TIMEOUT = 5.0

# Upper bound for something that only waits on the interpreter collecting an object.
COLLECT_TIMEOUT = 2.0


async def until(condition: Callable[[], bool], *, timeout: float = WAIT_TIMEOUT, message: str = "") -> None:
    """Drive the loop until `condition` holds.

    Args:
        condition: The predicate to wait for.
        timeout: Upper bound in seconds before the wait is declared failed.
        message: Extra context for the assertion error.

    Raises:
        AssertionError: If the condition did not hold within `timeout`.
    """
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= deadline:
            raise AssertionError(f"condition did not hold within {timeout}s. {message}".strip())
        await asyncio.sleep(POLL_INTERVAL)


async def until_async(
    check: Callable[[], Awaitable[bool]], *, timeout: float = WAIT_TIMEOUT, message: str = ""
) -> None:
    """Drive the loop until an awaitable condition holds.

    Args:
        check: The condition to wait for; awaited on every round.
        timeout: Upper bound in seconds before the wait is declared failed.
        message: Extra context for the assertion error.

    Raises:
        AssertionError: If the condition did not hold within `timeout`.
    """
    deadline = time.monotonic() + timeout
    while not await check():
        if time.monotonic() >= deadline:
            raise AssertionError(f"condition did not hold within {timeout}s. {message}".strip())
        await asyncio.sleep(POLL_INTERVAL)


async def eventually(condition: Callable[[], bool], *, timeout: float = COLLECT_TIMEOUT, message: str = "") -> None:
    """Drive the loop until `condition` holds, collecting garbage on every round.

    The in-flight slot of a stream the caller abandoned is released by a finalizer, so the condition
    can only become true once the wrapper is collected.

    Args:
        condition: The predicate to wait for.
        timeout: Upper bound in seconds before the wait is declared failed.
        message: Extra context for the assertion error.

    Raises:
        AssertionError: If the condition did not hold within `timeout`.
    """
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= deadline:
            raise AssertionError(f"condition did not hold within {timeout}s. {message}".strip())
        gc.collect()
        await asyncio.sleep(POLL_INTERVAL)
