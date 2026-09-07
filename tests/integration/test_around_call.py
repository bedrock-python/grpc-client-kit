"""The `around_call` contract on a live channel: one context across the yield, one outcome per call.

Neither property exists inside a single task, and a mocked channel has only one. `grpc.aio` runs an
interceptor chain in a task of its own, drains a response stream in whichever task the caller
iterates from, and resolves a streaming request's outcome after the chain has already returned — so
the setup of one generator and its teardown really do run in three different places, and what
`contextvars` and a failing teardown make of that is only visible from here.
"""

from __future__ import annotations

import logging

import pytest

from grpc_client_kit import flatten_interceptors
from tests.helpers import AROUND_LOGGER, RaisingTeardown, TokenAcrossYield

from .calls import EVERY_KIND, RpcKind
from .echo_bench import ClientFactory
from .waiting import until


@pytest.mark.parametrize("kind", EVERY_KIND)
async def test__around_call__contextvar_token__is_reset_after_the_yield_on_every_kind(
    make_client: ClientFactory,
    kind: RpcKind,
) -> None:
    # Arrange
    # A value scoped to one call — a request id, a correlation id, an OpenTelemetry context — is
    # the shortest useful thing this seam is written for, and the pair that scopes it is only legal
    # when both sides of the yield are stepped in one and the same context.
    interceptor = TokenAcrossYield()
    stub = await make_client(flatten_interceptors([interceptor])).connect()

    # Act
    response = await kind.invoke(stub)
    await until(lambda: interceptor.teardowns == 1, message="the teardown never ran")

    # Assert
    assert response == kind.expected
    assert interceptor.reset_error is None, "the teardown was stepped in a different context than the setup"


@pytest.mark.parametrize("kind", EVERY_KIND)
async def test__around_call__teardown_raises__the_call_keeps_the_response_it_had(
    make_client: ClientFactory,
    kind: RpcKind,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrange
    # One mistake, one treatment. Before this it had three: the exception replaced the response, it
    # truncated a response stream, or it landed in the event loop's exception handler where nobody
    # was looking — decided by nothing but which of the four kinds the call happened to be.
    caplog.set_level(logging.ERROR, logger=AROUND_LOGGER)
    interceptor = RaisingTeardown()
    stub = await make_client(flatten_interceptors([interceptor])).connect()

    # Act
    response = await kind.invoke(stub)
    await until(lambda: interceptor.teardowns == 1, message="the teardown never ran")

    # Assert
    assert response == kind.expected
    reported = [record for record in caplog.records if record.name == AROUND_LOGGER]
    assert len(reported) == 1, [record.getMessage() for record in reported]
    assert reported[0].exc_info is not None, "the failure was reported without the traceback that explains it"
    assert reported[0].exc_info[0] is RuntimeError
