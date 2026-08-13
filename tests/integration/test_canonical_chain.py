"""The canonical chain on a live channel: how it registers, and what a healthy call gets back.

A mocked channel answers neither question. A channel sorts interceptors into four lists by class at
creation time, so what a chain observes depends on how it was registered, and the first test reads
those lists off a real channel. And a continuation resolves to a `Call` when the RPC is *created*,
never raising, so a chain only learns an outcome by awaiting that call; a unit test whose
continuation returns or raises the response itself passes either way.
"""

from __future__ import annotations

import logging

import pytest

from grpc_client_kit.interceptors import logical_interceptor

from .calls import EVERY_KIND, RpcKind
from .chains import CLIENT_SERVICE_NAME, canonical_chain
from .echo_bench import ECHO, ClientFactory, RunningServer
from .recording import RecordingMetrics

# What a channel does with an interceptor list: four lists, filled by class.
_CHANNEL_INTERCEPTOR_LISTS = (
    "_unary_unary_interceptors",
    "_unary_stream_interceptors",
    "_stream_unary_interceptors",
    "_stream_stream_interceptors",
)


async def test__chain__on_a_live_channel__every_layer_lands_in_all_four_lists(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # The channel is the only witness that counts here: it decides at creation time which of its
    # four lists an interceptor goes into, and a layer missing from a list never runs for that kind
    # of call. Seven layers, four kinds, one entry each — anything less is a layer with a blind spot.
    chain = canonical_chain(metrics, metadata={"x-request-id": "req-1"})
    stub = await make_client(chain).connect()

    # Act
    registered = {name: getattr(stub.channel, name) for name in _CHANNEL_INTERCEPTOR_LISTS}

    # Assert
    layers = {
        name: [type(logical_interceptor(entry)).__name__ for entry in entries] for name, entries in registered.items()
    }
    assert all(entries for entries in registered.values()), layers
    assert layers["_unary_unary_interceptors"] == [
        "AsyncClientContextInterceptor",
        "AsyncLoggingInterceptor",
        "AsyncClientTracingInterceptor",
        "AsyncClientMetricsInterceptor",
        "AsyncTimeoutInterceptor",
        "AsyncRetryInterceptor",
        "AsyncCircuitBreakerInterceptor",
    ]
    # The same layers in the same order in all four lists: one chain, whichever kind is called.
    assert len(set(map(tuple, layers.values()))) == 1


@pytest.mark.parametrize("kind", EVERY_KIND)
async def test__full_chain__every_rpc_kind__returns_the_server_response(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
    kind: RpcKind,
) -> None:
    # Arrange
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    response = await kind.invoke(stub)

    # Assert
    assert response == kind.expected


async def test__full_chain__unary_call__logs_one_completed_call(
    metrics: RecordingMetrics,
    make_client: ClientFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Arrange
    caplog.set_level(logging.INFO, logger=f"grpc.client.{CLIENT_SERVICE_NAME}")
    client = make_client(canonical_chain(metrics))
    stub = await client.connect()

    # Act
    await stub.echo(b"hello")

    # Assert
    logged = [record for record in caplog.records if record.getMessage() == "gRPC call successful"]
    assert len(logged) == 1
    assert logged[0].__dict__["grpc.method"] == ECHO
    assert logged[0].__dict__["grpc.status"] == "OK"


async def test__full_chain__injected_metadata__reaches_the_server(
    echo_server: RunningServer,
    metrics: RecordingMetrics,
    make_client: ClientFactory,
) -> None:
    # Arrange
    # The context interceptor sits outside logging, tracing and the resilience layers, so this also
    # proves that no layer below it drops the metadata on its way to the wire — the tracing layer
    # rebuilds the whole metadata list to inject its trace headers.
    chain = canonical_chain(metrics, metadata={"x-request-id": "req-42", "x-tenant": "acme"})
    client = make_client(chain)
    stub = await client.connect()

    # Act
    await stub.echo(b"hello")

    # Assert
    received = echo_server.service.unary_unary.metadata[0]
    assert received["x-request-id"] == "req-42"
    assert received["x-tenant"] == "acme"
