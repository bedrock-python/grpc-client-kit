"""Client configuration: what a channel is opened with, and how it is kept alive."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
)

import grpc


def _milliseconds(seconds: float) -> int:
    """Convert a duration in seconds into the milliseconds gRPC channel arguments are given in."""
    return round(seconds * 1000)


@dataclass(frozen=True, slots=True)
class ConnectivityConfig:
    """How a channel keeps its connection alive, and how fast it comes back after losing it.

    These are gRPC channel arguments, which means two things. They are set once, when the channel is
    created, and cannot be changed afterwards; and they are spelled as an untyped list of
    ``("grpc.some_thing_ms", 30000)`` pairs that every caller currently has to remember, in the
    right unit, by heart. This is that list, named, in seconds, and validated.

    Keepalive pings detect a connection that has silently gone away — a dropped NAT mapping, a load
    balancer that closed one side, a peer that vanished — which TCP alone can leave undetected until
    a call has already hung on it. The reconnect backoff decides how long a channel that has lost
    its connection waits before dialing again, and gRPC's own upper bound for that (two minutes) is
    a long time for a backend that restarts in seconds.

    Opt-in, deliberately. Pinging is a conversation the server has to agree to: it enforces its own
    minimum interval and answers a client that pings too often with ``GOAWAY`` and
    ``ENHANCE_YOUR_CALM``, so a client library that switched this on for everybody would be a way to
    get connections dropped by servers that were never configured for it. The defaults here are what
    a caller gets once they have decided to tune the channel at all: conservative enough for a
    server left at its own defaults, which permits pings on a connection with calls in flight.

    Attributes:
        keepalive_time: Seconds of inactivity before a keepalive ping is sent. ``None`` leaves
            gRPC's default, which is not to ping at all.
        keepalive_timeout: Seconds to wait for the ping to be answered before the connection is
            considered dead.
        permit_without_calls: Whether to keep pinging while no call is in flight. Off by default:
            this is the case servers police most tightly, and an idle connection that dies is
            usually cheaper to re-establish than to keep watching.
        max_pings_without_data: How many pings may be sent on a connection carrying no data before
            the client stops; 0 means no limit.
        initial_reconnect_backoff: Seconds to wait before the first reconnect attempt.
        min_reconnect_backoff: Lower bound for the wait between reconnect attempts. ``None`` leaves
            gRPC's default, which also bounds how long a single connect attempt may take.
        max_reconnect_backoff: Upper bound for the wait between reconnect attempts, which is what
            decides how long a client keeps failing after a backend is already back.
    """

    keepalive_time: float | None = 30.0
    keepalive_timeout: float | None = 10.0
    permit_without_calls: bool = False
    max_pings_without_data: int | None = 2
    initial_reconnect_backoff: float | None = 1.0
    min_reconnect_backoff: float | None = None
    max_reconnect_backoff: float | None = 30.0

    def __post_init__(self) -> None:
        """Validate the configuration.

        Raises:
            ValueError: If a duration is not positive, if the ping allowance is negative, or if the
                reconnect bounds contradict each other.
        """
        durations = {
            "keepalive_time": self.keepalive_time,
            "keepalive_timeout": self.keepalive_timeout,
            "initial_reconnect_backoff": self.initial_reconnect_backoff,
            "min_reconnect_backoff": self.min_reconnect_backoff,
            "max_reconnect_backoff": self.max_reconnect_backoff,
        }
        for name, value in durations.items():
            # None is how a field says "leave gRPC's default"; 0 would be a different setting
            # altogether, and not one any of these arguments has a meaning for.
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive, or None to keep the gRPC default")

        if self.max_pings_without_data is not None and self.max_pings_without_data < 0:
            raise ValueError("max_pings_without_data must be non-negative (0 means no limit)")

        if (
            self.min_reconnect_backoff is not None
            and self.max_reconnect_backoff is not None
            and self.min_reconnect_backoff > self.max_reconnect_backoff
        ):
            raise ValueError("min_reconnect_backoff must not exceed max_reconnect_backoff")

    def to_options(self) -> list[tuple[str, Any]]:
        """Return these settings as gRPC channel arguments.

        The order is the declaration order of the fields and never varies, which is what keeps a
        channel's identity stable: options are part of the channel pool's key (see
        `channel.ChannelKey`), so a list assembled differently from one call to the next would open
        a new channel each time.

        Returns:
            The channel arguments, in seconds converted to the milliseconds gRPC expects. Fields
            left at ``None`` contribute nothing, so gRPC keeps its own default for them.
        """
        options: list[tuple[str, Any]] = []

        if self.keepalive_time is not None:
            options.append(("grpc.keepalive_time_ms", _milliseconds(self.keepalive_time)))
        if self.keepalive_timeout is not None:
            options.append(("grpc.keepalive_timeout_ms", _milliseconds(self.keepalive_timeout)))

        options.append(("grpc.keepalive_permit_without_calls", int(self.permit_without_calls)))

        if self.max_pings_without_data is not None:
            options.append(("grpc.http2.max_pings_without_data", self.max_pings_without_data))
        if self.initial_reconnect_backoff is not None:
            options.append(("grpc.initial_reconnect_backoff_ms", _milliseconds(self.initial_reconnect_backoff)))
        if self.min_reconnect_backoff is not None:
            options.append(("grpc.min_reconnect_backoff_ms", _milliseconds(self.min_reconnect_backoff)))
        if self.max_reconnect_backoff is not None:
            options.append(("grpc.max_reconnect_backoff_ms", _milliseconds(self.max_reconnect_backoff)))

        return options


@dataclass(slots=True)
class GrpcClientConfig:
    """Core configuration for establishing a gRPC channel connection.

    Attributes:
        target: The target address (host:port) or a gRPC-compatible resolver URI.
        insecure: Whether to use an insecure channel (default: False).
        credentials: SSL/TLS credentials for secure channels.
        options: A list of key-value pairs to configure the gRPC channel. Anything set here wins
            over `connectivity`.
        compression: The compression algorithm to use for the channel.
        connectivity: Keepalive and reconnect tuning, spelled in seconds instead of as raw channel
            arguments. ``None`` leaves every one of those arguments at gRPC's own default.
    """

    target: str | None = None
    insecure: bool = False
    credentials: grpc.ChannelCredentials | None = None
    options: list[tuple[str, Any]] | None = None
    compression: grpc.Compression | None = None
    connectivity: ConnectivityConfig | None = None

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.insecure and self.credentials:
            raise ValueError("Cannot provide credentials for an insecure channel")

    def channel_options(self) -> list[tuple[str, Any]] | None:
        """Return the full option list a channel is opened with.

        Explicit `options` come first and untouched, followed by the arguments `connectivity` stands
        for — minus any whose key the caller already used. Filtering those out is what makes "an
        explicit option wins" true in the only sense gRPC can honour it: the argument is passed once,
        with the caller's value, so there is no duplicate for gRPC to resolve one way or the other.

        The result is a pure function of the configuration, in a fixed order, which matters because
        the pool keys channels by their options among other things (see `channel.ChannelKey`): two
        clients configured alike share a channel, and one client asking twice does not open two.
        Without `connectivity` the caller's own list is handed back as it is — ``None`` included — so
        a client that does not tune anything keeps exactly the channel identity it always had.

        Returns:
            The options to open the channel with, or None when there are none at all.
        """
        if self.connectivity is None:
            return self.options

        explicit = self.options or []
        already_set = {key for key, _ in explicit}
        derived = [(key, value) for key, value in self.connectivity.to_options() if key not in already_set]

        if not derived:
            return self.options

        return [*explicit, *derived]


__all__ = ["ConnectivityConfig", "GrpcClientConfig"]
