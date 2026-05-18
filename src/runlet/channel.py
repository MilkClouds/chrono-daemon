"""Typed point-to-point channel — the sole communication primitive.

A ``Channel[T]`` is a bounded queue with two endpoints, ``send`` and ``recv``. Multiple
producers and multiple consumers may share the endpoints; each item is delivered to
**exactly one** waiting receiver (competing-consumers semantic). This is the only
messaging primitive: fanout / pub-sub is intentionally absent. If a user needs to
broadcast a single source to N consumers, they write an explicit ``tee`` themselves.

Closing the send side propagates ``EndOfStream`` to all waiting receivers.

The two endpoints are Protocols so future transport adapters (multiprocess,
network) can plug in without breaking the API.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from runlet._types import ChannelClosed, EndOfStream

__all__ = ["Channel", "ChannelStats", "ReceiveStream", "SendStream", "open_channel"]

T = TypeVar("T")


@dataclass(frozen=True)
class ChannelStats:
    """Snapshot of a channel's runtime state for diagnostics.

    The fields are deliberately transport-agnostic: any future transport
    (multi-process, network) is expected to fill them in best-effort. For
    network transports the counts may be approximate (e.g. ``waiters_send``
    may not be observable across hosts) — when a count cannot be determined,
    it is reported as ``-1``.
    """

    current_buffer_used: int
    """Items currently buffered in the channel."""

    max_buffer_size: float
    """Configured buffer capacity. ``math.inf`` if unbounded; ``0`` for rendezvous."""

    open_send_streams: int
    """How many ``SendStream`` instances are still open."""

    open_receive_streams: int
    """How many ``ReceiveStream`` instances are still open."""

    waiters_send: int
    """Tasks currently blocked in ``send()`` waiting for buffer space."""

    waiters_receive: int
    """Tasks currently blocked in ``receive()`` waiting for an item."""


class SendStream(Protocol[T]):
    """Sender side of a channel. Multiple producers may share one ``SendStream``."""

    async def send(self, item: T) -> None:
        """Send an item. Blocks (under the channel's backpressure) until a slot is free.

        Raises ``ChannelClosed`` if the receive side has been closed.
        """
        ...

    async def aclose(self) -> None:
        """Close the send side. Waiting receivers get ``EndOfStream`` after the buffer drains."""
        ...

    def statistics(self) -> ChannelStats:
        """Snapshot the channel's runtime state for diagnostics. Read-only and cheap."""
        ...


class ReceiveStream(Protocol[T]):
    """Receiver side of a channel. Multiple consumers may share one ``ReceiveStream`` and will compete."""

    async def receive(self) -> T:
        """Receive one item. Blocks until an item is available.

        Raises ``EndOfStream`` if the send side has been closed and the buffer is drained.
        """
        ...

    def __aiter__(self) -> AsyncIterator[T]:
        """Iterate until ``EndOfStream`` (which is swallowed by the iterator protocol)."""
        ...

    async def aclose(self) -> None:
        """Close the receive side. Producers calling ``send()`` get ``ChannelClosed``."""
        ...

    def statistics(self) -> ChannelStats:
        """Snapshot the channel's runtime state for diagnostics. Read-only and cheap."""
        ...


@dataclass
class Channel(Generic[T]):
    """A typed bounded channel.

    The ``send`` and ``recv`` attributes are independent endpoints; you typically pass
    one into a producer daemon and the other into a consumer daemon.
    """

    send: SendStream[T]
    recv: ReceiveStream[T]


def _stats_from_anyio(inner: MemoryObjectSendStream[T] | MemoryObjectReceiveStream[T]) -> ChannelStats:
    s = inner.statistics()
    return ChannelStats(
        current_buffer_used=s.current_buffer_used,
        max_buffer_size=s.max_buffer_size,
        open_send_streams=s.open_send_streams,
        open_receive_streams=s.open_receive_streams,
        waiters_send=s.tasks_waiting_send,
        waiters_receive=s.tasks_waiting_receive,
    )


class _Send(Generic[T]):
    """In-process ``SendStream`` wrapping an anyio memory object send stream."""

    def __init__(self, inner: MemoryObjectSendStream[T]) -> None:
        self._inner = inner

    async def send(self, item: T) -> None:
        try:
            await self._inner.send(item)
        except anyio.BrokenResourceError as e:
            raise ChannelClosed("receive side closed") from e
        except anyio.ClosedResourceError as e:
            raise ChannelClosed("send side already closed") from e

    async def aclose(self) -> None:
        await self._inner.aclose()

    def statistics(self) -> ChannelStats:
        return _stats_from_anyio(self._inner)


class _Recv(Generic[T]):
    """In-process ``ReceiveStream`` wrapping an anyio memory object receive stream."""

    def __init__(self, inner: MemoryObjectReceiveStream[T]) -> None:
        self._inner = inner

    async def receive(self) -> T:
        try:
            return await self._inner.receive()
        except anyio.EndOfStream as e:
            raise EndOfStream from e
        except anyio.ClosedResourceError as e:
            raise EndOfStream from e

    async def __aiter__(self) -> AsyncIterator[T]:  # type: ignore[override]
        # We can't simply `async for x in self._inner: yield x` because the protocol
        # requires our own EndOfStream conversion. Instead, loop with receive().
        while True:
            try:
                yield await self.receive()
            except EndOfStream:
                return

    async def aclose(self) -> None:
        await self._inner.aclose()

    def statistics(self) -> ChannelStats:
        return _stats_from_anyio(self._inner)


def open_channel(maxsize: int = 0) -> Channel[T]:
    """Open a new in-process channel.

    Parameters
    ----------
    maxsize:
        Buffer capacity in items. ``0`` (default) means *strict rendezvous*: every
        ``send()`` blocks until a receiver is ready. Use a small positive value
        (e.g. ``maxsize=16``) when you want decoupling and tolerate some queuing.

    Returns
    -------
    Channel[T]
        A channel with ``send`` and ``recv`` endpoints.

    Notes
    -----
    Multiple receivers calling ``recv.receive()`` will compete for items
    (one item → one consumer). This is the intended MPMC semantic. There is no
    broadcast / fanout — keep the wiring explicit.
    """
    send_inner, recv_inner = anyio.create_memory_object_stream[T](max_buffer_size=maxsize)
    return Channel(send=_Send(send_inner), recv=_Recv(recv_inner))
