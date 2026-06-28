"""Typed single-producer / single-consumer channel."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from chrono_daemon._types import ChannelClosed, ChannelInUse, EndOfStream, WouldBlock

__all__ = ["Channel", "ChannelStats", "ReceiveStream", "SendStream", "open_channel"]

T = TypeVar("T")


@dataclass(frozen=True)
class ChannelStats:
    """Transport-agnostic channel diagnostics."""

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
    """Sender side of a channel. Intended to be owned by one active producer."""

    async def send(self, item: T) -> None:
        """Send an item. Blocks (under the channel's backpressure) until a slot is free.

        Raises ``ChannelClosed`` if the receive side has been closed.
        """
        ...

    def send_nowait(self, item: T) -> None:
        """Send an item without blocking. Raises ``WouldBlock`` if the buffer is full.

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
    """Receiver side of a channel. Intended to be owned by one active consumer."""

    async def receive(self) -> T:
        """Receive one item. Blocks until an item is available.

        Raises ``EndOfStream`` if the send side has been closed and the buffer is drained.
        """
        ...

    def receive_nowait(self) -> T:
        """Receive one item without blocking. Raises ``WouldBlock`` if the buffer is empty.

        Raises ``EndOfStream`` if the send side has closed and the buffer is
        drained.
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
        self._busy = False

    async def send(self, item: T) -> None:
        if self._busy:
            raise ChannelInUse("send endpoint already has an active sender")
        self._busy = True
        try:
            await self._inner.send(item)
        except anyio.BrokenResourceError as e:
            raise ChannelClosed("receive side closed") from e
        except anyio.ClosedResourceError as e:
            raise ChannelClosed("send side already closed") from e
        finally:
            self._busy = False

    def send_nowait(self, item: T) -> None:
        try:
            self._inner.send_nowait(item)
        except anyio.WouldBlock as e:
            raise WouldBlock("channel buffer full") from e
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
        self._busy = False

    async def receive(self) -> T:
        if self._busy:
            raise ChannelInUse("receive endpoint already has an active receiver")
        self._busy = True
        try:
            return await self._inner.receive()
        except anyio.EndOfStream as e:
            raise EndOfStream from e
        except anyio.ClosedResourceError as e:
            raise EndOfStream from e
        finally:
            self._busy = False

    def receive_nowait(self) -> T:
        try:
            return self._inner.receive_nowait()
        except anyio.WouldBlock as e:
            raise WouldBlock("channel empty") from e
        except anyio.EndOfStream as e:
            raise EndOfStream from e
        except anyio.ClosedResourceError as e:
            raise EndOfStream from e

    async def __aiter__(self) -> AsyncIterator[T]:  # type: ignore[override]
        # Route through receive() for our EndOfStream conversion.
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

    ``maxsize=0`` is rendezvous. Positive values allow buffering. Concurrent
    blocking use of one endpoint raises ``ChannelInUse``.
    """
    send_inner, recv_inner = anyio.create_memory_object_stream[T](max_buffer_size=maxsize)
    return Channel(send=_Send(send_inner), recv=_Recv(recv_inner))
