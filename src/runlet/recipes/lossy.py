"""Lossy ``SendStream`` wrappers for producers that must not block."""

from __future__ import annotations

from typing import Generic, TypeVar

from runlet import ChannelStats, EndOfStream, ReceiveStream, SendStream, WouldBlock

__all__ = ["CoalesceSend", "DropNewestSend", "DropOldestSend"]

T = TypeVar("T")


class DropNewestSend(Generic[T]):
    """``SendStream`` wrapper that drops the *new* item when the buffer is full.

    Use when preserving already-buffered data matters more than accepting the
    newest sample.
    """

    def __init__(self, inner: SendStream[T]) -> None:
        self._inner = inner
        self._dropped = 0

    @property
    def dropped(self) -> int:
        """Cumulative count of items dropped by this wrapper."""
        return self._dropped

    async def send(self, item: T) -> None:
        try:
            self._inner.send_nowait(item)
        except WouldBlock:
            self._dropped += 1

    def send_nowait(self, item: T) -> None:
        try:
            self._inner.send_nowait(item)
        except WouldBlock:
            self._dropped += 1

    async def aclose(self) -> None:
        await self._inner.aclose()

    def statistics(self) -> ChannelStats:
        return self._inner.statistics()


class DropOldestSend(Generic[T]):
    """``SendStream`` wrapper that drops the *oldest* buffered item to make room.

    Use when freshness matters more than preserving stale buffered data. This
    wrapper is in-process only because it needs receive-side access.
    """

    def __init__(self, send: SendStream[T], recv: ReceiveStream[T]) -> None:
        self._send = send
        self._recv = recv
        self._dropped = 0

    @property
    def dropped(self) -> int:
        """Cumulative count of items dropped by this wrapper."""
        return self._dropped

    async def send(self, item: T) -> None:
        self._send_with_drop(item)

    def send_nowait(self, item: T) -> None:
        self._send_with_drop(item)

    def _send_with_drop(self, item: T) -> None:
        try:
            self._send.send_nowait(item)
            return
        except WouldBlock:
            pass
        # Buffer full: drop one then send.
        try:
            self._recv.receive_nowait()
            self._dropped += 1
        except (WouldBlock, EndOfStream):
            # Raced with a consumer; try the send anyway.
            pass
        try:
            self._send.send_nowait(item)
        except WouldBlock:
            self._dropped += 1

    async def aclose(self) -> None:
        await self._send.aclose()

    def statistics(self) -> ChannelStats:
        return self._send.statistics()


class CoalesceSend(DropOldestSend[T]):
    """Single-slot variant of :class:`DropOldestSend`; latest value wins.

    Construct from a ``maxsize=1`` channel. Wider buffers still drop their
    oldest item, which is usually not what "coalesce" implies.
    """
