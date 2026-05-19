"""Recipe: lossy backpressure wrappers around ``SendStream``.

ADR 0001 keeps ``Channel`` at "1:1 bounded queue with full backpressure" — no
per-channel drop policy. When a producer can't afford to block (sensor
stream, control loop), the right shape is a tiny wrapper at the
``SendStream`` boundary, not a Channel option.

Three policies cover the common cases:

- :class:`DropNewestSend` — buffer full → silently discard the new item.
  Producer never blocks. Cheapest; one ``send_nowait`` call.
- :class:`DropOldestSend` — buffer full → ``receive_nowait`` the oldest
  buffered item to free a slot, then send. Producer never blocks.
  **In-process only** because it needs ``ReceiveStream`` access; a future
  network transport would implement the same policy server-side.
- :class:`CoalesceSend` — semantically a single-slot ``DropOldestSend``.
  Documents the maxsize=1 use case (last-value-wins) explicitly so call
  sites read better.

Each wrapper tracks a ``dropped`` counter for diagnostics. The
``SendStream`` Protocol is satisfied so any daemon expecting a normal
``SendStream[T]`` works unchanged.

Import as ``from runlet.recipes.lossy import DropOldestSend, DropNewestSend,
CoalesceSend``. The recipe namespace (``runlet.recipes``) is best-effort —
see ``src/runlet/recipes/__init__.py`` for the stability contract.
"""

from __future__ import annotations

from typing import Generic, TypeVar

from runlet import ChannelStats, EndOfStream, ReceiveStream, SendStream, WouldBlock

__all__ = ["CoalesceSend", "DropNewestSend", "DropOldestSend"]

T = TypeVar("T")


class DropNewestSend(Generic[T]):
    """``SendStream`` wrapper that drops the *new* item when the buffer is full.

    Producer never blocks. Use when "old data is more important than the
    latest" — e.g. a fixed-size audit log where you'd rather lose recent
    samples than corrupt the existing record.

    Example::

        ch = open_channel(maxsize=8)
        lossy = DropNewestSend(ch.send)
        # Pass `lossy` anywhere a SendStream is expected; producer code
        # is identical to using ch.send directly.
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

    Producer never blocks. Use when "the latest data is more important than
    the old" — e.g. sensor stream where stale readings are worse than missing
    one. The wrapper needs access to ``ReceiveStream`` to drop, which makes
    this an **in-process-only** construct; a future network transport adapter
    would implement the same semantic server-side.

    Example::

        ch = open_channel(maxsize=8)
        lossy = DropOldestSend(ch.send, ch.recv)
        # Producer sees a normal SendStream; daemon code is unchanged.

    Multi-producer note: if two producers race to fill the last slot, one of
    them ends up dropping both the buffered item and its own new item
    (visible in ``dropped``). This is the "don't block" guarantee in action;
    use ``send`` (not this wrapper) when blocking is acceptable.
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
            # Raced with a consumer; the slot is already gone. Try the send
            # anyway — if it still fails we drop the new item too (the
            # "never block" guarantee).
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
    """Single-slot variant of :class:`DropOldestSend` — the latest value wins.

    Semantically identical to ``DropOldestSend`` on a ``maxsize=1`` channel.
    The class exists as a self-documenting alias for the "last-value-wins"
    use case (analogous to :class:`runlet.recipes.latest.Latest` but flowing
    through a channel, e.g. when a consumer wants to ``await`` for an update
    rather than poll).

    Construct from a ``maxsize=1`` channel; the channel size isn't enforced
    here but a wrapper on a wider buffer will simply drop the head, which
    isn't usually what you want under the "coalesce" name.
    """
