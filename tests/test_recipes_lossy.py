"""Lossy backpressure recipes: DropNewestSend, DropOldestSend, CoalesceSend.

These wrappers satisfy the ``SendStream`` Protocol and never block the
producer when the underlying channel buffer is full. Each test confirms
the policy on both asyncio and trio.
"""

from __future__ import annotations

import anyio
import pytest

from runlet import Channel, open_channel
from runlet.recipes.lossy import CoalesceSend, DropNewestSend, DropOldestSend

pytestmark = pytest.mark.anyio


async def test_drop_newest_discards_new_items_when_full() -> None:
    ch: Channel[int] = open_channel(maxsize=2)
    lossy = DropNewestSend(ch.send)

    await lossy.send(1)
    await lossy.send(2)
    # Buffer full; 3 and 4 are silently dropped.
    await lossy.send(3)
    await lossy.send(4)

    assert lossy.dropped == 2
    assert await ch.recv.receive() == 1
    assert await ch.recv.receive() == 2


async def test_drop_oldest_evicts_buffered_to_make_room() -> None:
    ch: Channel[int] = open_channel(maxsize=2)
    lossy = DropOldestSend(ch.send, ch.recv)

    await lossy.send(1)
    await lossy.send(2)
    # Buffer full → drop 1, store 3. Then drop 2, store 4.
    await lossy.send(3)
    await lossy.send(4)

    assert lossy.dropped == 2
    assert await ch.recv.receive() == 3
    assert await ch.recv.receive() == 4


async def test_drop_newest_send_nowait_also_counts_drops() -> None:
    ch: Channel[int] = open_channel(maxsize=1)
    lossy = DropNewestSend(ch.send)

    lossy.send_nowait(1)
    lossy.send_nowait(2)
    lossy.send_nowait(3)

    assert lossy.dropped == 2
    assert await ch.recv.receive() == 1


async def test_drop_oldest_send_nowait_evicts_too() -> None:
    ch: Channel[int] = open_channel(maxsize=1)
    lossy = DropOldestSend(ch.send, ch.recv)

    lossy.send_nowait(1)
    lossy.send_nowait(2)  # evicts 1
    lossy.send_nowait(3)  # evicts 2

    assert lossy.dropped == 2
    assert await ch.recv.receive() == 3


async def test_drop_oldest_with_concurrent_consumer() -> None:
    """When a consumer is actively draining, drops should be 0 — the buffer
    never stays full long enough for the wrapper to evict.
    """
    ch: Channel[int] = open_channel(maxsize=2)
    lossy = DropOldestSend(ch.send, ch.recv)
    received: list[int] = []

    async def consumer() -> None:
        for _ in range(10):
            received.append(await ch.recv.receive())

    async def producer() -> None:
        for i in range(10):
            await lossy.send(i)
            await anyio.sleep(0)  # let consumer drain

    async with anyio.create_task_group() as tg:
        tg.start_soon(consumer)
        tg.start_soon(producer)

    assert received == list(range(10))
    assert lossy.dropped == 0


async def test_coalesce_send_keeps_latest_on_single_slot() -> None:
    """CoalesceSend on maxsize=1 is the "last value wins" channel form."""
    ch: Channel[int] = open_channel(maxsize=1)
    lossy = CoalesceSend(ch.send, ch.recv)

    await lossy.send(1)
    await lossy.send(2)
    await lossy.send(3)

    assert lossy.dropped == 2
    assert await ch.recv.receive() == 3


async def test_lossy_wrapper_statistics_delegate_to_underlying() -> None:
    """statistics() returns the underlying channel's snapshot unchanged."""
    ch: Channel[int] = open_channel(maxsize=4)
    lossy = DropNewestSend(ch.send)

    stats = lossy.statistics()
    assert stats.current_buffer_used == 0
    assert stats.max_buffer_size == 4

    await lossy.send(1)
    assert lossy.statistics().current_buffer_used == 1


async def test_lossy_wrapper_aclose_closes_underlying() -> None:
    """aclose() forwards to the underlying send side."""
    ch: Channel[int] = open_channel(maxsize=4)
    lossy = DropNewestSend(ch.send)

    await lossy.send(1)
    await lossy.aclose()

    # Receiver drains then sees EndOfStream.
    assert await ch.recv.receive() == 1
    from runlet import EndOfStream

    with pytest.raises(EndOfStream):
        await ch.recv.receive()
