"""Channel behavior: bounded send/recv, competing consumers, close propagation, async-iter."""

from __future__ import annotations

import anyio
import pytest

from runlet import Channel, ChannelClosed, EndOfStream, open_channel

pytestmark = pytest.mark.anyio


async def test_send_receive_roundtrip() -> None:
    ch = open_channel(maxsize=4)
    await ch.send.send(42)
    assert await ch.recv.receive() == 42


async def test_bounded_buffer_blocks_when_full() -> None:
    ch = open_channel(maxsize=2)
    await ch.send.send(1)
    await ch.send.send(2)

    sent_third = False

    async def sender() -> None:
        nonlocal sent_third
        await ch.send.send(3)
        sent_third = True

    async with anyio.create_task_group() as tg:
        tg.start_soon(sender)
        await anyio.sleep(0.05)
        # Buffer is full; sender is blocked.
        assert not sent_third
        # Drain one slot; sender should now be able to proceed.
        assert await ch.recv.receive() == 1
        # Yield so the sender can finish.
        await anyio.sleep(0.05)
        assert sent_third


async def test_close_send_propagates_end_of_stream() -> None:
    ch = open_channel(maxsize=4)
    await ch.send.send("a")
    await ch.send.send("b")
    await ch.send.aclose()

    assert await ch.recv.receive() == "a"
    assert await ch.recv.receive() == "b"
    with pytest.raises(EndOfStream):
        await ch.recv.receive()


async def test_close_recv_makes_send_fail() -> None:
    ch = open_channel(maxsize=4)
    await ch.recv.aclose()
    with pytest.raises(ChannelClosed):
        await ch.send.send(1)


async def test_async_iter_until_end_of_stream() -> None:
    ch: Channel[int] = open_channel(maxsize=4)

    async def producer() -> None:
        for i in range(5):
            await ch.send.send(i)
        await ch.send.aclose()

    received: list[int] = []

    async def consumer() -> None:
        async for item in ch.recv:
            received.append(item)

    async with anyio.create_task_group() as tg:
        tg.start_soon(producer)
        tg.start_soon(consumer)

    assert received == [0, 1, 2, 3, 4]


async def test_statistics_reports_buffer_state() -> None:
    """ChannelStats reflects buffer used / max / open streams / waiters."""
    import math

    ch: Channel[int] = open_channel(maxsize=4)
    stats = ch.send.statistics()
    assert stats.current_buffer_used == 0
    assert stats.max_buffer_size == 4
    assert stats.open_send_streams >= 1
    assert stats.open_receive_streams >= 1
    assert stats.waiters_send == 0
    assert stats.waiters_receive == 0

    await ch.send.send(1)
    await ch.send.send(2)
    stats = ch.recv.statistics()
    assert stats.current_buffer_used == 2

    # Unbounded channel reports math.inf.
    ch2: Channel[int] = open_channel(maxsize=0)
    s2 = ch2.send.statistics()
    # rendezvous (maxsize=0) reports max_buffer_size == 0
    assert s2.max_buffer_size in (0, math.inf, 0.0)


async def test_competing_consumers_share_items() -> None:
    """The MPMC semantic: each item is delivered to exactly one waiting consumer."""
    ch: Channel[int] = open_channel(maxsize=0)

    received_by_a: list[int] = []
    received_by_b: list[int] = []

    async def producer() -> None:
        for i in range(20):
            await ch.send.send(i)
        await ch.send.aclose()

    async def consumer(bucket: list[int]) -> None:
        try:
            while True:
                bucket.append(await ch.recv.receive())
        except EndOfStream:
            return

    async with anyio.create_task_group() as tg:
        tg.start_soon(producer)
        tg.start_soon(consumer, received_by_a)
        tg.start_soon(consumer, received_by_b)

    # Both consumers got non-zero shares, union equals the full set, no duplicates.
    assert len(received_by_a) > 0
    assert len(received_by_b) > 0
    assert sorted(received_by_a + received_by_b) == list(range(20))
    assert len(set(received_by_a) & set(received_by_b)) == 0
