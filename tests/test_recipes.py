"""Smoke tests for the recipes namespace.

The recipes carry weaker stability guarantees than the core (see
``src/runlet/recipes/__init__.py``), but they must at minimum import
cleanly and execute their headline scenario. Behavioral coverage is
intentionally light here; deep recipe-specific tests live next to each
recipe if/when they're promoted.
"""

from __future__ import annotations

import anyio
import pytest

from runlet import Channel, EndOfStream, SimClock, open_channel
from runlet.recipes.fanout import tee
from runlet.recipes.select import select

pytestmark = pytest.mark.anyio


async def test_recipes_namespace_imports() -> None:
    """All four recipe modules import without error."""
    from runlet.recipes import batcher, fanout, sync_bridge  # noqa: F401


async def test_fanout_tee_delivers_each_item_to_every_destination() -> None:
    src: Channel[int] = open_channel(maxsize=4)
    dst_a: Channel[int] = open_channel(maxsize=4)
    dst_b: Channel[int] = open_channel(maxsize=4)

    async def producer() -> None:
        for i in range(5):
            await src.send.send(i)
        await src.send.aclose()

    received_a: list[int] = []
    received_b: list[int] = []

    async def drain(recv, bucket: list[int]) -> None:
        async for item in recv:
            bucket.append(item)

    async with anyio.create_task_group() as tg:
        tg.start_soon(producer)
        tg.start_soon(tee, src.recv, dst_a.send, dst_b.send)
        tg.start_soon(drain, dst_a.recv, received_a)
        tg.start_soon(drain, dst_b.recv, received_b)

    assert received_a == [0, 1, 2, 3, 4]
    assert received_b == [0, 1, 2, 3, 4]


async def test_select_returns_index_of_first_ready_receiver() -> None:
    clock = SimClock()
    ch_a: Channel[str] = open_channel(maxsize=1)
    ch_b: Channel[str] = open_channel(maxsize=1)

    winner: tuple[int, str] | None = None

    async def waiter() -> None:
        nonlocal winner
        winner = await select(ch_a.recv, ch_b.recv)

    async def fire_b() -> None:
        await clock.sleep(0.5)
        await ch_b.send.send("from-b")

    async with anyio.create_task_group() as tg:
        tg.start_soon(waiter)
        tg.start_soon(fire_b)
        await anyio.sleep(0)
        await clock.advance(1.0)

    assert winner == (1, "from-b")


async def test_select_raises_endofstream_when_all_closed() -> None:
    ch: Channel[int] = open_channel(maxsize=1)
    await ch.send.aclose()
    with pytest.raises(EndOfStream):
        await select(ch.recv)
