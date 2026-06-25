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

from runlet import Channel, EndOfStream, SendStream, SimClock, open_channel
from runlet.recipes.fanout import tee
from runlet.recipes.select import select

pytestmark = pytest.mark.anyio


async def test_recipes_namespace_imports() -> None:
    """All recipe modules import without error."""
    from runlet.recipes import (  # noqa: F401
        batcher,
        cooperative_every,
        fanout,
        latest,
        sync_bridge,
    )


async def test_latest_stores_and_returns_most_recent_value() -> None:
    from runlet.recipes.latest import Latest

    cache: Latest[int] = Latest()
    assert cache.get() is None
    cache.set(1)
    assert cache.get() == 1
    cache.set(2)
    assert cache.get() == 2


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


# -- batcher ---------------------------------------------------------------


async def test_batcher_dispatches_no_delay() -> None:
    """Without max_queue_delay, the batcher dispatches whatever is already queued."""
    from runlet.recipes.batcher import Pending, batcher_loop

    requests: Channel[Pending[int, int]] = open_channel(maxsize=8)
    replies: list[Channel[int | Exception]] = [open_channel(maxsize=1) for _ in range(3)]
    seen_batches: list[list[int]] = []

    async def doubler(reqs: list[int]) -> list[int]:
        seen_batches.append(list(reqs))
        return [r * 2 for r in reqs]

    for req, reply in zip([1, 2, 3], replies, strict=True):
        await requests.send.send(Pending(req=req, reply=reply))
    await requests.send.aclose()

    async with anyio.create_task_group() as tg:
        tg.start_soon(batcher_loop, requests.recv, doubler)

    assert seen_batches == [[1, 2, 3]]
    assert [await reply.recv.receive() for reply in replies] == [2, 4, 6]


async def test_batcher_timeout_window_under_simclock() -> None:
    """With max_queue_delay>0, the batcher waits up to the configured window under SimClock."""
    from runlet import SimClock
    from runlet.recipes.batcher import Pending, batcher_loop, submit

    clock = SimClock()
    requests: Channel[Pending[int, int]] = open_channel(maxsize=16)
    seen_batches: list[list[int]] = []

    async def identity(reqs: list[int]) -> list[int]:
        seen_batches.append(list(reqs))
        return list(reqs)

    async def call_after_delay(x: int, delay: float) -> None:
        await clock.sleep(delay)
        results.append(await submit(requests.send, x))

    async def run_batcher() -> None:
        await batcher_loop(
            requests.recv,
            identity,
            max_queue_delay=0.5,
            clock=clock,
            max_batch=32,
        )

    results: list[int] = []
    async with anyio.create_task_group() as tg:
        tg.start_soon(run_batcher)
        tg.start_soon(call_after_delay, 1, 0.0)
        tg.start_soon(call_after_delay, 2, 0.2)
        tg.start_soon(call_after_delay, 3, 1.0)
        await anyio.sleep(0)
        await clock.advance(2.0)
        await requests.send.aclose()

    assert sorted(results) == [1, 2, 3]
    assert seen_batches == [[1, 2], [3]]


# -- sync_bridge -----------------------------------------------------------


def test_sync_bridge_hosts_dispatcher_for_sync_callers() -> None:
    """Sync code can invoke an async dispatcher's methods via the portal.

    Not an anyio test — this exercises the sync-side surface of the recipe.
    """
    from runlet.recipes.sync_bridge import host_async_dispatcher

    class _Dispatcher:
        def __init__(self, send: SendStream[int]) -> None:
            self._send = send
            self._closed = False

        async def push(self, item: int) -> None:
            await self._send.send(item)

        async def close(self) -> None:
            if not self._closed:
                self._closed = True
                await self._send.aclose()

    collected: list[int] = []

    async def collector(ctx: object, recv: object) -> None:  # types relaxed in this closure
        from runlet import EndOfStream as EOS

        try:
            while True:
                collected.append(await recv.receive())  # type: ignore[attr-defined]
        except EOS:
            return

    async def setup(sup):  # type: ignore[no-untyped-def]
        from runlet import open_channel as _open_channel

        ch: Channel[int] = _open_channel(maxsize=4)
        sup.spawn(collector, ch.recv, name="collector")
        return _Dispatcher(ch.send)

    with host_async_dispatcher(setup) as (portal, dispatcher):
        portal.call(dispatcher.push, 1)
        portal.call(dispatcher.push, 2)
        portal.call(dispatcher.push, 3)
        portal.call(dispatcher.close)

    assert collected == [1, 2, 3]


# -- batcher (exception path) ----------------------------------------------


async def test_batcher_propagates_forward_exception_to_every_caller() -> None:
    """If forward raises, all callers in the batch see the same exception."""
    from runlet.recipes.batcher import Pending, batcher_loop, submit

    requests: Channel[Pending[int, int]] = open_channel(maxsize=8)

    async def bad_forward(reqs: list[int]) -> list[int]:
        raise RuntimeError(f"boom on batch of {len(reqs)}")

    async def caller(x: int, errors: list[Exception]) -> None:
        try:
            await submit(requests.send, x)
        except Exception as e:
            errors.append(e)

    errors: list[Exception] = []
    async with anyio.create_task_group() as tg:
        tg.start_soon(batcher_loop, requests.recv, bad_forward)
        async with anyio.create_task_group() as call_tg:
            call_tg.start_soon(caller, 1, errors)
            call_tg.start_soon(caller, 2, errors)
        await requests.send.aclose()

    assert len(errors) == 2
    assert all(isinstance(e, RuntimeError) for e in errors)
    assert all("boom" in str(e) for e in errors)


async def test_batcher_propagates_response_count_mismatch_to_caller() -> None:
    """If forward returns the wrong response count, callers get an error instead of hanging."""
    from runlet.recipes.batcher import Pending, batcher_loop, submit

    requests: Channel[Pending[int, int]] = open_channel(maxsize=8)

    async def bad_forward(reqs: list[int]) -> list[int]:
        assert reqs == [1]
        return []

    async with anyio.create_task_group() as tg:
        tg.start_soon(batcher_loop, requests.recv, bad_forward)
        with pytest.raises(RuntimeError, match="forward returned 0 responses for 1 requests"):
            await submit(requests.send, 1)
        await requests.send.aclose()
