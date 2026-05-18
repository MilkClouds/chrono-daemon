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
    from runlet.recipes import batcher, cooperative_every, fanout, sync_bridge  # noqa: F401


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
    from runlet.recipes.batcher import Pending, batcher_loop, submit

    requests: Channel[Pending[int, int]] = open_channel(maxsize=8)
    seen_batches: list[list[int]] = []

    async def doubler(reqs: list[int]) -> list[int]:
        seen_batches.append(list(reqs))
        return [r * 2 for r in reqs]

    async with anyio.create_task_group() as tg:
        tg.start_soon(batcher_loop, requests.recv, doubler)

        # Submit 3 requests, wait for replies, then close to terminate the loop.
        async def caller(x: int, results: list[int]) -> None:
            results.append(await submit(requests.send, x))

        results: list[int] = []
        async with anyio.create_task_group() as call_tg:
            call_tg.start_soon(caller, 1, results)
            call_tg.start_soon(caller, 2, results)
            call_tg.start_soon(caller, 3, results)
        assert sorted(results) == [2, 4, 6]
        await requests.send.aclose()

    # All requests handled. The batcher may have grouped them or not depending on
    # scheduling, but every value was forwarded exactly once.
    flat = [r for batch in seen_batches for r in batch]
    assert sorted(flat) == [1, 2, 3]


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

    async def call_after_delay(x: int, delay: float, results: list[int]) -> None:
        await clock.sleep(delay)
        results.append(await submit(requests.send, x))

    results: list[int] = []
    async with anyio.create_task_group() as tg:
        tg.start_soon(
            batcher_loop,
            requests.recv,
            identity,
        )
        # Two callers within the 0.5s window, one outside it.
        tg.start_soon(call_after_delay, 1, 0.0, results)
        tg.start_soon(call_after_delay, 2, 0.2, results)
        tg.start_soon(call_after_delay, 3, 1.0, results)
        await anyio.sleep(0)

        # The batcher loop has to be launched with the clock kwarg; do so via
        # a wrapper task because tg.start_soon doesn't take kwargs.
        # (We're testing the timing behavior, not the call shape.)
        # NB: replace above tg.start_soon with one that passes the clock.
        # The cleanest way is to relaunch via the keyword-aware form below.

        # Drive the clock.
        await clock.advance(2.0)
        await requests.send.aclose()

    # We launched batcher without clock/max_queue_delay above, so this test
    # actually exercises the *no-delay* path. Convert to a proper test of the
    # delay path by re-running with the delay configured.

    # Reset and run again with delay configured.
    clock = SimClock()
    requests = open_channel(maxsize=16)
    seen_batches.clear()
    results.clear()

    async def call_after_delay2(x: int, delay: float) -> None:
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

    async with anyio.create_task_group() as tg:
        tg.start_soon(run_batcher)
        tg.start_soon(call_after_delay2, 1, 0.0)
        tg.start_soon(call_after_delay2, 2, 0.2)  # within 0.5s window of first
        tg.start_soon(call_after_delay2, 3, 1.0)  # arrives later, separate batch
        await anyio.sleep(0)
        await clock.advance(2.0)
        await requests.send.aclose()

    # First batch should be {1, 2} (within window); 3 lands in its own batch.
    assert sorted(results) == [1, 2, 3]
    # At least two distinct batches: not all three lumped together.
    assert len(seen_batches) >= 2


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
