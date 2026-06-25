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
from runlet.recipes.load_balance import load_balance
from runlet.recipes.merge import merge
from runlet.recipes.select import select
from runlet.recipes.worker_pool import worker_pool

pytestmark = pytest.mark.anyio


async def test_recipes_namespace_imports() -> None:
    """All recipe modules import without error."""
    from runlet.recipes import (  # noqa: F401
        batcher,
        cooperative_every,
        fanout,
        latest,
        load_balance,
        merge,
        sync_bridge,
        worker_pool,
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


# -- topology recipes --------------------------------------------------------


async def test_merge_fans_in_multiple_sources_without_endpoint_sharing() -> None:
    src_a: Channel[tuple[str, int]] = open_channel(maxsize=4)
    src_b: Channel[tuple[str, int]] = open_channel(maxsize=4)
    dest: Channel[tuple[str, int]] = open_channel(maxsize=1)
    received: list[tuple[str, int]] = []

    async def produce(ch: Channel[tuple[str, int]], name: str) -> None:
        for i in range(3):
            await ch.send.send((name, i))
        await ch.send.aclose()

    async def drain() -> None:
        async for item in dest.recv:
            received.append(item)

    async with anyio.create_task_group() as tg:
        tg.start_soon(merge, [src_a.recv, src_b.recv], dest.send)
        tg.start_soon(drain)
        tg.start_soon(produce, src_a, "a")
        tg.start_soon(produce, src_b, "b")

    assert sorted(received) == [("a", 0), ("a", 1), ("a", 2), ("b", 0), ("b", 1), ("b", 2)]


async def test_load_balance_round_robins_to_destinations() -> None:
    source: Channel[int] = open_channel(maxsize=8)
    dests: list[Channel[int]] = [open_channel(maxsize=2) for _ in range(3)]
    buckets: list[list[int]] = [[], [], []]

    async def produce() -> None:
        for i in range(6):
            await source.send.send(i)
        await source.send.aclose()

    async def drain(ch: Channel[int], bucket: list[int]) -> None:
        async for item in ch.recv:
            bucket.append(item)

    async with anyio.create_task_group() as tg:
        tg.start_soon(load_balance, source.recv, [ch.send for ch in dests])
        tg.start_soon(produce)
        for ch, bucket in zip(dests, buckets, strict=True):
            tg.start_soon(drain, ch, bucket)

    assert buckets == [[0, 3], [1, 4], [2, 5]]


async def test_worker_pool_dispatches_to_ready_workers() -> None:
    incoming: Channel[int] = open_channel(maxsize=8)
    results: Channel[int | Exception] = open_channel(maxsize=8)
    running = 0
    peak_running = 0

    async def handle(item: int) -> int:
        nonlocal running, peak_running
        running += 1
        peak_running = max(peak_running, running)
        await anyio.sleep(0.01)
        running -= 1
        return item * 10

    async def produce() -> None:
        for i in range(6):
            await incoming.send.send(i)
        await incoming.send.aclose()

    async def run_pool() -> None:
        await worker_pool(incoming.recv, results.send, handle, workers=3)

    async with anyio.create_task_group() as tg:
        tg.start_soon(run_pool)
        tg.start_soon(produce)

    collected: list[int | Exception] = []
    async for item in results.recv:
        collected.append(item)

    values = [item for item in collected if isinstance(item, int)]
    assert sorted(values) == [0, 10, 20, 30, 40, 50]
    assert peak_running > 1


async def test_worker_pool_sends_handler_exceptions_as_results() -> None:
    incoming: Channel[int] = open_channel(maxsize=4)
    results: Channel[int | Exception] = open_channel(maxsize=4)

    async def handle(item: int) -> int:
        if item == 2:
            raise RuntimeError("bad job")
        return item

    await incoming.send.send(1)
    await incoming.send.send(2)
    await incoming.send.aclose()

    async def run_pool() -> None:
        await worker_pool(incoming.recv, results.send, handle, workers=2)

    async with anyio.create_task_group() as tg:
        tg.start_soon(run_pool)

    collected: list[int | Exception] = []
    async for item in results.recv:
        collected.append(item)

    assert 1 in collected
    errors = [item for item in collected if isinstance(item, RuntimeError)]
    assert len(errors) == 1
    assert str(errors[0]) == "bad job"


async def test_merge_keeps_dest_open_when_close_dest_false() -> None:
    src: Channel[int] = open_channel(maxsize=4)
    dest: Channel[int] = open_channel(maxsize=4)

    async def produce() -> None:
        for i in range(3):
            await src.send.send(i)
        await src.send.aclose()

    async with anyio.create_task_group() as tg:
        tg.start_soon(produce)
        await merge([src.recv], dest.send, close_dest=False)

    # close_dest=False leaves dest.send open for the caller to keep using.
    await dest.send.send(99)
    await dest.send.aclose()
    received = [item async for item in dest.recv]
    assert received == [0, 1, 2, 99]


async def test_load_balance_keeps_dests_open_when_close_dests_false() -> None:
    source: Channel[int] = open_channel(maxsize=4)
    dests: list[Channel[int]] = [open_channel(maxsize=4) for _ in range(2)]

    async def produce() -> None:
        for i in range(4):
            await source.send.send(i)
        await source.send.aclose()

    async with anyio.create_task_group() as tg:
        tg.start_soon(produce)
        await load_balance(source.recv, [ch.send for ch in dests], close_dests=False)

    # close_dests=False leaves every destination open; append a sentinel, then drain.
    buckets: list[list[int]] = []
    for ch in dests:
        await ch.send.send(-1)
        await ch.send.aclose()
        buckets.append([item async for item in ch.recv])
    assert buckets == [[0, 2, -1], [1, 3, -1]]


async def test_worker_pool_keeps_results_open_with_worker_buffer() -> None:
    incoming: Channel[int] = open_channel(maxsize=4)
    results: Channel[int | Exception] = open_channel(maxsize=4)
    collected: list[int | Exception] = []

    async def handle(item: int) -> int:
        return item * 2

    async def produce() -> None:
        for i in range(3):
            await incoming.send.send(i)
        await incoming.send.aclose()

    async def drain_three() -> None:
        for _ in range(3):
            collected.append(await results.recv.receive())

    async with anyio.create_task_group() as tg:
        tg.start_soon(produce)
        tg.start_soon(drain_three)
        await worker_pool(
            incoming.recv,
            results.send,
            handle,
            workers=2,
            worker_buffer=1,
            close_results=False,
        )

    # close_results=False leaves results.send open after the pool returns.
    await results.send.send(-1)
    await results.send.aclose()
    assert await results.recv.receive() == -1
    assert sorted(v for v in collected if isinstance(v, int)) == [0, 2, 4]


async def test_topology_recipes_validate_non_empty_shapes() -> None:
    ch: Channel[int] = open_channel()
    results: Channel[int | Exception] = open_channel()

    with pytest.raises(ValueError, match="at least one source"):
        await merge([], ch.send)
    with pytest.raises(ValueError, match="at least one destination"):
        await load_balance(ch.recv, [])
    with pytest.raises(ValueError, match="workers"):
        await worker_pool(ch.recv, results.send, _identity_int, workers=0)


async def _identity_int(x: int) -> int:
    return x


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
