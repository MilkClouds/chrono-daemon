"""Recipe: ready-worker pool built from SPSC channels.

Sharing one receive endpoint across workers would be a competing-consumer
pattern hidden inside ``Channel``. This recipe keeps the topology explicit:
the dispatcher owns the public input endpoint, each worker owns a private
input channel, and workers signal readiness through an internal channel.

Import as ``from runlet.recipes.worker_pool import worker_pool``. The recipe
namespace (``runlet.recipes``) is best-effort; see
``src/runlet/recipes/__init__.py`` for the stability contract.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

import anyio

from runlet import Channel, ReceiveStream, SendStream, open_channel
from runlet.recipes._routing import SendGate

Req = TypeVar("Req")
Resp = TypeVar("Resp")


async def worker_pool(
    incoming: ReceiveStream[Req],
    results: SendStream[Resp | Exception],
    handle: Callable[[Req], Awaitable[Resp]],
    *,
    workers: int = 1,
    worker_buffer: int = 0,
    close_results: bool = True,
) -> None:
    """Process requests with ``workers`` concurrent handlers.

    Each request is routed to an idle worker. Successful handler returns are
    sent to ``results``. If ``handle`` raises an ``Exception``, that exception
    object is sent to ``results`` and the worker continues with later jobs.
    ``BaseException`` subclasses still tear down the pool.

    ``results`` must be drained concurrently (or sized to hold every output): a
    worker blocked on a full ``results`` never re-announces readiness, which
    deadlocks dispatch.
    """
    if workers < 1:
        raise ValueError(f"workers must be >= 1, got {workers}")
    if worker_buffer < 0:
        raise ValueError(f"worker_buffer must be >= 0, got {worker_buffer}")

    ready: Channel[int] = open_channel(maxsize=workers)
    jobs: list[Channel[Req]] = [open_channel(maxsize=worker_buffer) for _ in range(workers)]
    ready_out = SendGate(ready.send)
    results_out = SendGate(results)

    async def _close_job_inputs() -> None:
        async with anyio.create_task_group() as tg:
            for ch in jobs:
                tg.start_soon(ch.send.aclose)

    async def _dispatch() -> None:
        try:
            async for item in incoming:
                worker_idx = await ready.recv.receive()
                await jobs[worker_idx].send.send(item)
        finally:
            await _close_job_inputs()

    async def _worker(idx: int, recv: ReceiveStream[Req]) -> None:
        await ready_out.send(idx)
        async for item in recv:
            try:
                result = await handle(item)
            except Exception as exc:
                await results_out.send(exc)
            else:
                await results_out.send(result)
            await ready_out.send(idx)

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_dispatch)
            for idx, ch in enumerate(jobs):
                tg.start_soon(_worker, idx, ch.recv)
    finally:
        await ready.send.aclose()
        if close_results:
            await results.aclose()
