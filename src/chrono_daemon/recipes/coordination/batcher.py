"""Dynamic-batching request/response on channels.

Callers use ``submit``. The batcher gathers up to ``max_batch`` requests,
calls ``forward`` once, and replies on each caller's private channel. If
``forward`` raises, every caller in that batch receives the same exception.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

import anyio

from chrono_daemon import (
    Channel,
    Clock,
    EndOfStream,
    ReceiveStream,
    SendStream,
    WouldBlock,
    open_channel,
)
from chrono_daemon.recipes._flow import send_when_ready

Req = TypeVar("Req")
Resp = TypeVar("Resp")


@dataclass
class Pending(Generic[Req, Resp]):
    """Internal request plus private reply channel."""

    req: Req
    reply: Channel[Resp | Exception]


async def batcher_loop(
    incoming: ReceiveStream[Pending[Req, Resp]],
    forward: Callable[[list[Req]], Awaitable[list[Resp]]],
    *,
    max_batch: int = 32,
    max_queue_delay: float = 0.0,
    clock: Clock | None = None,
) -> None:
    """Drain ``incoming``, batch up to ``max_batch`` requests, call ``forward``, route replies.

    ``forward`` must return one response per request. ``max_queue_delay`` uses
    ``clock`` so delay windows work under ``SimClock``.
    """
    if max_queue_delay < 0:
        raise ValueError(f"max_queue_delay must be >= 0, got {max_queue_delay}")
    if max_queue_delay > 0 and clock is None:
        raise ValueError("max_queue_delay > 0 requires a clock for the timer")
    if max_batch < 1:
        raise ValueError(f"max_batch must be >= 1, got {max_batch}")

    while True:
        try:
            first = await incoming.receive()
        except EndOfStream:
            return
        batch: list[Pending[Req, Resp]] = [first]

        # Phase 1: drain anything already queued without waiting.
        while len(batch) < max_batch:
            if not _try_recv_nowait(incoming, batch):
                break

        # Phase 2: if we still have headroom and a delay window is configured,
        # race additional receives against a single timer.
        if len(batch) < max_batch and max_queue_delay > 0:
            assert clock is not None  # checked above
            deadline = clock.now() + max_queue_delay
            while len(batch) < max_batch:
                remaining = deadline - clock.now()
                if remaining <= 0:
                    break
                got = await _recv_or_timeout(incoming, clock, remaining)
                if got is _CLOSED:
                    # send-side closed; flush what we have and exit after dispatch.
                    break
                if got is _TIMEOUT:
                    break
                batch.append(got)  # type: ignore[arg-type]

        await _dispatch_batch(batch, forward)


# Sentinels for the racer. Distinct objects let callers compare by identity.
_CLOSED = object()
_TIMEOUT = object()


def _try_recv_nowait(incoming: ReceiveStream[Pending[Req, Resp]], batch: list[Pending[Req, Resp]]) -> bool:
    """Append one immediately available item, if any."""
    try:
        batch.append(incoming.receive_nowait())
        return True
    except (WouldBlock, EndOfStream):
        return False


async def _recv_or_timeout(
    incoming: ReceiveStream[Pending[Req, Resp]],
    clock: Clock,
    timeout: float,
) -> object:
    """Race a clock.sleep against incoming.receive. Returns the item, _TIMEOUT, or _CLOSED."""
    outcome: list[object] = [_TIMEOUT]

    async with anyio.create_task_group() as tg:

        async def _do_recv() -> None:
            try:
                item = await incoming.receive()
            except EndOfStream:
                outcome[0] = _CLOSED
                tg.cancel_scope.cancel()
                return
            outcome[0] = item
            tg.cancel_scope.cancel()

        async def _do_sleep() -> None:
            await clock.sleep(timeout)
            tg.cancel_scope.cancel()

        tg.start_soon(_do_recv)
        tg.start_soon(_do_sleep)

    return outcome[0]


async def _dispatch_batch(
    batch: list[Pending[Req, Resp]],
    forward: Callable[[list[Req]], Awaitable[list[Resp]]],
) -> None:
    """Call ``forward`` and route responses."""
    try:
        responses = await forward([p.req for p in batch])
        if len(responses) != len(batch):
            raise RuntimeError(f"forward returned {len(responses)} responses for {len(batch)} requests")
    except Exception as exc:
        for p in batch:
            await p.reply.send.send(exc)
            await p.reply.send.aclose()
        return

    for p, resp in zip(batch, responses, strict=True):
        await p.reply.send.send(resp)
        await p.reply.send.aclose()


async def submit(
    out: SendStream[Pending[Req, Resp]],
    req: Req,
) -> Resp:
    """Submit one request and await its response."""
    reply: Channel[Resp | Exception] = open_channel(maxsize=1)
    pending = Pending(req=req, reply=reply)
    await send_when_ready(out, pending)
    result = await reply.recv.receive()
    if isinstance(result, Exception):
        raise result
    return result
