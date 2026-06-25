"""Recipe: dynamic-batching request/response on top of channels alone.

ADR 0001 keeps the comm primitive at 1:1 ``Channel``, with no service / RPC
construct. Request/response is reconstructed by embedding the *reply
channel* in the request: the caller opens a single-slot reply channel,
sends ``(request, reply)`` to the batcher, then awaits on its reply channel.
Because core channel endpoints are single-owner (ADR 0010), this recipe's
``submit`` helper implements deliberate fan-in with ``send_nowait`` plus
checkpoints rather than relying on multiple callers sharing blocking
``send()``.

This is the pattern behind dynamic-batching inference servers: N independent
callers each submit a request; a single batcher daemon accumulates up to
``max_batch`` requests, optionally waits up to ``max_queue_delay``
clock-seconds for more to arrive, calls ``forward`` once with the batch, and
fans the responses back to each caller's private reply channel.

Failure semantics: if ``forward`` raises, every caller in the batch sees
the same exception delivered through their reply channel. This avoids the
silent-partial-failure trap where one caller gets a result and another
hangs forever.

Import as ``from runlet.recipes.batcher import batcher_loop, submit``. The
recipe namespace (``runlet.recipes``) is best-effort; see
``src/runlet/recipes/__init__.py`` for the stability contract.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

import anyio

from runlet import (
    Channel,
    Clock,
    EndOfStream,
    ReceiveStream,
    SendStream,
    WouldBlock,
    open_channel,
)
from runlet.recipes._routing import send_when_ready

Req = TypeVar("Req")
Resp = TypeVar("Resp")


@dataclass
class Pending(Generic[Req, Resp]):
    """Internal envelope. Callers don't construct these directly; use ``submit``."""

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

    Parameters
    ----------
    incoming
        Receiver of :class:`Pending` envelopes (one per :func:`submit` call).
    forward
        Async callable that takes a list of requests and returns a same-length
        list of responses. Any exception it raises is propagated to every
        caller in the batch.
    max_batch
        Hard cap on batch size. After this many items are accumulated, the
        batch is dispatched even if the queue-delay window hasn't elapsed.
    max_queue_delay
        After receiving the first request of a batch, wait up to this many
        clock-seconds for more to arrive before dispatching. ``0.0`` (the
        default) means *no waiting* — only requests already queued at the
        moment of the first receive are batched together. Requires ``clock``
        when nonzero.
    clock
        Required when ``max_queue_delay > 0``. Used to time the delay window
        in a way that's compatible with :class:`runlet.SimClock` (the timer
        is implemented as ``ctx.clock.sleep`` racing against
        ``incoming.receive``, not as ``anyio.move_on_after``. That one is
        wall-clock and would silently misbehave under ``SimClock``).
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


def _try_recv_nowait(
    incoming: ReceiveStream[Pending[Req, Resp]],
    batch: list[Pending[Req, Resp]],
) -> bool:
    """Try a non-blocking receive; append to ``batch`` if one was available.

    Returns ``True`` if an item was added, ``False`` otherwise. Returns
    ``False`` on EOF too (caller drops out of the drain loop and dispatches
    what it has; the outer loop will hit EOF on the next ``receive``).
    """
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
    """Call ``forward`` on the batch and route responses (or the shared exception)."""
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
    """Submit one request to a batcher and await its response.

    Raises whatever ``forward`` raised, if it raised for this batch.
    """
    reply: Channel[Resp | Exception] = open_channel(maxsize=1)
    pending = Pending(req=req, reply=reply)
    await send_when_ready(out, pending)
    result = await reply.recv.receive()
    if isinstance(result, Exception):
        raise result
    return result
