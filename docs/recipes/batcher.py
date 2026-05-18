"""Recipe: dynamic-batching request/response on top of channels alone.

ADR 0001 keeps the comm primitive at 1:1 ``Channel``, with no service / RPC
construct. Request/response is reconstructed by embedding the *reply
channel* in the request: the caller opens a single-slot reply channel,
sends `(request, reply)` to the batcher, then awaits on its reply channel.

This is the pattern behind every dynamic-batching inference server: N
independent callers each submit a request; a single batcher daemon collates
up to ``max_batch`` requests, calls ``forward`` once with the batch, and
fans the responses back to each caller's private reply channel.

Failure semantics: if ``forward`` raises, every caller in the batch sees
the same exception on their reply channel (delivered via ``ChannelClosed``
+ a sentinel; see ``submit`` below). This avoids the silent-partial-failure
trap where one caller gets a result and another hangs forever.

Copy this file into your codebase as needed. It is not exported from
``runlet``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

import anyio

from runlet import (
    Channel,
    EndOfStream,
    ReceiveStream,
    SendStream,
    open_channel,
)

Req = TypeVar("Req")
Resp = TypeVar("Resp")


@dataclass
class Pending(Generic[Req, Resp]):
    """Internal envelope. Callers don't construct these directly — use ``submit``."""

    req: Req
    reply: Channel[Resp | Exception]


async def batcher_loop(
    incoming: ReceiveStream[Pending[Req, Resp]],
    forward: Callable[[list[Req]], Awaitable[list[Resp]]],
    *,
    max_batch: int = 32,
) -> None:
    """Drain ``incoming``, batch up to ``max_batch`` requests, call ``forward``, route replies.

    The "wait for more requests" budget is intentionally absent from this
    minimal recipe — the batcher waits for the first request, then drains
    whatever else is already available before forwarding. For a time-bounded
    accumulation window, layer a ``ctx.clock.sleep`` over this loop.
    """
    while True:
        try:
            first = await incoming.receive()
        except EndOfStream:
            return

        batch: list[Pending[Req, Resp]] = [first]
        # Drain anything already queued without waiting on the clock.
        while len(batch) < max_batch:
            with anyio.CancelScope() as scope:
                scope.deadline = -1.0  # already past; effectively non-blocking
                try:
                    batch.append(await incoming.receive())
                except EndOfStream:
                    break
            if scope.cancelled_caught:
                break

        try:
            responses = await forward([p.req for p in batch])
        except Exception as exc:  # forward errored — propagate to every caller
            for p in batch:
                await p.reply.send.send(exc)
                await p.reply.send.aclose()
            continue

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
    await out.send(Pending(req=req, reply=reply))
    result = await reply.recv.receive()
    if isinstance(result, Exception):
        raise result
    return result
