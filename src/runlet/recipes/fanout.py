"""Recipe: 1:N broadcast via an explicit ``tee`` daemon.

ADR 0001 keeps ``Channel`` 1:1 and ADR 0010 makes endpoints single-owner. When a single source
must reach N independent consumers — each with its own backpressure and its
own close lifecycle — wire it as one producer, one ``tee`` daemon, and N
destination channels.

The destinations are typically created by the caller and handed in; the tee
daemon does nothing except forward. Per-destination backpressure means: if
one consumer is slow, that destination's buffer fills first, and the
forwarder blocks on it (delaying all other destinations as well). If you
want a different policy (drop-oldest per slow consumer, lag-isolated
buffers), wrap each destination with a small buffered relay daemon before
``tee``.

Copy this file into your codebase. It is not exported from ``runlet`` —
keeping ``tee`` out of the public API is the point of ADR 0001.
"""

from __future__ import annotations

from typing import TypeVar

import anyio

from runlet import ReceiveStream, SendStream

T = TypeVar("T")


async def tee(src: ReceiveStream[T], *dests: SendStream[T]) -> None:
    """Forward each item from ``src`` to every ``dest`` and close all dests on EOF.

    Per-item parallel send: each destination sees the item at the same logical
    instant, modulo its own backpressure. Closing any single destination
    causes the next forwarding round to raise ``ChannelClosed`` from ``tee``
    — wrap if you want fault isolation.
    """
    try:
        async for item in src:
            async with anyio.create_task_group() as tg:
                for d in dests:
                    tg.start_soon(d.send, item)
    finally:
        # Close every destination so downstream consumers see EndOfStream.
        async with anyio.create_task_group() as tg:
            for d in dests:
                tg.start_soon(d.aclose)
