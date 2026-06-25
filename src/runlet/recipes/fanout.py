"""1:N broadcast via an explicit ``tee`` daemon."""

from __future__ import annotations

from typing import TypeVar

import anyio

from runlet import ReceiveStream, SendStream

T = TypeVar("T")


async def tee(src: ReceiveStream[T], *dests: SendStream[T]) -> None:
    """Forward each item from ``src`` to every ``dest`` and close all dests on EOF.

    Sends happen in parallel. One slow destination still backpressures the
    whole tee.
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
