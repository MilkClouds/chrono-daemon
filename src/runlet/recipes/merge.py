"""Recipe: explicit N:1 fan-in on top of SPSC channels.

Core channel endpoints are single-owner (ADR 0010), so multiple producers
should not share one blocking ``send()`` endpoint. ``merge`` keeps that wiring
visible: each producer owns its own SPSC channel, and one routing daemon
forwards all items into a single output stream.

Import as ``from runlet.recipes.merge import merge``. The recipe namespace
(``runlet.recipes``) is best-effort; see ``src/runlet/recipes/__init__.py``
for the stability contract.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar

import anyio

from runlet import ReceiveStream, SendStream
from runlet.recipes._routing import SendGate

T = TypeVar("T")


async def merge(
    sources: Sequence[ReceiveStream[T]],
    dest: SendStream[T],
    *,
    close_dest: bool = True,
) -> None:
    """Forward all items from ``sources`` into ``dest``.

    When ``close_dest`` is true, ``dest`` is closed on exit — after every source
    drains, and also if forwarding aborts (a source error or cancellation).
    Ordering is per-source FIFO; global interleave follows scheduler arrival.
    """
    if not sources:
        raise ValueError("merge() requires at least one source")

    output = SendGate(dest)

    async def _forward(src: ReceiveStream[T]) -> None:
        async for item in src:
            await output.send(item)

    try:
        async with anyio.create_task_group() as tg:
            for src in sources:
                tg.start_soon(_forward, src)
    finally:
        if close_dest:
            await dest.aclose()
