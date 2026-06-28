"""Recipe: explicit 1:N competing-consumer routing.

``load_balance`` is the deliberate form of "one input, several consumers":
one routing daemon owns the source receive endpoint and every destination send
endpoint, then sends each item to exactly one destination in round-robin order.

Import as ``from chrono_daemon.recipes.load_balance import load_balance``. The recipe
namespace (``chrono_daemon.recipes``) is best-effort; see
``src/chrono_daemon/recipes/__init__.py`` for the stability contract.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TypeVar

import anyio

from chrono_daemon import ReceiveStream, SendStream

T = TypeVar("T")


async def load_balance(
    source: ReceiveStream[T],
    dests: Sequence[SendStream[T]],
    *,
    close_dests: bool = True,
) -> None:
    """Forward each source item to exactly one destination.

    Distribution is deterministic round-robin. If the selected destination is
    backpressured, the balancer waits for it rather than skipping ahead. This
    keeps the policy simple and visible; use ``worker_pool`` when dispatching
    specifically to ready workers.
    """
    if not dests:
        raise ValueError("load_balance() requires at least one destination")

    idx = 0
    try:
        async for item in source:
            await dests[idx].send(item)
            idx = (idx + 1) % len(dests)
    finally:
        if close_dests:
            async with anyio.create_task_group() as tg:
                for dest in dests:
                    tg.start_soon(dest.aclose)
