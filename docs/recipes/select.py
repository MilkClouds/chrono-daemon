"""Recipe: wait on the first of several receivers.

The Go ``select`` equivalent: given N ``ReceiveStream``s, return the index of
the one that produced an item first along with the item itself. The losers
remain unconsumed.

anyio does not ship this as a primitive because the structured-concurrency
idiom (``create_task_group`` + ``cancel_scope``) subsumes it: we race N
receive coroutines in a task group and cancel the others as soon as the
first one delivers a value. This recipe wraps that idiom into a single
``await select(...)`` call.

For the multi-rate reactive pattern (e.g. an S1 inference loop that must
react to both an obs stream and a subgoal stream), this is often cleaner
than holding a background "drainer" task that updates a cached latest-value.

Copy this file into your codebase as needed. It is not exported from
``runlet``.
"""

from __future__ import annotations

from typing import TypeVar

import anyio

from runlet import EndOfStream, ReceiveStream

T = TypeVar("T")


class _Done(Exception):
    """Internal sentinel raised inside the task group when the winner is set."""


async def select(*receivers: ReceiveStream[T]) -> tuple[int, T]:
    """Return ``(index, item)`` for the first receiver to deliver an item.

    All other receive attempts are cancelled. If every receiver closes before
    any of them delivers, raises ``EndOfStream``.
    """
    if not receivers:
        raise ValueError("select() requires at least one receiver")

    winner: tuple[int, T] | None = None
    closed: list[bool] = [False] * len(receivers)

    async def _race(idx: int, recv: ReceiveStream[T], scope: anyio.CancelScope) -> None:
        nonlocal winner
        try:
            item = await recv.receive()
        except EndOfStream:
            closed[idx] = True
            if all(closed):
                scope.cancel()
            return
        if winner is None:
            winner = (idx, item)
            scope.cancel()

    async with anyio.create_task_group() as tg:
        for idx, recv in enumerate(receivers):
            tg.start_soon(_race, idx, recv, tg.cancel_scope)

    if winner is None:
        raise EndOfStream
    return winner
