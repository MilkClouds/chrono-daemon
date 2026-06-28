"""Wait on the first of several receivers."""

from __future__ import annotations

from typing import TypeVar

import anyio

from chrono_daemon import EndOfStream, ReceiveStream

T = TypeVar("T")


class _Done(Exception):
    """Internal sentinel raised inside the task group when the winner is set."""


async def select(*receivers: ReceiveStream[T]) -> tuple[int, T]:
    """Return ``(index, item)`` for the first receiver to deliver an item.

    Raises ``EndOfStream`` if all receivers close before any item arrives.
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
