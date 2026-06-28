"""Internal helpers shared by routing recipes."""

from __future__ import annotations

from typing import Generic, TypeVar

from anyio import Lock
from anyio.lowlevel import checkpoint

from chrono_daemon import SendStream, WouldBlock

T = TypeVar("T")


class SendGate(Generic[T]):
    """Serialize deliberate fan-in through one send endpoint."""

    def __init__(self, out: SendStream[T]) -> None:
        self._out = out
        self._lock = Lock()

    async def send(self, item: T) -> None:
        async with self._lock:
            await self._out.send(item)


async def send_when_ready(out: SendStream[T], item: T) -> None:
    """Fan-in for independent callers that cannot share a ``SendGate``.

    Core ``send()`` is single-owner (raises ``ChannelInUse`` on concurrent
    blocking use). When producers are independent — e.g. ``batcher.submit``,
    where each caller only holds ``out`` — there is no shared object to host a
    ``SendGate`` lock, so this uses ``send_nowait`` plus scheduler checkpoints.

    Tradeoff: under sustained backpressure this polls instead of parking on a
    space wakeup, so waiters spin. Prefer ``SendGate`` whenever one owner drives
    the sends (as merge/load_balance/worker_pool do).
    """
    while True:
        try:
            out.send_nowait(item)
            return
        except WouldBlock:
            await checkpoint()
