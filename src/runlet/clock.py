"""Clock abstractions: ``WallClock`` for production, ``SimClock`` for deterministic replay.

The ``Clock`` protocol is the only async-time interface daemons should touch. Calling
``anyio.sleep`` directly bypasses ``SimClock``, which would silently break burst-step
replay determinism — so the rule (enforced by convention) is: always reach for
``ctx.clock.sleep(...)``.

``SimClock`` lets a driver task advance virtual time in bulk via ``await clock.advance(dt)``.
Sleepers register a deadline; ``advance_to`` walks the heap, sets each woken sleeper's
``anyio.Event``, and yields a checkpoint so the woken task can run and possibly register
a new sleep before the next iteration.
"""

from __future__ import annotations

import heapq
import itertools
from collections.abc import AsyncIterator
from typing import Protocol

import anyio
import anyio.lowlevel

__all__ = ["Clock", "SimClock", "WallClock"]

# Number of `checkpoint()` yields to perform between each SimClock wake event,
# giving the just-woken task time to register a follow-up sleep before we look
# at the heap again. asyncio settles in 1 round; trio sometimes needs a few.
# 8 is empirically generous; raise only if a test flakes under load.
_SETTLE_ROUNDS = 8


class Clock(Protocol):
    """Minimal time interface. All async code in runlet should use this — never anyio.sleep directly."""

    def now(self) -> float:
        """Return current time in seconds. Monotonic; the units (epoch vs zero-based) are clock-specific."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Sleep for ``seconds`` of clock-time. Non-positive values yield once and return."""
        ...

    def every(self, period: float) -> AsyncIterator[float]:
        """Yield clock-time stamps every ``period`` seconds.

        The first tick is at ``now() + period``. If the consumer falls behind, ticks
        are skipped (monotonic-target schedule) — there is no catch-up burst.
        """
        ...


class WallClock:
    """Real-time clock delegating to anyio's monotonic clock and sleep."""

    def now(self) -> float:
        return anyio.current_time()

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await anyio.lowlevel.checkpoint()
            return
        await anyio.sleep(seconds)

    async def every(self, period: float) -> AsyncIterator[float]:
        if period <= 0:
            raise ValueError(f"every() period must be positive, got {period}")
        next_t = self.now() + period
        while True:
            now = self.now()
            if next_t > now:
                await self.sleep(next_t - now)
            yield next_t
            # Skip missed ticks to maintain monotonic target schedule.
            now = self.now()
            while next_t <= now:
                next_t += period


class SimClock:
    """Deterministic virtual clock.

    Time only moves when a driver task calls ``advance(dt)`` or ``advance_to(t)``.
    All sleepers are released exactly at their registered deadline, in deadline order,
    with insertion order as the tiebreaker.

    Example
    -------
    >>> async def worker(clock, log):
    ...     await clock.sleep(1.0)
    ...     log.append(("worker", clock.now()))
    ...
    >>> async def main():
    ...     clock = SimClock()
    ...     log: list[tuple[str, float]] = []
    ...     async with anyio.create_task_group() as tg:
    ...         tg.start_soon(worker, clock, log)
    ...         await clock.advance(2.0)
    ...     assert log == [("worker", 1.0)]
    """

    def __init__(self, t0: float = 0.0) -> None:
        self._t = float(t0)
        # Heap entries are (deadline, seq, event). seq breaks ties stably.
        self._waiters: list[tuple[float, int, anyio.Event]] = []
        self._seq = itertools.count()

    def now(self) -> float:
        return self._t

    async def sleep(self, seconds: float) -> None:
        # Always go through a checkpoint first so callers in a freshly-cancelled
        # scope get the CancelledError before they enqueue a deadline that would
        # then have to be cleaned up.
        await anyio.lowlevel.checkpoint()
        if seconds <= 0:
            return
        ev = anyio.Event()
        heapq.heappush(self._waiters, (self._t + seconds, next(self._seq), ev))
        await ev.wait()

    async def advance(self, dt: float) -> None:
        """Advance virtual time by ``dt`` seconds."""
        if dt < 0:
            raise ValueError(f"cannot advance backwards (dt={dt})")
        await self.advance_to(self._t + dt)

    async def advance_to(self, t: float) -> None:
        """Advance virtual time to absolute ``t``. Idempotent for already-past values.

        Wakes sleepers one at a time in (deadline, insertion-order) order. After each
        wake we yield ``_SETTLE_ROUNDS`` times so the woken task can run and possibly
        register a follow-up sleep — that follow-up may itself be in range and will
        be picked up on the next iteration. When the heap settles with nothing due
        by ``t``, we finalize ``self._t`` and return.

        The settle loop exists because trio's scheduler doesn't necessarily resume
        a woken task on a single ``checkpoint()`` round; asyncio usually does. We
        spin a fixed budget of yields rather than calling backend-specific helpers.
        """
        if t < self._t:
            return
        while True:
            if not (self._waiters and self._waiters[0][0] <= t):
                # Heap appears empty; let any pending wakeups settle and check again.
                for _ in range(_SETTLE_ROUNDS):
                    await anyio.lowlevel.checkpoint()
                if not (self._waiters and self._waiters[0][0] <= t):
                    break
                continue
            deadline, _, ev = heapq.heappop(self._waiters)
            self._t = deadline
            ev.set()
            for _ in range(_SETTLE_ROUNDS):
                await anyio.lowlevel.checkpoint()
        self._t = max(self._t, t)

    async def every(self, period: float) -> AsyncIterator[float]:
        if period <= 0:
            raise ValueError(f"every() period must be positive, got {period}")
        next_t = self._t + period
        while True:
            delta = next_t - self._t
            if delta > 0:
                await self.sleep(delta)
            yield next_t
            next_t += period
