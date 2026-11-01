"""Clock abstractions for real time and deterministic replay.

Daemons should use ``ctx.clock`` instead of ``anyio.sleep`` so ``SimClock`` can
control time during tests.
"""

from __future__ import annotations

import heapq
import itertools
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal, Protocol

import anyio
import anyio.lowlevel

__all__ = ["Clock", "SimClock", "WallClock"]

# Checkpoints yielded between heap inspections so woken tasks can register
# follow-up sleeps. anyio has no portable "run until idle" primitive.
_DEFAULT_SETTLE_ROUNDS = 8


class Clock(Protocol):
    """Minimal time interface used by chrono-daemon daemons."""

    def now(self) -> float:
        """Return current time in seconds. Monotonic; the units (epoch vs zero-based) are clock-specific."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Sleep for ``seconds`` of clock-time. Non-positive values yield once and return."""
        ...

    async def wait_until(self, deadline: float) -> None:
        """Sleep until absolute clock-time ``deadline``. Past deadlines yield once and return."""
        ...

    def every(self, period: float, *, first: Literal["now", "after"] = "after") -> AsyncIterator[float]:
        """Yield clock-time stamps every ``period`` seconds.

        ``first`` controls the first tick: ``"after"`` (default) fires at
        ``now() + period``; ``"now"`` fires immediately at ``now()``, then every
        ``period``. If the consumer falls behind, missed ticks are skipped.
        """
        ...


class WallClock:
    """Real-time clock delegating to anyio's monotonic clock and sleep."""

    def now(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await anyio.lowlevel.checkpoint()
            return
        await anyio.sleep(seconds)

    async def wait_until(self, deadline: float) -> None:
        await self.sleep(deadline - self.now())

    async def every(self, period: float, *, first: Literal["now", "after"] = "after") -> AsyncIterator[float]:
        if period <= 0:
            raise ValueError(f"every() period must be positive, got {period}")
        next_t = self.now() if first == "now" else self.now() + period
        while True:
            now = self.now()
            if next_t > now:
                await self.sleep(next_t - now)
            yield next_t
            # Skip missed ticks to maintain monotonic target schedule.
            now = self.now()
            while next_t <= now:
                next_t += period


@dataclass
class _Waiter:
    """One sleeper registered against a SimClock."""

    deadline: float
    seq: int
    event: anyio.Event = field(compare=False)
    cancelled: bool = field(default=False, compare=False)

    def __lt__(self, other: _Waiter) -> bool:
        return (self.deadline, self.seq) < (other.deadline, other.seq)


class SimClock:
    """Deterministic virtual clock.

    Time only moves when a driver task calls ``advance(dt)`` or ``advance_to(t)``.
    All sleepers are released exactly at their registered deadline, in deadline order,
    with insertion order as the tiebreaker. A driver that knows when woken work has
    finished passes ``settle=`` to wake one sleeper per settle; see ADR 0012.
    """

    def __init__(self, t0: float = 0.0, *, settle_rounds: int = _DEFAULT_SETTLE_ROUNDS) -> None:
        if settle_rounds < 1:
            raise ValueError(f"settle_rounds must be >= 1, got {settle_rounds}")
        self._t = float(t0)
        self._settle_rounds = settle_rounds
        # Min-heap of pending sleepers ordered by (deadline, seq). seq breaks
        # ties stably. Cancelled entries stay in the heap with cancelled=True
        # and are skipped on pop.
        self._waiters: list[_Waiter] = []
        self._seq = itertools.count()

    def now(self) -> float:
        return self._t

    async def sleep(self, seconds: float) -> None:
        # Checkpoint first so a freshly-cancelled caller exits before enqueueing.
        await anyio.lowlevel.checkpoint()
        if seconds <= 0:
            return
        await self.wait_until(self._t + seconds)

    async def wait_until(self, deadline: float) -> None:
        # Checkpoint first so a freshly-cancelled caller exits before enqueueing.
        await anyio.lowlevel.checkpoint()
        if deadline <= self._t:
            return
        waiter = _Waiter(deadline=deadline, seq=next(self._seq), event=anyio.Event())
        heapq.heappush(self._waiters, waiter)
        try:
            await waiter.event.wait()
        finally:
            # Lazy delete: skip cancelled entries on pop instead of removing
            # them from the middle of the heap.
            if not waiter.event.is_set():
                waiter.cancelled = True
                while self._waiters and self._waiters[0].cancelled:
                    heapq.heappop(self._waiters)

    def next_deadline(self) -> float | None:
        """Deadline of the earliest pending sleeper, or ``None`` if none is pending."""
        self._drop_cancelled_tops()
        return self._waiters[0].deadline if self._waiters else None

    async def advance(self, dt: float, *, settle: Callable[[], Awaitable[object]] | None = None) -> None:
        """Advance virtual time by ``dt`` seconds (``settle`` as in ``advance_to``)."""
        if dt < 0:
            raise ValueError(f"cannot advance backwards (dt={dt})")
        await self.advance_to(self._t + dt, settle=settle)

    async def advance_to(self, t: float, *, settle: Callable[[], Awaitable[object]] | None = None) -> None:
        """Advance virtual time to absolute ``t``. A past ``t`` is a no-op.

        With ``settle``, sleepers due by ``t`` are woken one at a time and ``await settle()``
        runs before each wake and after the last, in place of the ``settle_rounds`` budget.
        """
        if t < self._t:
            return
        if settle is not None:
            await self._advance_settled(t, settle)
            return
        while True:
            self._drop_cancelled_tops()
            if not self._waiters or self._waiters[0].deadline > t:
                if not await self._poll_for_inrange(t):
                    break
                continue
            waiter = heapq.heappop(self._waiters)
            if waiter.cancelled:
                # Raced with cancellation between top-skip and pop; just retry.
                continue
            self._t = waiter.deadline
            waiter.event.set()
            for _ in range(self._settle_rounds):
                await anyio.lowlevel.checkpoint()
        self._t = max(self._t, t)

    async def _advance_settled(self, t: float, settle: Callable[[], Awaitable[object]]) -> None:
        while True:
            await settle()
            # No await between this check and the wake, so the popped waiter is live.
            if (deadline := self.next_deadline()) is None or deadline > t:
                break
            waiter = heapq.heappop(self._waiters)
            self._t = deadline
            waiter.event.set()
        self._t = max(self._t, t)

    def _drop_cancelled_tops(self) -> None:
        """Pop any cancelled entries sitting at the top of the heap."""
        while self._waiters and self._waiters[0].cancelled:
            heapq.heappop(self._waiters)

    async def _poll_for_inrange(self, t: float) -> bool:
        """Settle woken tasks and report whether a due sleeper appeared."""
        for _ in range(self._settle_rounds):
            await anyio.lowlevel.checkpoint()
        self._drop_cancelled_tops()
        return bool(self._waiters) and self._waiters[0].deadline <= t

    async def every(self, period: float, *, first: Literal["now", "after"] = "after") -> AsyncIterator[float]:
        if period <= 0:
            raise ValueError(f"every() period must be positive, got {period}")
        # Ticks are t0 + n * period, so float error does not accumulate over long runs.
        t0, n = self._t, 0 if first == "now" else 1
        while True:
            next_t = t0 + n * period
            if next_t > self._t:
                await self.wait_until(next_t)
            yield next_t
            # Skip missed ticks instead of catching up in a burst.
            n += 1
            while t0 + n * period <= self._t:
                n += 1
