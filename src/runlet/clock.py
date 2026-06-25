"""Clock abstractions: ``WallClock`` for production, ``SimClock`` for deterministic replay.

The ``Clock`` protocol is the only async-time interface daemons should touch. Calling
``anyio.sleep`` directly bypasses ``SimClock``, which would silently break burst-step
replay determinism — so the rule (enforced by convention) is: always reach for
``ctx.clock.sleep(...)`` or ``ctx.clock.wait_until(...)``.

``SimClock`` lets a driver task advance virtual time in bulk via ``await clock.advance(dt)``.
Sleepers register a deadline; ``advance_to`` walks the heap, sets each woken sleeper's
``anyio.Event``, and yields a checkpoint so the woken task can run and possibly register
a new sleep before the next iteration.
"""

from __future__ import annotations

import heapq
import itertools
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Protocol

import anyio
import anyio.lowlevel

__all__ = ["Clock", "SimClock", "WallClock"]

# Default number of checkpoints yielded between heap inspections to let woken
# tasks resume and register follow-up sleeps. anyio does not expose a portable
# "run until all tasks are idle" primitive, so SimClock makes this settlement
# budget explicit and configurable instead of pretending it can infer global
# quiescence.
_DEFAULT_SETTLE_ROUNDS = 8


class Clock(Protocol):
    """Minimal time interface. All async code in runlet should use this — never anyio.sleep directly."""

    def now(self) -> float:
        """Return current time in seconds. Monotonic; the units (epoch vs zero-based) are clock-specific."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Sleep for ``seconds`` of clock-time. Non-positive values yield once and return."""
        ...

    async def wait_until(self, deadline: float) -> None:
        """Sleep until absolute clock-time ``deadline``. Past deadlines yield once and return."""
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
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await anyio.lowlevel.checkpoint()
            return
        await anyio.sleep(seconds)

    async def wait_until(self, deadline: float) -> None:
        await self.sleep(deadline - self.now())

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


@dataclass
class _Waiter:
    """One sleeper registered against a SimClock.

    ``cancelled`` is a lazy-delete flag: a cancelled sleeper leaves its entry in
    the heap and ``advance_to`` skips it on pop. This is the standard heapq
    pattern for cheap cancellation (avoids the O(n) list remove + heapify the
    old eager-delete code used).
    """

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
            # Lazy delete: mark cancelled and let advance_to skip on pop. Cheaper
            # than the O(n) list-remove + heapify the older eager-delete used,
            # and asymptotically correct because cancelled entries cannot
            # outlive the pops that would have woken them. We also opportunistically
            # drain cancelled entries from the top of the heap so a lone cancelled
            # sleeper doesn't linger past its own cancellation.
            if not waiter.event.is_set():
                waiter.cancelled = True
                while self._waiters and self._waiters[0].cancelled:
                    heapq.heappop(self._waiters)

    async def advance(self, dt: float) -> None:
        """Advance virtual time by ``dt`` seconds."""
        if dt < 0:
            raise ValueError(f"cannot advance backwards (dt={dt})")
        await self.advance_to(self._t + dt)

    async def advance_to(self, t: float) -> None:
        """Advance virtual time to absolute ``t``. Idempotent for already-past values.

        Wakes sleepers one at a time in (deadline, insertion-order) order. After
        each wake we yield ``settle_rounds`` checkpoints so the woken task can
        run, send/receive through downstream channels, and possibly register a
        follow-up sleep before the next heap inspection. When the heap settles
        with nothing due by ``t``, we yield the same budget once more and
        finalize if no new in-range sleeper appeared.
        """
        if t < self._t:
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

    def _drop_cancelled_tops(self) -> None:
        """Pop any cancelled entries sitting at the top of the heap."""
        while self._waiters and self._waiters[0].cancelled:
            heapq.heappop(self._waiters)

    async def _poll_for_inrange(self, t: float) -> bool:
        """Yield ``settle_rounds`` checkpoints and report whether a new
        in-range sleeper appeared during the settle. The full budget is
        required: a shorter poll races against in-flight task chains that
        register their follow-up sleeps several checkpoints after waking.
        """
        for _ in range(self._settle_rounds):
            await anyio.lowlevel.checkpoint()
        self._drop_cancelled_tops()
        return bool(self._waiters) and self._waiters[0].deadline <= t

    async def every(self, period: float) -> AsyncIterator[float]:
        if period <= 0:
            raise ValueError(f"every() period must be positive, got {period}")
        next_t = self._t + period
        while True:
            delta = next_t - self._t
            if delta > 0:
                await self.wait_until(next_t)
            yield next_t
            # Maintain a monotonic-target schedule (the Clock protocol contract):
            # if virtual time has already passed beyond the next target — e.g.
            # because a single advance(dt) overshoots multiple periods — skip the
            # missed ticks rather than firing them all in a catch-up burst.
            next_t += period
            now = self._t
            while next_t <= now:
                next_t += period
