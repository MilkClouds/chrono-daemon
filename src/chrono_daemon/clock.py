"""Clock abstractions for real time and deterministic replay.

Daemons should use ``ctx.clock`` instead of ``anyio.sleep`` so ``SimClock`` can
control time during tests.
"""

from __future__ import annotations

import heapq
import itertools
import math
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
_DEFAULT_MAX_SETTLES = 1000


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
    registered_at: float = field(compare=False)
    cancelled: bool = field(default=False, compare=False)

    def __lt__(self, other: _Waiter) -> bool:
        return (self.deadline, self.seq) < (other.deadline, other.seq)


class SimClock:
    """Deterministic virtual clock.

    Time only moves when a driver task calls ``advance(dt)`` or ``advance_to(t)``.
    All sleepers are released exactly at their registered deadline, in deadline order,
    with insertion order as the tiebreaker. ``settle_rounds`` checkpoints after each wake
    let woken work run; a driver that knows of more pending work passes ``settle=``
    to ``advance_to``; see ADR 0012.
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
        self._advancing = False

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
        waiter = _Waiter(deadline=deadline, seq=next(self._seq), event=anyio.Event(), registered_at=self._t)
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

    async def advance(
        self,
        dt: float,
        *,
        settle: Callable[[], Awaitable[bool]] | None = None,
        max_settles: int = _DEFAULT_MAX_SETTLES,
        rtol: float = 1e-9,
        atol: float = 0.0,
    ) -> None:
        """Advance virtual time by ``dt`` seconds; keywords as in ``advance_to``."""
        if dt < 0:
            raise ValueError(f"cannot advance backwards (dt={dt})")
        await self.advance_to(self._t + dt, settle=settle, max_settles=max_settles, rtol=rtol, atol=atol)

    async def advance_to(
        self,
        t: float,
        *,
        settle: Callable[[], Awaitable[bool]] | None = None,
        max_settles: int = _DEFAULT_MAX_SETTLES,
        rtol: float = 1e-9,
        atol: float = 0.0,
    ) -> None:
        """Advance virtual time to absolute ``t``. A past ``t`` is a no-op; ``now()`` ends exactly at ``t``.

        Due sleepers wake one at a time in (deadline, registration) order. After each wake, and once
        more when none is due, ``settle_rounds`` checkpoints run; then, if given, ``await settle()``
        runs, and while it returns True (it waited for host-known work) the rounds and ``settle``
        repeat. ``max_settles`` True results in a row raise ``RuntimeError``, leaving time at the
        last wake. One advance at a time: a concurrent or nested one raises. See ADR 0012.

        A deadline ``d`` registered before time reached ``t`` is due at ``t`` when it is close to ``t``
        in ``math.isclose``'s symmetric sense: ``d <= t + max(rtol * max(abs(d), abs(t)), atol)``.
        ``rtol=atol=0`` disables the tolerance.
        """
        if max_settles < 1:
            raise ValueError(f"max_settles must be >= 1, got {max_settles}")
        if rtol < 0 or atol < 0:
            raise ValueError(f"rtol and atol must be >= 0, got rtol={rtol}, atol={atol}")
        if self._advancing:
            raise RuntimeError("SimClock is already advancing: one driver at a time, and not from settle()")
        if t < self._t:
            return
        self._advancing = True
        try:
            await self._advance_to(t, settle, max_settles, rtol, atol)
        finally:
            self._advancing = False
        self._t = t

    async def _advance_to(
        self, t: float, settle: Callable[[], Awaitable[bool]] | None, max_settles: int, rtol: float, atol: float
    ) -> None:
        while True:
            if (waiter := self._pop_due(t, rtol, atol)) is None:
                await self._settle(settle, max_settles)  # once more at the end: woken work may sleep again in range
                if self._pop_due(t, rtol, atol, peek=True) is None:
                    return
                continue
            self._t = min(waiter.deadline, t)  # never past t, even for a deadline inside the tolerance
            waiter.event.set()
            await self._settle(settle, max_settles)

    def _pop_due(self, t: float, rtol: float, atol: float, *, peek: bool = False) -> _Waiter | None:
        """Remove and return the earliest sleeper due by ``t``, or ``None``.

        Only sleepers registered before time reached ``t`` get the tolerance, so a tiny re-sleep at ``t`` cannot keep
        waking forever (nor hide an older sleeper due within it).
        """
        self._drop_cancelled_tops()
        if not self._waiters:
            return None
        if self._waiters[0].deadline <= t:
            return self._waiters[0] if peek else heapq.heappop(self._waiters)
        due = [
            w
            for w in self._waiters
            if not w.cancelled
            and math.isfinite(w.deadline)
            and w.registered_at < t
            and w.deadline <= t + max(rtol * max(abs(w.deadline), abs(t)), atol)
        ]
        if not due or peek:
            return min(due, default=None)
        waiter = min(due)
        self._waiters = [w for w in self._waiters if w is not waiter]
        heapq.heapify(self._waiters)
        return waiter

    async def _settle(self, settle: Callable[[], Awaitable[bool]] | None, max_settles: int) -> None:
        """Run the round budget, then ``settle`` until it has nothing to wait for."""
        for _ in range(max_settles):
            for _ in range(self._settle_rounds):
                await anyio.lowlevel.checkpoint()
            if settle is None or not await settle():
                return
        raise RuntimeError(f"settle() still waiting after {max_settles} settles at t={self._t}")

    def _drop_cancelled_tops(self) -> None:
        """Pop any cancelled entries sitting at the top of the heap."""
        while self._waiters and self._waiters[0].cancelled:
            heapq.heappop(self._waiters)

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
