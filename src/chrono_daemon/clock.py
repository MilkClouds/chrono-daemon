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

    async def sleep(self, seconds: float, *, priority: int = 0) -> None:
        """Sleep for ``seconds`` of clock-time. Non-positive values yield once and return.

        ``priority`` orders wakes at the same instant on a ``SimClock`` (lower first); real time ignores it.
        """
        ...

    async def wait_until(self, deadline: float, *, priority: int = 0) -> None:
        """Sleep until absolute clock-time ``deadline``. Past deadlines yield once and return."""
        ...

    def every(
        self, period: float, *, first: Literal["now", "after"] = "after", priority: int = 0
    ) -> AsyncIterator[float]:
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

    async def sleep(self, seconds: float, *, priority: int = 0) -> None:
        if seconds <= 0:
            await anyio.lowlevel.checkpoint()
            return
        await anyio.sleep(seconds)

    async def wait_until(self, deadline: float, *, priority: int = 0) -> None:
        await self.sleep(deadline - self.now())

    async def every(
        self, period: float, *, first: Literal["now", "after"] = "after", priority: int = 0
    ) -> AsyncIterator[float]:
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
    """One sleeper (``event``) or ``call_at`` callback registered against a SimClock."""

    deadline: float
    priority: int
    seq: int
    event: anyio.Event | None = field(compare=False)
    registered_at: float = field(compare=False)
    callback: Callable[[], object] | None = field(default=None, compare=False)
    cancelled: bool = field(default=False, compare=False)

    def __lt__(self, other: _Waiter) -> bool:
        return (self.deadline, self.priority, self.seq) < (other.deadline, other.priority, other.seq)

    def cancel(self) -> None:
        self.cancelled = True


class SimClock:
    """Deterministic virtual clock.

    Time only moves when a driver task calls ``advance(dt)`` or ``advance_to(t)``.
    All sleepers are released exactly at their registered deadline, in deadline order;
    those at the same instant by ``priority`` (lower first), then insertion order. ``settle_rounds`` checkpoints after each wake
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
        self._deferred: list[_Waiter] = []  # re-sleeps at the target inside its tolerance, back in the heap after
        # The current instant: its anchor (earliest deadline), latest member deadline, first seq registered during
        # it, and a heap of its entries by (priority, seq).
        self._anchor: float | None = None
        self._instant_end = -math.inf
        self._instant_seq = 0
        self._instant: list[tuple[int, int, _Waiter]] = []

    def now(self) -> float:
        return self._t

    async def sleep(self, seconds: float, *, priority: int = 0) -> None:
        # Checkpoint first so a freshly-cancelled caller exits before enqueueing.
        await anyio.lowlevel.checkpoint()
        if seconds <= 0:
            return
        await self.wait_until(self._t + seconds, priority=priority)

    async def wait_until(self, deadline: float, *, priority: int = 0) -> None:
        # Checkpoint first so a freshly-cancelled caller exits before enqueueing.
        await anyio.lowlevel.checkpoint()
        if deadline <= self._t:
            return
        event = anyio.Event()
        waiter = _Waiter(deadline, priority, next(self._seq), event, registered_at=self._t)
        heapq.heappush(self._waiters, waiter)
        try:
            await event.wait()
        finally:
            # Lazy delete: skip cancelled entries on pop instead of removing
            # them from the middle of the heap.
            if not event.is_set():
                waiter.cancelled = True
                while self._waiters and self._waiters[0].cancelled:
                    heapq.heappop(self._waiters)

    def call_at(self, deadline: float, callback: Callable[[], object], *, priority: int = 0) -> Callable[[], None]:
        """Run ``callback()`` in the advance that reaches ``deadline``, ordered like a sleeper there; returns its
        cancel. A past ``deadline`` runs in the next advance. An exception from it propagates out of the advance."""
        waiter = _Waiter(deadline, priority, next(self._seq), None, registered_at=self._t, callback=callback)
        heapq.heappush(self._waiters, waiter)
        return waiter.cancel

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

        Due sleepers wake one at a time in (deadline, priority, registration) order; deadlines close to each other
        under the tolerance below are one instant, ordered by (priority, registration) and seeing one ``now()``. After each wake, and once
        more when none is due, ``settle_rounds`` checkpoints run; then, if given, ``await settle()``
        runs, and while it returns True (it waited for host-known work) the rounds and ``settle``
        repeat. With ``settle``, this also runs first, before any wake. ``max_settles`` True results in a row raise ``RuntimeError``, leaving time at the
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
            # An instant left by a failure goes back to the heap: the next advance re-forms it under its own target
            # and tolerance, with anything registered in between.
            for w in (*self._deferred, *(w for _, _, w in self._instant)):
                heapq.heappush(self._waiters, w)
            self._deferred.clear()
            self._instant.clear()
            self._anchor = None
        self._t = t

    async def _advance_to(
        self, t: float, settle: Callable[[], Awaitable[bool]] | None, max_settles: int, rtol: float, atol: float
    ) -> None:
        if settle is not None:  # work started before this advance (or left by a failed one) finishes at now()
            await self._settle(settle, max_settles)
        while True:
            if (waiter := self._pop_due(t, rtol, atol)) is None:
                await self._settle(settle, max_settles)  # once more at the end: woken work may sleep again in range
                if self._pop_due(t, rtol, atol, peek=True) is None:
                    return
                continue
            if waiter.callback is not None:
                waiter.callback()
            else:
                assert waiter.event is not None
                waiter.event.set()
            await self._settle(settle, max_settles)

    def _pop_due(self, t: float, rtol: float, atol: float, *, peek: bool = False) -> _Waiter | None:
        """Remove (or with ``peek``, find) the next entry due by ``t``, or ``None``, setting ``now()`` to its instant.

        An instant is the earliest due deadline (its anchor) and every due deadline close to it, served in
        (priority, seq) order at ``min(t, latest deadline)``: no entry wakes before its deadline (but for ``t``'s
        tolerance) and time never moves back. An entry registered during the instant joins it only at or before
        ``now()``, so a tiny re-sleep cannot keep it open.
        """
        while True:
            if self._anchor is None:
                if (first := self._heap_due(t, rtol, atol)) is None:
                    return None
                self._anchor, self._instant_end, self._instant_seq = first.deadline, -math.inf, next(self._seq)
            while (w := self._heap_due(t, rtol, atol)) is not None and (
                w.deadline <= self._t
                or (w.seq < self._instant_seq and math.isclose(w.deadline, self._anchor, rel_tol=rtol, abs_tol=atol))
            ):
                heapq.heappop(self._waiters)
                heapq.heappush(self._instant, (w.priority, w.seq, w))
                self._instant_end = max(self._instant_end, w.deadline)
            while self._instant and self._instant[0][2].cancelled:
                heapq.heappop(self._instant)
            if self._instant:
                self._t = max(self._t, min(t, self._instant_end))
                return self._instant[0][2] if peek else heapq.heappop(self._instant)[2]
            self._anchor = None

    def _heap_due(self, t: float, rtol: float, atol: float) -> _Waiter | None:
        """The heap's earliest live entry if it is due by ``t``, else ``None``.

        Only sleepers registered before time reached ``t`` get the tolerance, so a tiny re-sleep at ``t`` cannot keep
        waking forever. Such re-sleeps are set aside until the advance ends, so they hide no older due sleeper.
        """
        while self._waiters:
            w = self._waiters[0]
            close = math.isfinite(w.deadline) and w.deadline <= t + max(rtol * max(abs(w.deadline), abs(t)), atol)
            if w.cancelled or (close and w.deadline > t and w.registered_at >= t):
                heapq.heappop(self._waiters)
                if not w.cancelled:
                    self._deferred.append(w)
            elif w.deadline <= t or close:
                return w
            else:
                return None
        return None

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

    async def every(
        self, period: float, *, first: Literal["now", "after"] = "after", priority: int = 0
    ) -> AsyncIterator[float]:
        if period <= 0:
            raise ValueError(f"every() period must be positive, got {period}")
        # Ticks are t0 + n * period, so float error does not accumulate over long runs.
        t0, n = self._t, 0 if first == "now" else 1
        while True:
            next_t = t0 + n * period
            if next_t > self._t:
                await self.wait_until(next_t, priority=priority)
            yield next_t
            # Skip missed ticks instead of catching up in a burst.
            n += 1
            while t0 + n * period <= self._t:
                n += 1
