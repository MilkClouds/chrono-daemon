"""Clock behavior under WallClock and SimClock."""

from __future__ import annotations

import anyio
import pytest

from runlet import SimClock, WallClock

pytestmark = pytest.mark.anyio


# -- WallClock -----------------------------------------------------------------


def test_wallclock_now_is_sync_safe() -> None:
    clock = WallClock()
    assert isinstance(clock.now(), float)


async def test_wallclock_now_is_monotonic() -> None:
    clock = WallClock()
    t0 = clock.now()
    await anyio.sleep(0.01)
    t1 = clock.now()
    assert t1 >= t0
    assert t1 - t0 >= 0.005  # at least roughly the slept duration


async def test_wallclock_sleep_zero_is_a_checkpoint() -> None:
    clock = WallClock()
    await clock.sleep(0)
    await clock.sleep(-1)


async def test_wallclock_wait_until_past_deadline_is_a_checkpoint() -> None:
    clock = WallClock()
    await clock.wait_until(clock.now() - 1.0)


# -- SimClock ------------------------------------------------------------------


async def test_simclock_now_does_not_advance_without_advance() -> None:
    clock = SimClock(t0=10.0)
    assert clock.now() == 10.0
    await anyio.sleep(0)
    assert clock.now() == 10.0


async def test_simclock_burst_step_deterministic_order() -> None:
    """CANARY: three sleepers wake at t=1/2/3 in that exact order under one advance(5)."""
    clock = SimClock()
    log: list[tuple[str, float]] = []

    async def sleeper(name: str, dt: float) -> None:
        await clock.sleep(dt)
        log.append((name, clock.now()))

    async with anyio.create_task_group() as tg:
        tg.start_soon(sleeper, "A", 1.0)
        tg.start_soon(sleeper, "B", 2.0)
        tg.start_soon(sleeper, "C", 3.0)
        await anyio.sleep(0)
        await clock.advance(5.0)

    assert log == [("A", 1.0), ("B", 2.0), ("C", 3.0)]
    assert clock.now() == 5.0


async def test_simclock_nested_sleeps_advance_consistently() -> None:
    """A task that sleeps 1s then 2s more should wake at t=1 and t=3."""
    clock = SimClock()
    log: list[float] = []

    async def worker() -> None:
        await clock.sleep(1.0)
        log.append(clock.now())
        await clock.sleep(2.0)
        log.append(clock.now())

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker)
        await anyio.sleep(0)
        await clock.advance(5.0)

    assert log == [1.0, 3.0]


async def test_simclock_advance_stops_at_deadline() -> None:
    clock = SimClock()

    async def worker() -> None:
        await clock.sleep(2.0)

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker)
        await anyio.sleep(0)
        await clock.advance(0.5)
        assert clock.now() == 0.5
        await clock.advance(1.5)

    assert clock.now() == 2.0


async def test_simclock_advance_to_idempotent_past() -> None:
    clock = SimClock(t0=5.0)
    await clock.advance_to(3.0)  # should be no-op
    assert clock.now() == 5.0


async def test_simclock_wait_until_absolute_deadline() -> None:
    clock = SimClock(t0=10.0)
    log: list[float] = []

    async def worker() -> None:
        await clock.wait_until(12.5)
        log.append(clock.now())

    async with anyio.create_task_group() as tg:
        tg.start_soon(worker)
        await anyio.sleep(0)
        await clock.advance_to(20.0)

    assert log == [12.5]
    assert clock.now() == 20.0


async def test_simclock_wait_until_past_deadline_is_a_checkpoint() -> None:
    clock = SimClock(t0=10.0)
    await clock.wait_until(9.0)
    assert clock.now() == 10.0


async def test_simclock_advance_negative_raises() -> None:
    clock = SimClock()
    with pytest.raises(ValueError):
        await clock.advance(-1.0)


def test_simclock_settle_rounds_must_be_positive() -> None:
    with pytest.raises(ValueError, match="settle_rounds"):
        SimClock(settle_rounds=0)


async def test_simclock_every_yields_at_each_period() -> None:
    clock = SimClock()
    ticks: list[float] = []

    async def ticker() -> None:
        async for t in clock.every(0.5):
            ticks.append(t)
            if len(ticks) >= 4:
                return

    async with anyio.create_task_group() as tg:
        tg.start_soon(ticker)
        await anyio.sleep(0)
        await clock.advance(2.5)

    assert ticks == [0.5, 1.0, 1.5, 2.0]


async def test_simclock_sleep_zero_is_a_checkpoint() -> None:
    clock = SimClock(t0=42.0)
    await clock.sleep(0)
    assert clock.now() == 42.0


async def test_simclock_every_skips_missed_ticks_like_wallclock() -> None:
    """An overshooting advance yields one tick, not a catch-up burst."""
    clock = SimClock()
    seen: list[float] = []

    async def ticker() -> None:
        async for t in clock.every(0.1):
            seen.append(t)
            if len(seen) >= 1:
                return

    async with anyio.create_task_group() as tg:
        tg.start_soon(ticker)
        await anyio.sleep(0)
        await clock.advance(0.5)

    assert seen == [0.1]


async def test_simclock_sleep_removes_entry_on_cancellation() -> None:
    """Cancelled sleepers must not accumulate in the heap."""
    import anyio.lowlevel

    clock = SimClock()
    started = anyio.Event()

    async def sleeper() -> None:
        started.set()
        await clock.sleep(100.0)

    async with anyio.create_task_group() as tg:
        tg.start_soon(sleeper)
        await started.wait()
        for _ in range(5):
            await anyio.lowlevel.checkpoint()
        assert len(clock._waiters) == 1
        tg.cancel_scope.cancel()

    assert len(clock._waiters) == 0, "cancelled sleepers must leave the heap"


async def test_simclock_mid_heap_cancellation_cleared_by_advance() -> None:
    """A cancelled mid-heap entry is skipped during advance."""
    import anyio.lowlevel

    clock = SimClock()
    started_a, started_b, started_c = anyio.Event(), anyio.Event(), anyio.Event()
    log: list[str] = []

    async def sleeper(name: str, dt: float, started: anyio.Event) -> None:
        started.set()
        try:
            await clock.sleep(dt)
        except BaseException:
            log.append(f"{name}-cancelled")
            raise
        else:
            log.append(f"{name}-woke")

    b_scope: anyio.CancelScope | None = None

    async def b_with_scope() -> None:
        nonlocal b_scope
        with anyio.CancelScope() as scope:
            b_scope = scope
            await sleeper("B", 2.0, started_b)

    async with anyio.create_task_group() as tg:
        tg.start_soon(sleeper, "A", 1.0, started_a)
        tg.start_soon(b_with_scope)
        tg.start_soon(sleeper, "C", 3.0, started_c)
        await started_a.wait()
        await started_b.wait()
        await started_c.wait()
        for _ in range(5):
            await anyio.lowlevel.checkpoint()
        assert b_scope is not None
        b_scope.cancel()
        await clock.advance(5.0)

    assert "A-woke" in log
    assert "C-woke" in log
    assert "B-cancelled" in log
    assert all(w.cancelled for w in clock._waiters)


async def test_simclock_advance_breaks_early_when_no_followup_sleep() -> None:
    """Empty-heap polling still catches later in-range sleepers."""
    clock = SimClock()
    woke: list[str] = []

    async def one_shot(name: str, dt: float) -> None:
        await clock.sleep(dt)
        woke.append(name)

    async with anyio.create_task_group() as tg:
        for i, dt in enumerate([0.5, 1.0, 1.5]):
            tg.start_soon(one_shot, f"d{i}", dt)
        await anyio.sleep(0)
        await clock.advance(2.0)

    assert woke == ["d0", "d1", "d2"]
    assert clock.now() == 2.0
