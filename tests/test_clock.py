"""Clock behavior under WallClock and SimClock."""

from __future__ import annotations

import anyio
import pytest

from chrono_daemon import SimClock, WallClock

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


async def test_simclock_every_first_now_fires_immediately() -> None:
    """``first="now"`` fires at ``now()`` first, then every period."""
    clock = SimClock()
    ticks: list[float] = []

    async def ticker() -> None:
        async for t in clock.every(0.5, first="now"):
            ticks.append(t)
            if len(ticks) >= 3:
                return

    async with anyio.create_task_group() as tg:
        tg.start_soon(ticker)
        await anyio.sleep(0)
        await clock.advance(1.0)

    assert ticks == [0.0, 0.5, 1.0]


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


async def test_simclock_every_ticks_do_not_drift() -> None:
    """Ticks are ``t0 + n * period``: summing 0.1 a thousand times would miss these."""
    clock = SimClock(t0=1.0)
    ticks: list[float] = []

    async def ticker() -> None:
        async for t in clock.every(0.1):
            ticks.append(t)
            if len(ticks) >= 1000:
                return

    async with anyio.create_task_group() as tg:
        tg.start_soon(ticker)
        await anyio.sleep(0)
        await clock.advance(100.0)

    assert ticks == [1.0 + n * 0.1 for n in range(1, 1001)]


# -- SimClock: next_deadline and host-driven settle -----------------------------


async def test_simclock_next_deadline_skips_cancelled_sleepers() -> None:
    clock = SimClock()
    assert clock.next_deadline() is None
    early = anyio.CancelScope()

    async def cancellable() -> None:
        with early:
            await clock.wait_until(1.0)

    async with anyio.create_task_group() as tg:
        tg.start_soon(clock.wait_until, 3.0)
        tg.start_soon(cancellable)
        await anyio.wait_all_tasks_blocked()
        assert clock.next_deadline() == 1.0
        early.cancel()
        await anyio.wait_all_tasks_blocked()
        assert clock.next_deadline() == 3.0
        await clock.advance(5.0)
    assert clock.next_deadline() is None


async def test_simclock_settle_hook_wakes_one_sleeper_per_settle() -> None:
    """Each woken sleeper's work, longer than any checkpoint budget, finishes before the next wake; same-deadline
    sleepers wake in registration order and a follow-up sleep in range is woken in deadline order."""
    clock = SimClock(settle_rounds=1)
    log: list[tuple[str, str, float]] = []
    settles = 0

    async def settle() -> None:
        nonlocal settles
        settles += 1
        await anyio.wait_all_tasks_blocked()

    async def sleeper(name: str, deadline: float, then: float | None = None) -> None:
        await clock.wait_until(deadline)
        log.append(("wake", name, clock.now()))
        for _ in range(50):
            await anyio.lowlevel.checkpoint()
        log.append(("done", name, clock.now()))
        if then is not None:
            await sleeper(f"{name}'", then)

    async with anyio.create_task_group() as tg:
        for name, deadline, then in [("A", 1.0, 1.5), ("D", 2.0, None), ("B", 1.0, None), ("C", 1.0, None)]:
            tg.start_soon(sleeper, name, deadline, then)
            await anyio.wait_all_tasks_blocked()  # registered before the next starts: trio shuffles runnable tasks
        await clock.advance_to(2.0, settle=settle)

    order = [(name, t) for kind, name, t in log if kind == "wake"]
    assert order == [("A", 1.0), ("B", 1.0), ("C", 1.0), ("A'", 1.5), ("D", 2.0)]
    assert [kind for kind, _, _ in log] == ["wake", "done"] * 5
    assert settles == 6  # before each wake and after the last
    assert clock.now() == 2.0


async def test_simclock_settle_hook_runs_without_sleepers_and_stops_on_error() -> None:
    clock = SimClock()
    calls = 0

    async def settle() -> None:
        nonlocal calls
        calls += 1

    await clock.advance(1.0, settle=settle)
    assert (calls, clock.now()) == (1, 1.0)

    woke: list[float] = []

    async def sleeper(deadline: float) -> None:
        await clock.wait_until(deadline)
        woke.append(clock.now())

    async def failing() -> None:
        await anyio.wait_all_tasks_blocked()
        if woke:
            raise RuntimeError("host gave up")

    error: RuntimeError | None = None
    async with anyio.create_task_group() as tg:
        tg.start_soon(sleeper, 2.0)
        tg.start_soon(sleeper, 3.0)
        await anyio.wait_all_tasks_blocked()
        try:
            await clock.advance_to(4.0, settle=failing)
        except RuntimeError as exc:
            error = exc
        tg.cancel_scope.cancel()
    assert str(error) == "host gave up"
    assert woke == [2.0] and clock.now() == 2.0  # time stays at the last wake
