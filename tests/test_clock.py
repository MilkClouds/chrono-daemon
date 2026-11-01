"""Clock behavior under WallClock and SimClock."""

from __future__ import annotations

from typing import Any

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


# -- SimClock: host settle hook and deadline tolerance --------------------------


async def test_simclock_settle_hook_waits_for_host_work_before_next_wake() -> None:
    """A wake's host-known work, longer than the round budget, finishes before the next wake; same-deadline sleepers
    wake in registration order and a follow-up sleep in range wakes in deadline order."""
    clock = SimClock()
    log: list[tuple[str, str, float]] = []
    pending: list[anyio.Event] = []
    calls = 0

    async def settle() -> bool:
        nonlocal calls
        calls += 1
        if not pending:
            return False
        await pending.pop().wait()
        return True

    async def sleeper(name: str, deadline: float, then: float | None = None) -> None:
        await clock.wait_until(deadline)
        log.append(("wake", name, clock.now()))
        done = anyio.Event()
        pending.append(done)
        for _ in range(50):
            await anyio.lowlevel.checkpoint()
        log.append(("done", name, clock.now()))
        done.set()
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
    assert calls == 1 + 5 * 2 + 1  # first; per wake one wait and one "nothing left"; after the last
    assert clock.now() == 2.0


async def test_simclock_settle_hook_runs_without_sleepers() -> None:
    clock = SimClock()
    calls = 0

    async def settle() -> bool:
        nonlocal calls
        calls += 1
        return False

    await clock.advance(1.0, settle=settle)
    assert (calls, clock.now()) == (2, 1.0)  # first and last


async def test_simclock_settle_errors_and_max_settles_leave_time_at_last_wake() -> None:
    clock = SimClock()
    woke: list[float] = []

    async def sleeper(deadline: float) -> None:
        await clock.wait_until(deadline)
        woke.append(clock.now())

    async def failing() -> bool:
        if woke:
            raise ValueError("host gave up")
        return False

    async def always_waiting() -> bool:
        return True

    async with anyio.create_task_group() as tg:
        for d in (2.0, 3.0, 5.0):
            tg.start_soon(sleeper, d)
        await anyio.wait_all_tasks_blocked()
        with pytest.raises(ValueError, match="host gave up"):
            await clock.advance_to(4.0, settle=failing)
        assert woke == [2.0] and clock.now() == 2.0
        with pytest.raises(RuntimeError, match="after 3 settles at t=2.0"):
            await clock.advance_to(4.0, settle=always_waiting, max_settles=3)
        assert woke == [2.0] and clock.now() == 2.0  # unfinished work holds time before the next wake
        with pytest.raises(ValueError, match="max_settles"):
            await clock.advance_to(4.0, max_settles=0)
        tg.cancel_scope.cancel()


async def test_simclock_round_budget_bounds_unsettled_chains() -> None:
    """Without ``settle``, a woken chain finishes before the next wake only within ``settle_rounds`` hops."""

    async def run(settle_rounds: int, hops: int) -> list[str]:
        clock = SimClock(settle_rounds=settle_rounds)
        log: list[str] = []

        async def chained() -> None:
            await clock.wait_until(1.0)
            for _ in range(hops):
                await anyio.lowlevel.checkpoint()
            log.append("A done")

        async def later() -> None:
            await clock.wait_until(2.0)
            log.append("B woke")

        async with anyio.create_task_group() as tg:
            tg.start_soon(chained)
            tg.start_soon(later)
            await anyio.wait_all_tasks_blocked()
            await clock.advance_to(3.0)
        return log

    assert await run(8, 4) == ["A done", "B woke"]
    assert await run(64, 40) == ["A done", "B woke"]
    assert await run(1, 40) == ["B woke", "A done"]


async def test_simclock_tolerance_absorbs_float_mismatch_and_ends_exactly_at_t() -> None:
    """Deadlines just past ``t`` are due at ``t`` (rtol at large t, atol near 0); ``now()`` ends exactly at ``t``."""

    async def run(deadline: float, t: float, **tol: Any) -> tuple[list[float], float]:
        clock = SimClock()
        woke: list[float] = []

        async def sleeper() -> None:
            await clock.wait_until(deadline)
            woke.append(clock.now())

        async with anyio.create_task_group() as tg:
            tg.start_soon(sleeper)
            await anyio.wait_all_tasks_blocked()
            await clock.advance_to(t, **tol)  # library defaults unless given
            end = clock.now()
            tg.cancel_scope.cancel()
        return woke, end

    assert 0.1 * 3 > 0.3
    assert await run(0.1 * 3, 0.3) == ([0.3], 0.3)
    assert await run(0.1 * 3, 0.3, rtol=0.0) == ([], 0.3)
    # Summed float drift at large t: rtol scales with |t|, an absolute 1e-9 would not.
    drifted = sum([0.1] * 1_000_000)
    assert drifted - 100_000.0 > 1e-6
    assert await run(drifted, 100_000.0) == ([100_000.0], 100_000.0)
    assert await run(drifted, 100_000.0, rtol=0.0, atol=1e-9) == ([], 100_000.0)
    # Near 0, rtol * |t| is tiny; atol catches a deadline just past t.
    assert await run(1e-12, 1e-13) == ([], 1e-13)
    assert await run(1e-12, 1e-13, atol=1e-9) == ([1e-13], 1e-13)
    with pytest.raises(ValueError, match="rtol and atol"):
        await SimClock().advance(1.0, atol=-1.0)


async def test_simclock_tolerance_does_not_rewake_a_sleep_registered_at_t() -> None:
    """A re-sleep inside the tolerance of ``t`` waits for the next advance instead of looping at ``t``."""
    clock = SimClock()
    woke: list[float] = []

    async def tiny_loop() -> None:
        while True:
            await clock.sleep(1e-12)
            woke.append(clock.now())

    async with anyio.create_task_group() as tg:
        tg.start_soon(tiny_loop)
        await anyio.wait_all_tasks_blocked()
        with anyio.fail_after(5):
            await clock.advance_to(0.5e-12, atol=1e-9)
        assert woke == [0.5e-12]
        tg.cancel_scope.cancel()


async def test_simclock_tolerance_re_sleep_does_not_hide_an_older_due_sleeper() -> None:
    clock = SimClock()
    woke: list[str] = []

    async def first() -> None:
        await clock.wait_until(1 + 1e-10)
        woke.append("first")
        await clock.sleep(1e-12)  # registered at t: ahead of second in the heap, not due at t
        woke.append("first again")

    async def second() -> None:
        await clock.wait_until(1 + 2e-10)
        woke.append("second")

    async with anyio.create_task_group() as tg:
        tg.start_soon(first)
        await anyio.wait_all_tasks_blocked()
        tg.start_soon(second)
        await anyio.wait_all_tasks_blocked()
        await clock.advance_to(1.0)
        assert woke == ["first", "second"]
        await clock.advance_to(2.0)
    assert woke == ["first", "second", "first again"]


async def test_simclock_tolerance_never_wakes_an_infinite_sleep() -> None:
    clock = SimClock()
    woke: list[float] = []

    async def forever() -> None:
        await clock.sleep(float("inf"))
        woke.append(clock.now())

    async with anyio.create_task_group() as tg:
        tg.start_soon(forever)
        await anyio.wait_all_tasks_blocked()
        await clock.advance_to(1.0)
        assert woke == []
        tg.cancel_scope.cancel()


async def test_simclock_rejects_nested_and_concurrent_advances() -> None:
    clock = SimClock()

    async def nested() -> bool:
        await clock.advance_to(2.0)
        return False

    with pytest.raises(RuntimeError, match="already advancing"):
        await clock.advance_to(1.0, settle=nested)
    assert clock.now() == 0.0

    gate = anyio.Event()

    async def held() -> bool:
        await gate.wait()
        return False

    async with anyio.create_task_group() as tg:
        tg.start_soon(lambda: clock.advance_to(1.0, settle=held))
        await anyio.wait_all_tasks_blocked()
        with pytest.raises(RuntimeError, match="already advancing"):
            await clock.advance_to(2.0)
        gate.set()
    assert clock.now() == 1.0
    await clock.advance_to(2.0)  # the guard clears after an error too
    assert clock.now() == 2.0


async def test_simclock_settle_runs_before_the_first_wake_and_after_a_failed_advance() -> None:
    """Work pending when an advance starts, or left by one that failed, finishes before the next sleeper wakes."""
    clock = SimClock()
    log: list[str] = []
    state = {"busy": True, "finish": True}  # work started before the first advance

    async def settle() -> bool:
        if not state["busy"]:
            return False
        if state["finish"]:
            log.append(f"settled at {clock.now()}")
            state["busy"] = False
        return True

    async def sleeper(name: str, deadline: float) -> None:
        await clock.wait_until(deadline)
        log.append(name)
        state["busy"] = True

    async with anyio.create_task_group() as tg:
        tg.start_soon(sleeper, "a", 1.0)
        await anyio.wait_all_tasks_blocked()
        tg.start_soon(sleeper, "b", 2.0)
        await anyio.wait_all_tasks_blocked()
        await clock.advance_to(0.5, settle=settle)
        assert log == ["settled at 0.0"]
        state["finish"] = False  # a's work outlasts the budget
        with pytest.raises(RuntimeError, match="after 2 settles"):
            await clock.advance_to(3.0, settle=settle, max_settles=2)
        state["finish"] = True
        await clock.advance_to(3.0, settle=settle)
    assert log == ["settled at 0.0", "a", "settled at 1.0", "b", "settled at 2.0"]


async def test_simclock_tolerance_wakes_many_sleepers_without_rescanning_the_heap() -> None:
    """4000 sleepers at ``0.1 * 3`` wake at ``0.3`` in well under a second; a rescan per wake took ~14 s."""
    clock = SimClock()
    woke = 0

    async def sleeper() -> None:
        nonlocal woke
        await clock.wait_until(0.1 * 3)
        woke += 1

    async with anyio.create_task_group() as tg:
        for _ in range(4000):
            tg.start_soon(sleeper)
        await anyio.wait_all_tasks_blocked()
        with anyio.fail_after(6):
            await clock.advance_to(0.3)
    assert woke == 4000


async def test_simclock_tolerance_re_sleeps_at_t_are_set_aside_once() -> None:
    """2000 tolerance wakes that each re-sleep inside the tolerance stay fast (re-scanning them per wake took ~10 s)."""
    clock = SimClock()
    woke = again = 0

    async def sleeper() -> None:
        nonlocal woke, again
        await clock.wait_until(1 + 2e-10)
        woke += 1
        await clock.sleep(1e-12)
        again += 1

    async with anyio.create_task_group() as tg:
        for _ in range(2000):
            tg.start_soon(sleeper)
        await anyio.wait_all_tasks_blocked()
        with anyio.fail_after(4):
            await clock.advance_to(1.0)
        assert (woke, again) == (2000, 0)
        await clock.advance_to(2.0)
    assert again == 2000


async def test_simclock_exclusive_advance_leaves_sleepers_at_t_for_the_next_advance() -> None:
    """``inclusive=False`` wakes what is due before ``t``, ends at ``t``, and leaves ``t``'s sleepers (both sides of
    the tolerance) to a following ``advance_to(t)``, which sees the host's input for ``t`` first."""
    clock = SimClock()
    log: list[tuple[str, float, Any]] = []
    inbox: list[str] = []

    async def sleeper(name: str, deadline: float) -> None:
        await clock.wait_until(deadline)
        log.append((name, clock.now(), list(inbox)))

    below = 0.3 - 0.3 * 1e-12
    assert 0.1 * 3 > 0.3 > below
    async with anyio.create_task_group() as tg:
        for name, deadline in [("early", 0.2), ("above", 0.1 * 3), ("exact", 0.3), ("below", below), ("late", 0.4)]:
            tg.start_soon(sleeper, name, deadline)
            await anyio.wait_all_tasks_blocked()
        await clock.advance_to(0.3, inclusive=False)
        assert clock.now() == 0.3
        assert log == [("early", 0.2, [])]
        inbox.append("obs@0.3")
        await clock.advance_to(0.3)
        assert [name for name, *_ in log] == ["early", "below", "exact", "above"]
        assert all(now == 0.3 and seen == ["obs@0.3"] for _, now, seen in log[1:])
        await clock.advance(0.1, inclusive=False)  # `advance` forwards it; a deadline exactly at t stays
        assert [name for name, *_ in log][-1] == "above"
        await clock.advance_to(0.4)
        assert log[-1][:2] == ("late", 0.4)
        # rtol=atol=0: only exactly t stays; just below it is before t.
        tg.start_soon(sleeper, "tight-below", 0.5 - 1e-15)
        await anyio.wait_all_tasks_blocked()
        await clock.advance_to(0.5, inclusive=False, rtol=0.0)
        assert log[-1][0] == "tight-below"


async def test_simclock_exclusive_then_inclusive_runs_input_work_before_timers_at_t() -> None:
    """Work the host's input starts at ``t`` settles (pre-wake settle) before a sleeper due at ``t`` wakes."""
    clock = SimClock()
    log: list[str] = []
    pending = {"n": 0}
    done = anyio.Event()

    async def settle() -> bool:
        if not pending["n"]:
            return False
        await done.wait()
        pending["n"] = 0
        return True

    async def tick() -> None:
        await clock.wait_until(1.0)
        log.append(f"tick@{clock.now()}")

    async def handle_input() -> None:
        await anyio.sleep(0.01)  # real work outside the clock, tracked by the host
        log.append(f"input@{clock.now()}")
        done.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(tick)
        await anyio.wait_all_tasks_blocked()
        await clock.advance_to(1.0, settle=settle, inclusive=False)
        pending["n"] = 1
        tg.start_soon(handle_input)
        await clock.advance_to(1.0, settle=settle)
    assert log == ["input@1.0", "tick@1.0"]
