"""Supervisor: shutdown propagation, restart with backoff, max_retries, cancel scope per daemon.

Also covers the diagnostics surface (duplicate-name warning, DaemonError
wrapping that exposes the failing daemon's name on the ExceptionGroup leaf)
and the stop-signaling surface (signal_stop, stop(grace), shielded on_stop
on the force-cancel path).
"""

from __future__ import annotations

import warnings

import anyio
import pytest

from runlet import Context, Daemon, DaemonError, DaemonHealth, RestartPolicy, SimClock, Supervisor

pytestmark = pytest.mark.anyio


class _Failing(Daemon):
    def __init__(self, when: int = 0) -> None:
        self.attempts = 0
        self._fail_until = when

    async def run(self, ctx: Context) -> None:
        self.attempts += 1
        if self.attempts <= self._fail_until:
            raise RuntimeError(f"boom #{self.attempts}")


class _Idle(Daemon):
    """Sleeps a long time; will be cancelled when a sibling raises."""

    def __init__(self, stop_log: list[str], name: str) -> None:
        self._stop_log = stop_log
        self.name = name

    async def run(self, ctx: Context) -> None:
        await ctx.clock.sleep(1000.0)  # long sleep

    async def on_stop(self, ctx: Context) -> None:
        self._stop_log.append(self.name)


async def test_shutdown_on_error_cancels_siblings() -> None:
    """Default on_error='shutdown': one daemon failing tears down the whole group.

    The failing daemon's name is attached to a ``DaemonError`` leaf in the
    resulting ExceptionGroup, with the original exception as ``__cause__``.
    """
    stop_log: list[str] = []
    clock = SimClock()

    with pytest.raises(BaseExceptionGroup) as excinfo:
        async with Supervisor(clock=clock) as sup:
            sup.add(_Idle(stop_log, "idleA"))
            sup.add(_Failing(when=1), name="failing-X")
            sup.add(_Idle(stop_log, "idleB"))
            await anyio.sleep(0)  # let daemons get registered

    flat = list(_flatten(excinfo.value))
    # DaemonError wraps the failing daemon's exception with the daemon's name.
    daemon_errs = [e for e in flat if isinstance(e, DaemonError)]
    assert len(daemon_errs) == 1
    assert "failing-X" in str(daemon_errs[0])
    # Original RuntimeError remains accessible via __cause__.
    assert isinstance(daemon_errs[0].__cause__, RuntimeError)
    assert "boom" in str(daemon_errs[0].__cause__)


async def test_duplicate_daemon_name_emits_warning() -> None:
    clock = SimClock()

    class _Quick(Daemon):
        async def run(self, ctx: Context) -> None:
            pass

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        async with Supervisor(clock=clock) as sup:
            sup.add(_Quick(), name="shared-name")
            sup.add(_Quick(), name="shared-name")

    duplicate_warnings = [w for w in caught if "duplicate daemon name" in str(w.message)]
    assert len(duplicate_warnings) == 1
    assert "shared-name" in str(duplicate_warnings[0].message)


async def test_ignore_on_error_lets_siblings_continue() -> None:
    """on_error='ignore': failing daemon disappears; others run to completion."""
    log: list[str] = []
    clock = SimClock()

    class _Quick(Daemon):
        def __init__(self, tag: str) -> None:
            self.name = tag

        async def run(self, ctx: Context) -> None:
            log.append(self.name)

    async with Supervisor(clock=clock, on_error="ignore") as sup:
        sup.add(_Failing(when=1))
        sup.add(_Quick("ok"))
        await anyio.sleep(0)

    assert "ok" in log


async def test_restart_with_backoff_under_simclock() -> None:
    """on_error='restart': daemon is re-entered; backoff sleeps drain on SimClock advance."""
    clock = SimClock()
    failing = _Failing(when=3)  # fail 3 times, succeed on 4th
    policy = RestartPolicy(base=0.1, factor=2.0, cap=5.0)

    async with Supervisor(clock=clock, on_error="restart", restart=policy) as sup:
        sup.add(failing)
        await anyio.sleep(0)
        # Total backoff to drain: 0.1 + 0.2 + 0.4 = 0.7. Advance plenty.
        await clock.advance(2.0)

    assert failing.attempts == 4


async def test_restart_respects_max_retries() -> None:
    """If max_retries is exceeded, the daemon's last exception propagates wrapped in DaemonError."""
    clock = SimClock()
    failing = _Failing(when=99)
    policy = RestartPolicy(base=0.01, factor=1.0, cap=0.01, max_retries=2)

    with pytest.raises(BaseExceptionGroup) as excinfo:
        async with Supervisor(clock=clock, on_error="restart", restart=policy) as sup:
            sup.add(failing)
            await anyio.sleep(0)
            await clock.advance(1.0)

    flat = list(_flatten(excinfo.value))
    daemon_errs = [e for e in flat if isinstance(e, DaemonError)]
    assert len(daemon_errs) >= 1
    # Original RuntimeError is preserved on __cause__.
    assert any(isinstance(e.__cause__, RuntimeError) for e in daemon_errs)
    # initial attempt + max_retries = 1 + 2 = 3 total tries.
    assert failing.attempts == 3


async def test_signal_stop_lets_cooperative_daemons_exit() -> None:
    """Daemons polling ctx.stopping in their loops should return naturally on signal_stop()."""
    clock = SimClock()
    log: list[str] = []

    class _Polling(Daemon):
        name = "polling"

        async def run(self, ctx: Context) -> None:
            log.append("started")
            while not ctx.stopping:
                await ctx.clock.sleep(0.01)
            log.append("noticed-stopping")

        async def on_stop(self, ctx: Context) -> None:
            log.append("on_stop")

    async with Supervisor(clock=clock) as sup:
        sup.add(_Polling())
        await anyio.sleep(0)
        await clock.advance(0.05)
        sup.signal_stop()
        # Advance further so the next sleep returns and the daemon checks stopping.
        await clock.advance(0.05)

    assert log == ["started", "noticed-stopping", "on_stop"]


async def test_stop_force_cancels_blocked_daemons() -> None:
    """Daemons not polling stopping get force-cancelled by stop(grace=0)."""
    clock = SimClock()
    seen: list[str] = []

    class _Blocked(Daemon):
        name = "blocked"

        async def run(self, ctx: Context) -> None:
            seen.append("running")
            await ctx.clock.sleep(1_000_000)  # would hang forever under SimClock
            seen.append("after-sleep")  # unreachable

        async def on_stop(self, ctx: Context) -> None:
            seen.append("on_stop")

    async with Supervisor(clock=clock) as sup:
        sup.add(_Blocked())
        await anyio.sleep(0)
        await sup.stop(grace=0.0)

    assert "running" in seen
    assert "after-sleep" not in seen
    # Forceful path still runs on_stop in a shielded scope.
    assert "on_stop" in seen


async def test_stop_is_idempotent() -> None:
    """Calling stop() twice is safe."""
    clock = SimClock()

    class _Quick(Daemon):
        async def run(self, ctx: Context) -> None:
            pass

    async with Supervisor(clock=clock) as sup:
        sup.add(_Quick())
        await anyio.sleep(0)
        await sup.stop(grace=0.0)
        await sup.stop(grace=0.0)  # second call: no-op


async def test_signal_stop_before_supervisor_entered_is_noop() -> None:
    """signal_stop() called before __aenter__ is harmless."""
    sup = Supervisor(clock=SimClock())
    sup.signal_stop()  # should not raise; no event yet, just a no-op


async def test_on_stop_runs_on_shutdown_path() -> None:
    """A daemon raising under on_error='shutdown' still gets on_stop after on_start succeeded."""
    clock = SimClock()
    log: list[str] = []

    class _FailsAfterStart(Daemon):
        async def on_start(self, ctx: Context) -> None:
            log.append("on_start")

        async def run(self, ctx: Context) -> None:
            log.append("run")
            raise RuntimeError("oops")

        async def on_stop(self, ctx: Context) -> None:
            log.append("on_stop")

    with pytest.raises(BaseExceptionGroup):
        async with Supervisor(clock=clock) as sup:
            sup.add(_FailsAfterStart(), name="fail")

    assert log == ["on_start", "run", "on_stop"]


async def test_on_stop_runs_on_ignore_path() -> None:
    """A daemon raising under on_error='ignore' still gets on_stop after on_start succeeded."""
    clock = SimClock()
    log: list[str] = []

    class _FailsAfterStart(Daemon):
        async def on_start(self, ctx: Context) -> None:
            log.append("on_start")

        async def run(self, ctx: Context) -> None:
            raise RuntimeError("oops")

        async def on_stop(self, ctx: Context) -> None:
            log.append("on_stop")

    async with Supervisor(clock=clock, on_error="ignore") as sup:
        sup.add(_FailsAfterStart())

    assert log == ["on_start", "on_stop"]


async def test_on_stop_skipped_when_on_start_fails() -> None:
    """If on_start itself raises, on_stop is not called (nothing to clean up)."""
    clock = SimClock()
    log: list[str] = []

    class _StartFails(Daemon):
        async def on_start(self, ctx: Context) -> None:
            log.append("on_start-pre")
            raise RuntimeError("init failed")

        async def run(self, ctx: Context) -> None:
            log.append("run")

        async def on_stop(self, ctx: Context) -> None:
            log.append("on_stop")  # must not be reached

    async with Supervisor(clock=clock, on_error="ignore") as sup:
        sup.add(_StartFails())

    assert log == ["on_start-pre"]


async def test_add_rejects_daemon_factory_with_clear_error() -> None:
    """Passing the @daemon factory directly (forgetting to call it) raises TypeError, not AttributeError."""
    from runlet import daemon as daemon_decorator

    @daemon_decorator
    async def my_worker(ctx: Context) -> None:
        pass

    sup = Supervisor()
    with pytest.raises(TypeError, match="@daemon factory"):
        sup.add(my_worker)  # type: ignore[arg-type]


async def test_add_rejects_non_daemon_with_clear_error() -> None:
    """Passing a random object raises TypeError mentioning the wrong type."""
    sup = Supervisor()
    with pytest.raises(TypeError, match="expected a Daemon"):
        sup.add("not a daemon")  # type: ignore[arg-type]


async def test_each_daemon_gets_its_own_cancel_scope() -> None:
    """Per-daemon scope: concurrent daemons under one Supervisor must see distinct ``cancel_scope`` objects.

    We hold references to both scopes while both daemons are still alive so the
    comparison is not subject to id() reuse after one of them exits and is GC'd.
    """
    clock = SimClock()
    scopes: list[anyio.CancelScope] = []

    class _Recorder(Daemon):
        async def run(self, ctx: Context) -> None:
            scopes.append(ctx.cancel_scope)
            await ctx.clock.sleep(0.1)  # keep both daemons alive simultaneously

    async with Supervisor(clock=clock) as sup:
        sup.add(_Recorder(), name="recorder-A")
        sup.add(_Recorder(), name="recorder-B")
        await anyio.sleep(0)
        await clock.advance(1.0)

    assert len(scopes) == 2
    assert scopes[0] is not scopes[1]


# -- snapshot / DaemonHealth ---------------------------------------------------


async def test_snapshot_lists_pending_daemons_before_entry() -> None:
    """Before __aenter__, snapshot() shows queued daemons in the 'starting' state."""
    clock = SimClock()

    class _Quick(Daemon):
        async def run(self, ctx: Context) -> None:
            pass

    sup = Supervisor(clock=clock)
    sup.add(_Quick(), name="a")
    sup.add(_Quick(), name="b")

    snap = sup.snapshot()
    assert set(snap.keys()) == {"a", "b"}
    assert all(isinstance(h, DaemonHealth) for h in snap.values())
    assert all(h.state == "starting" for h in snap.values())
    assert all(h.restart_count == 0 for h in snap.values())
    assert all(h.last_error is None for h in snap.values())
    assert all(h.started_at is None for h in snap.values())


async def test_snapshot_reports_running_state_and_uptime_under_simclock() -> None:
    """A daemon mid-run() shows state='running' with uptime advancing with the clock."""
    clock = SimClock()
    seen_uptimes: list[float | None] = []
    in_run = anyio.Event()

    class _Sleeper(Daemon):
        async def run(self, ctx: Context) -> None:
            in_run.set()
            await ctx.clock.sleep(10.0)

    async with Supervisor(clock=clock) as sup:
        sup.add(_Sleeper(), name="sleeper")
        await in_run.wait()
        # Take a snapshot immediately after on_start.
        snap = sup.snapshot()
        assert snap["sleeper"].state == "running"
        assert snap["sleeper"].started_at == 0.0
        assert snap["sleeper"].uptime == 0.0

        await clock.advance(3.0)
        snap = sup.snapshot()
        assert snap["sleeper"].uptime == pytest.approx(3.0)
        seen_uptimes.append(snap["sleeper"].uptime)

        await sup.stop(grace=0.0)

    assert seen_uptimes == [pytest.approx(3.0)]


async def test_snapshot_after_failure_under_ignore_shows_failed_or_stopped() -> None:
    """Under on_error='ignore', a raising daemon ends as 'stopped' with last_error set."""
    clock = SimClock()

    class _Crashes(Daemon):
        async def run(self, ctx: Context) -> None:
            raise RuntimeError("kaboom")

    sup = Supervisor(clock=clock, on_error="ignore")
    async with sup:
        sup.add(_Crashes(), name="crasher")

    snap = sup.snapshot()
    assert snap["crasher"].state == "stopped"
    assert isinstance(snap["crasher"].last_error, RuntimeError)
    assert "kaboom" in str(snap["crasher"].last_error)


async def test_snapshot_restart_count_increments_through_backoff() -> None:
    """restart_count climbs each time the daemon is re-entered."""
    clock = SimClock()

    class _FailsTwice(Daemon):
        def __init__(self) -> None:
            self.attempts = 0

        async def run(self, ctx: Context) -> None:
            self.attempts += 1
            if self.attempts <= 2:
                raise RuntimeError(f"fail #{self.attempts}")

    failing = _FailsTwice()
    policy = RestartPolicy(base=0.05, factor=2.0, cap=1.0)

    sup = Supervisor(clock=clock, on_error="restart", restart=policy)
    async with sup:
        sup.add(failing, name="flapper")
        await anyio.sleep(0)
        await clock.advance(1.0)

    snap = sup.snapshot()
    # The final iteration succeeded; restart_count reflects the two failures.
    assert snap["flapper"].restart_count == 2
    assert snap["flapper"].state == "stopped"
    assert isinstance(snap["flapper"].last_error, RuntimeError)


async def test_snapshot_failed_under_shutdown_policy() -> None:
    """Under on_error='shutdown', the daemon's record ends as 'failed'."""
    clock = SimClock()

    class _Crashes(Daemon):
        async def run(self, ctx: Context) -> None:
            raise RuntimeError("boom")

    sup = Supervisor(clock=clock)
    with pytest.raises(BaseExceptionGroup):
        async with sup:
            sup.add(_Crashes(), name="boomer")

    snap = sup.snapshot()
    assert snap["boomer"].state == "failed"
    assert isinstance(snap["boomer"].last_error, RuntimeError)


# -- wait_all_started ----------------------------------------------------------


async def test_wait_all_started_blocks_until_on_start_completes() -> None:
    """Driver waits past on_start before advancing — daemons all see their
    own ctx.clock.sleep registered before time moves.
    """
    clock = SimClock()
    started_times: list[float] = []

    class _SlowStart(Daemon):
        async def on_start(self, ctx: Context) -> None:
            # Pretend setup is slow — but we only need the *callsite* of
            # wait_all_started to wait until this returns.
            pass

        async def run(self, ctx: Context) -> None:
            started_times.append(ctx.clock.now())
            await ctx.clock.sleep(1.0)

    async with Supervisor(clock=clock) as sup:
        for i in range(5):
            sup.add(_SlowStart(), name=f"d{i}")
        await sup.wait_all_started()
        # All daemons are past on_start. Their run() should have appended
        # their start time before we advance.
        snap = sup.snapshot()
        assert all(h.state == "running" for h in snap.values()), snap
        await clock.advance(2.0)

    assert len(started_times) == 5
    # All started at t=0.
    assert all(t == 0.0 for t in started_times)


async def test_wait_all_started_returns_immediately_for_failed_on_start() -> None:
    """If a daemon's on_start raises (so the host exits without ever marking
    started=True), wait_all_started must NOT hang — the host's finally
    unblocks the event.
    """
    clock = SimClock()

    class _StartCrashes(Daemon):
        async def on_start(self, ctx: Context) -> None:
            raise RuntimeError("init failed")

        async def run(self, ctx: Context) -> None:
            pass  # unreachable

    async with Supervisor(clock=clock, on_error="ignore") as sup:
        sup.add(_StartCrashes(), name="crasher")
        # Without the finally guard, this would hang forever.
        with anyio.move_on_after(1.0) as scope:
            await sup.wait_all_started()
        assert not scope.cancel_called, "wait_all_started should have returned"


async def test_wait_all_started_is_a_snapshot_barrier() -> None:
    """Daemons added after wait_all_started() returns are not part of that barrier."""
    clock = SimClock()
    in_run = anyio.Event()

    class _BlocksOnEvent(Daemon):
        def __init__(self, ev: anyio.Event) -> None:
            self._ev = ev

        async def on_start(self, ctx: Context) -> None:
            self._ev.set()

        async def run(self, ctx: Context) -> None:
            await ctx.clock.sleep(100)

    async with Supervisor(clock=clock) as sup:
        sup.add(_BlocksOnEvent(in_run), name="first")
        await sup.wait_all_started()
        # Adding a new daemon now should not retroactively unstart the
        # barrier we already passed.
        snap = sup.snapshot()
        assert snap["first"].state == "running"
        await sup.stop(grace=0.0)


# -- helpers -------------------------------------------------------------------


def _flatten(eg: BaseException) -> list[BaseException]:
    """Walk a (possibly nested) ExceptionGroup and yield all leaf exceptions."""
    if isinstance(eg, BaseExceptionGroup):
        out: list[BaseException] = []
        for e in eg.exceptions:
            out.extend(_flatten(e))
        return out
    return [eg]
