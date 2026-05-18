"""Supervisor: shutdown propagation, restart with backoff, max_retries, cancel scope per daemon.

Also covers v0.1 additions: duplicate-name warning, DaemonError wrapping
exposing the failing daemon's name on the ExceptionGroup leaf.
"""

from __future__ import annotations

import warnings

import anyio
import pytest

from runlet import Context, Daemon, DaemonError, RestartPolicy, SimClock, Supervisor

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


# -- helpers -------------------------------------------------------------------


def _flatten(eg: BaseException) -> list[BaseException]:
    """Walk a (possibly nested) ExceptionGroup and yield all leaf exceptions."""
    if isinstance(eg, BaseExceptionGroup):
        out: list[BaseException] = []
        for e in eg.exceptions:
            out.extend(_flatten(e))
        return out
    return [eg]
