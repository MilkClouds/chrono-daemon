"""Supervisor: structured-concurrency root for hosting daemons.

A ``Supervisor`` is an async context manager wrapping ``anyio.create_task_group``. Each
daemon gets its own ``Context`` (with its own ``cancel_scope``, ``logger`` child, and
``name``). On unhandled exception, the supervisor's ``on_error`` policy decides what
to do:

- ``"shutdown"`` (default): re-raise wrapped in :class:`DaemonError` so the failing
  daemon's name reaches the resulting ``ExceptionGroup`` leaf. anyio's TaskGroup
  cancels every sibling.
- ``"restart"``: sleep on ``ctx.clock`` per ``RestartPolicy`` (exponential
  backoff), then re-enter ``on_start``/``run``/``on_stop``. Because backoff goes
  through ``ctx.clock.sleep``, restart timing is deterministic under ``SimClock``.
- ``"ignore"``: log and let the daemon exit; siblings keep running.

Graceful shutdown (ADR 0009):

- :meth:`Supervisor.signal_stop` is sync, fire-and-forget. Sets a shared event
  on every daemon's :class:`Context` (``ctx.stop_event``). Cooperative daemons
  poll ``ctx.stopping`` and exit cleanly so ``on_stop`` runs as part of the
  normal return path.
- :meth:`Supervisor.stop` is async: signals stop, waits up to ``grace`` for
  daemons to honor it, then force-cancels any still running. On the force
  path, each daemon's ``on_stop`` still gets a best-effort shielded
  invocation bounded by ``finalize_timeout``.
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Literal

import anyio
import anyio.abc

from runlet._logging import ClockAwareLoggerAdapter
from runlet._types import DaemonError, OnError
from runlet.clock import Clock, WallClock
from runlet.context import Context
from runlet.daemon import Daemon, _FnDaemon

__all__ = ["DaemonHealth", "DaemonState", "RestartPolicy", "Supervisor"]

DaemonState = Literal["starting", "running", "restarting", "stopped", "failed"]
"""Lifecycle state surfaced by :class:`DaemonHealth`.

- ``starting``: inside ``on_start`` or before it returns.
- ``running``: ``on_start`` returned; ``run`` is executing.
- ``restarting``: ``on_error="restart"`` is waiting out the backoff before
  the next ``on_start``.
- ``stopped``: the host returned normally (clean exit, ``on_stop`` ran).
- ``failed``: the host exited because an exception escaped past all retries
  (shutdown / ignore terminal / restart exhausted). ``last_error`` carries
  the cause.
"""


@dataclass
class RestartPolicy:
    """Exponential backoff for ``on_error="restart"``.

    The first restart waits ``base`` seconds, the second waits ``base * factor``, etc.,
    capped at ``cap``. If ``max_retries`` is set and exceeded, the latest exception is
    re-raised (which under the default supervisor will then cancel siblings).
    """

    base: float = 0.1
    factor: float = 2.0
    cap: float = 5.0
    max_retries: int | None = None


@dataclass(frozen=True)
class DaemonHealth:
    """Snapshot of one hosted daemon's runtime state.

    Returned by :meth:`Supervisor.snapshot`. All times are read from the
    supervisor's :class:`runlet.Clock`, so they advance under ``SimClock``
    burst replay the same way the daemon's own timestamps do.

    Frozen so consumers can pass it around without worrying about it
    mutating mid-read; the supervisor holds the underlying mutable state.
    """

    name: str
    state: DaemonState
    restart_count: int
    """How many times this daemon has been re-entered after a failure under
    ``on_error="restart"``. Zero on first attempt; bumped before the next
    ``on_start``.
    """
    last_error: BaseException | None
    """The most recent exception that escaped ``run()`` (or ``on_start`` on
    the failure paths). ``None`` if the daemon has never raised.
    """
    started_at: float | None
    """Clock-time when the current attempt's ``on_start`` returned. ``None``
    if the daemon has not yet finished ``on_start``. Reset on each restart.
    """
    uptime: float | None
    """``clock.now() - started_at`` for the current attempt, or ``None`` if
    ``started_at`` is. Computed at snapshot time so successive reads of
    the same ``DaemonHealth`` don't drift.
    """


@dataclass
class _DaemonRecord:
    """Mutable per-daemon state owned by the supervisor's host loop."""

    name: str
    state: DaemonState = "starting"
    restart_count: int = 0
    last_error: BaseException | None = None
    started_at: float | None = None
    # Set by the host loop when on_start returns successfully (first attempt
    # or any restart). Awaited by Supervisor.wait_all_started().
    started_event: anyio.Event = field(default_factory=anyio.Event)


@dataclass
class _PendingDaemon:
    daemon: Daemon
    name: str


class Supervisor:
    """Structured-concurrency root. Use as ``async with Supervisor(...) as sup``.

    Daemons added before ``__aenter__`` are launched on entry; daemons added inside the
    ``async with`` block are launched immediately.
    """

    def __init__(
        self,
        *,
        clock: Clock | None = None,
        on_error: OnError = "shutdown",
        restart: RestartPolicy | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._clock: Clock = clock or WallClock()
        self._on_error: OnError = on_error
        self._restart: RestartPolicy = restart or RestartPolicy()
        self._logger: logging.Logger = logger or logging.getLogger("runlet")
        self._tg: anyio.abc.TaskGroup | None = None
        self._pending: list[_PendingDaemon] = []
        # Per-daemon mutable health state, keyed by daemon name. Populated on
        # add(); kept in sync by the host loop as the daemon transitions
        # between starting / running / restarting / stopped / failed.
        self._records: dict[str, _DaemonRecord] = {}
        # Track names we've seen to surface accidental duplicates (which would
        # otherwise share a logger child and complicate cross-task diagnosis).
        self._seen_names: set[str] = set()
        # Stop-coordination state (populated in __aenter__).
        self._stop_event: anyio.Event | None = None
        self._all_done: anyio.Event | None = None
        self._active_count: int = 0
        # finalize_timeout is set by stop(); the force-cancel path consults
        # it via the host loop. Default applies if a daemon is cancelled
        # without a preceding stop() call.
        self._finalize_timeout: float = 2.0

    # -- properties exposed to daemons (read-only) -----------------------------

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def stop_event(self) -> anyio.Event:
        """The shared stop event. Valid only inside ``async with Supervisor(...)``."""
        if self._stop_event is None:
            raise RuntimeError("supervisor not entered; stop_event unavailable")
        return self._stop_event

    # -- async context manager -------------------------------------------------

    async def __aenter__(self) -> Supervisor:
        self._tg = anyio.create_task_group()
        await self._tg.__aenter__()
        self._stop_event = anyio.Event()
        self._all_done = anyio.Event()
        # Drain anything queued before entry.
        pending, self._pending = self._pending, []
        for p in pending:
            self._active_count += 1
            self._tg.start_soon(self._host, p.daemon, p.name)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        tg = self._tg
        self._tg = None
        assert tg is not None
        return await tg.__aexit__(exc_type, exc, tb)

    # -- daemon registration ---------------------------------------------------

    def add(self, daemon: Daemon, *, name: str | None = None) -> None:
        """Register a Daemon instance. Launched immediately if the supervisor is running.

        Emits a :class:`UserWarning` if ``name`` collides with a previously
        registered daemon — duplicate names share a child logger and make
        cross-task error attribution harder. Pass a unique ``name=...`` to
        silence intentionally.

        Raises :class:`TypeError` if ``daemon`` is not a :class:`Daemon`
        instance. A common mistake is to pass the ``@daemon`` factory itself
        rather than calling it; the error message points that out explicitly.
        """
        if not isinstance(daemon, Daemon):
            if callable(daemon) and getattr(daemon, "_runlet_daemon_factory", False):
                fn_name = getattr(daemon, "__name__", "<factory>")
                raise TypeError(
                    f"add() got a @daemon factory ({fn_name!r}), not a Daemon instance. "
                    f"Call it first to construct one: sup.add({fn_name}(...))"
                )
            raise TypeError(f"add() expected a Daemon instance, got {type(daemon).__name__}")
        chosen = name or daemon.name or type(daemon).__name__
        if chosen in self._seen_names:
            warnings.warn(
                f"duplicate daemon name {chosen!r}; both will share a child logger. "
                "Pass a unique `name=...` to disambiguate.",
                UserWarning,
                stacklevel=2,
            )
        self._seen_names.add(chosen)
        # Create a fresh record per add(); the host loop mutates it as the
        # daemon's state changes. Duplicate names will overwrite — the
        # duplicate-name warning above already flagged that case.
        self._records[chosen] = _DaemonRecord(name=chosen)
        if self._tg is None:
            self._pending.append(_PendingDaemon(daemon, chosen))
        else:
            self._active_count += 1
            self._tg.start_soon(self._host, daemon, chosen)

    def spawn(
        self,
        fn: Callable[..., Coroutine[Any, Any, None]],
        *args: Any,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Adapter for ad-hoc async fns: wraps ``fn`` into a one-off Daemon and registers it.

        ``fn`` must accept ``ctx`` as its first parameter.
        """
        chosen = name or fn.__name__
        d = _FnDaemon(fn, args, kwargs, chosen)
        self.add(d, name=chosen)

    # -- stop coordination -----------------------------------------------------

    def signal_stop(self) -> None:
        """Sync, fire-and-forget. Set the stop event so cooperative daemons can exit.

        Idempotent. Safe to call from inside a daemon — the calling daemon should
        then ``return`` normally so its ``on_stop`` runs as part of the standard
        path. To additionally wait for all daemons to finish (and force-cancel
        any that don't), use :meth:`stop` from the supervisor's main task.
        """
        if self._stop_event is not None:
            self._stop_event.set()

    async def stop(self, grace: float = 5.0, finalize_timeout: float = 2.0) -> None:
        """Signal stop, wait up to ``grace`` for cooperative exit, then force-cancel.

        Each daemon's ``on_stop`` runs naturally when it cooperates. On the
        force-cancel path, ``on_stop`` is still given a best-effort shielded
        invocation bounded by ``finalize_timeout``.

        Idempotent. Safe to call before the supervisor has entered its async
        context (no-op) or after all daemons have exited (returns immediately).
        Should be called from the supervisor's main task, not from inside a
        daemon that wants to terminate itself — use :meth:`signal_stop` for that.
        """
        # Set finalize_timeout *before* signaling stop so the host loop sees
        # the right value if cancellation arrives near-immediately.
        self._finalize_timeout = finalize_timeout
        self.signal_stop()
        if self._tg is None or self._all_done is None:
            return
        if self._active_count == 0:
            return
        if grace > 0:
            with anyio.move_on_after(grace):
                await self._all_done.wait()
        if not self._all_done.is_set() and self._tg is not None:
            self._tg.cancel_scope.cancel()

    # -- readiness barrier -----------------------------------------------------

    async def wait_all_started(self) -> None:
        """Block until every currently-hosted daemon's ``on_start`` has resolved.

        "Resolved" means the host loop reached one of:
        - ``on_start`` returned (record.state transitions to ``running``), or
        - the daemon's lifecycle finished (failed / stopped before/during
          ``on_start`` — see the failure paths in ``_host``).

        Use this when a driver task wants to be sure every daemon is past its
        setup phase before doing something time-sensitive (e.g. calling
        :meth:`runlet.SimClock.advance` for deterministic burst replay — under
        trio the scheduler can otherwise start ``advance`` before all daemons
        register their first sleep, costing byte-determinism across runs).

        Snapshot semantics: waits for the daemons known at call time; daemons
        added afterwards aren't tracked by this call (call it again if you
        need a fresh barrier). Safe to call before ``__aenter__`` (returns
        immediately if nothing is registered yet) or after exit (events are
        all set during host teardown, so this is a no-op).
        """
        # Snapshot the events so daemons added concurrently with this call
        # don't shift the barrier.
        events = [rec.started_event for rec in self._records.values()]
        for ev in events:
            await ev.wait()

    # -- diagnostics -----------------------------------------------------------

    def snapshot(self) -> dict[str, DaemonHealth]:
        """Return a name -> :class:`DaemonHealth` map for all known daemons.

        Cheap and read-only — safe to call from any task at any time, including
        before ``__aenter__`` (returns daemons queued via ``add()``) and after
        ``__aexit__`` (returns the terminal state of each daemon).

        Uptime is computed at call time as ``clock.now() - started_at`` for
        each ``running`` daemon. Under :class:`runlet.SimClock`, this advances
        in lockstep with the rest of the simulation.
        """
        now = self._clock.now()
        out: dict[str, DaemonHealth] = {}
        for name, rec in self._records.items():
            uptime: float | None
            if rec.started_at is not None and rec.state == "running":
                uptime = now - rec.started_at
            elif rec.started_at is not None:
                # Daemon has stopped / failed / is restarting — uptime reflects
                # how long the *last* attempt ran for. We can't tell exactly
                # without recording an exit timestamp; report None to avoid lying.
                uptime = None
            else:
                uptime = None
            out[name] = DaemonHealth(
                name=name,
                state=rec.state,
                restart_count=rec.restart_count,
                last_error=rec.last_error,
                started_at=rec.started_at,
                uptime=uptime,
            )
        return out

    # -- internals -------------------------------------------------------------

    def _on_daemon_exit(self) -> None:
        # Only ever called from _host's finally, which only runs inside the task
        # group entered by __aenter__ — so both events are guaranteed live here.
        assert self._stop_event is not None and self._all_done is not None
        self._active_count -= 1
        if self._active_count == 0 and self._stop_event.is_set():
            self._all_done.set()

    async def _host(self, daemon: Daemon, name: str) -> None:
        """Lifecycle wrapper running inside the task group for one daemon.

        Lifecycle guarantee: if ``on_start(ctx)`` returned successfully, ``on_stop(ctx)``
        is invoked exactly once before the host returns or re-raises — on the normal
        path, the Exception paths (shutdown/restart/ignore), and the forceful cancel
        path. The cancel/shutdown/ignore paths run ``on_stop`` inside a shielded
        scope bounded by ``self._finalize_timeout``; the restart path runs it inline
        between attempts so each iteration sees ``on_start`` paired with ``on_stop``.
        """
        delay = self._restart.base
        retries = 0
        cancelled_cls = anyio.get_cancelled_exc_class()
        assert self._stop_event is not None  # set in __aenter__
        record = self._records[name]
        try:
            while True:
                # Each attempt gets a fresh cancel scope so the daemon can cancel
                # itself without leaking that cancellation into the supervisor.
                scope = anyio.CancelScope()
                ctx = Context(
                    clock=self._clock,
                    cancel_scope=scope,
                    logger=ClockAwareLoggerAdapter(self._logger.getChild(name), self._clock),
                    name=name,
                    supervisor=self,
                    stop_event=self._stop_event,
                )
                on_start_done = False
                record.state = "starting"
                record.started_at = None
                try:
                    with scope:
                        await daemon.on_start(ctx)
                        on_start_done = True
                        record.state = "running"
                        record.started_at = self._clock.now()
                        record.started_event.set()
                        await daemon.run(ctx)
                        await daemon.on_stop(ctx)
                    record.state = "stopped"
                    return
                except BaseException as exc:
                    # If on_start succeeded, on_stop is owed regardless of how we exit.
                    # Run it under a shield so cancellation cannot cut cleanup short.
                    if on_start_done:
                        await self._run_on_stop_shielded(daemon, ctx)
                    if isinstance(exc, cancelled_cls):
                        # Cancellation isn't a failure of the daemon's own logic
                        # — record stopped, then propagate.
                        record.state = "stopped"
                        raise
                    if not isinstance(exc, Exception):
                        record.state = "failed"
                        record.last_error = exc
                        raise
                    record.last_error = exc
                    ctx.logger.exception("daemon raised")
                    if self._on_error == "shutdown":
                        record.state = "failed"
                        raise DaemonError(f"daemon {name!r} failed: {exc}") from exc
                    if self._on_error == "ignore":
                        record.state = "stopped"
                        return
                    # restart path — but bail out if stop was already signaled.
                    if self._stop_event.is_set():
                        record.state = "stopped"
                        return
                    retries += 1
                    record.restart_count = retries
                    if self._restart.max_retries is not None and retries > self._restart.max_retries:
                        record.state = "failed"
                        ctx.logger.error("daemon exceeded max_retries=%d; giving up", self._restart.max_retries)
                        raise DaemonError(f"daemon {name!r} exceeded max_retries={self._restart.max_retries}") from exc
                    record.state = "restarting"
                    await self._clock.sleep(delay)
                    delay = min(delay * self._restart.factor, self._restart.cap)
        finally:
            # Unblock wait_all_started() even if on_start never succeeded —
            # otherwise a daemon that fails before/inside on_start would hang
            # any concurrent waiter forever. The waiter is expected to inspect
            # state via snapshot() if it needs to distinguish started-cleanly
            # from never-started.
            record.started_event.set()
            self._on_daemon_exit()

    async def _run_on_stop_shielded(self, daemon: Daemon, ctx: Context) -> None:
        """Run ``daemon.on_stop`` under a shielded scope with a finite time budget.

        Used on all non-normal exit paths (cancel + shutdown + restart + ignore) so
        ``on_stop`` is reached even when the surrounding code is being torn down.
        Exceptions raised by ``on_stop`` are logged and swallowed — the outer path's
        decision (re-raise, restart, etc.) takes precedence.
        """
        with anyio.CancelScope(shield=True):
            with anyio.move_on_after(self._finalize_timeout):
                try:
                    await daemon.on_stop(ctx)
                except Exception:
                    ctx.logger.exception("on_stop raised during teardown")
