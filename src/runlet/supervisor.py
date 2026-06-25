"""Structured-concurrency root for hosting daemons."""

from __future__ import annotations

import logging
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

__all__ = ["DaemonFailurePhase", "DaemonHealth", "DaemonState", "RestartPolicy", "Supervisor"]

DaemonState = Literal["starting", "running", "restarting", "stopped", "failed"]
"""Lifecycle state surfaced by :class:`DaemonHealth`."""

DaemonFailurePhase = Literal["on_start", "run", "on_stop"]
"""Lifecycle phase that produced ``DaemonHealth.last_error``."""


@dataclass
class RestartPolicy:
    """Exponential backoff for ``on_error="restart"``."""

    base: float = 0.1
    factor: float = 2.0
    cap: float = 5.0
    max_retries: int | None = None


@dataclass(frozen=True)
class DaemonHealth:
    """Snapshot of one hosted daemon's runtime state."""

    name: str
    state: DaemonState
    restart_count: int
    """Number of restarts after failures."""
    last_error: BaseException | None
    """Most recent lifecycle exception, if any."""
    last_error_phase: DaemonFailurePhase | None
    """Lifecycle phase that produced ``last_error``."""
    started_at: float | None
    """Clock-time when the current attempt's ``on_start`` returned."""
    uptime: float | None
    """Current-at-snapshot uptime for running daemons."""


@dataclass
class _DaemonRecord:
    """Mutable per-daemon state owned by the supervisor's host loop."""

    name: str
    state: DaemonState = "starting"
    restart_count: int = 0
    last_error: BaseException | None = None
    last_error_phase: DaemonFailurePhase | None = None
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

        Raises ``ValueError`` on duplicate names and ``TypeError`` for
        non-daemon values, including an uncalled ``@daemon`` factory.
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
            raise ValueError(f"duplicate daemon name {chosen!r}; pass a unique `name=...`")
        self._seen_names.add(chosen)
        # Create a fresh record per add(); the host loop mutates it as the
        # daemon's state changes. Names are unique, so snapshot() can key by
        # daemon name without hiding a sibling.
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
        """Wrap ``fn(ctx, *args, **kwargs)`` into a one-off daemon."""
        chosen = name or fn.__name__
        d = _FnDaemon(fn, args, kwargs, chosen)
        self.add(d, name=chosen)

    # -- stop coordination -----------------------------------------------------

    def signal_stop(self) -> None:
        """Sync, fire-and-forget. Set the stop event so cooperative daemons can exit.

        Safe to call from inside a daemon. Use :meth:`stop` when the caller
        also needs to wait and force-cancel laggards.
        """
        if self._stop_event is not None:
            self._stop_event.set()

    async def stop(self, grace: float = 5.0, finalize_timeout: float = 2.0) -> None:
        """Signal stop, wait up to ``grace`` for cooperative exit, then force-cancel.

        Idempotent. Calling it before entry is a no-op. Daemons that want to
        stop their own supervisor should call :meth:`signal_stop` and return.
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

        Waits for the daemon set known at call time. Safe before entry or
        after exit; both are no-ops.
        """
        if self._tg is None:
            return
        # Snapshot the events so daemons added concurrently with this call
        # don't shift the barrier.
        events = [rec.started_event for rec in self._records.values()]
        for ev in events:
            await ev.wait()

    # -- diagnostics -----------------------------------------------------------

    def snapshot(self) -> dict[str, DaemonHealth]:
        """Return a name -> :class:`DaemonHealth` map for all known daemons.

        Safe before entry and after exit. Running-daemon uptime is computed at
        call time from the supervisor clock.
        """
        now: float | None = None
        out: dict[str, DaemonHealth] = {}
        for name, rec in self._records.items():
            uptime: float | None
            if rec.started_at is not None and rec.state == "running":
                if now is None:
                    now = self._clock.now()
                uptime = now - rec.started_at
            elif rec.started_at is not None:
                # No exit timestamp is stored, so terminal uptime is unknown.
                uptime = None
            else:
                uptime = None
            out[name] = DaemonHealth(
                name=name,
                state=rec.state,
                restart_count=rec.restart_count,
                last_error=rec.last_error,
                last_error_phase=rec.last_error_phase,
                started_at=rec.started_at,
                uptime=uptime,
            )
        return out

    # -- internals -------------------------------------------------------------

    def _on_daemon_exit(self) -> None:
        # _host only runs after __aenter__, so both events are live here.
        assert self._stop_event is not None and self._all_done is not None
        self._active_count -= 1
        if self._active_count == 0 and self._stop_event.is_set():
            self._all_done.set()

    async def _host(self, daemon: Daemon, name: str) -> None:
        """Run one daemon's lifecycle inside the task group."""
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
                on_stop_started = False
                phase: DaemonFailurePhase = "on_start"
                record.state = "starting"
                record.started_at = None
                try:
                    with scope:
                        await daemon.on_start(ctx)
                        on_start_done = True
                        record.state = "running"
                        record.started_at = self._clock.now()
                        record.started_event.set()
                        phase = "run"
                        await daemon.run(ctx)
                    on_stop_started = True
                    phase = "on_stop"
                    await daemon.on_stop(ctx)
                    record.state = "stopped"
                    return
                except BaseException as exc:
                    # If on_start succeeded, on_stop is owed on every exit path.
                    if on_start_done and not on_stop_started:
                        await self._run_on_stop_shielded(daemon, ctx)
                    if isinstance(exc, cancelled_cls):
                        # Cancellation is not a daemon logic failure.
                        record.state = "stopped"
                        raise
                    if not isinstance(exc, Exception):
                        record.state = "failed"
                        record.last_error = exc
                        record.last_error_phase = phase
                        raise
                    record.last_error = exc
                    record.last_error_phase = phase
                    ctx.logger.exception("daemon raised")
                    if phase == "on_stop" and self._on_error != "ignore":
                        record.state = "failed"
                        raise DaemonError(f"daemon {name!r} cleanup failed: {exc}") from exc
                    if self._on_error == "shutdown":
                        record.state = "failed"
                        raise DaemonError(f"daemon {name!r} failed: {exc}") from exc
                    if self._on_error == "ignore":
                        record.state = "stopped"
                        return
                    # Restart path; stop wins over retry.
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
            # Avoid hanging wait_all_started() if startup failed or was cancelled.
            record.started_event.set()
            self._on_daemon_exit()

    async def _run_on_stop_shielded(self, daemon: Daemon, ctx: Context) -> None:
        """Run ``daemon.on_stop`` under a shielded finite budget."""
        with anyio.CancelScope(shield=True):
            with anyio.move_on_after(self._finalize_timeout):
                try:
                    await daemon.on_stop(ctx)
                except Exception:
                    ctx.logger.exception("on_stop raised during teardown")
