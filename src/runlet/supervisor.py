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
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import anyio
import anyio.abc

from runlet._logging import ClockAwareLoggerAdapter
from runlet._types import DaemonError, OnError
from runlet.clock import Clock, WallClock
from runlet.context import Context
from runlet.daemon import Daemon, _FnDaemon

__all__ = ["RestartPolicy", "Supervisor"]


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
        """
        chosen = name or daemon.name or type(daemon).__name__
        if chosen in self._seen_names:
            warnings.warn(
                f"duplicate daemon name {chosen!r}; both will share a child logger. "
                "Pass a unique `name=...` to disambiguate.",
                UserWarning,
                stacklevel=2,
            )
        self._seen_names.add(chosen)
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

    # -- internals -------------------------------------------------------------

    def _on_daemon_exit(self) -> None:
        self._active_count -= 1
        if (
            self._active_count == 0
            and self._stop_event is not None
            and self._stop_event.is_set()
            and self._all_done is not None
        ):
            self._all_done.set()

    async def _host(self, daemon: Daemon, name: str) -> None:
        """Lifecycle wrapper running inside the task group for one daemon."""
        delay = self._restart.base
        retries = 0
        cancelled_cls = anyio.get_cancelled_exc_class()
        assert self._stop_event is not None  # set in __aenter__
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
                try:
                    with scope:
                        await daemon.on_start(ctx)
                        await daemon.run(ctx)
                        await daemon.on_stop(ctx)
                    return
                except BaseException as exc:
                    if isinstance(exc, cancelled_cls):
                        # Forceful cancel path — try on_stop with shield + timeout.
                        with anyio.CancelScope(shield=True):
                            with anyio.move_on_after(self._finalize_timeout):
                                try:
                                    await daemon.on_stop(ctx)
                                except Exception:
                                    ctx.logger.exception("on_stop raised under cancel")
                        raise
                    if not isinstance(exc, Exception):
                        raise
                    ctx.logger.exception("daemon raised")
                    if self._on_error == "shutdown":
                        raise DaemonError(f"daemon {name!r} failed: {exc}") from exc
                    if self._on_error == "ignore":
                        return
                    # restart path — but bail out if stop was already signaled.
                    if self._stop_event.is_set():
                        return
                    retries += 1
                    if self._restart.max_retries is not None and retries > self._restart.max_retries:
                        ctx.logger.error("daemon exceeded max_retries=%d; giving up", self._restart.max_retries)
                        raise DaemonError(f"daemon {name!r} exceeded max_retries={self._restart.max_retries}") from exc
                    await self._clock.sleep(delay)
                    delay = min(delay * self._restart.factor, self._restart.cap)
        finally:
            self._on_daemon_exit()
