"""Host a supervisor inside an externally-owned task group."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from inspect import isawaitable
from typing import Any

import anyio
import anyio.lowlevel

from chrono_daemon.supervisor import DaemonHealth, Supervisor

StopCallback = Callable[[], Any]

__all__ = ["SupervisorHost"]


async def _maybe_await(value: Any) -> None:
    if isawaitable(value):
        await value


@dataclass
class SupervisorHost:
    """Run a supervisor in a caller-owned task group.

    This is a recipe for servers that already own their top-level task group
    but need a child supervisor with a start barrier and a stop handle.
    """

    supervisor: Supervisor
    on_stopping: StopCallback | None = None
    default_grace: float = 0.0
    default_finalize_timeout: float = 2.0
    _started: bool = field(default=False, init=False)
    _ready: anyio.Event | None = field(default=None, init=False)
    _stop: anyio.Event | None = field(default=None, init=False)
    _done: anyio.Event | None = field(default=None, init=False)
    _startup_error: BaseException | None = field(default=None, init=False)

    async def start(self, task_group: anyio.abc.TaskGroup) -> None:
        if self._started:
            raise RuntimeError("supervisor host already started")
        self._ready = anyio.Event()
        self._stop = anyio.Event()
        self._done = anyio.Event()
        self._startup_error = None
        self._started = True
        task_group.start_soon(self._run)
        await self._ready.wait()
        if self._startup_error is not None:
            self._started = False
            raise self._startup_error

    async def stop(self, *, grace: float | None = None, finalize_timeout: float | None = None) -> None:
        if not self._started:
            return
        assert self._stop is not None and self._done is not None
        if grace is not None:
            self.default_grace = grace
        if finalize_timeout is not None:
            self.default_finalize_timeout = finalize_timeout
        self._stop.set()
        await self._done.wait()
        self._started = False

    def snapshot(self) -> dict[str, DaemonHealth]:
        return self.supervisor.snapshot()

    async def _run(self) -> None:
        assert self._ready is not None and self._stop is not None and self._done is not None
        try:
            async with self.supervisor:
                try:
                    await self.supervisor.wait_all_started()
                    await anyio.lowlevel.checkpoint()
                    failures = [h.name for h in self.supervisor.snapshot().values() if h.state == "failed"]
                    if failures:
                        names = ", ".join(failures)
                        raise RuntimeError(f"supervisor daemons failed to start: {names}")
                    self._ready.set()
                    await self._stop.wait()
                finally:
                    callback_error: BaseException | None = None
                    if self.on_stopping is not None:
                        try:
                            await _maybe_await(self.on_stopping())
                        except BaseException as exc:
                            callback_error = exc
                    await self.supervisor.stop(
                        grace=self.default_grace,
                        finalize_timeout=self.default_finalize_timeout,
                    )
                    if callback_error is not None:
                        raise callback_error
        except BaseException as exc:
            if not self._ready.is_set():
                self._startup_error = exc
                self._ready.set()
                return
            raise
        finally:
            if not self._ready.is_set():
                self._ready.set()
            self._done.set()
