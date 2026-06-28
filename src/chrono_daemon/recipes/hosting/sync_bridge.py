"""Host an async chrono-daemon supervisor behind a synchronous API.

Usage::

    async def setup(sup: Supervisor) -> MyDispatcher:
        sup.add(MyWorker(...))
        return MyDispatcher(...)

    with host_async_dispatcher(setup) as (portal, dispatcher):
        portal.call(dispatcher.push, item)

The supervisor uses :class:`chrono_daemon.WallClock` unless ``clock=`` is provided.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Coroutine, Generator
from contextlib import contextmanager
from typing import Any, TypeVar

import anyio
from anyio.from_thread import BlockingPortal, start_blocking_portal

from chrono_daemon import Clock, Supervisor, WallClock
from chrono_daemon.recipes.hosting.supervisor_host import SupervisorHost

__all__ = ["host_async_dispatcher"]

D = TypeVar("D")


@contextmanager
def host_async_dispatcher(
    setup: Callable[[Supervisor], Coroutine[Any, Any, D]],
    *,
    clock: Clock | None = None,
    backend: str = "asyncio",
    ready_timeout: float = 10.0,
) -> Generator[tuple[BlockingPortal, D], None, None]:
    """Spin up a supervisor on a private event loop; yield ``(portal, dispatcher)``.

    ``setup(supervisor)`` runs on the background loop and returns the
    dispatcher object for sync callers.
    """
    box: list[D | None] = [None]
    error: list[BaseException | None] = [None]
    ready = threading.Event()
    shutdown_box: list[anyio.Event | None] = [None]

    async def _serve() -> None:
        sup = Supervisor(clock=clock or WallClock())
        host = SupervisorHost(sup)
        shutdown = anyio.Event()
        shutdown_box[0] = shutdown
        try:
            async with anyio.create_task_group() as tg:
                try:
                    dispatcher = await setup(sup)
                    await host.start(tg)
                    box[0] = dispatcher
                except BaseException as exc:
                    error[0] = exc
                    raise
                finally:
                    ready.set()
                await shutdown.wait()
                await host.stop()
        except BaseException as exc:
            if error[0] is None:
                error[0] = exc

    with start_blocking_portal(backend=backend) as portal:
        portal.start_task_soon(_serve)
        if not ready.wait(timeout=ready_timeout):
            raise RuntimeError(
                f"async setup did not complete within {ready_timeout}s; "
                "check that setup(supervisor) is async and returns promptly"
            )
        if error[0] is not None:
            raise RuntimeError("async setup failed") from error[0]
        dispatcher = box[0]
        assert dispatcher is not None
        try:
            yield portal, dispatcher
        finally:
            shutdown = shutdown_box[0]
            if shutdown is not None:
                try:
                    portal.call(shutdown.set)
                except Exception:
                    pass
