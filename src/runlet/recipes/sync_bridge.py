"""Recipe: host an async runlet supervisor behind a synchronous API.

Many deployments hand you a synchronous outer boundary (a model-server
callback, a ROS subscriber, a CLI handler) but want the inside to be a
long-lived runlet supervisor. anyio ships ``BlockingPortal`` for exactly
this; ``host_async_dispatcher`` wraps that into the shape every "async
dispatcher behind a sync ABC" deployment ends up at (e.g. worv-ai/reflex
PR #191's ``ReFlExDualDispatcherServer``).

Usage::

    async def setup(sup: Supervisor) -> MyDispatcher:
        sup.add(MyWorker(...))
        return MyDispatcher(...)

    with host_async_dispatcher(setup) as (portal, dispatcher):
        portal.call(dispatcher.push, item)

The supervisor uses :class:`runlet.WallClock` by default; pass ``clock=``
to override.

Import as ``from runlet.recipes.sync_bridge import host_async_dispatcher``.
The recipe namespace (``runlet.recipes``) is best-effort.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Coroutine, Generator
from contextlib import contextmanager
from typing import Any, TypeVar

from anyio.from_thread import BlockingPortal, start_blocking_portal

from runlet import Clock, Supervisor, WallClock

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

    ``setup(supervisor)`` is awaited once on the background loop. Whatever it
    returns is the dispatcher object handed to sync callers; they invoke its
    async methods via ``portal.call(dispatcher.method, ...)``.

    The supervisor is torn down (``stop(grace=0)``) when the ``with`` block
    exits; the portal is then shut down.

    Raises :class:`RuntimeError` if ``setup`` does not return within
    ``ready_timeout`` wall-clock seconds.
    """
    box: list[D | None] = [None]
    error: list[BaseException | None] = [None]
    ready = threading.Event()
    sup_box: list[Supervisor | None] = [None]

    async def _serve() -> None:
        sup = Supervisor(clock=clock or WallClock())
        sup_box[0] = sup
        try:
            async with sup:
                try:
                    box[0] = await setup(sup)
                except BaseException as exc:
                    error[0] = exc
                    raise
                finally:
                    ready.set()
                await sup.stop_event.wait()
                await sup.stop(grace=0.0)
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
            sup = sup_box[0]
            if sup is not None:
                try:
                    portal.call(sup.signal_stop)
                except Exception:
                    pass
