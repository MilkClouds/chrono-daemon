"""Recipe: call into a long-lived runlet supervisor from synchronous code.

Many deployments have a synchronous outer ABC (a model-server callback, a
ROS subscriber, a CLI request handler) but want the inside to be a runlet
supervisor that survives across calls. anyio ships ``BlockingPortal`` for
exactly this pattern; this recipe shows the shape.

The idea: spin up the supervisor inside ``portal.wrap_async_context_manager``
on a dedicated event-loop thread, then ``portal.call(async_fn, *args)``
from sync code to push work in or pull results out. The portal handles
threading; runlet stays oblivious.

Copy this file into your codebase as needed. It is not exported from
``runlet``.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generic, TypeVar

import anyio
from anyio.from_thread import BlockingPortal, start_blocking_portal

from runlet import Channel, Supervisor, WallClock, open_channel

T = TypeVar("T")


class SyncSupervisorHandle(Generic[T]):
    """Sync-callable handle around an async supervisor + a request channel.

    Construct via ``open_sync_supervisor(...)`` (the context manager below).
    The supervisor runs on a private event-loop thread. ``submit(req)`` is
    a regular blocking call; under the hood it forwards through the portal
    onto the supervisor's loop.
    """

    def __init__(self, portal: BlockingPortal, req_send) -> None:
        self._portal = portal
        self._req_send = req_send

    def submit(self, req: T) -> None:
        """Synchronously enqueue a request to the supervisor."""
        self._portal.call(self._req_send.send, req)


@contextmanager
def open_sync_supervisor():
    """Context manager spinning up a supervisor on a dedicated thread.

    Yields a ``SyncSupervisorHandle`` that sync code can call. The supervisor
    is torn down when the context exits.

    Implementation note: this skeleton stands up the portal and the request
    channel but does not add any daemons by default. Replace the body of
    ``_serve`` to add your own daemons (e.g. a worker reading from
    ``req_recv`` and producing actions).
    """
    request_channel: Channel = open_channel(maxsize=16)

    async def _serve() -> None:
        async with Supervisor(clock=WallClock()) as sup:
            # Replace this stub with your own daemons. Hand sup the
            # request_channel.recv side so workers can consume submissions.
            _ = sup  # keep the supervisor alive until cancelled
            await anyio.sleep_forever()

    with start_blocking_portal() as portal:
        # Start the supervisor task on the portal's loop.
        portal.start_task_soon(_serve)
        handle = SyncSupervisorHandle(portal, request_channel.send)
        try:
            yield handle
        finally:
            # Cancel the supervisor by closing the request channel; the
            # portal context manager will then drain the remaining work and
            # tear down its loop.
            portal.call(request_channel.send.aclose)
