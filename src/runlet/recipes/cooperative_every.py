"""Recipe: stop-aware periodic iteration.

Same as ``ctx.clock.every(period)``, but exits cleanly when the supervisor
signals stop via :meth:`Supervisor.signal_stop` or :meth:`Supervisor.stop`.
Wraps the common pattern::

    async for t in ctx.clock.every(period):
        if ctx.stopping:
            return
        ...

so daemons don't repeat the check at every tick. Use as::

    @daemon
    async def sensor(ctx, out):
        async for t in cooperative_every(ctx, 0.1):
            await out.send.send(read())

For daemons that need to react *immediately* to stop (rather than at the
next tick), race ``await ctx.stop_event.wait()`` in a task group instead —
the recipe trades responsiveness for one-line ergonomics.

Import as ``from runlet.recipes.cooperative_every import cooperative_every``.
The recipe namespace (``runlet.recipes``) is best-effort — see
``src/runlet/recipes/__init__.py`` for the stability contract.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from runlet import Context


async def cooperative_every(ctx: Context, period: float) -> AsyncIterator[float]:
    """Yield clock ticks at ``period`` until ``ctx.stopping`` becomes True.

    Polls ``ctx.stopping`` at every yield point. Note: if stop is signaled
    while the daemon is *inside* a ``ctx.clock.sleep`` call (which the
    underlying ``ctx.clock.every`` is doing between ticks), the daemon won't
    notice until the sleep completes — unless something else cancels it.

    Under :class:`runlet.WallClock`, ``Supervisor.stop(grace=N)`` will let
    polling daemons exit naturally during the ``grace`` window. Under
    :class:`runlet.SimClock`, the supervisor cannot make sim time advance
    on the daemon's behalf, so ``stop()`` will reach for a force-cancel
    after ``grace`` expires; daemons clean up through their ``finally``
    blocks (or the supervisor's shielded ``on_stop`` invocation).
    """
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")
    async for t in ctx.clock.every(period):
        if ctx.stopping:
            return
        yield t
