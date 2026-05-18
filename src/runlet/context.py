"""Per-daemon execution context."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import anyio

if TYPE_CHECKING:
    from runlet._logging import ClockAwareLoggerAdapter
    from runlet.clock import Clock
    from runlet.supervisor import Supervisor

__all__ = ["Context"]


@dataclass(frozen=True, slots=True)
class Context:
    """Handle passed to each daemon's ``run(ctx)``.

    A daemon should reach for ``ctx.clock`` (never ``anyio.sleep`` directly) so that
    ``SimClock`` can intercept time. ``ctx.cancel_scope`` is per-daemon — cancelling it
    stops only this daemon; siblings keep running. To stop the whole supervisor,
    call ``ctx.supervisor.signal_stop()`` (fire-and-forget, ADR 0009).

    ``ctx.stop_event`` and the ``ctx.stopping`` shorthand let cooperative daemons
    react to the supervisor's stop signal. Poll ``ctx.stopping`` between work
    units, or ``await ctx.stop_event.wait()`` to block until stop is requested.
    ``runlet.recipes.cooperative_every`` wraps the common periodic pattern.

    ``ctx.logger`` is a :class:`runlet._logging.ClockAwareLoggerAdapter` wrapping the
    supervisor's logger and the active clock. Every record carries ``sim_time`` in
    its ``extra`` mapping — use a format string like
    ``"%(sim_time).3f %(name)s %(message)s"`` to surface it.
    """

    clock: Clock
    cancel_scope: anyio.CancelScope
    logger: ClockAwareLoggerAdapter | logging.Logger
    name: str
    supervisor: Supervisor
    stop_event: anyio.Event

    @property
    def stopping(self) -> bool:
        """``True`` once ``supervisor.signal_stop()`` (or ``stop()``) has been called."""
        return self.stop_event.is_set()
