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

    Use ``ctx.clock`` for sleeps. ``ctx.cancel_scope`` is per-daemon; use
    ``ctx.supervisor.signal_stop()`` to request whole-supervisor shutdown.
    ``ctx.stopping`` and ``ctx.stop_event`` expose that stop signal.
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
