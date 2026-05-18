"""Internal: clock-aware logging adapter.

Wraps a stdlib ``logging.Logger`` so each emitted record carries the clock's
current ``now()`` as ``sim_time`` in the record's ``extra``. Users that want
to see sim-time in their log format use the format string
``"%(sim_time)s %(message)s"`` (or similar) and get the virtual instant the
log line was emitted at — even under ``SimClock`` burst replay where wall
time advances by microseconds while sim time advances by seconds.

Without this, logging under ``SimClock`` is nearly useless: every line shows
the same wall-clock second and the relative ordering is invisible. With it,
the log is the most direct trace of "what fired when" in the simulation.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from runlet.clock import Clock


__all__ = ["ClockAwareLoggerAdapter"]


class ClockAwareLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter that injects ``clock.now()`` into every record as ``sim_time``.

    Users pass this adapter where they would pass a ``Logger``. The adapter
    quacks like a logger for all standard call shapes (``info``, ``debug``,
    ``exception``, etc.) and forwards through with the extra field.

    The clock reference is held weakly-by-attribute, not by name — if the
    supervisor's clock is swapped out, an existing adapter keeps pointing at
    the original. This is intentional: per-daemon adapters are created at
    daemon-host time and live for the daemon's lifetime.
    """

    def __init__(self, logger: logging.Logger, clock: Clock) -> None:
        super().__init__(logger, {})
        self._clock = clock

    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        # Don't overwrite an explicit sim_time the user already passed.
        extra.setdefault("sim_time", self._clock.now())
        return msg, kwargs
