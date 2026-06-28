"""Internal: clock-aware logging adapter."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from chrono_daemon.clock import Clock


__all__ = ["ClockAwareLoggerAdapter"]


class ClockAwareLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter that adds ``clock.now()`` as ``sim_time``."""

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
