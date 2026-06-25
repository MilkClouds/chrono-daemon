"""Internal type aliases and exceptions for runlet."""

from __future__ import annotations

from typing import Literal

__all__ = [
    "ChannelClosed",
    "ChannelInUse",
    "DaemonError",
    "EndOfStream",
    "OnError",
    "WouldBlock",
]


class EndOfStream(Exception):
    """Raised after the send side is closed and the buffer is drained."""


class ChannelClosed(Exception):
    """Raised after the receive side has closed."""


class ChannelInUse(Exception):
    """Raised when a channel endpoint is used concurrently."""


class WouldBlock(Exception):
    """Raised when a nowait operation cannot complete immediately."""


class DaemonError(Exception):
    """Wraps an exception that escaped a daemon lifecycle method."""


OnError = Literal["shutdown", "restart", "ignore"]
"""Supervisor policy for uncaught daemon exceptions."""
