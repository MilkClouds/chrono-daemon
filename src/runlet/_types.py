"""Internal type aliases and exceptions for runlet."""

from __future__ import annotations

from typing import Literal

__all__ = [
    "ChannelClosed",
    "DaemonError",
    "EndOfStream",
    "OnError",
]


class EndOfStream(Exception):
    """Raised by `ReceiveStream.receive()` after the sender side is closed and the buffer is drained.

    Mirrors anyio's `EndOfStream` so users don't have to import anyio.
    """


class ChannelClosed(Exception):
    """Raised by `SendStream.send()` after the receive side has been closed.

    The send-side has nowhere to deliver, so the call cannot make progress.
    """


class DaemonError(Exception):
    """Wraps an exception that escaped a daemon's `run()`.

    The supervisor attaches the failing daemon's name (``"daemon 'X' failed: ..."``)
    so the failing unit is identifiable in the resulting ``ExceptionGroup``.
    The original exception is preserved as ``__cause__``.
    """


OnError = Literal["shutdown", "restart", "ignore"]
"""Supervisor policy when a hosted daemon raises an uncaught exception.

- ``"shutdown"`` (default): re-raise; anyio's TaskGroup cancels all siblings.
- ``"restart"``: re-enter ``on_start``/``run``/``on_stop`` after a backoff governed by ``RestartPolicy``.
- ``"ignore"``: log and let the daemon exit silently; siblings keep running.
"""
