"""Tiny anyio-based concurrency primitives.

Public surface:

- :class:`Channel`, :class:`SendStream`, :class:`ReceiveStream`, :func:`open_channel`,
  :exc:`EndOfStream`, :exc:`ChannelClosed`, :exc:`ChannelInUse`
- :class:`Clock`, :class:`WallClock`, :class:`SimClock`
- :class:`Context`
- :class:`Daemon`, :func:`daemon`
- :class:`Supervisor`, :class:`RestartPolicy`, :data:`OnError`
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _package_version

from chrono_daemon._types import ChannelClosed, ChannelInUse, DaemonError, EndOfStream, OnError, WouldBlock
from chrono_daemon.channel import Channel, ChannelStats, ReceiveStream, SendStream, open_channel
from chrono_daemon.clock import Clock, SimClock, WallClock
from chrono_daemon.context import Context
from chrono_daemon.daemon import Daemon, daemon
from chrono_daemon.supervisor import DaemonFailurePhase, DaemonHealth, DaemonState, RestartPolicy, Supervisor

try:
    __version__ = _package_version("chrono-daemon")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "Channel",
    "ChannelClosed",
    "ChannelInUse",
    "ChannelStats",
    "Clock",
    "Context",
    "Daemon",
    "DaemonError",
    "DaemonFailurePhase",
    "DaemonHealth",
    "DaemonState",
    "EndOfStream",
    "OnError",
    "ReceiveStream",
    "RestartPolicy",
    "SendStream",
    "SimClock",
    "Supervisor",
    "WallClock",
    "WouldBlock",
    "__version__",
    "daemon",
    "open_channel",
]
