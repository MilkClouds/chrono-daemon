"""runlet — tiny anyio-based concurrency primitives.

Public surface:

- :class:`Channel`, :class:`SendStream`, :class:`ReceiveStream`, :func:`open_channel`,
  :exc:`EndOfStream`, :exc:`ChannelClosed`
- :class:`Clock`, :class:`WallClock`, :class:`SimClock`
- :class:`Context`
- :class:`Daemon`, :func:`daemon`
- :class:`Supervisor`, :class:`RestartPolicy`, :data:`OnError`
"""

from __future__ import annotations

from runlet._types import ChannelClosed, DaemonError, EndOfStream, OnError
from runlet.channel import Channel, ChannelStats, ReceiveStream, SendStream, open_channel
from runlet.clock import Clock, SimClock, WallClock
from runlet.context import Context
from runlet.daemon import Daemon, daemon
from runlet.supervisor import RestartPolicy, Supervisor

__version__ = "0.1.0"

__all__ = [
    "Channel",
    "ChannelClosed",
    "ChannelStats",
    "Clock",
    "Context",
    "Daemon",
    "DaemonError",
    "EndOfStream",
    "OnError",
    "ReceiveStream",
    "RestartPolicy",
    "SendStream",
    "SimClock",
    "Supervisor",
    "WallClock",
    "__version__",
    "daemon",
    "open_channel",
]
