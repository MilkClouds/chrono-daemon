"""Daemon: a long-running async unit with lifecycle hooks.

Two equivalent ways to define one:

1. Subclass ``Daemon`` and override ``run`` (and optionally ``on_start`` / ``on_stop``).
2. Use the ``@daemon`` decorator on an ``async def fn(ctx, *args, **kwargs)``. The
   decorated callable becomes a *factory*: calling it returns a ``Daemon`` instance.

The two paths produce the same kind of object. The decorator is sugar for the
99% case where you have no lifecycle state beyond the function body.
"""

from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from runlet.context import Context

__all__ = ["Daemon", "daemon"]


class Daemon(ABC):
    """Abstract long-running async unit. Subclass and override ``run``.

    The default ``on_start`` and ``on_stop`` are no-ops; override them if you have
    setup / teardown that should run inside the supervisor's task group.
    """

    name: str = ""

    async def on_start(self, ctx: Context) -> None:
        """Called once before ``run``. Defaults to no-op."""

    @abstractmethod
    async def run(self, ctx: Context) -> None:
        """Main body. When this returns, ``on_stop`` is called and the daemon is done.

        To sleep, call ``await ctx.clock.sleep(...)`` — *not* ``anyio.sleep`` — so
        ``SimClock`` can intercept time.
        """

    async def on_stop(self, ctx: Context) -> None:
        """Called once after ``run`` completes (or after a final restart failure). Defaults to no-op."""


class _FnDaemon(Daemon):
    """Internal adapter wrapping a function into a Daemon. Returned by ``@daemon``."""

    def __init__(
        self,
        fn: Callable[..., Coroutine[Any, Any, None]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        name: str,
    ) -> None:
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self.name = name

    async def run(self, ctx: Context) -> None:
        await self._fn(ctx, *self._args, **self._kwargs)


_AsyncFn = Callable[..., Coroutine[Any, Any, None]]


@overload
def daemon(fn: _AsyncFn, /) -> Callable[..., Daemon]: ...
@overload
def daemon(*, name: str | None = None) -> Callable[[_AsyncFn], Callable[..., Daemon]]: ...


def daemon(  # type: ignore[misc]
    fn: Callable[..., Coroutine[Any, Any, None]] | None = None,
    *,
    name: str | None = None,
) -> Any:
    """Turn an ``async def fn(ctx, *args, **kwargs)`` into a Daemon factory.

    Usage::

        @daemon
        async def ticker(ctx, period: float, out):
            async for _ in ctx.clock.every(period):
                await out.send(ctx.clock.now())

        sup.add(ticker(0.1, out_send))   # ticker(...) returns a Daemon instance.

    The function's first parameter MUST be ``ctx`` (the runlet ``Context``).
    Additional positional/keyword args are forwarded by the factory.
    """

    def wrap(f: Callable[..., Coroutine[Any, Any, None]]) -> Callable[..., Daemon]:
        chosen_name = name or f.__name__

        @functools.wraps(f)
        def factory(*args: Any, **kwargs: Any) -> Daemon:
            return _FnDaemon(f, args, kwargs, chosen_name)

        # Marker for debug/repr tooling; not part of the public API.
        factory._runlet_daemon_factory = True  # type: ignore[attr-defined]
        return factory

    if fn is not None:
        return wrap(fn)
    return wrap
