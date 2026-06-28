"""Daemon lifecycle base class and decorator."""

from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from chrono_daemon.context import Context

__all__ = ["Daemon", "daemon"]


class Daemon(ABC):
    """Abstract long-running async unit."""

    name: str = ""

    async def on_start(self, ctx: Context) -> None:
        """Called once before ``run``. Defaults to no-op."""

    @abstractmethod
    async def run(self, ctx: Context) -> None:
        """Main body. Use ``ctx.clock`` for sleeps."""

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
    """Turn ``async def fn(ctx, *args, **kwargs)`` into a Daemon factory."""

    def wrap(f: Callable[..., Coroutine[Any, Any, None]]) -> Callable[..., Daemon]:
        chosen_name = name or f.__name__

        @functools.wraps(f)
        def factory(*args: Any, **kwargs: Any) -> Daemon:
            return _FnDaemon(f, args, kwargs, chosen_name)

        # Marker for debug/repr tooling; not part of the public API.
        factory._chrono_daemon_factory = True  # type: ignore[attr-defined]
        return factory

    if fn is not None:
        return wrap(fn)
    return wrap
