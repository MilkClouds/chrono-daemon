"""Daemon: ABC and decorator should be equivalent, lifecycle hooks fire in order."""

from __future__ import annotations

import pytest

from runlet import Context, Daemon, SimClock, Supervisor, daemon

pytestmark = pytest.mark.anyio


class _Recorder(Daemon):
    def __init__(self, log: list[str], suffix: str) -> None:
        self._log = log
        self._suffix = suffix
        self.name = f"recorder-{suffix}"

    async def on_start(self, ctx: Context) -> None:
        self._log.append(f"start:{self._suffix}")

    async def run(self, ctx: Context) -> None:
        self._log.append(f"run:{self._suffix}")

    async def on_stop(self, ctx: Context) -> None:
        self._log.append(f"stop:{self._suffix}")


async def test_class_daemon_lifecycle_order() -> None:
    log: list[str] = []
    async with Supervisor(clock=SimClock()) as sup:
        sup.add(_Recorder(log, "A"))
    assert log == ["start:A", "run:A", "stop:A"]


async def test_decorator_daemon_runs_body_with_args() -> None:
    log: list[str] = []

    @daemon
    async def saying(ctx: Context, msg: str) -> None:
        log.append(f"said:{msg}")

    factory = saying
    instance = factory("hello")
    assert isinstance(instance, Daemon)
    assert instance.name == "saying"

    async with Supervisor(clock=SimClock()) as sup:
        sup.add(instance)
    assert log == ["said:hello"]


async def test_decorator_with_explicit_name() -> None:
    @daemon(name="custom-name")
    async def fn(ctx: Context) -> None:
        pass

    instance = fn()
    assert instance.name == "custom-name"


async def test_supervisor_spawn_wraps_ad_hoc_fn() -> None:
    log: list[int] = []

    async def adder(ctx: Context, x: int, y: int) -> None:
        log.append(x + y)

    async with Supervisor(clock=SimClock()) as sup:
        sup.spawn(adder, 2, 3)
    assert log == [5]
