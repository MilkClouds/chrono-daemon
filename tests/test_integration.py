"""End-to-end: a 3-daemon sensor → controller → motor pipeline driven by SimClock burst.

This is the "would chrono-daemon replace simple_env" check. A driver task calls
``await clock.advance(2.0)`` once and the entire pipeline runs deterministically;
the final motor state matches the expected closed-form value.
"""

from __future__ import annotations

import anyio
import pytest

from chrono_daemon import Channel, Context, SimClock, Supervisor, daemon, open_channel

pytestmark = pytest.mark.anyio


@daemon
async def sensor(ctx: Context, out: Channel[float], period: float, n: int) -> None:
    """Emit ``n`` readings spaced by ``period``. Reading at tick k is value ``k+1``."""
    for k in range(n):
        await ctx.clock.sleep(period)
        await out.send.send(float(k + 1))
    await out.send.aclose()


@daemon
async def controller(ctx: Context, src: Channel[float], dst: Channel[float], gain: float) -> None:
    """Forward each reading multiplied by ``gain``."""
    async for reading in src.recv:
        await dst.send.send(reading * gain)
    await dst.send.aclose()


@daemon
async def motor(ctx: Context, src: Channel[float], state: list[float]) -> None:
    """Accumulate each command into ``state[0]``."""
    async for cmd in src.recv:
        state[0] += cmd


async def test_three_daemon_pipeline_under_simclock() -> None:
    clock = SimClock()
    raw: Channel[float] = open_channel(maxsize=4)
    commanded: Channel[float] = open_channel(maxsize=4)
    state: list[float] = [0.0]

    async with Supervisor(clock=clock) as sup:
        sup.add(sensor(raw, period=0.1, n=10))
        sup.add(controller(raw, commanded, gain=0.5))
        sup.add(motor(commanded, state))
        await anyio.sleep(0)
        await clock.advance(2.0)

    # Sensor emits 1..10; controller multiplies by 0.5 → 0.5, 1.0, 1.5, ..., 5.0;
    # motor accumulates → 0.5 * (1 + 2 + ... + 10) = 0.5 * 55 = 27.5
    assert state[0] == pytest.approx(27.5)


async def test_pipeline_is_deterministic_across_runs() -> None:
    """Two independent runs with the same SimClock+inputs must produce identical state."""

    async def run_once() -> float:
        clock = SimClock()
        raw: Channel[float] = open_channel(maxsize=4)
        commanded: Channel[float] = open_channel(maxsize=4)
        state: list[float] = [0.0]
        async with Supervisor(clock=clock) as sup:
            sup.add(sensor(raw, period=0.1, n=10))
            sup.add(controller(raw, commanded, gain=0.5))
            sup.add(motor(commanded, state))
            await anyio.sleep(0)
            await clock.advance(2.0)
        return state[0]

    a = await run_once()
    b = await run_once()
    assert a == b
