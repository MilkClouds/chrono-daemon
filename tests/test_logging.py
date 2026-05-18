"""ClockAwareLoggerAdapter attaches sim_time to every record."""

from __future__ import annotations

import logging

import pytest

from runlet import Context, Daemon, SimClock, Supervisor

pytestmark = pytest.mark.anyio


async def test_logger_records_carry_sim_time(caplog: pytest.LogCaptureFixture) -> None:
    """A daemon's ctx.logger should put the clock's current time into record.sim_time."""
    clock = SimClock(t0=100.0)

    seen: list[tuple[str, float]] = []

    class _Logger(Daemon):
        name = "logger-daemon"

        async def run(self, ctx: Context) -> None:
            ctx.logger.info("first")
            await ctx.clock.sleep(2.5)
            ctx.logger.info("second")

    with caplog.at_level(logging.INFO, logger="runlet"):
        async with Supervisor(clock=clock) as sup:
            sup.add(_Logger())
            await clock.advance(10.0)

    records = [r for r in caplog.records if r.name.endswith("logger-daemon")]
    assert len(records) == 2
    seen = [(r.getMessage(), getattr(r, "sim_time", -1.0)) for r in records]
    assert seen == [("first", 100.0), ("second", 102.5)]


async def test_explicit_extra_sim_time_is_not_overwritten() -> None:
    """If the user passes their own sim_time, the adapter respects it."""
    from runlet._logging import ClockAwareLoggerAdapter

    clock = SimClock(t0=5.0)
    base = logging.getLogger("test_runlet.logger_passthrough")
    adapter = ClockAwareLoggerAdapter(base, clock)

    # Adapter.process is the entry point for record extras.
    _, kwargs = adapter.process("hi", {"extra": {"sim_time": 999.0}})
    assert kwargs["extra"]["sim_time"] == 999.0  # user-supplied wins
