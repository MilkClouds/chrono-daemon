"""Stop-aware periodic iteration."""

from __future__ import annotations

from collections.abc import AsyncIterator

from runlet import Context


async def cooperative_every(ctx: Context, period: float) -> AsyncIterator[float]:
    """Yield clock ticks until ``ctx.stopping`` becomes True."""
    if period <= 0:
        raise ValueError(f"period must be positive, got {period}")
    async for t in ctx.clock.every(period):
        if ctx.stopping:
            return
        yield t
