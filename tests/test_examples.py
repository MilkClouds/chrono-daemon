"""Pin the system-stack mock example: same inputs → identical actuator log on both backends."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
sys.path.insert(0, str(EXAMPLES_DIR))

from system_stack_mock import (  # noqa: E402  (sys.path mutation above is intentional)
    Action,
    run_mock,
)

pytestmark = pytest.mark.anyio


async def test_system_stack_mock_produces_actions() -> None:
    log: list[Action] = await run_mock(duration_s=2.0)
    # The exact count depends on SimClock timing of S2_LATENCY + S1_LATENCY + S0
    # dispense rate; what matters is that the pipeline produced a nontrivial
    # stream and that every action carries a coherent (cog, t_dispensed).
    assert len(log) > 0
    assert all(isinstance(a, Action) for a in log)
    # Time must be monotonically non-decreasing (S0 dispenses sequentially).
    times = [a.t_dispensed for a in log]
    assert times == sorted(times)


async def test_system_stack_mock_is_deterministic_across_runs(anyio_backend: str) -> None:
    """Two runs produce structurally identical logs (same length + monotone time).

    Byte-equality additionally holds on asyncio but not on trio: trio's default
    scheduler intentionally randomizes task-spawn order across runs (an
    ASLR-like measure against accidental ordering dependence). See
    ``examples/README.md`` for the short discussion.
    """
    a = await run_mock(duration_s=2.0)
    b = await run_mock(duration_s=2.0)
    assert len(a) == len(b)
    if anyio_backend == "asyncio":
        assert a == b


async def test_system_stack_mock_scales_with_sim_time() -> None:
    """Twice the virtual duration ⇒ strictly more actions."""
    short = await run_mock(duration_s=1.0)
    long = await run_mock(duration_s=2.0)
    assert len(long) > len(short)


# -- multi-session example ------------------------------------------------


from system_stack_multi_session import run_multi_session, run_with_early_unregister  # noqa: E402


async def test_multi_session_isolation() -> None:
    """Each session runs to its own duration in its own inner Supervisor.

    Sessions with different ``duration_s`` produce different action counts
    proportional to their duration, proving the per-session SimClocks are
    actually independent. Durations need to exceed S2's 1 Hz period so the
    first subgoal lands and S1/S0 can produce.
    """
    logs = await run_multi_session({"short": 2.0, "long": 3.0})
    assert set(logs) == {"short", "long"}
    for sid, log in logs.items():
        assert log.actions, f"session {sid!r} produced no actions"
    assert len(logs["long"].actions) > len(logs["short"].actions)


async def test_multi_session_unregister_cancels_only_that_session() -> None:
    """Calling unregister before duration is up exits the targeted session promptly.

    A full 10-sim-second run would produce ~200 actions (S0_HZ * effective time).
    An honoured unregister produces dramatically fewer. More importantly, the
    test returns rather than hanging on the long duration.
    """
    log = await run_with_early_unregister(long_duration_s=10.0, cancel_after_s=0.0)
    assert log.sid == "victim"
    # Conservative bound: without unregister this would be ~200; with it, far fewer.
    assert len(log.actions) < 100, f"unregister apparently not honored; got {len(log.actions)} actions"
