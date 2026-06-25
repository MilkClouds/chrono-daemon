"""system-stack mock: multi-session dispatcher via nested Supervisors.

Extends ``system_stack_mock`` to the N-concurrent-sessions shape used by a
dispatcher-backed inference service. The structure:

- The outer :class:`Supervisor` hosts a :class:`MockDispatcher` that
  exposes ``register(sid, duration_s)`` and ``unregister(sid)``. These are the
  per-session lifecycle operations exposed to the outer application.
- Each registered session runs in an *inner* :class:`Supervisor` (with its
  own :class:`SimClock`) spawned as one daemon on the outer one. The
  inner supervisor owns the per-session S2/S1/S0/actuator daemons and the
  per-session ``Latest`` caches.
- ``unregister(sid)`` sets a per-session ``anyio.Event``; the session's
  outer-side daemon notices on its next loop iteration, calls
  ``inner.stop(grace=0)``, and returns. The outer supervisor's
  ``_on_daemon_exit`` then forgets the session.

This mirrors a per-session ``S{N}`` loop set plus per-session clock ownership
without adding any new primitives to runlet: the supervisor primitive composes
recursively, and ``anyio.Event`` carries the cancel signal.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import anyio

# Allow `python examples/system_stack_multi_session.py` to import the sibling
# single-session module without needing `examples/` to be a package.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from system_stack_mock import (  # noqa: E402
    OBS_RATE_HZ,
    Action,
    Chunk,
    Obs,
    Subgoal,
    actuator,
    s0_dispenser,
    s1_policy,
    s2_planner,
)

from runlet import Channel, Context, SimClock, Supervisor, daemon, open_channel  # noqa: E402
from runlet.recipes.latest import Latest  # noqa: E402


@dataclass
class SessionLog:
    sid: str
    actions: list[Action] = field(default_factory=list)


@daemon
async def session_runner(
    ctx: Context,
    sid: str,
    log: SessionLog,
    duration_s: float,
    cancel_event: anyio.Event,
) -> None:
    """Host one session's S2/S1/S0/actuator pipeline in an inner Supervisor.

    Returns when either (a) ``duration_s`` virtual seconds have elapsed on
    the session's local sim clock, or (b) ``cancel_event`` fires (caller
    called ``unregister``). Both paths shut the inner supervisor down with
    ``stop(grace=0)``.
    """
    inner_clock = SimClock()
    obs_cache: Latest[Obs] = Latest()
    subgoal_cache: Latest[Subgoal] = Latest()
    chunk_channel: Channel[Chunk] = open_channel(maxsize=4)
    action_channel: Channel[Action] = open_channel(maxsize=64)

    async with Supervisor(clock=inner_clock) as inner:
        inner.add(s2_planner(obs_cache, subgoal_cache), name=f"{sid}/s2")
        inner.add(s1_policy(obs_cache, subgoal_cache, chunk_channel), name=f"{sid}/s1")
        inner.add(s0_dispenser(chunk_channel, action_channel), name=f"{sid}/s0")
        inner.add(actuator(action_channel, log.actions), name=f"{sid}/act")

        obs_period = 1.0 / OBS_RATE_HZ
        n_steps = max(1, int(round(duration_s / obs_period)))
        for seq in range(n_steps):
            if cancel_event.is_set():
                break
            obs_cache.set(Obs(t=inner_clock.now(), seq=seq))
            await anyio.sleep(0)
            await inner_clock.advance(obs_period)

        await inner.stop(grace=0.0)


class MockDispatcher:
    """Outer-side handle exposing register/unregister over a Supervisor.

    A small dispatcher surface limited to the parts that matter for lifecycle.
    ``push_obs``/``tick`` are absent because under runlet each session's inner
    SimClock and obs cache are driven inside ``session_runner``; a downstream
    application can replace those by routing harness calls through this object.
    """

    def __init__(self, sup: Supervisor) -> None:
        self._sup = sup
        self._cancel: dict[str, anyio.Event] = {}
        self._logs: dict[str, SessionLog] = {}

    def register(self, sid: str, *, duration_s: float) -> SessionLog:
        if sid in self._cancel:
            raise ValueError(f"session {sid!r} already registered")
        ev = anyio.Event()
        log = SessionLog(sid=sid)
        self._cancel[sid] = ev
        self._logs[sid] = log
        self._sup.add(session_runner(sid, log, duration_s, ev), name=f"sess/{sid}")
        return log

    def unregister(self, sid: str) -> None:
        ev = self._cancel.pop(sid, None)
        if ev is not None:
            ev.set()

    def log_for(self, sid: str) -> SessionLog:
        return self._logs[sid]


async def run_multi_session(sessions: dict[str, float]) -> dict[str, SessionLog]:
    """Run N sessions concurrently and return their logs.

    Each session runs to its own configured ``duration_s`` and then exits
    naturally; the outer Supervisor blocks on ``__aexit__`` until they all
    do. See :func:`run_with_early_unregister` for the dynamic-unregister
    shape that mirrors an external episode-end call.
    """
    async with Supervisor() as outer:
        d = MockDispatcher(outer)
        for sid, dur in sessions.items():
            d.register(sid, duration_s=dur)
    return d._logs  # all session_runner daemons have returned by now


async def run_with_early_unregister(
    long_duration_s: float,
    cancel_after_s: float,
) -> SessionLog:
    """Start one long-running session, then ``unregister`` it after a brief delay.

    Demonstrates the per-session cancel path: only the registered session is
    stopped, not the supervisor as a whole.
    """
    async with Supervisor() as outer:
        d = MockDispatcher(outer)
        log = d.register("victim", duration_s=long_duration_s)
        await anyio.sleep(cancel_after_s)
        d.unregister("victim")
    return log


def _main() -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
    print("--- two concurrent sessions, each runs to its own duration ---")
    logs = anyio.run(run_multi_session, {"sess-A": 2.0, "sess-B": 3.0})
    for sid, log in logs.items():
        print(f"  {sid}: {len(log.actions)} actions")
    print()
    print("--- one long session, dispatcher.unregister fires after 0s wall-clock ---")
    log = anyio.run(run_with_early_unregister, 10.0, 0.0)
    print(f"  {log.sid}: {len(log.actions)} actions (would be ~200 without cancel)")


if __name__ == "__main__":
    _main()
