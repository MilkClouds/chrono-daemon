"""reflex-dual mock — multi-session dispatcher via nested Supervisors.

Extends ``reflex_dual_mock`` to the N-concurrent-sessions shape of
worv-ai/reflex PR #191's ``HarnessDispatcher`` + ``TimeServerRegistry``.
The structure:

- The outer :class:`Supervisor` is the dispatcher. Its main task plays the
  harness role for *every* session: pushes obs per session, advances the
  shared sim clock, eventually calls ``register`` for new sessions and
  ``unregister`` for finished ones.
- Each session is hosted in an *inner* :class:`Supervisor` spawned as a
  daemon on the outer one. The inner supervisor owns the per-session
  S2/S1/S0/actuator daemons plus the per-session ``Latest`` caches.
- Tearing down a session means cancelling its inner supervisor's task —
  done by calling ``await inner.stop(grace=0)`` from inside the wrapping
  daemon, then returning. Outer supervisor's ``_on_daemon_exit`` removes
  the session from the registry.

This mirrors production's per-session ``S{N}Loop`` + ``TimeServerRegistry``
without adding any new primitives to runlet — the supervisor primitive
composes recursively.
"""

from __future__ import annotations

from dataclasses import dataclass

import anyio

# Reuse the per-session daemons from the single-session example.
from examples.reflex_dual_mock import (  # type: ignore[import-not-found]
    OBS_RATE_HZ,
    Action,
    Chunk,
    Latest,
    Obs,
    Subgoal,
    actuator,
    s0_dispenser,
    s1_policy,
    s2_planner,
)
from runlet import Channel, Context, SimClock, Supervisor, daemon, open_channel


@dataclass
class SessionLog:
    sid: str
    actions: list[Action]


@daemon
async def session_runner(
    ctx: Context,
    sid: str,
    log: SessionLog,
    duration_s: float,
) -> None:
    """Run one session's S2/S1/S0/actuator pipeline in an inner supervisor.

    Returns when ``duration_s`` virtual seconds have elapsed *on this
    session's local sim clock*. Per-session clock isolation is the whole
    point of nesting.
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
            obs_cache.set(Obs(t=inner_clock.now(), seq=seq))
            await anyio.sleep(0)
            await inner_clock.advance(obs_period)

        await inner.stop(grace=0.0)


async def run_multi_session(
    sessions: dict[str, float],
) -> dict[str, SessionLog]:
    """Run several sessions concurrently under one outer Supervisor.

    ``sessions`` maps session id → duration in virtual seconds. Each session
    runs in its own inner Supervisor with its own SimClock.
    """
    logs: dict[str, SessionLog] = {sid: SessionLog(sid=sid, actions=[]) for sid in sessions}
    async with Supervisor() as outer:
        for sid, dur in sessions.items():
            outer.add(session_runner(sid, logs[sid], dur), name=f"runner/{sid}")
    return logs


def _main() -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
    logs = anyio.run(run_multi_session, {"sess-A": 1.0, "sess-B": 2.0, "sess-C": 0.5})
    for sid, log in logs.items():
        print(f"{sid}: {len(log.actions)} actions")


if __name__ == "__main__":
    _main()
