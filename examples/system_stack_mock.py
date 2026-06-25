"""System 2 / 1 / 0 mock pipeline with deterministic model stubs.

This is an application-style three-rate inference pipeline. Each
``S{N}Service`` call is replaced by ``await ctx.clock.sleep(latency)`` plus
a deterministic toy computation, so the entire scenario runs in ~0
wall-clock time under ``SimClock``.

Byte-equality across repeated runs holds on the asyncio backend; trio's
default scheduler randomizes task-spawn order across runs, so cross-run logs
on trio agree in length, in monotone time, and in distribution, but not
bit-for-bit. See ``examples/README.md`` for the discussion.

In the service shape this models, observations are pushed into the dispatcher
from outside. This mock follows the same shape: the supervisor's main task
plays the harness role, writes the latest obs into ``obs_cache``, and advances
the SimClock between writes. There is no in-pipeline ``sensor`` daemon; obs is
an external input.

Pipeline layout (rates are virtual under ``SimClock``; numbers below mirror
the rates of a typical slow-planner / fast-policy / high-rate-actuator stack):

    main task (harness) ─▶ obs_cache  (Latest[Obs])
                                │
                  ┌─────────────┴──────────────┐
                  ▼                            ▼
            s2_planner (1 Hz)              s1_policy (10 Hz)
            reads obs_cache,                reads obs_cache + subgoal_cache,
            produces Subgoal                produces Chunk (8 actions)
                      │                              │
                      ▼                              ▼
                subgoal_cache (Latest)        chunk_channel
                                                     │
                                                     ▼
                                             s0_dispenser
                                             pops chunk, emits one Action
                                             every 1/S0_HZ (20 Hz)
                                                     │
                                                     ▼
                                             action_channel
                                                     │
                                                     ▼
                                             actuator (logs)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import anyio

from runlet import Channel, Context, SimClock, Supervisor, daemon, open_channel
from runlet.recipes.cooperative_every import cooperative_every
from runlet.recipes.latest import Latest

# --- data shapes ----------------------------------------------------------


@dataclass(frozen=True)
class Obs:
    t: float
    seq: int


@dataclass(frozen=True)
class Subgoal:
    target: float
    cog: int
    t_planned: float


@dataclass(frozen=True)
class Chunk:
    actions: tuple[float, ...]
    cog: int
    t_emitted: float


@dataclass(frozen=True)
class Action:
    cmd: float
    cog: int
    t_dispensed: float


# --- rates and mock latencies ---------------------------------------------

# Rates mirror a representative hierarchical inference stack:
#   - S2 fires at 1 Hz (period_ms: 1000)
#   - S1 fires at 10 Hz (period_ms: 100)
#   - S0 dispenses at the robot control rate (~20 Hz)
# The harness pushes obs at S1's obs_sampling_rate_hz (20 Hz) so the inference
# obs rate matches what S1 was trained against.
OBS_RATE_HZ = 20.0
S2_HZ = 1.0
S1_HZ = 10.0
S0_HZ = 20.0
CHUNK_SIZE = 8

S2_LATENCY = 0.05  # virtual seconds spent inside the mocked S2 inference
S1_LATENCY = 0.02


# --- daemons ---------------------------------------------------------------
# Note: no sensor daemon. Obs is pushed by the harness (the supervisor's
# main task in run_mock, mirroring an external observation callback).


@daemon
async def s2_planner(
    ctx: Context,
    obs_cache: Latest[Obs],
    subgoal_cache: Latest[Subgoal],
) -> None:
    """Mock S2: every 1/S2_HZ second, plan a subgoal from the latest obs."""
    async for _t in cooperative_every(ctx, 1.0 / S2_HZ):
        obs = obs_cache.get()
        if obs is None:
            continue
        await ctx.clock.sleep(S2_LATENCY)  # mock model forward
        sg = Subgoal(
            target=obs.t * 0.5,
            cog=(obs.seq * 31) % 997,
            t_planned=ctx.clock.now(),
        )
        subgoal_cache.set(sg)


@daemon
async def s1_policy(
    ctx: Context,
    obs_cache: Latest[Obs],
    subgoal_cache: Latest[Subgoal],
    chunk_out: Channel[Chunk],
) -> None:
    """Mock S1: every 1/S1_HZ second, produce an action chunk from latest obs+subgoal.

    Closes ``chunk_out.send`` on the way out so downstream daemons see
    ``EndOfStream`` and can exit on their own ``async for``.
    """
    try:
        async for _t in cooperative_every(ctx, 1.0 / S1_HZ):
            obs = obs_cache.get()
            sg = subgoal_cache.get()
            if obs is None or sg is None:
                continue
            await ctx.clock.sleep(S1_LATENCY)  # mock model forward
            actions = tuple(sg.target + 0.01 * i + obs.t * 0.001 for i in range(CHUNK_SIZE))
            await chunk_out.send.send(Chunk(actions=actions, cog=sg.cog, t_emitted=ctx.clock.now()))
    finally:
        await chunk_out.send.aclose()


@daemon
async def s0_dispenser(
    ctx: Context,
    chunk_in: Channel[Chunk],
    action_out: Channel[Action],
) -> None:
    """Pop chunks; dispense each action at 1/S0_HZ second.

    Polls ``ctx.stopping`` mid-chunk so a long chunk doesn't delay shutdown.
    Closes ``action_out.send`` so the actuator exits when we do.
    """
    period = 1.0 / S0_HZ
    try:
        async for chunk in chunk_in.recv:
            for cmd in chunk.actions:
                if ctx.stopping:
                    return
                await action_out.send.send(Action(cmd=cmd, cog=chunk.cog, t_dispensed=ctx.clock.now()))
                await ctx.clock.sleep(period)
    finally:
        await action_out.send.aclose()


@daemon
async def actuator(
    ctx: Context,
    action_in: Channel[Action],
    log: list[Action],
) -> None:
    """Drain action stream into ``log`` for downstream assertions.

    Terminates naturally when ``action_in.send`` is closed (cascading from
    ``s0_dispenser`` after stop signaling).
    """
    async for action in action_in.recv:
        log.append(action)


# --- entry point ----------------------------------------------------------


async def run_mock(duration_s: float = 2.0) -> list[Action]:
    """Run the mock pipeline for ``duration_s`` virtual seconds.

    The supervisor's main task plays the harness role: each iteration writes
    an Obs into the shared cache (the in-process equivalent of
    ``dispatcher.push_obs``) and then advances the SimClock by one obs period
    (the equivalent of ``dispatcher.tick``). When the simulated duration is
    exhausted it calls ``await sup.stop(grace=0)`` to force-cancel any daemon
    still sleeping on the (now-frozen) sim clock. Each daemon's
    ``try/finally`` closes its downstream channel so the actuator drains
    cleanly.

    Returns the actuator log. Deterministic across runs on the asyncio backend.
    """
    clock = SimClock()
    obs_cache: Latest[Obs] = Latest()
    subgoal_cache: Latest[Subgoal] = Latest()
    chunk_channel: Channel[Chunk] = open_channel(maxsize=4)
    action_channel: Channel[Action] = open_channel(maxsize=64)
    log: list[Action] = []

    async with Supervisor(clock=clock) as sup:
        sup.add(s2_planner(obs_cache, subgoal_cache))
        sup.add(s1_policy(obs_cache, subgoal_cache, chunk_channel))
        sup.add(s0_dispenser(chunk_channel, action_channel))
        sup.add(actuator(action_channel, log))

        # Harness loop: push obs, advance, repeat.
        obs_period = 1.0 / OBS_RATE_HZ
        n_steps = max(1, int(round(duration_s / obs_period)))
        for seq in range(n_steps):
            obs_cache.set(Obs(t=clock.now(), seq=seq))
            await anyio.sleep(0)  # let daemons schedule before time advances
            await clock.advance(obs_period)

        await sup.stop(grace=0.0)

    return log


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
    log = anyio.run(run_mock, 2.0)
    print(f"actuator received {len(log)} actions over 2.0 sim-seconds")
    if log:
        print(f"first: {log[0]}")
        print(f"last:  {log[-1]}")


if __name__ == "__main__":
    _main()
