"""reflex-dual mock pipeline — System 2 / 1 / 0 inference, every model call mocked.

A direct mock of the production architecture in worv-ai/reflex PR #191. Each
``S{N}Service`` call is replaced by ``await ctx.clock.sleep(latency)`` plus a
deterministic toy computation, so the entire scenario runs in ~0 wall-clock
time under ``SimClock``.

Byte-equality across repeated runs holds on the asyncio backend; trio's
default scheduler randomizes task-spawn order across runs, so cross-run logs
on trio agree in length, in monotone time, and in distribution — but not
bit-for-bit. See ``examples/README.md`` for the discussion and a sketch of
the "ready gate" workaround.

The point of this example is to stress-test runlet's ergonomics on a realistic
multi-rate reactive pattern. The post-mortem — what felt clean, what felt
like boilerplate, where ``Supervisor.stop()`` is missed — is in
``examples/README.md``.

Pipeline layout (rates are virtual under ``SimClock``):

    sensor (10 Hz) ─▶ obs_cache  (Latest[Obs])
                            │
              ┌─────────────┴──────────────┐
              ▼                            ▼
        s2_planner (2 Hz)              s1_policy (10 Hz)
        reads obs_cache,                reads obs_cache + subgoal_cache,
        produces Subgoal                produces Chunk (8 actions)
                  │                              │
                  ▼                              ▼
            subgoal_cache (Latest)        chunk_channel
                                                 │
                                                 ▼
                                         s0_dispenser
                                         pops chunk, emits one Action
                                         every 1/S0_HZ
                                                 │
                                                 ▼
                                         action_channel
                                                 │
                                                 ▼
                                         actuator (logs)

The ``driver`` daemon advances the SimClock by ``duration_s`` and then raises
``_PipelineDone`` to bring the supervisor down cleanly. See the post-mortem
for the discussion of why an explicit ``Supervisor.stop()`` would be nicer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Generic, TypeVar

import anyio

from runlet import Channel, Context, DaemonError, SimClock, Supervisor, daemon, open_channel

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


# --- one-slot cache for "latest value" state across tasks ------------------
# Single-attribute reads/writes are atomic in Python; no lock needed for the
# at-most-one-writer / many-reader pattern we use below.


T = TypeVar("T")


class Latest(Generic[T]):
    __slots__ = ("_v",)

    def __init__(self) -> None:
        self._v: T | None = None

    def get(self) -> T | None:
        return self._v

    def set(self, value: T) -> None:
        self._v = value


# --- rates and mock latencies ---------------------------------------------

SENSOR_HZ = 10.0
S2_HZ = 2.0
S1_HZ = 10.0
S0_HZ = 100.0
CHUNK_SIZE = 8

S2_LATENCY = 0.05  # virtual seconds spent inside the mocked S2 inference
S1_LATENCY = 0.02


# --- daemons ---------------------------------------------------------------


@daemon
async def sensor(ctx: Context, obs_cache: Latest[Obs]) -> None:
    """Emit one Obs per 1/SENSOR_HZ second into the shared cache."""
    seq = 0
    async for t in ctx.clock.every(1.0 / SENSOR_HZ):
        obs_cache.set(Obs(t=t, seq=seq))
        seq += 1


@daemon
async def s2_planner(
    ctx: Context,
    obs_cache: Latest[Obs],
    subgoal_cache: Latest[Subgoal],
) -> None:
    """Mock S2: every 1/S2_HZ second, plan a subgoal from the latest obs."""
    async for _t in ctx.clock.every(1.0 / S2_HZ):
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
    """Mock S1: every 1/S1_HZ second, produce an action chunk from latest obs+subgoal."""
    async for _t in ctx.clock.every(1.0 / S1_HZ):
        obs = obs_cache.get()
        sg = subgoal_cache.get()
        if obs is None or sg is None:
            continue
        await ctx.clock.sleep(S1_LATENCY)  # mock model forward
        actions = tuple(sg.target + 0.01 * i + obs.t * 0.001 for i in range(CHUNK_SIZE))
        await chunk_out.send.send(Chunk(actions=actions, cog=sg.cog, t_emitted=ctx.clock.now()))


@daemon
async def s0_dispenser(
    ctx: Context,
    chunk_in: Channel[Chunk],
    action_out: Channel[Action],
) -> None:
    """Pop chunks; dispense each action at 1/S0_HZ second."""
    period = 1.0 / S0_HZ
    async for chunk in chunk_in.recv:
        for cmd in chunk.actions:
            await action_out.send.send(Action(cmd=cmd, cog=chunk.cog, t_dispensed=ctx.clock.now()))
            await ctx.clock.sleep(period)


@daemon
async def actuator(
    ctx: Context,
    action_in: Channel[Action],
    log: list[Action],
) -> None:
    """Drain action stream into ``log`` for downstream assertions."""
    async for action in action_in.recv:
        log.append(action)


# --- the bit where v0 misses a Supervisor.stop() --------------------------


class _PipelineDone(Exception):
    """Sentinel raised by ``driver`` to terminate the supervisor cleanly.

    Until ``Supervisor.stop()`` exists (see roadmap), this is the cleanest
    way to bring a pipeline of infinite-loop daemons down: have a driver
    daemon raise, let ``on_error="shutdown"`` cancel the rest, and catch the
    expected leaf at the call site.
    """


@daemon
async def driver(ctx: Context, duration_s: float) -> None:
    """Advance virtual time by ``duration_s``, then end the run."""
    await anyio.sleep(0)  # let sibling daemons register their first sleep
    await ctx.clock.advance(duration_s)
    raise _PipelineDone


# --- entry point ----------------------------------------------------------


def _is_pipeline_done(exc: BaseException) -> bool:
    if isinstance(exc, _PipelineDone):
        return True
    # Supervisor wraps the daemon exception in DaemonError (ADR 0008) so the
    # failing daemon's name reaches the group leaf — but the sentinel we care
    # about is on __cause__.
    if isinstance(exc, DaemonError) and isinstance(exc.__cause__, _PipelineDone):
        return True
    if isinstance(exc, BaseExceptionGroup):
        return all(_is_pipeline_done(e) for e in exc.exceptions)
    return False


async def run_mock(duration_s: float = 2.0) -> list[Action]:
    """Run the mock pipeline for ``duration_s`` virtual seconds.

    Returns the actuator log. Deterministic across runs and backends.
    """
    clock = SimClock()
    obs_cache: Latest[Obs] = Latest()
    subgoal_cache: Latest[Subgoal] = Latest()
    chunk_channel: Channel[Chunk] = open_channel(maxsize=4)
    action_channel: Channel[Action] = open_channel(maxsize=64)
    log: list[Action] = []

    try:
        async with Supervisor(clock=clock) as sup:
            sup.add(sensor(obs_cache))
            sup.add(s2_planner(obs_cache, subgoal_cache))
            sup.add(s1_policy(obs_cache, subgoal_cache, chunk_channel))
            sup.add(s0_dispenser(chunk_channel, action_channel))
            sup.add(actuator(action_channel, log))
            sup.add(driver(duration_s))
    except BaseExceptionGroup as eg:
        # Anything other than _PipelineDone is a real error; re-raise it.
        if not _is_pipeline_done(eg):
            raise

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
