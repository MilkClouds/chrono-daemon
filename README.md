# chrono-daemon

[![CI](https://github.com/MilkClouds/chrono-daemon/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MilkClouds/chrono-daemon/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/chrono-daemon.svg)](https://pypi.org/project/chrono-daemon/)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

chrono-daemon is a tiny [AnyIO](https://anyio.readthedocs.io/en/stable/) runtime
for time-driven async daemon graphs in Python.

Use it when your system is made of long-running components connected by message
edges: planners, policies, evaluators, rollout workers, control loops, agent
pipelines, dispatchers. The useful shape is familiar from robotics middleware:
components, messages, clocks, and supervision.

chrono-daemon keeps that shape deliberately plain: async functions become
daemons, typed queues become message edges, an AnyIO task group becomes the
supervisor, and an injected clock drives time. There is no broker, graph
compiler, background runtime service, generated message package, ROS distro, or
DDS stack to adopt.

The same graph can run on `WallClock` for real-time execution or `SimClock` for
driver-controlled replay, rollout, evaluation, simulation, scenario driving, and
tests.

```text
Supervisor + Clock
  Daemon A -- Channel[T] --> Daemon B
  Daemon C -- Channel[U] --> Daemon D
```

## Quick Example

```python
import anyio

from chrono_daemon import Channel, Context, SimClock, Supervisor, daemon, open_channel

Command = tuple[int, float]


@daemon
async def planner(ctx: Context, obs: Channel[int], commands: Channel[Command]) -> None:
    try:
        async for seq in obs.recv:
            await ctx.clock.sleep(0.25)  # model latency: real or simulated
            await commands.send.send((seq, ctx.clock.now()))
    finally:
        await commands.send.aclose()


@daemon
async def actuator(ctx: Context, commands: Channel[Command], log: list[Command]) -> None:
    async for command in commands.recv:
        log.append(command)


async def main() -> None:
    clock = SimClock()
    obs: Channel[int] = open_channel(maxsize=4)
    commands: Channel[Command] = open_channel(maxsize=4)
    log: list[Command] = []

    async with Supervisor(clock=clock) as sup:
        sup.add(planner(obs, commands))
        sup.add(actuator(commands, log))

        await obs.send.send(1)
        await clock.advance(0.25)  # run 250 ms of graph time immediately

        assert log == [(1, 0.25)]
        await obs.send.aclose()
        await sup.stop(grace=0)


anyio.run(main)
```

Nothing in `planner` is tied to `SimClock`. Daemons sleep on `ctx.clock`; the
owner chooses whether that clock follows wall time or is advanced explicitly by a
scenario driver.

## Core API

| Primitive | Role |
|---|---|
| `Daemon` | Long-running async unit with `on_start`, `run`, and `on_stop`; use a subclass or the `@daemon` decorator. |
| `Channel[T]` | Typed bounded single-producer / single-consumer queue; the only core inter-daemon communication primitive. |
| `Clock` | `WallClock` for real time, `SimClock` for driver-controlled virtual time with `advance(dt)` and `advance_to(t)`. |
| `Supervisor` | Structured-concurrency root that hosts daemons, names them, stops them, applies error policy, and exposes diagnostics. |

Everything else is layered on top. Routing, fanout, merge, worker pools,
batching, latest-value state, lossy buffers, and sync/async bridges live under
`chrono_daemon.recipes.*`.

## Compared With Familiar Tools

- [ROS 2](https://docs.ros.org/en/rolling/) is deployed robotics middleware.
  chrono-daemon borrows the component/message/clock/supervision shape for local
  Python graphs, without ROS runtime dependencies.
- [dora](https://dora-rs.ai/) is distributed dataflow. chrono-daemon is an
  embeddable in-process graph runtime with explicit wiring and a
  driver-controlled clock.
- [SimPy](https://simpy.readthedocs.io/en/latest/) is discrete-event simulation.
  chrono-daemon runs ordinary `async def` daemons on either real time or
  simulated time.

## Scope

chrono-daemon is for in-process async graphs where explicit message edges and
controlled time matter: control mocks, evaluation rollouts, replay harnesses,
agent pipelines, and service internals.

It is not a ROS compatibility layer, topic broker, service registry, RPC
framework, deployment runtime, or numerical simulator. The default transport is
in-process; optional ZMQ channel endpoints are available for asyncio-backed
deployments.

## Install / Dev

```bash
pip install chrono-daemon
pip install "chrono-daemon[zmq]"  # optional ZMQ transport
```

Python 3.11+. The only required runtime dependency is
[`anyio>=4`](https://anyio.readthedocs.io/en/stable/).

```bash
uv sync --dev --extra zmq
make check        # ruff + pyrefly
make test         # pytest on asyncio + trio
make all          # format + check + test
```

## More

- [`docs/concepts.md`](docs/concepts.md): primitive semantics and invariants.
- [`docs/recipes.md`](docs/recipes.md): helpers intentionally kept out of core.
- [`examples/`](examples/): runnable System 2/1/0 and multi-session demos.
- [`docs/adr/`](docs/adr/): design decisions such as channel-only communication,
  pluggable clocks, and anyio-only core dependency.
