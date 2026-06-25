# runlet

[![CI](https://github.com/MilkClouds/runlet/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/MilkClouds/runlet/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/runlet.svg)](https://pypi.org/project/runlet/)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

runlet is a small Python library for long-running async components whose time
can be replayed deterministically. It wraps
[anyio](https://anyio.readthedocs.io/) with four primitives:

- `Channel[T]`: typed single-producer / single-consumer queues.
- `Clock`: real time with `WallClock`, virtual time with `SimClock`.
- `Daemon`: a lifecycle unit with `on_start`, `run`, and `on_stop`.
- `Supervisor`: a structured-concurrency root for hosting daemons.

The practical payoff is simple: production daemons sleep on `ctx.clock`; tests
swap in `SimClock` and advance seconds of work without waiting for wall time.

## Quick Example

```python
from runlet import Channel, Context, SimClock, Supervisor, daemon, open_channel

@daemon
async def producer(ctx: Context, out: Channel[int]) -> None:
    for i in range(10):
        await ctx.clock.sleep(0.1)
        await out.send.send(i)
    await out.send.aclose()

@daemon
async def consumer(ctx: Context, src: Channel[int]) -> None:
    async for item in src.recv:
        ctx.logger.info("got %d", item)

async def main() -> None:
    ch: Channel[int] = open_channel(maxsize=4)
    clock = SimClock()
    async with Supervisor(clock=clock) as sup:
        sup.add(producer(ch))
        sup.add(consumer(ch))
        await clock.advance(1.5)  # replay 1.5 s of work immediately
```

## Why Use It

- Time-dependent async code is testable without sleeps, polling, or fake
  task schedulers.
- Wiring is explicit. Every edge is a named SPSC channel, so ownership and
  backpressure stay visible.
- Lifecycle behavior is structured. Daemons get startup, shutdown, logging,
  cancellation, and error policy in one place.
- The runtime surface is small: pure Python, `anyio` underneath, and no
  runtime dependency beyond `anyio`.

## Core API

| | What it is |
|---|---|
| **`Channel[T]`** | typed bounded SPSC queue, the only inter-daemon communication primitive |
| **`Clock`** | `WallClock` (real time) or `SimClock` (deterministic, burst `advance(dt)` / `advance_to(t)`) |
| **`Daemon`** | long-running async unit; `on_start` / `run` / `on_stop` hooks. Use a subclass or the `@daemon` decorator |
| **`Supervisor`** | `async with Supervisor(...)` structured-concurrency root; hosts daemons, dispatches errors (`shutdown` / `restart` / `ignore`) |

See [`docs/concepts.md`](docs/concepts.md) for details.

## Scope

runlet is for in-process async systems where explicit wiring and deterministic
time matter: evaluation loops, agent pipelines, robotics-style control mocks,
and testable service internals.

It is intentionally not a topic broker, service registry, RPC framework,
parameter server, CLI launcher, network runtime, or continuous-time numerical
simulator.

## Install / dev

```bash
uv sync --dev
make check        # ruff + pyrefly
make test         # pytest on asyncio + trio
make all          # format + check + test
```

Python 3.11+. Only runtime dependency is `anyio>=4`.

## Where to look next

- [`docs/concepts.md`](docs/concepts.md): what each primitive is and the
  invariants the test suite pins.
- [`docs/adr/`](docs/adr/): why each decision looks the way it does
  (Topic-less, on-error-shutdown by default, anyio-only).
- [`docs/recipes.md`](docs/recipes.md): patterns kept off the core
  surface but importable under `runlet.recipes.*`: fanout, fan-in,
  load balancing, worker pools, batcher, select, sync/async bridge.
  Source in `src/runlet/recipes/`.
- [`docs/roadmap.md`](docs/roadmap.md): what's planned next and what's
  deliberately deferred.
- [`examples/system_stack_mock.py`](examples/system_stack_mock.py): multi-rate
  System 2/1/0 mock pipeline, with notes in
  [`docs/archive/system-stack-postmortem.md`](docs/archive/system-stack-postmortem.md).

## Status

Early. In-process only. The `Channel` protocol is shaped so multi-process
and network transports can be added later without breaking changes
(see ADR 0006).
