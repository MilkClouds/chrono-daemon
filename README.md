# runlet

A small, general-purpose concurrency library on top of [anyio](https://anyio.readthedocs.io/).
Four primitives, no Topic, no QoS, no parameter server — and a `SimClock`
that lets you replay a 10-second multi-daemon scenario in microseconds of
wall time, byte-deterministic, on both asyncio and trio.

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
        await clock.advance(1.5)   # burst-replay 1.5 s of work in 0 wall-time
```

## The four primitives

| | What it is |
|---|---|
| **`Channel[T]`** | typed bounded queue, MPMC competing consumers, the only inter-daemon communication primitive |
| **`Clock`** | `WallClock` (anyio passthrough) or `SimClock` (deterministic, burst `advance(dt)`) |
| **`Daemon`** | long-running async unit; `on_start` / `run` / `on_stop` hooks. Use a subclass or the `@daemon` decorator |
| **`Supervisor`** | `async with Supervisor(...)` structured-concurrency root; hosts daemons, dispatches errors (`shutdown` / `restart` / `ignore`) |

See [`docs/concepts.md`](docs/concepts.md) for what each one does in detail
and how they compose.

## When to use, when not to

| Use runlet if you want | Don't use runlet if you want |
|---|---|
| Multi-daemon async code with explicit wiring | dynamic topic discovery / pub-sub broadcast |
| Deterministic burst-replay of time-dependent code | a CLI / runtime / launcher |
| `anyio` underneath; both asyncio and trio supported | a GPU-aware streaming engine (use [Holoscan](https://github.com/nvidia-holoscan/holoscan-sdk)) |
| ~700 LOC you can read end-to-end in an afternoon | continuous-time numerical simulation (use [Drake](https://github.com/RobotLocomotion/drake)) |
| Zero runtime dependencies beyond `anyio` | a ROS replacement (use [dora-rs](https://github.com/dora-rs/dora) or ROS2) |

The closest comparable projects — dora-rs, Apollo CyberRT, HORUS, Drake,
Holoscan — are surveyed in the design background. runlet's niche is the
intersection of "small, pure Python" and "deterministic burst replay
first-class"; everything else is deliberately not-runlet.

## Install / dev

```bash
cd projects/runlet
uv sync --dev
make check        # ruff + pyrefly
make test         # pytest on asyncio + trio
make all          # format + check + test
```

Python 3.11+. Only runtime dependency is `anyio>=4`.

## Where to look next

- [`docs/concepts.md`](docs/concepts.md) — what each primitive is and the
  invariants the test suite pins.
- [`docs/adr/`](docs/adr/) — why each decision looks the way it does
  (Topic-less, on-error-shutdown by default, anyio-only, …).
- [`docs/recipes.md`](docs/recipes.md) — patterns kept off the core
  surface but importable under `runlet.recipes.*`: fanout, batcher,
  select, sync↔async bridge. Source in `src/runlet/recipes/`.
- [`docs/roadmap.md`](docs/roadmap.md) — what's planned for v0.x and what's
  deliberately deferred.
- [`examples/reflex_dual_mock.py`](examples/reflex_dual_mock.py) — full
  multi-rate System 2/1/0 mock pipeline, with an ergonomics post-mortem in
  [`examples/README.md`](examples/README.md).

## Status

v0. In-process only. The `Channel` protocol is shaped so multi-process and
network transports can land in v0.x without breaking changes
(see ADR 0006).
