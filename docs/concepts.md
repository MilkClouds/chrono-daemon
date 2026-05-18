# Concepts

runlet is four small primitives on top of `anyio`. Everything else — fanout,
batching, multi-channel waits, sync↔async bridging — is built by the user
from these four, with examples in `recipes/`.

## The four primitives

### `Channel[T]`

A typed bounded queue. Two endpoints, `send` and `recv`. Multiple producers
and multiple consumers may share an endpoint; each item is delivered to
**exactly one** waiting receiver (competing-consumers semantic). Closing the
send side propagates `EndOfStream` to every receiver after the buffer drains.

`Channel` is the only inter-daemon communication primitive in runlet. There
is no `Topic`, no broadcast, no services, no RPC, no parameter system. The
reasoning is in ADR 0001.

### `Clock`

A small protocol with `now()`, `async sleep(seconds)`, and `every(period)`.
Two implementations ship:

- `WallClock` delegates to `anyio.current_time` and `anyio.sleep`. Use it in
  production.
- `SimClock` is a deterministic virtual clock. Time only moves when a driver
  task calls `await clock.advance(dt)` (or `advance_to(t)`). Sleepers register
  a deadline, the driver pops them in deadline order, and `_SETTLE_ROUNDS`
  yields between wakes let woken tasks register follow-up sleeps before the
  next iteration looks at the heap.

Daemons must reach for `ctx.clock.sleep(...)` — never `anyio.sleep(...)`
directly — or `SimClock` cannot intercept time. The reasoning is in ADR 0002.

### `Daemon`

A long-running async unit with three lifecycle hooks: `on_start(ctx)`,
`run(ctx)`, `on_stop(ctx)`. Subclass `Daemon` and override `run`, or use the
`@daemon` decorator on an `async def fn(ctx, *args)` for the no-state case.
Both produce the same kind of object. The reasoning is in ADR 0003.

We do not ship lifecycle states beyond these three (no `configured`,
`active`, `inactive`, etc.). ADR 0005 explains why.

### `Supervisor`

The structured-concurrency root. `async with Supervisor(clock=...) as sup:`
wraps an `anyio.create_task_group`. Inside the block you `sup.add(daemon)` or
`sup.spawn(async_fn, *args)`. Each hosted daemon gets its own `Context` with
its own cancel scope and a child logger.

On uncaught exception, `Supervisor.on_error` chooses:

- `"shutdown"` (default) — re-raise; the task group cancels every sibling
  and the exception escapes inside an `ExceptionGroup`. ADR 0004 explains
  why this is the default.
- `"restart"` — sleep on `ctx.clock` per `RestartPolicy` (exponential
  backoff), then re-enter `on_start`/`run`/`on_stop`. Because backoff goes
  through `ctx.clock.sleep`, restart timing is deterministic under
  `SimClock`.
- `"ignore"` — log and let the daemon exit; siblings keep running.

## Composition

The runtime shape is always:

```
Supervisor                       (one per process, typically)
├── Clock                        (one, shared)
├── Daemon A ──send──▶ Channel X ──recv──▶ Daemon B
├── Daemon C ──send──▶ Channel Y ──recv──▶ Daemon A
└── ...
```

Wiring is explicit: every channel and every consumer is a named reference in
your code, not a runtime-discovered topic name. That's the deliberate tradeoff
recorded in ADR 0001.

## Invariants you can rely on

These are the properties the test suite pins; if any of them break, it is a
bug, not a tuning knob:

- Under `SimClock`, the order and timing of sleeper wakeups depend only on
  `(deadline, registration order)` — not on backend (asyncio vs trio) and not
  on wall-clock progress during the test.
- `Channel.send.aclose()` causes every present and future `Channel.recv.receive()`
  to raise `EndOfStream` once the buffer drains, on both backends.
- A daemon's `cancel_scope` cancellation never affects sibling daemons.
- `Supervisor.on_error="shutdown"` causes a failing daemon's exception to
  reach the `async with Supervisor` exit inside an `ExceptionGroup`, with the
  daemon's name attached (`DaemonError("daemon 'X' failed: ...")` as the
  group's leaf; see ADR 0008).

## What is intentionally not here

- Topic / pub-sub broadcast → recipes/fanout.py.
- Services, RPC, parameter system, discovery.
- Multi-process or network transport in v0 (the API is shaped so they can
  land in v0.x without breaking changes — ADR 0006).
- Lifecycle states beyond `on_start`/`run`/`on_stop`.
- Dependency on anything other than `anyio` (ADR 0007).

If you find yourself wanting one of these, check `recipes/` and the
`roadmap.md` first; the relevant ADR explains the reasoning if you want to
push back on the decision.
