# Concepts

runlet is four small primitives on top of `anyio`. Everything else, including
fanout, batching, multi-channel waits, and sync↔async bridging, is built from
these four, with examples in `recipes/`.

## The four primitives

### `Channel[T]`

A typed bounded queue. Two endpoints, `send` and `recv`. The intended shape
is single producer / single consumer: one daemon owns `send`, one daemon owns
`recv`. Concurrent blocking use of the same endpoint is a wiring error and
raises `ChannelInUse`. Closing the send side propagates `EndOfStream` to the
receiver after the buffer drains.

`Channel` is the only inter-daemon communication primitive in runlet. There
is no `Topic`, no broadcast, no services, no RPC, no parameter system. The
reasoning is in ADR 0001 and the single-owner refinement is in ADR 0010.

### `Clock`

A small protocol with `now()`, `async sleep(seconds)`,
`async wait_until(deadline)`, and `every(period)`. Two implementations ship:

- `WallClock` uses `time.monotonic()` for `now()` and `anyio.sleep` for
  sleeping. Use it in production.
- `SimClock` is a deterministic virtual clock. Time only moves when a driver
  task calls `await clock.advance(dt)` (or `advance_to(t)`). Sleepers register
  an absolute deadline, the driver pops them in deadline order, and the
  configurable `settle_rounds` checkpoint budget between wakes lets woken
  tasks register follow-up sleeps before the next heap inspection.

Daemons must reach for `ctx.clock.sleep(...)`, not `anyio.sleep(...)`
directly, or `SimClock` cannot intercept time. The reasoning is in ADR 0002.

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
`sup.spawn(async_fn, *args)`. `sup.add` raises `TypeError` if you pass a
non-`Daemon`; the most common mistake, passing the `@daemon` factory instead
of calling it, gets a dedicated error message. Daemon names key diagnostics
such as `snapshot()`, so duplicate names raise `ValueError`. Each hosted
daemon gets its own `Context` with its own cancel scope and a child logger.

On uncaught exception, `Supervisor.on_error` chooses:

- `"shutdown"` (default): re-raise wrapped in `DaemonError`; the task
  group cancels every sibling and the exception escapes inside an
  `ExceptionGroup`. ADR 0004 explains why this is the default.
- `"restart"`: sleep on `ctx.clock` per `RestartPolicy` (exponential
  backoff), then re-enter after `on_start` or `run` failures. A normal-path
  `on_stop` cleanup failure is terminal unless `on_error="ignore"`, because
  retrying cleanup can duplicate side effects. Because backoff goes through
  `ctx.clock.sleep`, restart timing is deterministic under `SimClock`.
- `"ignore"`: log and let the daemon exit; siblings keep running.

Shutdown surface (ADR 0009):

- `sup.signal_stop()`: sync, fire-and-forget. Sets the shared stop event
  every `Context` carries. Cooperative daemons (polling `ctx.stopping` or
  using `runlet.recipes.cooperative_every`) exit naturally so `on_stop`
  runs on the standard return path. Safe to call from inside a daemon.
- `await sup.stop(grace, finalize_timeout)`: async. Signals stop, waits
  up to `grace` wall-clock seconds for daemons to exit, then force-cancels
  any still running. Even on the force-cancel path, each daemon's
  `on_stop` is invoked best-effort inside a shielded scope bounded by
  `finalize_timeout`. If `on_start` succeeded, `on_stop` is guaranteed on
  every exit path (normal, shutdown, restart, ignore, cancel).

Leaving the `async with Supervisor(...)` block does not itself signal a
stop. If a hosted daemon is designed to run forever, the owner task must call
`signal_stop()` or `await stop(...)`, or arrange for the daemon to return.

`Context` carries `clock`, `cancel_scope`, `logger` (a
`ClockAwareLoggerAdapter` that injects `sim_time` onto every log record),
`name`, `supervisor`, plus the read-only `stop_event` and `stopping`
shortcut for stop-aware loops.

`sup.snapshot()` returns `DaemonHealth` records keyed by daemon name. Health
records include state, restart count, last error, the lifecycle phase that
raised it (`on_start`, `run`, or `on_stop`), start time, and current uptime
for running daemons.

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

Supervisors compose recursively. A daemon may itself open an inner
`async with Supervisor(...)` and host its own sub-pipeline with its own
`SimClock`. `examples/reflex_dual_multi_session.py` uses this pattern for
per-session isolation. Each session is a daemon on the outer supervisor
that nests its own inner supervisor + clock. No new primitive needed.

## Invariants you can rely on

These are the properties the test suite pins; if any of them break, it is a
bug, not a tuning knob:

- Under `SimClock`, the order and timing of sleeper wakeups depend only on
  `(deadline, registration order)`, not on backend (asyncio vs trio) and not
  on wall-clock progress during the test.
- `Channel.send.aclose()` causes every present and future `Channel.recv.receive()`
  to raise `EndOfStream` once the buffer drains, on both backends.
- Concurrent blocking `send()` / `receive()` calls on the same channel endpoint
  raise `ChannelInUse`; channels are SPSC by default.
- A daemon's `cancel_scope` cancellation never affects sibling daemons.
- `Supervisor.on_error="shutdown"` causes a failing daemon's exception to
  reach the `async with Supervisor` exit inside an `ExceptionGroup`, with the
  daemon's name attached (`DaemonError("daemon 'X' failed: ...")` as the
  group's leaf; see ADR 0008).

## What is intentionally not here

- Topic / pub-sub broadcast → `runlet.recipes.fanout.tee` (see docs/recipes.md).
- Services, RPC, parameter system, discovery.
- Multi-process or network transport in the current release. The API is
  shaped so they can be added later without breaking changes; see ADR 0006.
- Lifecycle states beyond `on_start`/`run`/`on_stop`.
- Dependency on anything other than `anyio` (ADR 0007).

If you find yourself wanting one of these, check `docs/recipes.md` (and
its source in `src/runlet/recipes/`) and `roadmap.md` first; the relevant
ADR explains the reasoning if you want to push back on the decision.
