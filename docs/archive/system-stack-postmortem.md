# examples: end-to-end demos and ergonomic stress-tests

This folder holds full, runnable demos of runlet on realistic patterns:
larger than `runlet.recipes` helpers, smaller than a production system. They
double as **ergonomic stress-tests**: building each one is a check that the
core primitives compose cleanly on a real workload.

Each demo is self-contained in one file and is exercised by
`tests/test_examples.py` so changes that break the demo break CI.

## Index

- [`system_stack_mock.py`](../../examples/system_stack_mock.py): single-session System 2 /
  1 / 0 inference pipeline (mocked). Models a production-style three-rate
  inference architecture.
- [`system_stack_multi_session.py`](../../examples/system_stack_multi_session.py): extends
  the single-session example with a `MockDispatcher` exposing
  `register(sid, duration_s)` / `unregister(sid)`. Each session runs in an
  inner `Supervisor` with its own `SimClock`; unregister fires a
  per-session `anyio.Event` for clean targeted teardown.

---

## Ergonomic post-mortem: `system_stack_mock.py`

The mock implements a three-stage cascade: slow planner (S2), fast policy
(S1), and high-rate motor dispenser (S0). Every model
forward replaced by `await ctx.clock.sleep(latency)` plus a deterministic
toy computation. Under `SimClock` the whole 2-second scenario runs in
microseconds of wall time. On asyncio the actuator log is byte-identical
across runs; trio support has the scheduler-order caveat below.

What we wanted to check by building this:

- Does explicit channel wiring (no `Topic`, ADR 0001) feel acceptable on a
  realistic multi-rate pattern, or does the boilerplate get out of hand?
- Does the "always use `ctx.clock`" rule (ADR 0002) pay for itself when
  several daemons sleep at different rates?
- Where, if anywhere, do you wish for primitives the current release
  doesn't ship?

### Determinism caveat (found while building this)

The claim "byte-identical across runs" holds on asyncio. It does **not**
hold on trio: trio's default scheduler intentionally randomizes task-spawn
order across runs as an ASLR-like measure against accidental ordering
dependence. On trio our pipeline produces logs of the same length, with
monotone time and matching action distributions, but the precise sequence
of actions emitted in the first few SimClock ticks can shift between
otherwise identical runs.

This is a runtime property, not a runlet bug, but it does limit the "byte
equality" form of replay determinism to one backend. Workarounds: either
pin the backend (use asyncio in tests), or gate every daemon behind a
shared `anyio.Event` and `set()` it after all daemons are registered, so
all first-iteration sleeps register at the same simulated instant. The
shared-event gate is short enough (5 lines on every daemon) that it's a
recipe candidate rather than a core feature. See `docs/roadmap.md`.

### What felt clean

- **Each S-loop is one daemon.** `@daemon async def s2_planner(ctx, ...)`
  is a literal line-for-line of the conceptual pipeline diagram. No
  scheduler bookkeeping, no callback registration, no lifecycle state
  machine.
- **Rate control is one line.** `async for t in ctx.clock.every(1/S2_HZ):`
  reads as "wake every 0.5 s of *sim* time", and that's exactly what it
  does under `SimClock`. No `rclpy.Rate`-style fudging.
- **`SimClock` made the whole thing free to test.** A 10-second scenario
  takes microseconds of wall time, and `pytest.mark.parametrize` over
  asyncio + trio is just two free runs. The determinism property
  (`run_mock(2.0) == run_mock(2.0)` byte-for-byte) is a one-liner test.
- **Channels are 1:1, and that turned out fine** here. The shared "latest
  obs" / "latest subgoal" state is a 12-line `Latest[T]` class, not a
  pub/sub topic. Wiring is visible: every consumer holds an explicit
  reference to the cache, and the producer's writer is also explicit.
- **Backpressure is automatic.** `chunk_channel` is bounded at 4; if S0
  gets behind, S1 blocks. No QoS profile to configure, no decision to make
  about drop vs queue.

### Where boilerplate showed up

- **`Latest[T]` was hand-rolled at first.** The "shared latest value"
  pattern is the obvious result of "Channel is 1:1, but I want N
  consumers to see the most recent producer output without consuming a
  queue." Promoted to `runlet.recipes.latest` after both examples needed it.
- **Decimation (`if counter % decimate: continue`) is manual.** ROS-style
  systems with `obs_sampling_rate_hz` express this as a config field; we
  express it as a one-line if. The one-line if is fine, but it's an
  argument for *not* adding a decimation primitive because three different
  daemons would each want their own definition.

### Gaps surfaced (and what we did with them)

- **No external way to bring the supervisor down without a sentinel.**
  The first draft had a `_PipelineDone` exception raised by a `driver`
  daemon, swallowed at the call site with `except* _PipelineDone`. That
  pattern was both noisy and wrong: every long-running deployment would
  reinvent it. Promoted to `Supervisor.signal_stop()` / `await stop()`
  (ADR 0009). The example's main task now does
  `await clock.advance(N); await sup.stop(grace=0)`; no sentinel.
- **Sim-time wasn't on log timestamps.** With wall-clock `%(asctime)s`,
  every log line under SimClock looked like it fired at the same
  microsecond. Promoted to `ClockAwareLoggerAdapter`, with
  `record.sim_time` available to any format string (ADR 0008).
- **Exception path lost daemon identity.** A daemon failing under
  `on_error="shutdown"` produced a plain `RuntimeError` inside the
  `ExceptionGroup`, with no name attached. Promoted to `DaemonError`
  wrapping the daemon name as the leaf (ADR 0008).

### Honest verdict (single-session)

For this multi-rate reactive pattern, runlet's
primitives compose into a single-file, end-to-end-deterministic mock with
no `Topic`, no QoS, and no lifecycle state machine. The pain points
identified while building it (`Supervisor.stop()`, sim-aware logging,
`Latest[T]` recipe) were all small targeted fixes that didn't change the
shape of the library.

If we'd had to build this on raw `anyio`, the wins lost would have been:
the deterministic clock, the lifecycle-managed daemon, and the typed
channel. Those *are* the value runlet provides.

---

## Post-mortem: `system_stack_multi_session.py`

Extends the single-session demo to the N-concurrent-sessions shape (one
inner `Supervisor` per session, each with its own `SimClock`). The closest
service analogue is a session dispatcher plus a per-session clock registry:
`register(session)` brings up the per-session loops, and `unregister` tears
them back down.

### What worked without new primitives

- **Supervisors nest.** The outer supervisor holds session daemons; each
  daemon opens its own `async with Supervisor(...)` for the inner
  S2/S1/S0/actuator set. No new core API needed; composition is a
  property of `async with`.
- **Per-session cancel via `anyio.Event`.** `MockDispatcher.register`
  threads a fresh `anyio.Event` into the session daemon; `unregister(sid)`
  sets it. The daemon checks `cancel_event.is_set()` between harness
  steps and exits the inner supervisor with `inner.stop(grace=0)`;
  cancellation is targeted, siblings are untouched.
- **Each session keeps its own SimClock.** Independent virtual clocks fall
  out of "inner supervisors get their own clock kwarg." Sessions with
  different `duration_s` produce action counts proportional to their
  duration; the test pins this.

### Gaps not closed

- **External tick fan-out is *not* shown.** Some service designs use
  `Dispatcher.tick(now_ns)` as the single wake source for every per-session
  `S{N}` loop. The example uses self-tick inside each inner supervisor's
  harness loop instead. This is runlet-native, but it differs from an
  external-tick invariant. The `fanout.tee` recipe is the path if you need to
  mirror that shape exactly.
- **Multi-clock advance fan-out** (driving N inner SimClocks from one
  outer tick) is likewise not shown; each session self-advances. Same
  trade-off: simpler, and not a byte-for-byte match for an external-tick design.
