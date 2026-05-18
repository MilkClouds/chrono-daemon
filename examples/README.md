# examples — end-to-end demos and ergonomic stress-tests

This folder holds full, runnable demos of runlet on realistic patterns —
larger than `runlet.recipes` helpers, smaller than a production system. They
double as **ergonomic stress-tests**: building each one is a check that the
core primitives compose cleanly on a real workload.

Each demo is self-contained in one file and is exercised by
`tests/test_examples.py` so changes that break the demo break CI.

## Index

- [`reflex_dual_mock.py`](reflex_dual_mock.py) — System 2 / 1 / 0 inference
  pipeline (mocked). Models the production architecture in
  [worv-ai/reflex PR #191](https://github.com/worv-ai/reflex/pull/191).

---

## Ergonomic post-mortem: `reflex_dual_mock.py`

The mock implements the same three-stage cascade as #191 — slow planner
(S2), fast policy (S1), high-rate motor dispenser (S0) — with every model
forward replaced by `await ctx.clock.sleep(latency)` plus a deterministic
toy computation. Under `SimClock` the whole 2-second scenario runs in
microseconds of wall time, and the actuator log is byte-identical across
runs and across both anyio backends.

What we wanted to check by building this:

- Does explicit channel wiring (no `Topic`, ADR 0001) feel acceptable on a
  realistic multi-rate pattern, or does the boilerplate get out of hand?
- Does the "always use `ctx.clock`" rule (ADR 0002) pay for itself when
  several daemons sleep at different rates?
- Where, if anywhere, do you wish for primitives that v0 doesn't ship?

### Determinism caveat (found while building this)

The claim "byte-identical across runs" holds on asyncio. It does **not**
hold on trio: trio's default scheduler intentionally randomizes task-spawn
order across runs as an ASLR-like measure against accidental ordering
dependence. On trio our pipeline produces logs of the same length, with
monotone time and matching action distributions — but the precise sequence
of actions emitted in the first few SimClock ticks can shift between
otherwise identical runs.

This is a runtime property, not a runlet bug, but it does limit the "byte
equality" form of replay determinism to one backend. Workarounds: either
pin the backend (use asyncio in tests), or gate every daemon behind a
shared `anyio.Event` and `set()` it after all daemons are registered, so
all first-iteration sleeps register at the same simulated instant. The
shared-event gate is short enough (5 lines on every daemon) that it's a
recipe candidate rather than a core feature — see `docs/roadmap.md`.

### What felt clean

- **Each S-loop is one daemon.** `@daemon async def s2_planner(ctx, ...)`
  is a literal line-for-line of the conceptual pipeline diagram. No
  scheduler bookkeeping, no callback registration, no lifecycle state
  machine.
- **Rate control is one line.** `async for t in ctx.clock.every(1/S2_HZ):`
  reads as "wake every 0.5 s of *sim* time" — and that's exactly what it
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

- **`Latest[T]` had to be hand-rolled.** The "shared latest value" pattern
  is the obvious result of "Channel is 1:1, but I want N consumers to see
  the most recent producer output without consuming a queue." It's 12
  lines and obvious in this example — but if every project re-rolls it,
  that's a recipe candidate. Calling this out in the roadmap.
- **Decimation (`if counter % decimate: continue`) is manual.** ROS-style
  systems with `obs_sampling_rate_hz` express this as a config field; we
  express it as a one-line if. The one-line if is fine, but it's an
  argument for *not* adding a decimation primitive — three different
  daemons would each want their own definition.

### Where v0 was missed

- **`Supervisor.stop()` would have saved a sentinel exception.** The
  `driver` daemon currently advances the SimClock by `duration_s` and then
  raises `_PipelineDone` to bring the supervisor down. The caller has to
  match-and-swallow it with `except* _PipelineDone`. With a planned
  `Supervisor.stop()` (roadmap), the driver could call `sup.stop()` and
  exit normally — no sentinel, no `BaseExceptionGroup` handling at the
  call site.
- **`ctx.logger.exception` fires for the expected `_PipelineDone`.** The
  supervisor's `_host` logs every exception before deciding the policy.
  This is correct for real failures, noisy for expected sentinel sentinels.
  An `on_error="quiet"` policy or a "raise this without logging" channel
  is a v0.x consideration; for now the noise is one stderr block per run.
- **Sim-time isn't on log timestamps yet.** When running the example with
  logging on, `%(asctime)s` is wall-clock — meaning every line shows
  microsecond-range timestamps within the same second. Coming in v0.1
  (ADR 0008): a `ClockAwareLoggerAdapter` that injects `ctx.clock.now()`
  onto records so log lines actually reflect the simulated instant.
- **A `Latest[T]` recipe.** This pattern (drain channel into a cache,
  let consumers read the most recent) is reusable; promoting it to
  `runlet.recipes.latest` would have replaced the inline class. Queued
  on the roadmap.

### Honest verdict

For the multi-rate reactive pattern that #191 implements, runlet's
primitives compose into a 220-line, end-to-end-deterministic mock. There
is no `Topic`, no QoS, no lifecycle state machine — and the absence of
each was unambiguously a win for this workload. The pain points
(`Supervisor.stop()`, sim-aware logging, `Latest[T]` recipe) are all
small, targeted fixes that don't change the shape of the library.

If we'd had to build this on raw `anyio`, the wins lost would have been:
the deterministic clock, the lifecycle-managed daemon, and the typed
channel. Those *are* the value runlet provides.
