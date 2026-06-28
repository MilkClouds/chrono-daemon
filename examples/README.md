# Examples

The files in this directory are runnable end-to-end demos. They are larger
than recipes, but still small enough to inspect in one sitting. The tests in
`tests/test_examples.py` execute them so example drift is caught by CI.

## Index

- `system_stack_mock.py`: a single-session System 2 / 1 / 0 inference pipeline.
  Model calls are mocked with `ctx.clock.sleep(...)` and deterministic toy
  computations.
- `system_stack_multi_session.py`: a multi-session dispatcher built from nested
  `Supervisor` instances. Each session owns its own `SimClock`.

## Single Session

`system_stack_mock.py` implements S2 planner, S1 policy, and S0 dispenser
daemons. Model calls are `ctx.clock.sleep(latency)` plus deterministic toy
math, so a multi-second run finishes in near-zero wall time.

What this confirms:

- One daemon per S-loop maps cleanly to the conceptual pipeline.
- `ctx.clock.every(...)` gives concise rate control under both `WallClock`
  and `SimClock`.
- SPSC channels are enough; shared latest state belongs in
  `chrono_daemon.recipes.latest.Latest`.
- Bounded channels provide backpressure without QoS settings.

Gaps surfaced by this example became stable APIs or diagnostics:

- Finite demos needed an in-band stop path:
  `Supervisor.signal_stop()` / `Supervisor.stop()` (ADR 0009).
- SimClock logs needed virtual timestamps:
  `ClockAwareLoggerAdapter` (ADR 0008).
- Daemon failures needed names and phases:
  `DaemonError` and `DaemonHealth.last_error_phase` (ADR 0008).

## Multi Session

`system_stack_multi_session.py` nests one supervisor per session under an
outer supervisor. `MockDispatcher.register()` adds a session daemon;
`unregister()` sets that session's cancel event.

What this confirms:

- Supervisors compose recursively without new core API.
- Per-session cancellation uses `anyio.Event` plus `inner.stop(grace=0)`.
- Independent session clocks are natural when each session owns its inner
  `SimClock`.

What it intentionally leaves out:

- External tick fan-out from one dispatcher clock to many session clocks.
- Multi-clock advance fan-out from one outer tick.

Those shapes should stay explicit recipes unless they become common enough
to justify a stable API.

## Determinism Note

`SimClock` controls time, not task scheduling. On asyncio, the single-session
mock has byte-identical replay across repeated runs. On trio, task-spawn order
can vary by design, so the logs keep the same shape and monotonic virtual time
but may differ in early item order. Use asyncio for byte-equality replay tests,
or gate daemon startup explicitly when cross-backend order must be identical.
