# System-Stack Example Notes

These runnable examples stress-test runlet on a realistic System 2 / 1 / 0
shape. Both are covered by `tests/test_examples.py`.

## Index

- [`system_stack_mock.py`](../../examples/system_stack_mock.py): one session,
  three rates, mocked model latency, deterministic `SimClock` replay.
- [`system_stack_multi_session.py`](../../examples/system_stack_multi_session.py):
  many sessions, one inner `Supervisor` and `SimClock` per session.

## Single Session

`system_stack_mock.py` implements S2 planner, S1 policy, and S0 dispenser
daemons. Model calls are `ctx.clock.sleep(latency)` plus deterministic toy
math, so a multi-second run finishes in near-zero wall time.

What this confirmed:

- One daemon per S-loop maps cleanly to the conceptual pipeline.
- `ctx.clock.every(...)` gives concise rate control under both `WallClock`
  and `SimClock`.
- SPSC channels were enough; shared latest state belongs in
  `runlet.recipes.latest.Latest`.
- Bounded channels provide the needed backpressure without QoS settings.

Gaps it surfaced:

- Finite demos needed an in-band stop path. That became
  `Supervisor.signal_stop()` / `Supervisor.stop()` (ADR 0009).
- SimClock logs needed virtual timestamps. That became
  `ClockAwareLoggerAdapter` (ADR 0008).
- Daemon failures needed names and phases in diagnostics. That became
  `DaemonError` and `DaemonHealth.last_error_phase` (ADR 0008).

Determinism note: asyncio runs are byte-identical. Trio can vary task-spawn
order, so tests assert structural equality there: same length, monotone time,
and matching action distribution.

## Multi Session

`system_stack_multi_session.py` nests one supervisor per session under an
outer supervisor. `MockDispatcher.register()` adds a session daemon;
`unregister()` sets that session's cancel event.

What this confirmed:

- Supervisors compose recursively without new core API.
- Per-session cancellation uses `anyio.Event` plus `inner.stop(grace=0)`.
- Independent session clocks are natural when each session owns its inner
  `SimClock`.

What it does not show:

- External tick fan-out from one dispatcher clock to many session clocks.
- Multi-clock advance fan-out from one outer tick.

Those shapes should stay explicit recipes unless they become common enough
to justify a stable API.
