# ADR 0002: Wall and Sim clocks as a pluggable Clock protocol

Status: Accepted (2026-05-18)

## Context

The library's distinguishing capability vs. dora-rs, HORUS, Apollo CyberRT,
and any "ROS for Python" attempt is **deterministic burst-step replay**: a
driver task should be able to advance an entire scenario's worth of time in
a single call (`await clock.advance(10.0)`) and observe daemons fire their
sleeps and timers in exactly the order and at exactly the virtual instants
they would have at wall-clock speed.

`anyio` provides `current_time()` and `sleep()` as monotonic, backend-agnostic
primitives, but does not provide a controllable virtual clock. trio's
`MockClock` exists but is trio-specific. asyncio has nothing comparable.

If daemon code is free to call `anyio.sleep` directly, no virtual clock can
intercept it. Determinism would then depend on every daemon author
remembering to use a `Clock` object. a passive convention is not strong
enough.

## Decision

Time is reached through a `Clock` protocol with four methods: `now() -> float`,
`async sleep(seconds)`, `async wait_until(deadline)`, and
`every(period) -> AsyncIterator[float]`. Two
implementations are provided:

- `WallClock` uses `time.monotonic()` for `now()` and delegates sleeps to
  `anyio.sleep`.
- `SimClock` keeps an internal heap of `(deadline, sequence, anyio.Event)`
  tuples. `sleep` enqueues; `advance(dt)` walks the heap in deadline order,
  setting each event and yielding to let the woken task register a
  follow-up sleep before the next iteration looks at the heap.

`Context` exposes the active clock as `ctx.clock`. The CLAUDE.md editing rule
"daemons must use `ctx.clock.sleep`, never `anyio.sleep`" is the convention
that makes the interception complete; it is enforced socially, not by code.

`SimClock.advance_to` performs a configurable `settle_rounds` checkpoint
budget between successive wakes. The default is 8. This is empirically
required on the trio backend, which does not necessarily resume a woken
task on a single fairness round. anyio does not expose a backend-agnostic
"run until all tasks are idle" primitive, so the budget is explicit rather
than inferred.

## Consequences

+ Deterministic burst-step replay is a first-class capability, not a
  research-mode toggle.
+ Production daemons swap `SimClock` for `WallClock` with no code change.
+ Restart backoff, periodic timers, and any "wait until X seconds elapsed"
  pattern all become deterministic under simulation, because they all go
  through `Clock.sleep`.
- The "always use `ctx.clock`" rule is a discipline, not a constraint
  enforced by the type system. A daemon that imports `anyio.sleep` directly
  silently breaks `SimClock`. The test suite has integration tests that would
  detect it for in-tree code; user code is on the honor system.
- `settle_rounds` is still a scheduler-settlement budget, not a proof of
  global quiescence. Raising or lowering it changes how much downstream task
  chaining one `advance_to()` call will absorb before finalizing.
- Backend-agnostic deterministic primitives are an ongoing area of churn
  in the anyio ecosystem; if a future anyio version ships an equivalent,
  `SimClock` will likely be re-implemented on top of it rather than
  remaining a hand-rolled heap.

## Related

- ADR 0008 (sim-aware logging) extends this by making `Context.logger`
  carry sim-time on records, so burst-replay log output is interpretable.
- `tests/test_clock.py::test_simclock_burst_step_deterministic_order` is
  the canary: if this test fails on either backend, the rest of the system
  is unreliable.
