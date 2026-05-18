# ADR 0002 — Wall and Sim clocks as a pluggable Clock protocol

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
remembering to use a `Clock` object — a passive convention is not strong
enough.

## Decision

Time is reached through a `Clock` protocol with three methods: `now() -> float`,
`async sleep(seconds)`, and `every(period) -> AsyncIterator[float]`. Two
implementations ship in v0:

- `WallClock` delegates to `anyio.current_time` and `anyio.sleep`.
- `SimClock` keeps an internal heap of `(deadline, sequence, anyio.Event)`
  tuples. `sleep` enqueues; `advance(dt)` walks the heap in deadline order,
  setting each event and yielding to let the woken task register a
  follow-up sleep before the next iteration looks at the heap.

`Context` exposes the active clock as `ctx.clock`. The CLAUDE.md editing rule
"daemons must use `ctx.clock.sleep`, never `anyio.sleep`" is the convention
that makes the interception complete; it is enforced socially, not by code.

`SimClock.advance_to` performs `_SETTLE_ROUNDS` (currently 8) checkpoint
yields between successive wakes. This is empirically required on the trio
backend, which does not necessarily resume a woken task on a single
fairness round.

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
- `_SETTLE_ROUNDS` is a magic number with a backend-specific justification.
  Raising or lowering it across backends would require trio-internal
  knowledge; the chosen value is generous to avoid flakes at the cost of a
  fixed yield budget per wake.
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
