# ADR 0013: Exclusive SimClock advance for host input at t

Status: Accepted (2026-11-01)

## Context

A simulator host steps an environment and hands its observation for time `t` to daemons that also run timers on
`t`'s grid. With ADR 0012's `advance_to(t)`, every sleeper due at `t` wakes inside the advance, so a timer at `t`
runs before the host can deliver `t`'s input and reads the previous one. In a real system the sensor reading at `t`
exists when the controller ticks at `t`; the host needs "everything before `t`, then my input, then `t`'s timers".

## Decision

- `advance_to(t, *, inclusive=True)`, and `advance(dt, ...)` forwarding it. `inclusive=False` wakes only sleepers
  due before `t`: a deadline at `t`, or within the `rtol`/`atol` tolerance of `t` from either side, stays queued.
  `now()` still ends at `t`.
- A following `advance_to(t)` wakes the sleepers left at `t`. Its pre-wake settle (ADR 0012) lets work the host's
  input started finish at `t` before them.
- A sleeper woken at a deadline below `now()` (one left at `t` from just below it) sees `now()`; time never moves
  back.

## Consequences

+ A host orders "before `t`, input at `t`, timers at `t`" with two calls and no heap inspection.
- Between the two calls `now() == t` while sleepers due at `t` are still waiting; a host that skips the second call
  leaves them for its next advance.

## Related

- [0012: SimClock settle hook and deadline tolerance](0012-simclock-settle-hook-and-tolerance.md)
