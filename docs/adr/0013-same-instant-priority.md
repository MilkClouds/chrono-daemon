# ADR 0013: Same-instant priority and call_at on SimClock

Status: Accepted (2026-11-01)

## Context

A simulator host hands daemons its observation for time `t` while daemons also run timers on `t`'s grid. With ADR
0012 every sleeper due at `t` wakes in registration order, so a timer registered earlier runs before the host's input
for `t` and reads the previous one. Which of two events at one instant goes first is a convention the user must be
able to choose, as in discrete-event simulators (SimPy orders events by `(time, priority, id)`).

## Decision

- Sleepers take `priority: int = 0` (`sleep`, `wait_until`, `every`; lower first). `WallClock` accepts and ignores
  it, so daemon code stays clock-agnostic.
- One instant is the earliest due deadline and every due deadline within the `rtol`/`atol` tolerance of it (ADR
  0012). Its entries wake in `(priority, registration)` order, one at a time with a settle after each, and all see
  `now() == min(t, latest deadline of the instant)`: no entry wakes before its deadline (but for `t`'s tolerance), and
  time never moves back.
- Entries registered during an instant (by a woken task, a callback or `settle`) join it at or before `now()`;
  later ones, however close, form a later instant. An instant interrupted by an exception is re-formed by the next
  advance under that advance's target and tolerance.
- `SimClock.call_at(deadline, callback, *, priority=0)` schedules a host callback ordered like a sleeper, without a
  task. It returns a cancel function; a past deadline runs in the next advance; an exception from the callback
  propagates out of the advance, leaving time at that instant.

## Consequences

+ A host puts its input first at `t` with one advance: `call_at(t, put_input, priority=-1)`, then `advance_to(t)`;
  the input's work settles before `t`'s other wakes.
- Sleepers at float-close deadlines inside an advance now share one `now()` (the instant's latest deadline) instead
  of each seeing its own.

## Related

- [0012: SimClock settle hook and deadline tolerance](0012-simclock-settle-hook-and-tolerance.md)
