# ADR 0012: Host-driven settle between SimClock wakes

Status: Accepted (2026-11-01)

## Context

ADR 0002 settles woken tasks with a fixed `settle_rounds` checkpoint budget,
because anyio has no portable "run until idle" primitive. A budget is not a
proof of quiescence: a woken task that hops through many checkpoints, hands
work to another task, or waits on a worker thread is still running when the
next sleeper wakes, and same-deadline sleepers wake in a burst.

Some hosts know more than the library. A simulator stepping a policy knows
which tasks belong to an episode and which model calls are in flight, so it can
tell when woken work has finished. It needs two things `SimClock` did not
offer: a way to see the next deadline, and a way to wake one sleeper at a time
and run its own settle step between wakes.

## Decision

- `SimClock.next_deadline() -> float | None` returns the earliest pending
  sleeper's deadline, skipping cancelled entries.
- `SimClock.advance_to(t, *, settle=None)` (and `advance(dt, *, settle=None)`)
  accept an async `settle` callable. When given, sleepers due by `t` wake one
  at a time in `(deadline, registration)` order, and `await settle()` runs
  before each wake and once after the last, in place of `settle_rounds`.
  Without `settle`, behaviour is unchanged.
- `SimClock.every()` computes ticks as `t0 + n * period` instead of summing
  `period`, so ticks stay comparable with a host's own `k * dt` grid over long
  runs.

## Consequences

+ A host can make "woken work finished" exact for its own tasks, without the
  library guessing, on any backend its settle step supports.
+ Hosts can implement their own same-instant tolerance (`next_deadline()`
  within an epsilon of the target) without reaching into the heap.
- Correctness of a `settle`-driven advance is the host's responsibility: a
  settle that returns too early reintroduces burst-like interleaving, and one
  that never returns stalls time. An exception from `settle` propagates and
  leaves time at the last woken deadline.
- `every()` tick values can differ from the previous summed values in the last
  bits.

## Related

- ADR 0002: Wall and Sim clocks as a pluggable Clock protocol.
