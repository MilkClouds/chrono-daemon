# ADR 0012: SimClock settle hook and deadline tolerance

Status: Accepted (2026-11-01)

## Context

ADR 0002 settles woken tasks with a fixed `settle_rounds` checkpoint budget,
because anyio has no portable "run until idle" primitive. The budget is a
heuristic: a woken task that hops through more checkpoints than the budget, or
waits on a worker thread or a socket, is still running when the next sleeper
wakes. Nothing in asyncio or trio's public API can tell when *all* work
triggered by a wake has finished.

A host can know one narrower thing: work it tracks itself. A simulator stepping
a policy, for example, counts its own in-flight model calls. It needs a place
to wait for that work between wakes.

Hosts also drive time on their own grid (`k * dt`) while daemons tick on
theirs (`t0 + n * period`); the two can disagree in the last bits, so a tick
meant for `t` lands just past it and fires one step late.

## Decision

- `advance_to(t, *, settle=None, max_settles=1000, rtol=1e-9, atol=0.0)`, and
  `advance(dt, ...)` delegating to it. Due sleepers wake one at a time in
  `(deadline, registration)` order. After each wake, and once more when none
  is due, the `settle_rounds` checkpoints run; then `await settle()` runs if
  given. It returns True if it waited for something, and the rounds and
  `settle` repeat so continuations run before the next wake; False ends the
  loop. `max_settles` True results in a row raise `RuntimeError`: time must
  not move on silently. Without `settle`, behaviour is ADR 0002's.
- `settle` does not detect global completion. It is a hook for host-known work
  on top of the round heuristic; work the host does not track still relies on
  `settle_rounds`.
- Tolerance follows `math.isclose`'s symmetric form: a deadline `d` is due at
  `t` when `d <= t + max(rtol * max(|d|, |t|), atol)`. Defaults are
  `math.isclose`'s (`rtol=1e-9`, `atol=0.0`); `rtol=atol=0` disables it. A
  sleeper woken inside the tolerance sees `now() == t`, never later, and
  `now()` ends exactly at `t`. Only sleepers registered before time reached
  `t` get the tolerance, so a tiny re-sleep at `t` cannot loop forever.
- `every()` computes ticks as `t0 + n * period` instead of summing `period`,
  so ticks do not drift off a host's grid over long runs.
- No public `next_deadline()`: hosts get same-instant handling from the
  tolerance rather than by inspecting the heap.

## Consequences

+ A host makes "woken work finished" exact for the work it tracks, on any
  backend, without the library guessing.
+ Grid mismatches from float rounding no longer shift a tick by a whole step.
- Untracked async chains longer than `settle_rounds` hops can still overlap
  the next wake; raise `settle_rounds` for them.
- A `settle` that returns too early reintroduces overlap; one that keeps
  returning True hits `max_settles`. An exception from `settle` propagates and
  leaves time at the last woken deadline.
- `every()` tick values can differ from the previous summed values in the last
  bits.

## Related

- ADR 0002: Wall and Sim clocks as a pluggable Clock protocol.
