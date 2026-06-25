# ADR 0009: Cooperative stop signaling on Supervisor

Status: Accepted (2026-05-18)

## Context

The four-primitive core gave callers no in-band way to bring a supervisor
down. The only paths were:

- Raise from inside a daemon (ADR 0004 turns this into a sibling-cancel via
  `on_error="shutdown"`). Works, but requires sentinel exceptions for normal
  termination.
- Cancel the outer task. Yanks anyio's task-group cancel without giving
  any daemon a chance to clean up.

Neither was acceptable for the most common pattern: a main task that
drives the system for some duration and then asks every daemon to shut
down. ROS2 has `rclpy.shutdown`, dora has coordinator stop, Erlang has
`gen_server:stop`. We need the equivalent.

The interaction with the two clock implementations matters:

- `WallClock` makes "wait N seconds for daemons to honor stop" meaningful:
  real time advances regardless of supervisor state, polling daemons can
  observe `ctx.stopping` between work units, and a `grace` period genuinely
  buys cooperative shutdown.
- `SimClock` only moves when something calls `advance(...)`. The supervisor
  cannot make sim time pass on a daemon's behalf, so a daemon sleeping on
  `ctx.clock.sleep` during stop may need force-cancellation.

The design has to accept this asymmetry rather than paper over it.

## Decision

Two methods on `Supervisor`, plus two read-only fields on `Context`:

- `Supervisor.signal_stop()`. sync, idempotent. Sets a shared
  `anyio.Event` exposed on each daemon's `Context`. Safe to call from
  *inside* a daemon (fire-and-forget). The calling daemon should then
  return normally; the standard return path runs `on_stop`.
- `await Supervisor.stop(grace=5.0, finalize_timeout=2.0)`. async. Calls
  `signal_stop`, waits up to `grace` wall-clock seconds for daemons to
  finish, then force-cancels any still running. Should be called from the
  supervisor's main task; calling it from inside a daemon traps the daemon
  in its own grace-wait.
- `Context.stop_event`. The `anyio.Event` itself. Daemons may
  `await ctx.stop_event.wait()` when they want to block until stop.
- `Context.stopping`. A `bool` shortcut for `ctx.stop_event.is_set()`,
  intended for `if ctx.stopping: break` polling between work units.

On the force-cancel path, each daemon's `on_stop` still gets a best-effort
invocation inside `anyio.CancelScope(shield=True)` bounded by
`finalize_timeout`. Long-running cleanup that exceeds `finalize_timeout`
is itself cancelled. Daemons that need guaranteed cleanup should poll
`ctx.stopping` cooperatively rather than rely on the shielded path.

A recipe, `runlet.recipes.cooperative_every.cooperative_every(ctx, period)`,
wraps `ctx.clock.every` with the polling check at every yield point.

## Consequences

+ Finite demos can now call `await sup.stop(grace=0)` instead of raising a
  sentinel exception.
+ `signal_stop` gives daemons that need to terminate the whole supervisor
  a clean fire-and-forget primitive (`ctx.supervisor.signal_stop()`); they
  return normally and `on_stop` runs on the standard path.
+ Sync callers (e.g. signal handlers reached via
  `runlet.recipes.sync_bridge`) can use the portal to invoke either method.
+ On the force-cancel path, `on_stop` is no longer skipped; cleanup that
  fits inside `finalize_timeout` runs even for non-cooperative daemons.
- `stop(grace=N)` is graceful under `WallClock`; under `SimClock`, sleeping
  daemons may still need force-cancellation.
- `Context` now has two interchangeable surfaces for stop awareness
  (`stop_event` and `stopping`). The bool is the recommended default;
  the event is for daemons that genuinely want to await on it.
- `cooperative_every` is a recipe, not a core primitive. Daemons can also
  inline the `if ctx.stopping: break` check.

## Related

- ADR 0001. recipes namespace. `cooperative_every` lives there.
- ADR 0002. SimClock semantics. The asymmetry around graceful stop is a
  direct consequence of "sim time only moves on explicit `advance`."
- ADR 0004. `on_error="shutdown"` continues to wrap escapes in
  `DaemonError`; the force-cancel path inside `stop()` uses anyio's
  cancellation, not `DaemonError`, so the latter remains reserved for
  daemon-originated failures.
- ADR 0008. `ClockAwareLoggerAdapter` keeps logging readable through
  cancellation-driven shutdown.
