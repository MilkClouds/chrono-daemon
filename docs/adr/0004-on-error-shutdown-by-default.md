# ADR 0004 — `on_error="shutdown"` is the Supervisor default

Status: Accepted (2026-05-18)

## Context

When a daemon raises an unhandled exception, the supervisor has to choose
between three reasonable behaviors:

1. **Shutdown** — propagate the exception; the task group cancels all
   siblings; the `async with Supervisor` block exits with the original
   error wrapped in an `ExceptionGroup`.
2. **Restart** — sleep on a backoff and re-enter `on_start`/`run`/`on_stop`,
   keeping siblings alive in the meantime.
3. **Ignore** — log the failure, drop the daemon, keep siblings running.

Each is correct for some deployment. The default matters because the
default is what people experience before they read the docs, and what
half-finished prototypes silently rely on.

The argument for "restart" as default is uptime: most production
supervisors in Erlang/OTP, systemd, Kubernetes, etc. restart by default.
The argument for "shutdown" as default is debuggability: a process that
silently restarts a crashing daemon hides bugs, sometimes for weeks.

runlet's primary use cases (eval harnesses, simulation drivers, agent
orchestration) prioritize finding bugs over uptime. A control-loop deployment
that does want resilience can opt into `restart` explicitly and pin a
`RestartPolicy`.

## Decision

`Supervisor` defaults to `on_error="shutdown"`. A failing daemon re-raises;
anyio's `TaskGroup` cancels every sibling; the caller sees an
`ExceptionGroup` at the `async with` exit. The leaf exception is wrapped
in `DaemonError("daemon 'X' failed: ...")` so the failing daemon's name is
attached (see ADR 0008).

`on_error="restart"` requires the caller to pass a `RestartPolicy`
(or accept the defaults: 0.1s base, 2.0× factor, 5.0s cap, no max-retries).
Backoff goes through `ctx.clock.sleep`, so under `SimClock` the restart
timing is deterministic.

`on_error="ignore"` exists for the rare case where one daemon's failure is
genuinely uninteresting (a probe, a logging tap). The supervisor logs the
exception at `error` level and exits the daemon's host loop.

## Consequences

+ Crashing daemons in tests fail loudly and immediately — `pytest` shows
  the original exception inside the `ExceptionGroup`.
+ Production deployments that need resilience opt in explicitly, with a
  named policy that's reviewable in code.
+ Restart backoff under `SimClock` is reproducible, which makes
  "test the third restart" easy without sleeping in real time.
- A long-running prototype that wants "just keep going" has to remember
  to set `on_error="restart"`. The first time someone forgets, an obscure
  middle-of-the-night failure takes down the whole supervisor.
- `ExceptionGroup` is the runtime contract. Users have to write
  `try: ... except* RuntimeError: ...` (or `_flatten` helpers) rather than
  the un-grouped catch they may be used to. This is anyio's choice that we
  inherit.

## Related

- ADR 0008 extends this: `DaemonError` is the wrapper that puts the failing
  daemon's name on the exception chain.
- `tests/test_supervisor.py::test_shutdown_on_error_cancels_siblings`
  and `test_restart_with_backoff_under_simclock` pin the two behaviors.
