# ADR 0008: Sim-time-aware logging and supervisor diagnostics

Status: Accepted (2026-05-18); amended (2026-06-25)

## Context

The initial four-primitive core (`Channel`, `Clock`, `Daemon`, `Supervisor`)
shipped the minimum surface required to run a deterministic burst-replay
scenario. Building the `examples/reflex_dual_mock.py` pipeline and
re-reading the API surface against ROS2, dora-rs, and Apollo CyberRT surfaced
four small but non-deferable gaps. Each is a diagnostic hole that makes the
library's headline
capability (sim-time replay) harder to actually use:

1. `Context.logger` was plain stdlib `logging.Logger`. Records carried only
   wall-clock timestamps, so a `SimClock` run produced log lines all
   stamped within the same microsecond. There was invisible relative ordering, no
   way to correlate a log line to "what virtual instant did this happen
   at."
2. When a daemon raised under `on_error="shutdown"`, the resulting
   `ExceptionGroup` contained the original exception (e.g. `RuntimeError`)
   with no attribution back to which daemon produced it. `logger.exception`
   wrote the daemon name to the log, but the in-process programmatic path
   (`except* RuntimeError as eg: ...`) lost that information. Later cleanup
   work also showed that `last_error` alone was not enough; operators need to
   know whether the error came from `on_start`, `run`, or `on_stop`.
3. `Supervisor.add(d, name="X")` accepted duplicate names silently. Two
   daemons with the same name shared a child logger and one `snapshot()`
   record key; cross-task diagnosis was ambiguous.
4. `Channel.send` and `Channel.recv` exposed no introspection. anyio's
   `MemoryObjectStream.statistics()` was available internally but not on
   the runlet API. Debugging "is this channel full, who is waiting" needed
   private-attribute access.

None of these is a no-goal (CLAUDE.md slot); they were genuine omissions
that the initial plan didn't anticipate. They form a coherent diagnostics
package.

## Decision

Four targeted additions, each ~10-30 LOC:

- **`runlet._logging.ClockAwareLoggerAdapter`** (new internal module).
  Wraps a stdlib `Logger` plus a `Clock`. Every record produced through it
  carries `sim_time` in its `extra` mapping. The supervisor's `_host` wraps
  each daemon's child logger in this adapter when building `Context`.
  Users opting into a format string like `"%(sim_time).3f %(name)s
  %(message)s"` immediately see virtual-time-stamped logs. Existing users
  with no format change see no behavior change (the extra is just ignored).

- **`DaemonError` wraps the original exception in `_host`** before
  re-raising under `on_error="shutdown"`. The leaf in the
  `ExceptionGroup` is now `DaemonError("daemon 'X' failed: ...")` with the
  original exception as `__cause__`. Code that wants the original
  exception can `e.__cause__`; code that wants the daemon name can pattern-
  match on the message.

- **`DaemonHealth.last_error_phase` records where the error came from.**
  The value is one of `on_start`, `run`, `on_stop`, or `None` when
  `last_error` is `None`.

- **`Supervisor.add` rejects duplicate names.** A `ValueError` is raised if
  the chosen name was already registered. Daemon names key the supervisor's
  diagnostics, so allowing a collision would hide one daemon's health record
  behind another.

- **`Channel.send.statistics()` / `Channel.recv.statistics()`** return a
  new `ChannelStats` frozen dataclass with `current_buffer_used`,
  `max_buffer_size`, `open_send_streams`, `open_receive_streams`,
  `waiters_send`, `waiters_receive`. The in-process implementation
  forwards from anyio. Future transport adapters fill these in best-effort
  (using `-1` for fields they cannot determine, e.g. cross-host waiter
  counts).

## Consequences

+ Logs are now interpretable under `SimClock`; every line carries the
  virtual instant it was emitted at.
+ `ExceptionGroup` leaves identify the failing daemon by name, both in
  programmatic catches and in the stderr output.
+ Snapshot consumers can tell startup, runtime, and cleanup failures apart
  without parsing logs.
+ Accidental name collisions fail immediately instead of producing ambiguous
  logs and overwritten health records.
+ Channel diagnostics are a one-call away; backpressure debugging no
  longer requires private-attribute spelunking.
+ The logging, error wrapping, and channel statistics changes are additive.
  Duplicate-name rejection is a small breaking change for callers that relied
  on shared names, but those names already made diagnostics unsound.
- `Context.logger`'s declared type widens from `Logger` to
  `Logger | ClockAwareLoggerAdapter`. Code that called
  `logger.setLevel(...)` etc. still works (adapter forwards), but type
  checkers may flag direct attribute access on the underlying logger.
- `ChannelStats`'s `-1` sentinel for "not observable on this transport" is
  a compromise to keep the dataclass shape stable across future
  transports. A nullable Optional would be cleaner per-transport but
  forces every consumer to handle None even on in-process channels where
  the value is always known.
- The `sim_time` extra is always set on adapter-routed records, including
  records emitted under `WallClock` (where `sim_time` equals wall time
  and is largely redundant). The cost is one float assignment per record.

## Related

- ADR 0002: `SimClock` is the load-bearing primitive this ADR makes
  *observable*. Sim-time replay was incomplete without sim-time logs.
- ADR 0004: `on_error="shutdown"` is the default; wrapping in `DaemonError`
  is how the daemon's identity survives that path.
- ADR 0006: `ChannelStats` is part of the transport-agnostic Protocol
  surface; future transports must implement `statistics()`.
- `docs/archive/reflex-dual-postmortem.md`: the postmortem that surfaced
  these four gaps.
