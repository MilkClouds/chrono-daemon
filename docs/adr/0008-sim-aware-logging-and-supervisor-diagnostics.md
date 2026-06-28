# ADR 0008: Sim-time-aware logging and supervisor diagnostics

Status: Accepted (2026-05-18); amended (2026-06-25)

## Context

The initial four-primitive core (`Channel`, `Clock`, `Daemon`, `Supervisor`)
was enough to run deterministic replay, but not enough to diagnose it well.
`examples/system_stack_mock.py` surfaced these gaps:

1. `Context.logger` was plain stdlib `logging.Logger`. Records carried only
   wall-clock timestamps, so a `SimClock` run had no visible virtual time.
2. When a daemon raised under `on_error="shutdown"`, the resulting
   `ExceptionGroup` carried the original exception with no daemon name.
   Operators also needed the failing lifecycle phase.
3. `Supervisor.add(d, name="X")` accepted duplicate names silently. Two
   daemons with the same name shared a child logger and one `snapshot()`
   record key; cross-task diagnosis was ambiguous.
4. `Channel.send` and `Channel.recv` exposed no introspection. anyio's
   `MemoryObjectStream.statistics()` was available internally but not on
   the chrono-daemon API. Debugging "is this channel full, who is waiting" needed
   private-attribute access.

## Decision

Targeted additions:

- **`chrono_daemon._logging.ClockAwareLoggerAdapter`** (new internal module).
  Adds `sim_time` to daemon log records.

- **`DaemonError` wraps the original exception in `_host`** before
  re-raising under `on_error="shutdown"`. The original exception remains
  available via `__cause__`.

- **`DaemonHealth.last_error_phase` records where the error came from.**
  The value is one of `on_start`, `run`, `on_stop`, or `None` when
  `last_error` is `None`.

- **`Supervisor.add` rejects duplicate names.** A `ValueError` is raised if
  the chosen name was already registered.

- **`Channel.send.statistics()` / `Channel.recv.statistics()`** return a
  new `ChannelStats` frozen dataclass with `current_buffer_used`,
  `max_buffer_size`, `open_send_streams`, `open_receive_streams`,
  `waiters_send`, `waiters_receive`. The in-process implementation
  forwards from anyio. Future transport adapters use `-1` for fields they
  cannot determine.

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
- `Context.logger`'s declared type widens from `Logger` to
  `Logger | ClockAwareLoggerAdapter`. Code that called
  `logger.setLevel(...)` etc. still works (adapter forwards), but type
  checkers may flag direct attribute access on the underlying logger.
- `ChannelStats`'s `-1` sentinel for "not observable on this transport" is
  a compromise to keep the dataclass shape stable across future transports.
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
- `examples/README.md`: the example notes that summarize the gaps this ADR
  closed.
