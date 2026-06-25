# Architecture Decision Records

Each ADR captures one load-bearing decision: what we were facing, what we
chose, and what the choice locks in and locks out.

## Format

Nygard short: `Status`, `Context`, `Decision`, `Consequences`, and optional
`Related`. Keep each ADR under ~100 lines; split it if it needs more.

ADRs are immutable once `Accepted`. To revise a decision, write a new ADR
with `Status: Accepted; Supersedes 000X` and edit the old one to add
`Status: Superseded by 000Y`. Never silently mutate an accepted ADR.

## Numbering

Sequential, zero-padded to 4 digits. Filenames are kebab-case after the
number: `000N-short-summary.md`.

## Index

- [0001: Channel is the sole communication primitive](0001-channel-is-the-sole-comm-primitive.md)
- [0002: Wall and Sim clocks as a pluggable Clock protocol](0002-wall-and-sim-clocks-as-pluggable-protocol.md)
- [0003: Daemon dual API: class and decorator](0003-daemon-dual-api-class-and-decorator.md)
- [0004: `on_error="shutdown"` is the Supervisor default](0004-on-error-shutdown-by-default.md)
- [0005: No lifecycle states beyond `on_start`/`run`/`on_stop`](0005-no-lifecycle-states-beyond-start-run-stop.md)
- [0006: Ship in-process only at first, with a reserved transport-adapter slot](0006-in-process-v0-transport-adapter-slot.md)
- [0007: `anyio` is the only runtime dependency](0007-anyio-only-runtime-dependency.md)
- [0008: Sim-time-aware logging and supervisor diagnostics](0008-sim-aware-logging-and-supervisor-diagnostics.md)
- [0009: Cooperative stop signaling on Supervisor](0009-cooperative-stop-signaling.md)
- [0010: Channel endpoints are single-owner](0010-channel-endpoints-are-single-owner.md)
