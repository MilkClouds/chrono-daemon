# ADR 0007: `anyio` is the only runtime dependency

Status: Accepted (2026-05-18)

## Context

Pure-Python concurrency libraries tend to accumulate dependencies: a
serialization library (msgspec/pydantic), a structured logger (structlog), a
metrics/tracing library (OpenTelemetry), a typed-dict helper, a CLI
framework, and so on. Each addition is individually defensible; the
aggregate makes the library nontrivial to embed in another project, raises
the lockfile churn cost, and creates pinning conflicts in downstream apps.

The user's prior prototypes (`simple_env` and the per-project ROS-shaped
loops repeated across `projects/`) survived on no dependencies at all.
chrono-daemon's value proposition isn't "more features than asyncio"; it's "the
specific shape of `Channel` + `Clock` + `Daemon` + `Supervisor` on top of
anyio."

## Decision

`anyio>=4` is the only runtime dependency declared in `pyproject.toml`.
The dev dependency group adds `pytest`, `trio` (for two-backend testing),
`ruff`, and `pyrefly`. those are tooling, not runtime.

This rules out, at the runtime level:

- Serialization libraries (msgspec, pydantic, pickle-by-default).
  Cross-process transport (when it lands per ADR 0006) will force a choice,
  and that choice will be in its own ADR.
- Structured logging (structlog, loguru). `Context.logger` is stdlib
  `logging` with sim-time injected via a `LoggerAdapter` (ADR 0008).
- Tracing/metrics. Tools that need them compose with chrono-daemon, not the
  other way around.
- A CLI framework. chrono-daemon is a library; there is no `chrono-daemon run`.

## Consequences

+ `pip install chrono-daemon` pulls anyio and nothing else.
+ Embedding chrono-daemon in another project is risk-free: no transitive dependency
  is going to bring its own peer-dep conflict.
+ The lockfile is tiny; `uv lock --upgrade` rarely surfaces churn.
+ "What does chrono-daemon really do?" has a small answer: read the seven src
  modules. There is no behavior hidden behind a third-party library.
- Features that would be one import away in a different library
  (structured logging fields, automatic tracing, fast schema validation)
  have to be earned by the user composing their own stack.
- The first feature that genuinely needs a new dep will be a hard
  conversation. We will likely add it as an optional extra
  (`pip install chrono-daemon[zenoh]`) rather than a core dep.

## Related

- ADR 0006. transport-adapter slot is the most likely future source of
  dependency pressure. When it lands, the chosen serialization
  approach will be the topic of a new ADR.
- ADR 0008. sim-aware logging is implemented entirely with stdlib
  `logging.LoggerAdapter`, no structlog/loguru required.
