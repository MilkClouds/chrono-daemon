# Roadmap

What is planned next, and what is deliberately deferred or rejected. New
items are added here when an ADR defers them.

## Under consideration

- **`ready_gate` recipe and/or first-class primitive.** trio's default
  scheduler randomizes task-spawn ordering across runs, which limits the
  "byte equality across runs" form of replay determinism to asyncio. A
  shared `anyio.Event` set after all daemons register removes the
  ambiguity. Likely a recipe first. Surfaced by `examples/system_stack_mock.py`.
- **Multi-process and network transports.** `Channel` is already a Protocol
  (ADR 0006), so adapters such as `MultiprocessChannel`, `ZmqChannel`, or
  `ZenohChannel` can land without breaking the top-level API. The blockers
  are dependency policy, serialization policy, and how much transport behavior
  can preserve the SPSC fail-fast contract from ADR 0010. `merge`,
  `load_balance`, and `worker_pool` define the in-process topology semantics
  transports should preserve where possible.
- **Test fixtures.** A `pytest` plugin for common supervisor + SimClock setup.

## Maybe, Not Committed

- **`select(*receivers)` as a first-class API** instead of a recipe.
  Requires deciding on a cancellation semantic for the losers.
- **Strict-rate `every(mode="strict")`.** Today's `every` skips ticks when
  the consumer is slow; strict mode would queue them. The user would have
  to pick one per call site.

## Deliberately out of scope (no-goal)

These are recorded in ADRs; the corresponding ADR is the canonical
explanation.

- **`Topic` / pub-sub broadcast.** ADR 0001. Use `runlet.recipes.fanout.tee`.
- **Lifecycle states beyond `on_start`/`run`/`on_stop`.** ADR 0005.
- **Services, RPC, parameter system, discovery.** No ADR yet because no one
  has asked; if it comes up, ADR it.
- **Observability beyond `Context.logger`.** No tracing, no metrics, no
  built-in introspection server. Users compose with their own stack.
- **CLI.** runlet is a library; there is no `runlet run`.
- **Anything that adds a runtime dependency.** ADR 0007.
