# Roadmap

What is planned next, and what is deliberately deferred or rejected. New
items are added here when an ADR records the decision to defer rather than
ship.

## Under consideration

- **`ready_gate` recipe and/or first-class primitive.** trio's default
  scheduler randomizes task-spawn ordering across runs, which limits the
  "byte equality across runs" form of replay determinism to asyncio. A
  shared `anyio.Event` set after all daemons register removes the
  ambiguity. This is small enough to be a recipe, common enough that promoting
  it into the core is on the table. (Surfaced by
  `examples/reflex_dual_mock.py`.)
- **Multi-process and network transports.** `Channel` is already a Protocol
  (ADR 0006), so adapters such as `MultiprocessChannel`, `ZmqChannel`, or
  `ZenohChannel` can land without breaking the top-level API. The blockers
  are dependency policy, serialization policy, and how much transport behavior
  can preserve the SPSC fail-fast contract from ADR 0010.
- **Test fixtures.** A `pytest` plugin that gives the user
  `async def test_foo(supervisor: Supervisor, sim_clock: SimClock): ...`
  with the boilerplate of "enter supervisor, advance clock, exit cleanly"
  pre-baked. Would shave ~10 LOC off every integration test.

## Maybe, Not Committed

- **Deliberate fan-in / worker-pool recipe.** Core channels are SPSC
  (ADR 0010). If users need competing consumers or multiple producers, that
  should arrive as an explicit recipe or factory with a clear close/error
  policy, not by making endpoint sharing accidental.
- **`select(*receivers)` as a first-class API** instead of a recipe.
  Requires deciding on a cancellation semantic for the losers.
- **Strict-rate `every(mode="strict")`.** Today's `every` skips ticks when
  the consumer is slow; strict mode would queue them. The user would have
  to pick one per call site.

## Deliberately out of scope (no-goal)

These are recorded in ADRs; the corresponding ADR is the canonical
explanation. Listing them here so contributors don't accidentally re-litigate.

- **`Topic` / pub-sub broadcast.** ADR 0001. Use `runlet.recipes.fanout.tee`.
- **Lifecycle states beyond `on_start`/`run`/`on_stop`.** ADR 0005.
- **Services, RPC, parameter system, discovery.** No ADR yet because no one
  has asked; if it comes up, ADR it.
- **Observability beyond `Context.logger`.** No tracing, no metrics, no
  built-in introspection server. Users compose with their own stack.
- **CLI.** runlet is a library; there is no `runlet run`.
- **Anything that adds a runtime dependency.** ADR 0007.
