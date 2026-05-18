# Recipes

Patterns kept out of the four-primitive core but shipped as importable
helpers under `runlet.recipes`. The corresponding source lives in
`src/runlet/recipes/`.

Rule of thumb: if a pattern needs more than one daemon, one channel, and
one clock to express, but doesn't generalize to "everyone wants this," it
belongs here rather than in the top-level `runlet` namespace. Two
practical consequences:

- `runlet.recipes.*` is **best-effort**. Signatures may change between
  minor releases without a new ADR. If you depend on one in production,
  pin a version or vendor it.
- Core `runlet.*` follows strict semver: breaking changes require an ADR
  and a major bump (see ADR 0001 for the namespace boundary).

The stability contract is restated at the top of
`src/runlet/recipes/__init__.py`. Each recipe is exercised by tests in
`tests/test_recipes.py`.

## Index

- **`runlet.recipes.fanout.tee(src, *dests)`** —
  [`fanout.py`](../src/runlet/recipes/fanout.py). 1:N broadcast with
  per-destination backpressure. Replaces the pub/sub `Topic` we chose not
  to ship (ADR 0001).
- **`runlet.recipes.select.select(*receivers)`** —
  [`select.py`](../src/runlet/recipes/select.py). Wait on the first of
  several receivers to be ready. The Go `select` equivalent. anyio does
  not ship this as a primitive because the structured-concurrency idiom
  (`task_group` + `cancel_scope`) subsumes it; the recipe wraps that idiom.
- **`runlet.recipes.batcher.batcher_loop / submit`** —
  [`batcher.py`](../src/runlet/recipes/batcher.py). Collate-forward-split:
  gather N independent requests into one batched call, then route
  responses back. Supports a SimClock-compatible `max_queue_delay` timer
  (raced against `incoming.receive` via `ctx.clock.sleep`, *not*
  `anyio.move_on_after` — that one is wall-clock-only and would silently
  misbehave under `SimClock`). On `forward` exceptions, every caller in
  the batch sees the same exception (no silent partial failure).
- **`runlet.recipes.cooperative_every.cooperative_every(ctx, period)`** —
  [`cooperative_every.py`](../src/runlet/recipes/cooperative_every.py).
  Stop-aware periodic iteration: like `ctx.clock.every(period)`, but exits
  when `ctx.stopping` becomes True. The reasonable default for a daemon
  loop that should honour `Supervisor.stop`.
- **`runlet.recipes.latest.Latest`** —
  [`latest.py`](../src/runlet/recipes/latest.py). One-slot cache for the
  "most-recent value" pattern: producer calls `.set(value)`, N consumers
  call `.get()`. The standard way to share a latest-snapshot across
  daemons without a broadcast channel (ADR 0001).
- **`runlet.recipes.sync_bridge.host_async_dispatcher`** —
  [`sync_bridge.py`](../src/runlet/recipes/sync_bridge.py). Host a
  long-lived runlet supervisor on a dedicated event loop; sync callers
  invoke its dispatcher via `BlockingPortal`. The shape every "async
  dispatcher behind a sync ABC" deployment ends up at (e.g. PR #191's
  `ReFlExDualDispatcherServer`).
