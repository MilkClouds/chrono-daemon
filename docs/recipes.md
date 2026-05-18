# Recipes

Patterns the core library deliberately does not ship, but that recur often
enough that it's useful to have a worked, tested example to copy.

The rule of thumb: if a pattern needs more than one daemon, one channel, and
one clock to express, but doesn't generalize to "everyone wants this," it
belongs here rather than in `runlet/`. Users **copy these into their own
codebase**; we do not expose them as importable APIs. That keeps the core
surface small while still answering "how do I do X" in a single place.

Each recipe is:

- A single `.py` file, runnable on its own.
- Type-checked and ruff-clean (lives under the same `make check` as the rest).
- Backed by a test in `tests/test_recipes_*.py` that pins the behavior.

## Index

- [`fanout.py`](fanout.py) — `tee(src, *dests)`: broadcast one stream of
  messages to N consumers, with per-destination backpressure. Replaces the
  pub/sub `Topic` we explicitly chose not to ship (see ADR 0001).
- [`batcher.py`](batcher.py) — collate/forward/split: gather N independent
  requests into one batched call (e.g. a model forward pass), then route
  responses back to each caller. The pattern behind every dynamic-batching
  inference server.
- [`select.py`](select.py) — wait on the first of several receivers to be
  ready. The Go `select` equivalent. anyio does not ship this as a primitive
  because the structured-concurrency idiom (`task_group` + `cancel_scope`)
  subsumes it; the recipe shows the idiom.
- [`sync_bridge.py`](sync_bridge.py) — call into a long-lived runlet
  supervisor from synchronous code via `anyio.from_thread.BlockingPortal`.
  The shape every "async dispatcher behind a sync ABC" deployment ends up at.
