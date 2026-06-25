# Recipes

Patterns kept out of the four-primitive core but shipped as importable
helpers under `runlet.recipes`. The corresponding source lives in
`src/runlet/recipes/`.

Patterns that need extra wiring but should not be core live here. Consequences:

- `runlet.recipes.*` is **best-effort**. Signatures may change between
  minor releases without a new ADR. If you depend on one in production,
  pin a version or vendor it.
- Core `runlet.*` follows strict semver: breaking changes require an ADR
  and a major bump (see ADR 0001 for the namespace boundary).

The stability contract is restated at the top of
`src/runlet/recipes/__init__.py`. Each recipe is exercised by tests in
`tests/test_recipes.py`.

## Index

- **`runlet.recipes.fanout.tee(src, *dests)`**:
  [`fanout.py`](../src/runlet/recipes/fanout.py). 1:N broadcast with
  per-destination backpressure.
- **`runlet.recipes.merge.merge(sources, dest)`**:
  [`merge.py`](../src/runlet/recipes/merge.py). N:1 fan-in. Each producer owns
  its own source channel; the merge daemon owns the output send endpoint.
- **`runlet.recipes.load_balance.load_balance(source, dests)`**:
  [`load_balance.py`](../src/runlet/recipes/load_balance.py). Round-robin 1:N
  competing-consumer routing; each item goes to exactly one destination.
- **`runlet.recipes.worker_pool.worker_pool(incoming, results, handle)`**:
  [`worker_pool.py`](../src/runlet/recipes/worker_pool.py). Ready-worker job
  dispatch built from private SPSC worker channels. Handler returns and
  exceptions are sent to `results`; drain it concurrently or size it for
  the expected outputs.
- **`runlet.recipes.select.select(*receivers)`**:
  [`select.py`](../src/runlet/recipes/select.py). Wait on the first of
  several receivers to be ready.
- **`runlet.recipes.batcher.batcher_loop / submit`**:
  [`batcher.py`](../src/runlet/recipes/batcher.py). Collate-forward-split:
  gather N independent requests into one batched call, then route
  responses back. Use `submit()` for safe fan-in. Supports SimClock-compatible
  `max_queue_delay`. On `forward` exceptions, every caller in the batch sees
  the same exception.
- **`runlet.recipes.cooperative_every.cooperative_every(ctx, period)`**:
  [`cooperative_every.py`](../src/runlet/recipes/cooperative_every.py).
  Stop-aware periodic iteration: like `ctx.clock.every(period)`, but exits
  when `ctx.stopping` becomes True. The reasonable default for a daemon
  loop that should honour `Supervisor.stop`.
- **`runlet.recipes.latest.Latest`**:
  [`latest.py`](../src/runlet/recipes/latest.py). One-slot cache for the
  "most-recent value" pattern: producer calls `.set(value)`, N consumers
  call `.get()`. The standard way to share a latest-snapshot across
  daemons without a broadcast channel (ADR 0001).
- **`runlet.recipes.sync_bridge.host_async_dispatcher`**:
  [`sync_bridge.py`](../src/runlet/recipes/sync_bridge.py). Host a
  long-lived runlet supervisor on a dedicated event loop; sync callers
  invoke its dispatcher via `BlockingPortal`.
- **`runlet.recipes.lossy.DropNewestSend / DropOldestSend / CoalesceSend`**:
  [`lossy.py`](../src/runlet/recipes/lossy.py). `SendStream` wrappers
  that never block the producer when the buffer is full: drop newest, drop
  oldest, or single-slot coalesce. `DropOldestSend` and `CoalesceSend` need
  receive-side access, so they are in-process policies. Each tracks a
  `dropped` counter.
