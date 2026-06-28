# Recipes

Patterns kept out of the four-primitive core but shipped as importable
helpers under `chrono_daemon.recipes`. The corresponding source lives in
`src/chrono_daemon/recipes/`.

Patterns that need extra wiring but should not be core live here. Consequences:

- `chrono_daemon.recipes.*` is **best-effort**. Signatures may change between
  minor releases without a new ADR. If you depend on one in production,
  pin a version or vendor it.
- Core `chrono_daemon.*` follows strict semver: breaking changes require an ADR
  and a major bump (see ADR 0001 for the namespace boundary).

The stability contract is restated at the top of
`src/chrono_daemon/recipes/__init__.py`. Each recipe is exercised by tests in
`tests/test_recipes.py`.

## Index

Top-level modules such as `chrono_daemon.recipes.fanout` stay as compatibility
imports. Implementations are grouped below by category.

### Routing

- **`chrono_daemon.recipes.fanout.tee(src, *dests)`**:
  [`routing/fanout.py`](../src/chrono_daemon/recipes/routing/fanout.py). 1:N broadcast with
  per-destination backpressure.
- **`chrono_daemon.recipes.merge.merge(sources, dest)`**:
  [`routing/merge.py`](../src/chrono_daemon/recipes/routing/merge.py). N:1 fan-in. Each producer owns
  its source channel; the merge daemon owns the output send endpoint.
- **`chrono_daemon.recipes.load_balance.load_balance(source, dests)`**:
  [`routing/load_balance.py`](../src/chrono_daemon/recipes/routing/load_balance.py). Round-robin 1:N
  routing where each item goes to exactly one destination.
- **`chrono_daemon.recipes.worker_pool.worker_pool(incoming, results, handle)`**:
  [`routing/worker_pool.py`](../src/chrono_daemon/recipes/routing/worker_pool.py). Ready-worker job
  dispatch built from private SPSC worker channels. Handler returns and
  exceptions are sent to `results`; drain it concurrently or size it for
  the expected outputs.

### Coordination

- **`chrono_daemon.recipes.select.select(*receivers)`**:
  [`coordination/select.py`](../src/chrono_daemon/recipes/coordination/select.py). Wait on the first receiver
  to become ready.
- **`chrono_daemon.recipes.batcher.batcher_loop / submit`**:
  [`coordination/batcher.py`](../src/chrono_daemon/recipes/coordination/batcher.py). Gather independent
  requests into one batched call, then route responses back. `submit()` is
  the safe fan-in helper. `max_queue_delay` works with `SimClock`, and
  forward exceptions propagate to every caller in the batch.
- **`chrono_daemon.recipes.cooperative_every.cooperative_every(ctx, period)`**:
  [`coordination/cooperative_every.py`](../src/chrono_daemon/recipes/coordination/cooperative_every.py).
  Stop-aware periodic iteration for daemon loops.

### State and buffering

- **`chrono_daemon.recipes.latest.Latest`**:
  [`state/latest.py`](../src/chrono_daemon/recipes/state/latest.py). One-slot latest-value cache
  for sharing a snapshot without a broadcast channel.
- **`chrono_daemon.recipes.lossy.DropNewestSend / DropOldestSend / CoalesceSend`**:
  [`state/lossy.py`](../src/chrono_daemon/recipes/state/lossy.py). Non-blocking `SendStream`
  wrappers for drop-newest, drop-oldest, and single-slot coalescing policies.
  Drop-oldest and coalescing need receive-side access; each wrapper tracks
  `dropped`.

### Hosting

- **`chrono_daemon.recipes.supervisor_host.SupervisorHost`**:
  [`hosting/supervisor_host.py`](../src/chrono_daemon/recipes/hosting/supervisor_host.py). Host a
  supervisor inside a caller-owned task group with a start barrier, stop
  handle, and optional teardown callback.
- **`chrono_daemon.recipes.sync_bridge.host_async_dispatcher`**:
  [`hosting/sync_bridge.py`](../src/chrono_daemon/recipes/hosting/sync_bridge.py). Host a
  long-lived supervisor on a dedicated event loop so sync callers can invoke
  an async dispatcher through `BlockingPortal`.
