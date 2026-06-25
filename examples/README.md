# Examples

The files in this directory are runnable end-to-end demos. They are larger
than recipes, but still small enough to inspect in one sitting. The tests in
`tests/test_examples.py` execute them so example drift is caught by CI.

## Index

- `reflex_dual_mock.py`: a single-session System 2 / 1 / 0 inference pipeline.
  Model calls are mocked with `ctx.clock.sleep(...)` and deterministic toy
  computations.
- `reflex_dual_multi_session.py`: a multi-session dispatcher built from nested
  `Supervisor` instances. Each session owns its own `SimClock`.

The longer ergonomic postmortem lives in
`docs/archive/reflex-dual-postmortem.md`.

## Determinism Note

`SimClock` controls time, not task scheduling. On asyncio, the single-session
mock has byte-identical replay across repeated runs. On trio, task-spawn order
can vary by design, so the logs keep the same shape and monotonic virtual time
but may differ in early item order. Use asyncio for byte-equality replay tests,
or gate daemon startup explicitly when cross-backend order must be identical.
