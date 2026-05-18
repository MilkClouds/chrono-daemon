"""Patterns kept out of the core surface but shipped as importable helpers.

These modules implement composable patterns — fanout, batcher, select,
sync-bridge — that are common enough to want a tested reference but
specialized enough that we keep them outside the four-primitive core
(``Channel``, ``Clock``, ``Daemon``, ``Supervisor``). They live under
``runlet.recipes`` rather than the top-level namespace as a signal:

- The core primitives in ``runlet`` follow strict semver: breaking changes
  require an ADR and a major bump.
- ``runlet.recipes`` are best-effort references. Their signatures may
  change between minor releases without an ADR. If you depend on one in
  production code, pin a version or vendor it.

Use them by importing directly:

    from runlet.recipes.fanout import tee
    from runlet.recipes.select import select
    from runlet.recipes.batcher import batcher_loop, submit
    from runlet.recipes.sync_bridge import open_sync_supervisor

The corresponding design notes live in ``docs/recipes.md``.
"""
