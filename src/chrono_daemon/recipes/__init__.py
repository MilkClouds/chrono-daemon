"""Importable helpers kept outside the core surface.

- The core primitives in ``chrono_daemon`` follow strict semver: breaking changes
  require an ADR and a major bump.
- ``chrono_daemon.recipes`` are best-effort references. Their signatures may
  change between minor releases without an ADR. If you depend on one in
  production code, pin a version or vendor it.

Use them by importing directly:

    # Routing
    from chrono_daemon.recipes.fanout import tee
    from chrono_daemon.recipes.merge import merge
    from chrono_daemon.recipes.load_balance import load_balance
    from chrono_daemon.recipes.worker_pool import worker_pool

    # Coordination
    from chrono_daemon.recipes.select import select
    from chrono_daemon.recipes.batcher import batcher_loop, submit
    from chrono_daemon.recipes.cooperative_every import cooperative_every

    # State and buffering
    from chrono_daemon.recipes.latest import Latest
    from chrono_daemon.recipes.lossy import DropOldestSend, DropNewestSend, CoalesceSend

    # Hosting
    from chrono_daemon.recipes.supervisor_host import SupervisorHost
    from chrono_daemon.recipes.sync_bridge import host_async_dispatcher

The implementation is grouped into ``routing``, ``coordination``, ``state``,
and ``hosting`` subpackages. Top-level recipe modules remain compatibility
imports.

The corresponding design notes live in ``docs/recipes.md``.
"""
