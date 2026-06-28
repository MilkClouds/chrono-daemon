# ADR 0003: Daemon dual API: class and decorator

Status: Accepted (2026-05-18)

## Context

Long-running async units in the wild come in two shapes:

1. Stateful workers that benefit from a class body: an inference daemon
   holding a model handle, a sensor adapter holding a hardware connection,
   etc. These naturally want `__init__` for construction, attributes for
   state, and explicit `on_start` / `on_stop` for setup/teardown.
2. One-shot or trivially-stateless coroutines: a glue loop reading from one
   channel and forwarding to another, a periodic printer, a test fixture.
   These read more naturally as `async def fn(ctx, ...): ...`.

Forcing every daemon into a class makes the second case verbose
(boilerplate per recipe, per test fixture). Forcing every daemon into a
function makes the first case awkward (lifecycle hooks become closure-only,
and instance state has to be smuggled through `nonlocal` or default-arg
tricks).

ROS2 nodes commit to the class form. asyncio task-spawning APIs commit to
the function form. We do not want to commit either way. robotics control
loops want classes, the recipes folder wants functions.

## Decision

Two ways to define a daemon, both producing the same runtime object:

- Subclass `Daemon` and override `run(ctx)`. Optionally override
  `on_start(ctx)` and `on_stop(ctx)`. Instances are `Daemon` objects.
- Decorate `async def fn(ctx, *args, **kwargs): ...` with `@daemon`. The
  decorated callable becomes a *factory*: calling it returns a `Daemon`
  instance. Lifecycle hooks default to no-ops; if the user wants them, they
  reach for the class form.

Internally, the decorator wraps the function in a private `_FnDaemon` adapter
that is itself a `Daemon` subclass. The two paths converge on the same
ABC; supervisor code sees only `Daemon`.

## Consequences

+ The minimum-overhead expression of a daemon is three lines
  (`@daemon` + `async def` + body), but stateful daemons retain a class.
+ One ABC for type-checking; one path for the supervisor to handle.
+ Decorator and class are interchangeable in tests. `_FnDaemon` is a
  `Daemon`, so `isinstance(decorated_daemon(), Daemon)` is true.
- Two ways to do the same thing is a learning-curve tax. The documentation
  has to teach both, and explain which to pick.
- Hidden cost: the decorator returns a *factory*, not a Daemon. Forgetting
  to call the factory (`sup.add(ticker)` instead of `sup.add(ticker())`)
  produces a confusing error. The factory is annotated as
  `Callable[..., Daemon]` so type checkers catch it, but users running
  without a type checker will hit this once.

## Related

- ADR 0005 explains why neither form ships lifecycle states beyond
  `on_start`/`run`/`on_stop`.
- `src/chrono_daemon/daemon.py` has the `_FnDaemon` adapter implementation.
