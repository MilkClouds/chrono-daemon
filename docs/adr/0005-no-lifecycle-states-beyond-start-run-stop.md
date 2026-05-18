# ADR 0005 — No lifecycle states beyond `on_start`/`run`/`on_stop`

Status: Accepted (2026-05-18)

## Context

ROS2 ships a "managed/lifecycle" node concept with five explicit states
(`unconfigured` → `inactive` → `active` → `inactive` → `finalized`) and four
transition callbacks (`on_configure`, `on_activate`, `on_deactivate`,
`on_cleanup`). The pitch is that operators get a uniform way to bring a node
up in stages, pause it, and tear it down cleanly.

In practice — by inspection of large ROS2 deployments and the user's own
experience — the overwhelming majority of "lifecycle nodes" reduce to:

- `configure` ≡ what you would have done in `__init__`,
- `activate` ≡ what you would have done at the top of your main loop,
- `deactivate` and `cleanup` ≡ what you would have done in `on_stop`.

The remaining states (`unconfigured`, `inactive`) are book-keeping for a
state machine that nothing in the system actually queries. The transitions
are invoked over a side-channel (a lifecycle topic / service) that introduces
its own discovery, ordering, and authorization questions. The net effect on
the daemon author is a fivefold increase in callback surface for a
capability they could already express with `if` statements.

The minority of daemons that genuinely need pause/resume (e.g. an inference
worker that should release its GPU when idle) are better served by a domain-
specific protocol on a `Channel[Command]` than by a one-size-fits-all
lifecycle state machine.

## Decision

`Daemon` has three hooks: `on_start(ctx)`, `run(ctx)`, `on_stop(ctx)`. There
is no `on_pause`, no `on_resume`, no `state` enum, no transition API.

A daemon that needs pause/resume should accept a control channel
(`Channel[Command]`) at construction and branch on the commands it receives.
This keeps the protocol explicit in the type, visible in the wiring code,
and free of the global "what state am I in" question.

## Consequences

+ The daemon authoring surface is minimal: one required method, two
  optional hooks.
+ State transitions become regular dataflow — they go through the same
  `Channel` plumbing as everything else, with the same backpressure and
  deterministic-replay properties.
+ No lifecycle service to discover, version, or secure.
- Users coming from ROS2 looking for "lifecycle node" will not find it.
  The migration is "pass a command channel" — visible in the
  `docs/recipes/` folder if a clear pattern emerges.
- Domain-specific pause/resume protocols won't share a common signature
  across daemons. This is intentional — they shouldn't, because what
  "pause" means is daemon-specific.

## Related

- ADR 0003 (dual API) — both class and decorator daemons inherit the same
  three-hook contract.
