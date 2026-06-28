# chrono-daemon docs

This directory holds chrono-daemon's design and usage docs.

## Layout

- `concepts.md`: the four primitives (`Channel`, `Clock`, `Daemon`,
  `Supervisor`), how they compose, and which invariants hold.
- `adr/`: Architecture Decision Records. Each ADR is a frozen-in-time
  statement of a load-bearing decision. New ADRs are added rather than
  editing old ones; superseded ADRs link forward.
- `recipes.md`: the user-facing index for `chrono_daemon.recipes`, grouped as
  routing, coordination, state, buffering, and hosting helpers. Recipes are
  importable but carry weaker stability guarantees than the core surface.
- Optional transport adapters live in `src/chrono_daemon/transports/`. The first
  one is `chrono_daemon.transports.zmq`, covered by `concepts.md` and ADR 0011.
- `archive/`: long-form proposal notes preserved for design history, not as
  the primary user documentation.

## When to add what

- A user-facing API change → update `concepts.md` and (if a tradeoff was
  involved) a new ADR.
- A "let's not do X" decision → ADR.
- "How do I X" question that recurs → recipe.

## When not to write docs here

- Editing rules for agents belong outside the user-facing docs.
- Per-PR justifications belong in the PR description.
