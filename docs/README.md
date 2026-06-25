# runlet docs

This directory holds runlet's design and usage docs.

## Layout

- `concepts.md`: the four primitives (`Channel`, `Clock`, `Daemon`,
  `Supervisor`), how they compose, and which invariants hold.
- `adr/`: Architecture Decision Records. Each ADR is a frozen-in-time
  statement of a load-bearing decision. New ADRs are added rather than
  editing old ones; superseded ADRs link forward.
- `recipes.md`: the user-facing index for patterns shipped under
  `runlet.recipes` (broadcast/fanout, batching, multi-channel select,
  sync/async bridging). The source lives in `src/runlet/recipes/`;
  recipes are importable but carry weaker stability guarantees than the
  core surface.
- Optional transport adapters live in `src/runlet/transports/`. The first
  one is `runlet.transports.zmq`, covered by `concepts.md` and ADR 0011.
- `roadmap.md`: what's planned next and what's deliberately deferred.
- `archive/`: long-form proposal notes and postmortems. These are preserved
  for design history, not as the primary user documentation.

## When to add what

- A user-facing API change → update `concepts.md` and (if a tradeoff was
  involved) a new ADR.
- A "let's not do X" decision → ADR.
- "How do I X" question that recurs → recipe.
- A planned future feature → roadmap entry.

## When not to write docs here

- Editing rules for agents belong outside the user-facing docs.
- Per-PR justifications belong in the PR description.
