# runlet docs

This directory is the long-form home for runlet's design and usage docs. The
top-level `README.md` is a 5-minute pitch; the in-repo `CLAUDE.md` is a terse
editing-rule sheet for AI agents. Everything that explains *why* lives here.

## Layout

- `concepts.md` — the four primitives (`Channel`, `Clock`, `Daemon`,
  `Supervisor`), how they compose, and which invariants hold.
- `adr/` — Architecture Decision Records. Each ADR is a frozen-in-time
  statement of a load-bearing decision: the context, the choice, and what
  becomes true and not-true as a result. New ADRs are added rather than
  editing old ones; superseded ADRs link forward.
- `recipes.md` — the user-facing index for patterns shipped under
  `runlet.recipes` (broadcast/fanout, batching, multi-channel select,
  sync↔async bridging). The source lives in `src/runlet/recipes/`;
  recipes are importable but carry weaker stability guarantees than the
  core surface.
- `roadmap.md` — what's planned next and what's deliberately deferred.

## When to add what

- A user-facing API change → update `concepts.md` and (if a tradeoff was
  involved) a new ADR.
- A "let's not do X" decision → ADR.
- "How do I X" question that recurs → recipe.
- A planned future feature → roadmap entry.

## When *not* to write docs here

- Editing rules for the live codebase (e.g. "always go through `ctx.clock`")
  belong in `CLAUDE.md`, not here. Docs are the *why*; CLAUDE.md is the *do*.
- Per-PR justifications belong in the PR description, not in an ADR. ADRs are
  for decisions whose reasoning needs to survive across PRs.
