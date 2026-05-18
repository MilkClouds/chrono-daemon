# CLAUDE.md — runlet

Tiny anyio-based concurrency library. Four primitives: `Channel`, `Clock`
(Wall/Sim), `Daemon`, `Supervisor`. See `docs/concepts.md` for the user-facing
explanation and `docs/adr/` for why each piece looks the way it does.

This file is the editing-rule sheet for AI agents working on this project.
The *why* of any decision lives in an ADR, not here.

## Commands

```bash
uv sync --dev
make check    # ruff lint + format check + pyrefly
make test     # pytest on asyncio + trio
make all      # format + check + test
```

## Editing rules

Each of these has an ADR. Don't violate them without writing a superseding
ADR first.

- **`anyio` is the only direct runtime dependency.** No `msgspec`, no
  `structlog`, no `pydantic`. (ADR 0007.)
- **`Channel` is the sole communication primitive on the core surface.**
  Don't add `Topic`, pub/sub broadcast, services, RPC, or a parameter
  system to `runlet.*`. Fanout lives at `runlet.recipes.fanout.tee` —
  importable, but under the weaker-stability recipes namespace. (ADR 0001.)
- **Daemons must reach for `ctx.clock.sleep(...)`** — never `anyio.sleep`
  directly. Library-internal code must obey this so `SimClock` can
  intercept. (ADR 0002.)
- **`Daemon` has exactly three hooks**: `on_start`, `run`, `on_stop`. No
  pause/resume, no lifecycle state machine. (ADR 0005.)
- **`Channel` protocol signatures stay transport-agnostic.** Don't bake
  in-process Python-identity or sync assumptions; future transport adapters
  share the same surface. (ADR 0006.)

## Test discipline

- Every async test is parametrized over `["asyncio", "trio"]` via
  `tests/conftest.py`. New tests must pass on both.
- Canary: `tests/test_clock.py::test_simclock_burst_step_deterministic_order`
  — if this fails on either backend, the rest of the system is unreliable.
- `tests/test_integration.py` is the "would runlet replace simple_env"
  check. Don't weaken it.
- `tests/test_examples.py` pins the demos in `examples/`. Don't bypass it
  by skipping; fix the demo instead.

## What lives where

- `src/runlet/` — the seven core modules. Each one is short and has one job.
- `docs/concepts.md` — user-facing explanation of the four primitives.
- `docs/adr/` — frozen-in-time decision records. Immutable once accepted;
  add a new ADR to revise.
- `src/runlet/recipes/` — patterns kept off the core surface but
  importable under `runlet.recipes.*`. Weaker stability guarantees than
  the core (see `src/runlet/recipes/__init__.py`).
- `docs/recipes.md` — user-facing index for the above.
- `docs/roadmap.md` — v0.x candidates and explicit no-goals.
- `examples/` — end-to-end demos exercised by CI (`tests/test_examples.py`).
- `CLAUDE.md` (this file) — editing rules + commands only.
- Top-level `README.md` — the 5-minute pitch.
