"""Recipe: one-slot "latest value" cache.

When one daemon publishes values and N other daemons want the most-recent
one (older values are uninteresting), a tiny ``Latest[T]`` holder is the
right shape — no queue to drain, no lock needed for the at-most-one-writer
case (CPython attribute reads/writes are atomic).

Used in both reflex-dual examples to share the latest ``Obs`` / ``Subgoal``
across S2/S1/S0 daemons without a broadcast channel (ADR 0001 keeps the
core comm primitive at 1:1).

Import as ``from runlet.recipes.latest import Latest``. The recipe
namespace (``runlet.recipes``) is best-effort — see
``src/runlet/recipes/__init__.py``.
"""

from __future__ import annotations

from typing import Generic, TypeVar

__all__ = ["Latest"]

T = TypeVar("T")


class Latest(Generic[T]):
    """One-slot cache for the most-recently-produced value."""

    __slots__ = ("_v",)

    def __init__(self) -> None:
        self._v: T | None = None

    def get(self) -> T | None:
        return self._v

    def set(self, value: T) -> None:
        self._v = value
