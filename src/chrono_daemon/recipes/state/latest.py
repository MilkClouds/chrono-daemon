"""One-slot "latest value" cache."""

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
