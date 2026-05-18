"""Shared pytest fixtures: parametrize every async test across asyncio and trio backends."""

from __future__ import annotations

import pytest


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    """Run each async test on both anyio backends."""
    return request.param
