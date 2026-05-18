.PHONY: check test lint format install all

# Source paths that participate in lint/format/type-check.
SRC_PATHS = src/ tests/ examples/ docs/recipes/

check: lint
	uv run pyrefly check

lint:
	uv run ruff check $(SRC_PATHS)
	uv run ruff format --check $(SRC_PATHS)

format:
	uv run ruff format $(SRC_PATHS)
	uv run ruff check --fix $(SRC_PATHS)

test:
	uv run pytest tests/ -v

install:
	uv sync --dev

all: format check test
