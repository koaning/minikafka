.PHONY: install lint test notebooks build demo docs docs-build clean

install:
	uv sync --extra test --extra demo

lint:
	uv run --extra test ruff check .

test:
	uv run --extra test pytest -q

notebooks:
	uv run --extra demo marimo check notebooks/demo.py
	uv run --extra demo notebooks/demo.py

build:
	uv build

demo:
	uv run --extra demo marimo edit notebooks/demo.py

docs:
	uv run --extra docs zensical serve

docs-build:
	uv run --extra docs zensical build --clean

clean:
	rm -rf .pytest_cache .ruff_cache dist build *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
