default: prepare dead-code-check format lint type-check test coverage
check-all: prepare dead-code-check format-check lint-check type-check test coverage

run:
	uv run python examples/fproducer.py
	uv run python examples/fconsumer.py

.PHONY: prepare
prepare:
	uv lock --upgrade
	uv sync

dead-code-check:
	uv run vulture --min-confidence 70 src tests

lint:
	uv run ruff check --fix .

lint-check:
	uv run ruff check .

format:
	uv run ruff format .

format-check:
	uv run ruff format --diff .

type-check:
	uv run mypy --ignore-missing-imports src

test:
	uv run python -m pytest -svv

.PHONY: coverage
coverage: tests.xml coverage.xml

tests.xml:
	uv run python -m pytest -svv --junitxml=tests.xml

coverage.xml:
	uv run coverage run --source=src -m pytest
	uv run coverage report -m
	uv run coverage html
	uv run coverage xml

.PHONY: build
build:
	uv build

push:
	uv publish

build-and-push: build push

clean:
	rm -rf -- dist build htmlcov .coverage coverage.xml tests.xml
