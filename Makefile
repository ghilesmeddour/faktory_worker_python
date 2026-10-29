.PHONY: all format check test coverage upgrade build publish run clean

all: format check

format:
	uv run ruff check --fix .
	uv run ruff format .

check:
	uv run ruff check .
	uv run ruff format --diff .
	uv run mypy --ignore-missing-imports src
	uv run vulture --min-confidence 70 src tests
	uv run pytest

test:
	uv run pytest -svv

coverage:
	uv run coverage run --source=src -m pytest --junitxml=tests.xml
	uv run coverage report -m
	uv run coverage html
	uv run coverage xml

upgrade:
	uv lock --upgrade

build:
	uv build

publish: clean build
	uv publish

run:
	uv run python examples/fproducer.py
	uv run python examples/fconsumer.py

clean:
	rm -rf -- dist build htmlcov .coverage coverage.xml tests.xml
