default: prepare dead-code-check format isort type-check test coverage
check-all: prepare dead-code-check format-check type-check test coverage

run:
	poetry run python examples/fproducer.py
	poetry run python examples/fconsumer.py

.PHONY: prepare
prepare:
	poetry update
	poetry install

dead-code-check:
	poetry run vulture --min-confidence 70 src tests

isort:
	poetry run isort .

isort-check:
	poetry run isort -c --df .

format:
	poetry run black -v .

format-check:
	poetry run black --diff -v .

type-check:
	poetry run mypy --ignore-missing-imports src

test:
	poetry run python -m pytest -svv

.PHONY: coverage
coverage: tests.xml coverage.xml

tests.xml:
	poetry run python -m pytest -svv --junitxml=tests.xml

coverage.xml:
	poetry run coverage run --source=src -m pytest
	poetry run coverage report -m
	poetry run coverage html
	poetry run coverage xml

.PHONY: build
build:
	poetry build

push:
	poetry publish

build-and-push: build push

clean:
	rm -rf -- dist build htmlcov .coverage coverage.xml tests.xml
