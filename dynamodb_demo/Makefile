.PHONY: run black check test full-test image

SHELL := /bin/bash
format:
	poetry run black --config pyproject.toml .
	poetry run isort --settings-path pyproject.toml .

test-static:
	poetry run bash ./scripts/check.bash

test-py:
	poetry run bash ./scripts/tests.bash

test: test-static test-py

full-test: test-static
	poetry run bash ./scripts/integration-tests.bash
