set -e

black --check --config pyproject.toml .
isort -c --settings-path pyproject.toml .
flake8
mypy --config-file pyproject.toml -p dynamodb_demo
bandit -c pyproject.toml -lll -r .
