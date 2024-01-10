set -e

pytest --ignore tests/apis/integration_tests --cov=pomd.api tests
