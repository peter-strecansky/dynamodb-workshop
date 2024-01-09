import os

import boto3
import moto
import pytest
from moto.server import ThreadedMotoServer  # type: ignore


@pytest.fixture(autouse=True, scope="session")
def aws_credentials():
    """
    This fixture completely overrides AWS service endpoints and credentials.
    After this fixture is run, you may safely create and use AWS resources
    within your tests with the `boto3` cilent.
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

    # This check allows you to specify a concrete AWS_ENDPOINT_URL other than the transient
    # one from this fixture. This allows you to run a moto server via Docker and then
    # interact with it on the CLI to your heart's content to debug why your tests are failing.
    if os.environ.get("AWS_ENDPOINT_URL") is not None:
        yield
    else:
        os.environ["AWS_ENDPOINT_URL"] = "http://127.0.0.1:5000"
        server = ThreadedMotoServer()
        server.start()
        try:
            yield
        finally:
            server.stop()


@pytest.fixture(scope="function")
def dynamodb():
    """DynamoDB mock client."""
    with moto.mock_dynamodb():
        yield boto3.resource("dynamodb", region_name="us-east-1")


@pytest.fixture
def set_up_accounts_table(dynamodb):
    table = dynamodb.create_table(
        TableName="accounts",
        KeySchema=[
            {"AttributeName": "account_id", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "account_type", "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": "account_id", "AttributeType": "S"},
            {"AttributeName": "account_type", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    # Make sure that table exists
    assert table.table_status == "ACTIVE"
