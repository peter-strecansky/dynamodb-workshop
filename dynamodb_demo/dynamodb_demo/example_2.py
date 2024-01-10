# Basic dynamodb operations using boto3.client
import logging
from decimal import Decimal

import boto3

from .models import Account, AccountType

logger = logging.getLogger("dynamo_db_example_2")


def _format_response(
    raw_data: "dict| None",
) -> dict | None:
    if not raw_data:
        return None

    formatted_item = {}
    for key, value in raw_data.items():
        formatted_item[key] = list(value.values())[0]

    return formatted_item


def create_accounts_table():
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    """
    dynamodb = boto3.client("dynamodb")
    res = dynamodb.create_table(
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

    return res


def add_account(account: Account):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/put_item.html
    """
    dynamodb = boto3.client("dynamodb")
    response = dynamodb.put_item(
        TableName="accounts",
        Item={
            "account_id": {"S": str(account.account_id)},
            "account_type": {"S": account.account_type},
            "balance": {"N": str(account.balance)},
        },
        # Only this format can be used
        ConditionExpression="attribute_not_exists(account_id) AND attribute_not_exists(account_type)",
    )

    logger.info("account added")
    return response


def get_account(account_id: str, account_type: AccountType):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html#
    """
    dynamodb = boto3.client("dynamodb")
    res = dynamodb.get_item(
        TableName="accounts",
        Key={
            "account_id": {"S": account_id},
            "account_type": {"S": account_type},
        },
    )
    logger.info("account retrieved")
    return res


def query_account(account_id: str):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
    """
    dynamodb = boto3.client("dynamodb")
    res = dynamodb.query(
        TableName="accounts",
        KeyConditionExpression="account_id = :account_id",
        ExpressionAttributeValues={
            ":account_id": {"S": account_id},
        },
    )
    logger.info("account retrieved")
    return res


def update_account_balance(account_id: str, account_type: AccountType, new_balance: int | Decimal):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
    """
    dynamodb = boto3.client("dynamodb")
    res = dynamodb.update_item(
        TableName="accounts",
        Key={
            "account_id": {"S": account_id},
            "account_type": {"S": account_type},
        },
        UpdateExpression="set balance = :balance",
        ExpressionAttributeValues={
            ":balance": {"N": str(new_balance)},
        },
        ReturnValues="UPDATED_NEW",
    )
    logger.info("account balance updated")
    return res
