# Basic dynamoDB operations using boto3.resource
import logging
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import And, Attr

from .models import Account, AccountType

logger = logging.getLogger("dynamo_db_example_1")


def create_accounts_table():
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    """
    dynamodb = boto3.resource("dynamodb")
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
    logger.info("table created", extra=dict(table_name=table.name, status=table.table_status))
    return table


def add_account(account: Account):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("accounts")
    response = table.put_item(
        Item={
            "account_id": str(account.account_id),
            "account_type": account.account_type,
            "balance": account.balance,
            "version": account.version,
        },
        ConditionExpression=And(Attr("account_id").not_exists(), Attr("account_type").not_exists()),
        # Alternative:
        # ConditionExpression="attribute_not_exists(account_id) AND attribute_not_exists(account_type)",
    )

    logger.info("account added")
    return response


def get_account(account_id: str, account_type: AccountType):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("accounts")

    response = table.get_item(Key=dict(account_id=account_id, account_type=account_type))

    logger.info("account retrieved")
    return response


def query_account(account_id: str):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("accounts")

    response = table.query(
        KeyConditionExpression="account_id = :account_id",
        ExpressionAttributeValues={":account_id": account_id},
    )

    logger.info("accounts retrieved")
    return response


def update_account_balance(account_id: str, account_type: AccountType, new_balance: int | Decimal):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("accounts")

    # Note: it is possible to use arg item and pass in the whole account object
    response = table.update_item(
        Key=dict(account_id=account_id, account_type=account_type),
        UpdateExpression="set balance = :balance",
        ExpressionAttributeValues={":balance": new_balance},
        ReturnValues="UPDATED_NEW",
    )

    logger.info("account updated")
    return response
