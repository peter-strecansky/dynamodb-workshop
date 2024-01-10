# Positive locking example

# Basic dynamoDB operations using boto3.resource
import logging
import threading
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import And, Attr
from botocore.exceptions import ClientError
from time import sleep

from .models import Account
from .example_1 import add_account
from datetime import datetime

logger = logging.getLogger("dynamo_db_example_3")


def update_balance_with_positive_lock(account: Account, transaction_value: int | Decimal):
    """
    Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
    """
    # TODO: adjust this
    now = datetime.now()
    dynamodb = boto3.client("dynamodb")
    ddb = boto3.resource("dynamodb")
    table = ddb.Table("accounts")

    current_version = account.version
    new_balance = account.balance + transaction_value
    new_version = current_version + 1

    # acc = table.get_item(Key={"account_id": str(account.account_id), "account_type": account.account_type})["Item"]

    sleep(0.5)  # Simulate a long running transaction
    print(
        f"updating account balance: value: {transaction_value}, current_version:{current_version}, time:{now}, new_version:{new_version}"
    )

    try:
        res = dynamodb.update_item(
        TableName="accounts",
        Key={
            "account_id": {"S": str(account.account_id)},
            "account_type": {"S": str(account.account_type)},
        },
        ConditionExpression="#version = :current_version",
        UpdateExpression="SET #balance = :new_balance, #version = :new_version",
        ExpressionAttributeNames={
            "#balance": "balance",
            "#version": "version",
        },
        ExpressionAttributeValues={
            ":new_balance": {"N": str(new_balance)},
            ":new_version": {"N": str(new_version)},
            ":current_version": {"N": str(current_version)},
        },
    )
        return True
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Somebody changed the item in the db while we were changing it!
            return False
        else:
            raise err
