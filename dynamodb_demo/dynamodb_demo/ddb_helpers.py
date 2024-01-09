import logging
import typing
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError
from pydantic import BaseModel

from .models import Account

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as DDBClient
    from mypy_boto3_dynamodb.type_defs import GetItemOutputTypeDef, ItemResponseTypeDef

logger = logging.getLogger("dynamo")


def _format_get_item_response(
    model: BaseModel, response: "GetItemOutputTypeDef | ItemResponseTypeDef"
) -> Account | None:
    item = response.get("Item")
    if not item:
        return None

    formatted_item = {}
    for key, value in item.items():
        formatted_item[key] = list(value.values())[0]

    return Account.parse_obj(formatted_item)


def get_item_from_table(*, client: "DDBClient", table_name: str, keys: dict):
    """
    Gets an item from the table.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/get_item.html
    """
    try:
        response = client.get_item(
            TableName=table_name,
            Key=keys,
        )
    except ClientError as err:
        logger.error(
            "Couldn't get item from table",
            extra=dict(
                table=table_name,
                err_code=err.response["Error"]["Code"],
                err_msg=err.response["Error"]["Message"],
            ),
        )
        raise
    else:
        return response


def get_transact_item(*, client: "DDBClient", table_name: str, keys: dict):
    try:
        response = client.transact_get_items(
            TransactItems=[
                {
                    "Get": {
                        "TableName": table_name,
                        "Key": keys,
                    }
                },
            ]
        )
    except ClientError as err:
        logger.error(
            "Couldn't get item from table",
            extra=dict(
                table=table_name,
                err_code=err.response["Error"]["Code"],
                err_msg=err.response["Error"]["Message"],
            ),
        )
        raise
    else:
        return response


def transact_write(
    client, *, table_name: str, keys: dict, new_rate_limit_tokens: int, now: datetime
):
    client.transact_write_items(
        TransactItems=[
            {
                "Update": {
                    "TableName": table_name,
                    "Key": keys,
                    "UpdateExpression": "SET #rate_limit_tokens = :new_rate_limit_tokens, #last_token_refill_time = :now",
                    "ExpressionAttributeNames": {
                        "#rate_limit_tokens": "rate_limit_tokens",
                        "#last_token_refill_time": "last_token_refill_time",
                    },
                    "ExpressionAttributeValues": {
                        ":new_rate_limit_tokens": {"N": str(new_rate_limit_tokens)},
                        ":now": {"S": str(int(now.timestamp()))},
                    },
                }
            },
        ]
    )


def acquire_lock(
    *, client: "DDBClient", table: str, keys: dict, timeout_in_seconds: int, request_id: str
) -> bool:
    ex = client.exceptions
    now = str(int((datetime.now(tz=timezone.utc).timestamp())))
    new_timeout = str(
        int((datetime.now(tz=timezone.utc) + timedelta(seconds=timeout_in_seconds)).timestamp())
    )

    try:
        client.update_item(
            TableName=table,
            Key=keys,
            UpdateExpression="SET #rq_id = :rq_id, #timeout = :timeout",
            ExpressionAttributeNames={
                "#rq_id": "request_id",
                "#timeout": "timeout",
            },
            ExpressionAttributeValues={
                ":rq_id": {"S": request_id},
                ":timeout": {"N": new_timeout},
                ":now": {"N": now},
            },
            ConditionExpression="attribute_not_exists(timeout) OR timeout < :now",
        )
        return True

    except ex.ConditionalCheckFailedException:
        # It's already locked
        return False


def release_lock(*, client: "DDBClient", table: str, keys: dict, request_id: str) -> bool:
    ex = client.exceptions
    try:
        client.update_item(
            TableName=table,
            Key=keys,
            UpdateExpression="REMOVE #rq_id, #timeout",
            ExpressionAttributeNames={
                "#rq_id": "request_id",
                "#timeout": "timeout",
            },
            ExpressionAttributeValues={
                ":rq_id": {"S": request_id},
            },
            ConditionExpression="request_id = :rq_id",
        )
        return True

    except (ex.ConditionalCheckFailedException, ex.ResourceNotFoundException):
        return False
