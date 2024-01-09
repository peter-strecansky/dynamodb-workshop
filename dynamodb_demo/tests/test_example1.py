import uuid

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal

from dynamodb_demo.example_1 import (
    add_account,
    create_accounts_table,
    get_account,
    query_account,
    update_account_balance,
)
from dynamodb_demo.models import Account


def test_create_table():
    table = create_accounts_table()

    assert table.name == "accounts"
    assert table.table_status == "ACTIVE"


def test_add_account(set_up_accounts_table):
    account = Account(account_type="checking", balance=500)
    res = add_account(account)

    assert res["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_add_account_fail_already_exists(set_up_accounts_table):
    account = Account(account_type="checking", balance=500)
    add_account(account)

    with pytest.raises(ClientError) as exc_info:
        add_account(account)

    assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_get_account(set_up_accounts_table):
    account = Account(account_type="checking", balance=500)
    add_account(account)

    res = get_account(str(account.account_id), account.account_type)
    assert Account.parse_obj(res["Item"]) == account


def test_query_account(set_up_accounts_table):
    account_id = uuid.uuid4()
    account = Account(account_id=account_id, account_type="checking", balance=500)
    account2 = Account(account_id=account_id, account_type="savings", balance=1000)
    add_account(account)
    add_account(account2)

    res = query_account(str(account_id))
    assert res["Count"] == 2
    accounts = [Account.parse_obj(item) for item in res["Items"]]
    assert accounts == [account, account2]


@pytest.mark.parametrize("new_balance", [1000, Decimal("451.77")])
def test_update_account_balance(set_up_accounts_table, new_balance):
    account = Account(account_type="checking", balance=500)
    add_account(account)

    res = update_account_balance(str(account.account_id), account.account_type, new_balance)
    assert res["Attributes"]["balance"] == new_balance
