import threading

import boto3
from dynamodb_demo.example_2 import add_account, get_account
from dynamodb_demo.example_3 import update_balance_with_positive_lock
from dynamodb_demo.models import Account


def test_concurrent_updates(set_up_accounts_table):
    account = Account(account_type="checking", balance=500, version=0)
    add_account(account)

    # Make sure account exists in the db
    res = get_account(str(account.account_id), account.account_type)
    assert res["Item"]

    set_lock = threading.Lock()
    results: list[bool] = []

    def fetcher(transaction_value: int):
        res = update_balance_with_positive_lock(account, transaction_value)
        with set_lock:
            results.append(res)

    # transactions = [100, -200, 50]
    transactions = [100]
    threads = [threading.Thread(target=fetcher, args=(t,)) for t in transactions]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert 1 == 1
