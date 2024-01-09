import uuid
from enum import Enum

from pydantic import BaseModel, Field


class AccountType(str, Enum):
    SAVINGS = "savings"
    CHECKING = "checking"
    CURRENT = "current"


class Account(BaseModel):
    account_id: uuid.UUID = Field(uuid.uuid4())
    account_type: AccountType
    balance: int
    overdraft_limit: int = Field(default=-500, lt=0)
    # Optimistic locking field
    # version: int
