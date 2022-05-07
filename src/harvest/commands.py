from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List


@dataclass(frozen=True)
class SetBalance:
    account: str
    symbol: str
    date: date
    amount: Decimal


# from: https://stackoverflow.com/questions/16258553/how-can-i-define-algebraic-data-types-in-python
Command = SetBalance


def parse_command(
    command: str, account: str, symbol: str, date: date, *rest: List[str]
) -> Command:
    cmd = None
    if command == "set_balance" and len(rest) > 0:
        cmd = SetBalance(
            account=account,
            symbol=symbol,
            date=date,
            amount=Decimal(rest[0]),
        )

    return cmd
