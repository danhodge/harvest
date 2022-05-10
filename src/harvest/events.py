from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from json import JSONEncoder
from typing import List, Optional


@dataclass(frozen=True)
class UnknownEvent:
    event: str


@dataclass(frozen=True)
class SetBalance:
    account: str
    symbol: str
    date: date
    amount: Decimal


@dataclass(frozen=True)
class RunReport:
    date: date
    account: Optional[str]


# from: https://stackoverflow.com/questions/16258553/how-can-i-define-algebraic-data-types-in-python
Event = UnknownEvent | SetBalance | RunReport


class EventEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Event):
            return obj.__dict__
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return obj.to_eng_string()

        return JSONEncoder.default(self, obj)


def parse_event(
    evt: str, account: str, symbol: str, date: date, *rest: List[str]
) -> Event:
    event = UnknownEvent(event=str)
    if evt == "set_balance" and len(rest) > 0:
        event = SetBalance(
            account=account,
            symbol=symbol,
            date=date,
            amount=Decimal(rest[0]),
        )

    return event
