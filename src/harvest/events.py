from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from json import JSONEncoder
import json
from re import sub
from typing import Dict, List


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
class SetPrice:
    symbol: str
    date: date
    price: Decimal


@dataclass
class Allocation:
    equities: Decimal
    bonds: Decimal
    cash: Decimal
    other: Decimal = None

    def __post_init__(self):
        self.other = 100 - (self.equities + self.bonds + self.cash)

    def subtotals(self, total: Decimal) -> Dict[str, Decimal]:
        subtotals = {}
        subtotals["Equities"] = (self.equities / 100) * total
        subtotals["Bonds"] = (self.bonds / 100) * total
        subtotals["Cash"] = (self.cash / 100) * total
        subtotals["Other"] = (self.other / 100) * total

        return subtotals


@dataclass(frozen=True)
class SetAllocation:
    symbol: str
    date: date
    allocation: Allocation


@dataclass(frozen=True)
class RunReport:
    date: date
    account: str | None = None


@dataclass(frozen=True)
class FileWritten:
    path: str


# from: https://stackoverflow.com/questions/16258553/how-can-i-define-algebraic-data-types-in-python
Event = UnknownEvent | SetBalance | SetPrice | SetAllocation | RunReport | FileWritten


def event_matcher(date: date, account: str | None):
    def matcher(event: Event) -> bool:
        match event:
            case SetBalance(account, _, date, _):
                matches = event.date <= date
                if matches and account is not None:
                    matches = event.account == account
                return matches
            case SetPrice(_, date, _):
                return event.date <= date
            case SetAllocation(_, date, _):
                return event.date <= date
            case _:
                return False

    return matcher


class EventEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Event):
            return obj.__dict__
        elif isinstance(obj, Allocation):
            return obj.__dict__
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return obj.to_eng_string()

        return JSONEncoder.default(self, obj)


def parse_event_json(data: str) -> Event:
    evt = json.loads(data)
    dte = date.fromisoformat(evt["date"])

    if evt["type"] == "SetBalance":
        return parse_event(
            "set_balance", dte, evt["account"], evt["symbol"], evt["amount"]
        )
    elif evt["type"] == "SetPrice":
        return parse_event("set_price", dte, evt["symbol"], evt["price"])
    elif evt["type"] == "SetAllocation":
        return parse_event(
            "set_allocation",
            dte,
            evt["symbol"],
            evt["allocation"]["equities"],
            evt["allocation"]["bonds"],
            evt["allocation"]["cash"],
        )
    else:
        return UnknownEvent(event=data)


def parse_event(evt: str, date: date, *rest: List[str]) -> Event:
    event = UnknownEvent(event=str)

    if evt == "set_balance" and len(rest) > 2:
        event = SetBalance(
            account=rest[0],
            symbol=rest[1],
            date=date,
            amount=Decimal(rest[2]),
        )
    elif evt == "set_price" and len(rest) > 1:
        event = SetPrice(symbol=rest[0], date=date, price=Decimal(rest[1]))
    elif evt == "set_allocation" and len(rest) > 3:
        event = SetAllocation(
            symbol=rest[0],
            date=date,
            allocation=Allocation(
                equities=Decimal(rest[1]),
                bonds=Decimal(rest[2]),
                cash=Decimal(rest[3]),
                other=None,
            ),
        )
    elif evt == "run_report":
        kwargs = {"date": date}
        if len(rest) > 0:
            kwargs["account"] = rest[0]
        event = RunReport(**kwargs)

    return event
