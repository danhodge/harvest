from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from json import JSONEncoder
import json
import numbers
from re import sub
from typing import Dict, List, Set

from attr import attr


def each_slice(value: str, size: int = 3):
    i, j = -size, None
    slices = []

    while True:
        if j:
            slice = value[i:j]
        else:
            slice = value[i:]
        if slice:
            slices.append(slice)
            j = i
            i -= size
        else:
            break

    slices.reverse()
    for slice in slices:
        yield slice


class Money:
    def __init__(self, amount: Decimal):
        self.amount = amount

    def __repr__(self) -> str:
        rounded = str(round(self.amount, 2))
        dollars, cents = rounded.split(".")
        return ".".join([",".join(each_slice(dollars)), cents])

    def __add__(self, other):
        if isinstance(other, Money):
            return Money(self.amount + other.amount)
        elif isinstance(other, numbers.Number):
            return Money(self.amount + other)
        else:
            raise ValueError("Attempting to add {} to Money".format(type(other)))

    def __mul__(self, other):
        if isinstance(other, Money):
            return Money(self.amount * other.amount)
        elif isinstance(other, numbers.Number):
            return Money(self.amount * other)
        else:
            raise ValueError("Attempting to multiply {} and Money".format(type(other)))

    def __truediv__(self, other):
        if isinstance(other, Money):
            return self.amount / other.amount
        elif isinstance(other, numbers.Number):
            return self.amount / other
        else:
            raise ValueError("Attempting to divide Money by {}".format(type(other)))


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
    amount: Decimal


@dataclass
class Allocation:
    stock_large: Decimal
    stock_mid_small: Decimal
    stock_intl: Decimal
    bond_us: Decimal
    bond_intl: Decimal
    cash: Decimal
    other: Decimal = None

    @property
    def stock(self):
        return self.stock_large + self.stock_mid_small + self.stock_intl

    @property
    def bond(self):
        return self.bond_us + self.bond_intl

    def __post_init__(self):
        self.other = 100 - (self.stock + self.bond + self.cash)

    def subtotals(self, total: Money) -> Dict[str, Money]:
        subtotals = {}
        subtotals["Stock"] = total * (self.stock / 100)
        subtotals["Stock - Large"] = total * (self.stock_large / 100)
        subtotals["Stock - Mid/Small"] = total * (self.stock_mid_small / 100)
        subtotals["Stock - Intl"] = total * (self.stock_intl / 100)
        subtotals["Bond"] = total * (self.bond / 100)
        subtotals["Bond - US"] = total * (self.bond_us / 100)
        subtotals["Bond - Intl"] = total * (self.bond_intl / 100)
        subtotals["Cash"] = total * (self.cash / 100)
        subtotals["Other"] = total * (self.other / 100)

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
    incomplete_symbols: Set[str]


# from: https://stackoverflow.com/questions/16258553/how-can-i-define-algebraic-data-types-in-python
Event = UnknownEvent | SetBalance | SetPrice | SetAllocation | RunReport | FileWritten


def event_matcher(target_date: date, target_account: str | None):
    def matcher(event: Event) -> bool:
        match event:
            case SetBalance(account, _, date, _):
                matches = date <= target_date
                if matches and target_account is not None:
                    matches = account == target_account
                return matches
            case SetPrice(_, date, _):
                return date <= target_date
            case SetAllocation(_, date, _):
                return date <= target_date
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
        return parse_event("set_price", dte, evt["symbol"], evt["amount"])
    elif evt["type"] == "SetAllocation":
        return parse_event(
            "set_allocation",
            dte,
            evt["symbol"],
            evt["allocation"]["stock_large"],
            evt["allocation"]["stock_mid_small"],
            evt["allocation"]["stock_intl"],
            evt["allocation"]["bond_us"],
            evt["allocation"]["bond_intl"],
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
        event = SetPrice(symbol=rest[0], date=date, amount=Decimal(rest[1]))
    elif evt == "set_allocation" and len(rest) > 6:
        event = SetAllocation(
            symbol=rest[0],
            date=date,
            allocation=Allocation(
                stock_large=Decimal(rest[1]),
                stock_mid_small=Decimal(rest[2]),
                stock_intl=Decimal(rest[3]),
                bond_us=Decimal(rest[4]),
                bond_intl=Decimal(rest[5]),
                cash=Decimal(rest[6]),
                other=None,
            ),
        )
    elif evt == "run_report":
        kwargs = {"date": date}
        if len(rest) > 0:
            kwargs["account"] = rest[0]
        event = RunReport(**kwargs)

    return event
