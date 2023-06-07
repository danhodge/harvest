from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from json import JSONEncoder
import json
import numbers
from os import access
from re import sub
from sqlite3 import Time
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Set,
    Generator,
    Self,
    TypeVar,
    cast,
    SupportsFloat,
)

from attr import attr
from pyparsing import identbodychars


def each_slice(value: str, size: int = 3) -> Generator[str, None, None]:
    for i in range(0, len(value), size):
        yield value[i : i + size]


TMoney = TypeVar("TMoney", bound="Money")


class Money:
    def __init__(self, amount: Decimal):
        self.amount = amount

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Money):
            return self.amount == other.amount
        else:
            return self.amount == other

    def __repr__(self) -> str:
        sign = "" if abs(self.amount) == self.amount else "-"
        rounded = str(round(abs(self.amount), 2))
        dollars, cents = rounded.split(".")

        output = list(reversed(cents))
        output.append(".")
        for i, ch in enumerate(reversed(dollars)):
            if i != 0 and i % 3 == 0:
                output.append(",")
            output.append(ch)

        output.append(sign)

        return "".join(reversed(output))

    def __add__(self, other: TMoney | SupportsFloat) -> TMoney:
        if isinstance(other, Money):
            # cast() is a work-around for a mypy issue: https://github.com/python/mypy/issues/12800
            return cast(TMoney, Money(self.amount + other.amount))
        elif isinstance(other, SupportsFloat):
            return cast(TMoney, Money(self.amount + Decimal(float(other))))
        else:
            raise ValueError("Attempting to add {} to Money".format(type(other)))

    def __mul__(self, other: TMoney | SupportsFloat) -> TMoney:
        if isinstance(other, Money):
            return cast(TMoney, Money(self.amount * other.amount))
        elif isinstance(other, SupportsFloat):
            return cast(TMoney, Money(self.amount * Decimal(float(other))))
        else:
            raise ValueError("Attempting to multiply {} and Money".format(type(other)))

    def __truediv__(self, other: TMoney | SupportsFloat) -> Decimal:
        if isinstance(other, Money):
            return self.amount / other.amount
        elif isinstance(other, SupportsFloat):
            return self.amount / Decimal(float(other))
        else:
            raise ValueError("Attempting to divide Money by {}".format(type(other)))


AssetType = Literal["investment", "cash"]
TAsset = TypeVar("TAsset", bound="Asset")


@dataclass(eq=True, frozen=True)
class Asset:
    identifier: str
    type: AssetType

    @classmethod
    def for_symbol(cls: TAsset, symbol: str) -> TAsset:
        return cast(TAsset, Asset(identifier=symbol, type="investment"))

    @classmethod
    def cash(cls: TAsset, identifier: str = "cash") -> TAsset:
        return cast(TAsset, Asset(identifier=identifier, type="cash"))


@dataclass(frozen=True)
class UnknownEvent:
    event: str


@dataclass(frozen=True)
class SetBalance:
    account: str
    asset: Asset
    date: date
    amount: Decimal
    created_at: datetime


@dataclass(frozen=True)
class SetPrice:
    asset: Asset
    date: date
    amount: Decimal
    created_at: datetime


@dataclass
class Allocation:
    stock_large: Decimal
    stock_mid_small: Decimal
    stock_intl: Decimal
    bond_us: Decimal
    bond_intl: Decimal
    cash: Decimal
    other: Decimal = Decimal("0")

    @property
    def stock(self) -> Decimal:
        return self.stock_large + self.stock_mid_small + self.stock_intl

    @property
    def bond(self) -> Decimal:
        return self.bond_us + self.bond_intl

    def __post_init__(self) -> None:
        self.other = 100 - (self.stock + self.bond + self.cash)

    def subtotals(self, total: Money) -> Dict[str, Money]:
        subtotals: Dict[str, Money] = {}
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
class SetTargetAllocation:
    date: date
    allocation: Allocation
    created_at: datetime


@dataclass(frozen=True)
class SetAllocation:
    asset: Asset
    date: date
    allocation: Allocation
    created_at: datetime


@dataclass(frozen=True)
class RunReport:
    date: date
    account: str | None = None


@dataclass(frozen=True)
class FileWritten:
    path: str
    incomplete_symbols: Set[str]


# from: https://stackoverflow.com/questions/16258553/how-can-i-define-algebraic-data-types-in-python
Event = (
    UnknownEvent
    | SetBalance
    | SetPrice
    | SetAllocation
    | SetTargetAllocation
    | RunReport
    | FileWritten
)


def event_matcher(
    target_date: date, target_account: str | None
) -> Callable[[Event], bool]:
    def matcher(event: Event) -> bool:
        match event:
            case SetBalance(account, _, date, _, _):
                matches = date <= target_date
                if matches and target_account is not None:
                    matches = account == target_account
                return matches
            case SetPrice(_, date, _, _):
                return date <= target_date
            case SetAllocation(_, date, _, _):
                return date <= target_date
            case SetTargetAllocation(date, _, _):
                return date <= target_date
            case _:
                return False

    return matcher


class EventEncoder(JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Event):
            return obj.__dict__
        elif isinstance(obj, Allocation):
            return obj.__dict__
        elif isinstance(obj, Asset):
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
            "set_balance",
            dte,
            evt["account"],
            evt["asset"],
            evt["amount"],
            evt["created_at"],
        )
    elif evt["type"] == "SetPrice":
        return parse_event(
            "set_price", dte, evt["asset"], evt["amount"], evt["created_at"]
        )
    elif evt["type"] == "SetAllocation":
        return parse_event(
            "set_allocation",
            dte,
            evt["asset"],
            evt["allocation"]["stock_large"],
            evt["allocation"]["stock_mid_small"],
            evt["allocation"]["stock_intl"],
            evt["allocation"]["bond_us"],
            evt["allocation"]["bond_intl"],
            evt["allocation"]["cash"],
            evt["created_at"],
        )
    elif evt["type"] == "SetTargetAllocation":
        return parse_event(
            "set_target_allocation",
            dte,
            evt["allocation"]["stock_large"],
            evt["allocation"]["stock_mid_small"],
            evt["allocation"]["stock_intl"],
            evt["allocation"]["bond_us"],
            evt["allocation"]["bond_intl"],
            evt["allocation"]["cash"],
            evt["created_at"],
        )
    else:
        return UnknownEvent(event=data)


def parse_event(evt: str, date: date, *rest: Any) -> Event:
    event: Event = UnknownEvent(event=str)

    if evt == "set_balance" and len(rest) > 3:
        event = SetBalance(
            account=rest[0],
            asset=parse_asset(rest[1]),
            date=date,
            amount=Decimal(rest[2]),
            created_at=datetime.fromisoformat(rest[3]),
        )
    elif evt == "set_price" and len(rest) > 2:
        event = SetPrice(
            asset=parse_asset(rest[0]),
            date=date,
            amount=Decimal(rest[1]),
            created_at=datetime.fromisoformat(rest[2]),
        )
    elif evt == "set_allocation" and len(rest) > 7:
        event = SetAllocation(
            asset=parse_asset(rest[0]),
            date=date,
            allocation=Allocation(
                stock_large=Decimal(rest[1]),
                stock_mid_small=Decimal(rest[2]),
                stock_intl=Decimal(rest[3]),
                bond_us=Decimal(rest[4]),
                bond_intl=Decimal(rest[5]),
                cash=Decimal(rest[6]),
            ),
            created_at=datetime.fromisoformat(rest[7]),
        )
    elif evt == "set_target_allocation" and len(rest) > 6:
        event = SetTargetAllocation(
            date=date,
            allocation=Allocation(
                stock_large=Decimal(rest[0]),
                stock_mid_small=Decimal(rest[1]),
                stock_intl=Decimal(rest[2]),
                bond_us=Decimal(rest[3]),
                bond_intl=Decimal(rest[4]),
                cash=Decimal(rest[5]),
            ),
            created_at=datetime.fromisoformat(rest[6]),
        )
    elif evt == "run_report":
        kwargs = {"date": date}
        if len(rest) > 0:
            kwargs["account"] = rest[0]
        event = RunReport(**kwargs)

    return event


def parse_asset(asset: Dict[str, Any]) -> Asset:
    return Asset(identifier=asset["identifier"], type=asset["type"])
