import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import reduce
from typing import Dict, List, NamedTuple, Set
from harvest.events import (
    Allocation,
    Event,
    Money,
    RunReport,
    SetAllocation,
    SetBalance,
    SetPrice,
    event_matcher,
)


def partition(values, fn):
    included = []
    excluded = []
    for value in values:
        if fn(value):
            included.append(value)
        else:
            excluded.append(value)

    return (included, excluded)


def report_event_sort_key(event) -> List:
    match event:
        case SetBalance(_, _, date, _):
            return [0, date]
        case SetPrice(_, date, _):
            return [1, date]
        case SetAllocation(_, date, _):
            return [2, date]
        case _:
            return [3]


@dataclass
class ReportRecord:
    account: str
    symbol: str
    date: date
    amount: Decimal
    price: Decimal
    allocation: Allocation

    def is_incomplete(self) -> bool:
        return (
            self.account is None
            or self.symbol is None
            or self.date is None
            or self.amount is None
            or self.price is None
            or self.allocation is None
        )

    def subtotals(self) -> Dict[str, Money]:
        return self.allocation.subtotals(self.total())

    def total(self) -> Money:
        return Money(self.amount * self.price)


class Report:
    @classmethod
    def create(cls, report_event: RunReport, events: List[Event]):
        events = sorted(
            filter(event_matcher(report_event.date, report_event.account), events),
            key=report_event_sort_key,
        )
        records = {}

        def get_all(symbol):
            for key in (k for k in records.keys() if k[1] == symbol):
                yield records[key]

        for evt in events:
            match evt:
                case SetBalance(account, symbol, date, amount):
                    record = records.get((account, symbol))
                    if not record:
                        record = ReportRecord(
                            account=account,
                            symbol=symbol,
                            date=date,
                            amount=amount,
                            price=None,
                            allocation=None,
                        )
                        records[(account, symbol)] = record
                    else:
                        new_record = ReportRecord(
                            account=account,
                            symbol=symbol,
                            date=date,
                            amount=amount,
                            price=None,
                            allocation=record.allocation,
                        )
                        records[(account, symbol)] = new_record
                case SetPrice(symbol, date, price):
                    for rec in get_all(symbol):
                        rec.date = date
                        rec.price = price
                case SetAllocation(symbol, date, allocation):
                    for rec in get_all(symbol):
                        rec.allocation = allocation

        incomplete, complete = partition(
            records.values(), lambda rec: rec.is_incomplete()
        )

        return cls(
            records=sorted(
                complete,
                key=lambda rec: (rec.account, rec.symbol),
            ),
            incomplete_symbols={rec.symbol for rec in incomplete},
        )

    def __init__(self, records: List[ReportRecord], incomplete_symbols: Set[str]):
        self.records = records
        self.incomplete_symbols = incomplete_symbols

    def to_row(self, record: ReportRecord) -> List:
        subtotals = [v for k, v in record.subtotals().items()]
        return (
            [
                record.account,
                record.symbol,
                record.amount,
                record.price,
                str(record.date),
            ]
            + subtotals
            + [record.total()]
        )

    def compute(self) -> List[List]:
        if len(self.records) == 0:
            return []

        def reducer(memo: List[Decimal], val: List) -> List[Decimal]:
            return map(lambda arg: arg[1] + arg[0], zip(memo, val[5:]))

        rows = [
            [
                "Account",
                "Symbol",
                "Shares",
                "NAV",
                "As Of",
                "Stock",
                "Stock - Large",
                "Stock - Mid/Small",
                "Stock - Intl",
                "Bond",
                "Bond - US",
                "Bond - Intl",
                "Cash",
                "Other",
                "Total",
            ]
        ] + [self.to_row(record) for record in self.records]
        totals = list(reduce(reducer, rows[1:], [Decimal("0")] * 10))
        percentages = [round((sub / totals[-1]) * 100, 2) for sub in totals[:-1]]
        rows.append([""] * 5 + totals)
        rows.append([""] * 5 + list(percentages) + [""])

        return rows

    def write_to_file(self) -> str:
        filename = "harvest.csv"
        with open(filename, "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=",")
            for row in self.compute():
                writer.writerow(row)

        return filename
