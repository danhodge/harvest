import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import reduce
from typing import Dict, List, NamedTuple, Set
from harvest.events import (
    Allocation,
    Asset,
    Event,
    Money,
    RunReport,
    SetAllocation,
    SetBalance,
    SetPrice,
    SetTargetAllocation,
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
        case SetBalance(_, _, date, _, _):
            return [0, date]
        case SetPrice(_, date, _, _):
            return [1, date]
        case SetAllocation(_, date, _, _):
            return [2, date]
        case SetTargetAllocation(date, _, _):
            return [3, date]
        case _:
            return [3]


@dataclass
class ReportRecord:
    account: str
    asset: Asset
    date: date
    amount: Decimal
    price: Decimal
    allocation: Allocation

    def is_incomplete(self) -> bool:
        return (
            self.account is None
            or self.asset is None
            or self.date is None
            or self.amount is None
            or self.price is None
            or self.allocation
            is None  # note: cash failing because allocation is missing
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
        target_allocation = None

        def get_all(asset):
            for key in (k for k in records.keys() if k[1] == asset):
                yield records[key]

        for evt in events:
            match evt:
                case SetBalance(account, asset, date, amount):
                    record = records.get((account, asset))
                    if not record:
                        record = ReportRecord(
                            account=account,
                            asset=asset,
                            date=date,
                            amount=amount,
                            price=None,
                            allocation=None,
                        )
                        records[(account, asset)] = record
                    else:
                        new_record = ReportRecord(
                            account=account,
                            asset=asset,
                            date=date,
                            amount=amount,
                            price=None,
                            allocation=record.allocation,
                        )
                        records[(account, asset)] = new_record
                case SetPrice(asset, date, price):
                    for rec in get_all(asset):
                        rec.date = date
                        rec.price = price
                case SetAllocation(asset, date, allocation):
                    for rec in get_all(asset):
                        rec.allocation = allocation
                case SetTargetAllocation(date, allocation):
                    target_allocation = allocation

        incomplete, complete = partition(
            records.values(), lambda rec: rec.is_incomplete()
        )
        print(f"incomplete={incomplete}")

        return cls(
            records=sorted(
                complete,
                key=lambda rec: (rec.account, rec.asset.identifier),
            ),
            incomplete_assets={rec.asset for rec in incomplete},
            target_allocation=target_allocation,
        )

    def __init__(
        self,
        records: List[ReportRecord],
        incomplete_assets: Set[Asset],
        target_allocation: Allocation = None,
    ):
        self.records = records
        self.incomplete_assets = incomplete_assets
        self.target_allocation = target_allocation

    def to_row(self, record: ReportRecord) -> List:
        subtotals = [v for k, v in record.subtotals().items()]
        return (
            [
                record.account,
                record.asset.identifier,
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

        if self.target_allocation:
            rows.append(
                [""] * 5
                + [
                    self.target_allocation.stock,
                    self.target_allocation.stock_large,
                    self.target_allocation.stock_mid_small,
                    self.target_allocation.stock_intl,
                    self.target_allocation.bond,
                    self.target_allocation.bond_us,
                    self.target_allocation.bond_intl,
                    self.target_allocation.cash,
                    self.target_allocation.other,
                ]
                + [""]
            )

            corrections = map(
                lambda t: (totals[-1] * ((t[1] - t[0]) / 100)),
                zip(percentages, rows[-1][5:14]),
            )
            rows.append([""] * 5 + list(corrections) + [""])

        return rows

    def write_to_file(self) -> str:
        filename = "harvest.csv"
        with open(filename, "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=",")
            for row in self.compute():
                writer.writerow(row)

        return filename
