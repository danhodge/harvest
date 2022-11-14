import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import reduce
from gc import is_finalized
from typing import (
    Dict,
    List,
    NamedTuple,
    Set,
    Callable,
    Tuple,
    Sequence,
    TypeVar,
    Iterable,
    Generator,
)
import logging
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

logger = logging.getLogger(__name__)

T = TypeVar("T")


def partition(
    values: Iterable[T], fn: Callable[[T], bool]
) -> Tuple[Sequence[T], Sequence[T]]:
    included = []
    excluded = []
    for value in values:
        if fn(value):
            included.append(value)
        else:
            excluded.append(value)

    return (included, excluded)


def report_event_sort_key(event: Event) -> List:
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
            return [4]


@dataclass
class ReportRecord:
    account: str
    asset: Asset
    report_date: date
    amount: Decimal
    price: Decimal
    price_date: date
    allocation: Allocation

    def is_incomplete(self) -> bool:
        return (
            self.account is None
            or self.asset is None
            or self.report_date is None
            or self.amount is None
            or self.price is None
            or self.price_date is None
            or self.allocation is None
        )

    def subtotals(self) -> Dict[str, Money]:
        return self.allocation.subtotals(self.total())

    def total(self) -> Money:
        return Money(self.amount * self.price)


@dataclass(frozen=False)
class ReportRecordEvents:
    balance_event: SetBalance
    price_event: SetPrice | None = None
    allocation_event: SetAllocation | None = None

    @property
    def asset(self):
        self.balance_event.asset

    def to_report_record(self) -> ReportRecord | None:
        if (
            self.balance_event is None
            or self.price_event is None
            or self.allocation_event is None
        ):
            return None

        return ReportRecord(
            account=self.balance_event.account,
            asset=self.balance_event.asset,
            report_date=self.balance_event.date,
            amount=self.balance_event.amount,
            price=self.price_event.amount,
            price_date=self.price_event.date,
            allocation=self.allocation_event.allocation,
        )


class Report:
    @classmethod
    def create(cls, report_event: RunReport, events: List[Event]):
        events = sorted(
            filter(event_matcher(report_event.date, report_event.account), events),
            key=report_event_sort_key,
        )
        records: Dict[Tuple[str, Asset], ReportRecordEvents] = {}
        target_allocation = None

        def get_all(asset: Asset) -> Generator[ReportRecordEvents, None, None]:
            for key in (k for k in records.keys() if k[1] == asset):
                yield records[key]

        for evt in events:
            match evt:
                case SetBalance(account, asset, date, amount) as e:
                    record = records.get((account, asset))
                    if not record:
                        record = ReportRecordEvents(balance_event=e)
                        records[(account, asset)] = record
                    else:
                        record.balance_event = e
                case SetPrice(asset, date, price) as e:
                    for rec in get_all(asset):
                        rec.price_event = e
                case SetAllocation(asset, date, allocation) as e:
                    for rec in get_all(asset):
                        rec.allocation_event = e
                case SetTargetAllocation(date, allocation):
                    target_allocation = allocation

        report_records = [(rec, rec.to_report_record()) for rec in records.values()]
        complete, incomplete = partition(report_records, lambda rec: rec[1] is not None)
        print(f"incomplete={incomplete}")

        return cls(
            records=sorted(
                [c[1] for c in complete if c[1]],
                key=lambda rec: (rec.account, rec.asset.identifier),
            ),
            incomplete_assets={rec.asset for rec in [i[0] for i in incomplete]},
            target_allocation=target_allocation,
        )

    def __init__(
        self,
        records: List[ReportRecord],
        incomplete_assets: Set[Asset],
        target_allocation: Allocation | None = None,
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
                str(record.report_date),
                record.price,
                str(record.price_date),
            ]
            + subtotals
            + [record.total()]
        )

    def compute(self) -> List[List]:
        if len(self.records) == 0:
            return []

        prefix_cols = 6

        def reducer(memo: List[Decimal], val: List) -> List[Decimal]:
            return map(lambda arg: arg[1] + arg[0], zip(memo, val[prefix_cols:]))

        rows = [
            [
                "Account",
                "Symbol",
                "Shares",
                "As Of",
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
        rows.append(["Totals"] + ([""] * (prefix_cols - 1)) + totals)
        rows.append(
            ["Percentages"] + ([""] * (prefix_cols - 1)) + list(percentages) + [""]
        )

        if self.target_allocation:
            rows.append(
                ["Target Percentages"]
                + ([""] * (prefix_cols - 1))
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
                zip(percentages, rows[-1][prefix_cols:15]),
            )
            rows.append(
                ["Corrections"] + ([""] * (prefix_cols - 1)) + list(corrections) + [""]
            )

        return rows

    def write_to_file(self) -> str:
        filename = "harvest.csv"
        with open(filename, "w") as csv_file:
            writer = csv.writer(csv_file, delimiter=",")
            for row in self.compute():
                writer.writerow(row)

        return filename
