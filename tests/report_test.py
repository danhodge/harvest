from datetime import date, datetime, timezone
from decimal import Decimal
import pytest
from harvest.events import (
    RunReport,
    SetAllocation,
    SetBalance,
    SetPrice,
    Allocation,
    Asset,
    SetTargetAllocation,
)
from harvest.report import Report


# https://docs.pytest.org/en/7.1.x/explanation/goodpractices.html#test-discovery
# https://blog.ionelmc.ro/2014/05/25/python-packaging/#the-structure


def test_compute_report():
    xyz = Asset.for_symbol("XYZ")
    acct = "account1"
    allocation = Allocation(
        stock_large=Decimal("70.5"),
        stock_mid_small=Decimal("6.5"),
        stock_intl=Decimal("0.1"),
        bond_us=Decimal("7.71"),
        bond_intl=Decimal("1.2"),
        cash=Decimal("3.45"),
    )
    target_allocation = Allocation(
        stock_large=Decimal("25.5"),
        stock_mid_small=Decimal("20.2"),
        stock_intl=Decimal("12.4"),
        bond_us=Decimal("5.5"),
        bond_intl=Decimal("1.23"),
        cash=Decimal("8.33"),
    )
    cash_allocation = Allocation(
        stock_large=Decimal("0"),
        stock_mid_small=Decimal("0"),
        stock_intl=Decimal("0"),
        bond_us=Decimal("0"),
        bond_intl=Decimal("0"),
        cash=Decimal("100"),
    )

    events = [
        SetPrice(
            Asset.cash(),
            date.fromisoformat("2022-05-26"),
            Decimal("1.0"),
            datetime.now(timezone.utc).isoformat(),
        ),
        SetBalance(
            acct,
            xyz,
            date.fromisoformat("2022-05-01"),
            Decimal("567.89"),
            datetime.now(timezone.utc).isoformat(),
        ),
        SetAllocation(
            xyz,
            date.fromisoformat("2022-05-20"),
            allocation,
            datetime.now(timezone.utc).isoformat(),
        ),
        SetBalance(
            acct,
            xyz,
            date.fromisoformat("2022-05-20"),
            Decimal("123.45"),
            datetime.now(timezone.utc).isoformat(),
        ),
        SetPrice(
            xyz,
            date.fromisoformat("2022-05-21"),
            Decimal("23.45"),
            datetime.now(timezone.utc).isoformat(),
        ),
        SetAllocation(
            Asset.cash(),
            date.fromisoformat("2022-01-01"),
            cash_allocation,
            datetime.now(timezone.utc).isoformat(),
        ),
        SetBalance(
            acct,
            xyz,
            date.fromisoformat("2022-05-28"),
            Decimal("234.56"),
            datetime.now(timezone.utc).isoformat(),
        ),
        SetPrice(
            xyz,
            date.fromisoformat("2022-05-25"),
            Decimal("34.56"),
            datetime.now(timezone.utc).isoformat(),
        ),
        SetBalance(
            acct,
            Asset.cash(),
            date.fromisoformat("2022-05-19"),
            Decimal("123.45"),
            datetime.now(timezone.utc).isoformat(),
        ),
        SetTargetAllocation(
            date.fromisoformat("2022-01-01"),
            target_allocation,
            datetime.now(timezone.utc).isoformat(),
        ),
    ]

    report = Report.create(RunReport(date.fromisoformat("2022-05-27")), events)
    result = report.compute()

    assert len(result) == 7

    row1 = result[1]
    assert row1[0] == acct
    assert row1[1] == xyz.identifier
    assert row1[2] == Decimal("123.45")
    assert row1[3] == Decimal("34.56")
    assert row1[4] == "2022-05-25"

    total = row1[2] * row1[3]
    tolerance = Decimal("0.0001")
    assert row1[5] == pytest.approx((total * allocation.stock) / 100, rel=tolerance)
    assert row1[9] == pytest.approx((total * allocation.bond) / 100, rel=tolerance)
    assert row1[12] == pytest.approx((total * allocation.cash) / 100, rel=tolerance)
    assert row1[13] == pytest.approx((total * allocation.other) / 100, rel=tolerance)
    assert row1[14] == total

    row2 = result[2]
    assert row2[0] == acct
    assert row2[1] == Asset.cash().identifier
    assert row2[2] == Decimal("123.45")
    assert row2[3] == Decimal("1.0")
    assert row2[4] == "2022-05-26"

    total = row2[2] * row2[3]
    assert row2[5] == 0
    assert row2[9] == 0
    assert row2[12] == total
    assert row2[13] == 0
    assert row2[14] == total

    # totals
    row3 = result[3]
    assert row3[0] == ""
    assert row3[1] == ""
    assert row3[2] == ""
    assert row3[3] == ""
    assert row3[4] == ""
    assert row3[5] == row1[5] + row2[5]
    assert row3[6] == row1[6]
    assert row3[7] == row1[7]
    assert row3[8] == row1[8]
    assert row3[9] == row1[9] + row2[9]
    assert row3[10] == row1[10]
    assert row3[11] == row1[11]
    assert row3[12] == row1[12] + row2[12]
    assert row3[13] == row1[13]
    assert row3[14] == row1[14] + row2[14]

    # target allocation percentages
    row5 = result[5]
    assert row5[0] == ""
    assert row5[1] == ""
    assert row5[2] == ""
    assert row5[3] == ""
    assert row5[4] == ""
    assert row5[5] == row5[6] + row5[7] + row5[8]
    assert row5[6] == target_allocation.stock_large
    assert row5[7] == target_allocation.stock_mid_small
    assert row5[8] == target_allocation.stock_intl
    assert row5[9] == row5[10] + row5[11]
    assert row5[10] == target_allocation.bond_us
    assert row5[11] == target_allocation.bond_intl
    assert row5[12] == target_allocation.cash
    assert row5[13] == target_allocation.other
    assert row5[14] == ""

    # target allocation corrections
    row6 = result[6]
    assert row6[0] == ""
    assert row6[1] == ""
    assert row6[2] == ""
    assert row6[3] == ""
    assert row6[4] == ""
    assert row6[5] == "-738.82"
