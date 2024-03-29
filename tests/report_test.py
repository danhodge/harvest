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
    assert row1[3] == "2022-05-20"
    assert row1[4] == Decimal("34.56")
    assert row1[5] == "2022-05-25"

    total = row1[2] * row1[4]
    tolerance = Decimal("0.0001")
    assert row1[6] == pytest.approx((total * allocation.stock) / 100, rel=tolerance)
    assert row1[10] == pytest.approx((total * allocation.bond) / 100, rel=tolerance)
    assert row1[13] == pytest.approx((total * allocation.cash) / 100, rel=tolerance)
    assert row1[14] == pytest.approx((total * allocation.other) / 100, rel=tolerance)
    assert row1[15] == total

    row2 = result[2]
    assert row2[0] == acct
    assert row2[1] == Asset.cash().identifier
    assert row2[2] == Decimal("123.45")
    assert row2[3] == "2022-05-19"
    assert row2[4] == Decimal("1.0")
    assert row2[5] == "2022-05-26"

    total = row2[2] * row2[4]
    assert row2[6] == 0
    assert row2[10] == 0
    assert row2[13] == total
    assert row2[14] == 0
    assert row2[15] == total

    # totals
    row3 = result[3]
    assert row3[0] == "Totals"
    assert row3[1] == ""
    assert row3[2] == ""
    assert row3[3] == ""
    assert row3[4] == ""
    assert row3[5] == ""
    assert row3[6] == row1[6] + row2[6]
    assert row3[7] == row1[7]
    assert row3[8] == row1[8]
    assert row3[9] == row1[9]
    assert row3[10] == row1[10] + row2[10]
    assert row3[11] == row1[11]
    assert row3[12] == row1[12]
    assert row3[13] == row1[13] + row2[13]
    assert row3[14] == row1[14]
    assert row3[15] == row1[15] + row2[15]

    # target allocation percentages
    row5 = result[5]
    assert row5[0] == "Target Percentages"
    assert row5[1] == ""
    assert row5[2] == ""
    assert row5[3] == ""
    assert row5[4] == ""
    assert row5[5] == ""
    assert row5[6] == row5[7] + row5[8] + row5[9]
    assert row5[7] == target_allocation.stock_large
    assert row5[8] == target_allocation.stock_mid_small
    assert row5[9] == target_allocation.stock_intl
    assert row5[10] == row5[11] + row5[12]
    assert row5[11] == target_allocation.bond_us
    assert row5[12] == target_allocation.bond_intl
    assert row5[13] == target_allocation.cash
    assert row5[14] == target_allocation.other
    assert row5[15] == ""

    # target allocation corrections
    row6 = result[6]
    assert row6[0] == "Corrections"
    assert row6[1] == ""
    assert row6[2] == ""
    assert row6[3] == ""
    assert row6[4] == ""
    assert row6[5] == ""
    assert row6[6] == pytest.approx(Decimal("-738.82"), rel=tolerance)
    assert row6[7] == pytest.approx(Decimal("-1888.53"), rel=tolerance)
    assert row6[8] == pytest.approx(Decimal("609.32"), rel=tolerance)
    assert row6[9] == pytest.approx(Decimal("539.96"), rel=tolerance)
    assert row6[10] == pytest.approx(Decimal("-84.72"), rel=tolerance)
    assert row6[11] == pytest.approx(Decimal("-87.36"), rel=tolerance)
    assert row6[12] == pytest.approx(Decimal("2.63"), rel=Decimal("0.01"))
    assert row6[13] == pytest.approx(Decimal("94.82"), rel=tolerance)
    assert row6[14] == pytest.approx(Decimal("728.72"), rel=tolerance)
    assert row6[15] == ""
