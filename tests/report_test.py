from datetime import date
from decimal import Decimal
import pytest
from harvest.events import (
    RunReport,
    SetAllocation,
    SetBalance,
    SetPrice,
    Allocation,
    Asset,
)
from harvest.report import Report


# https://docs.pytest.org/en/7.1.x/explanation/goodpractices.html#test-discovery
# https://blog.ionelmc.ro/2014/05/25/python-packaging/#the-structure

# Requirements stuff, requirements-test.txt?
# https://towardsdatascience.com/requirements-vs-setuptools-python-ae3ee66e28af
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
    cash_allocation = Allocation(
        stock_large=Decimal("0"),
        stock_mid_small=Decimal("0"),
        stock_intl=Decimal("0"),
        bond_us=Decimal("0"),
        bond_intl=Decimal("0"),
        cash=Decimal("100"),
    )

    events = [
        SetPrice(Asset.cash(), date.fromisoformat("2022-05-26"), Decimal("1.0")),
        SetBalance(
            acct,
            xyz,
            date.fromisoformat("2022-05-01"),
            Decimal("567.89"),
        ),
        SetAllocation(xyz, date.fromisoformat("2022-05-20"), allocation),
        SetBalance(
            acct,
            xyz,
            date.fromisoformat("2022-05-20"),
            Decimal("123.45"),
        ),
        SetPrice(xyz, date.fromisoformat("2022-05-21"), Decimal("23.45")),
        SetAllocation(Asset.cash(), date.fromisoformat("2022-01-01"), cash_allocation),
        SetBalance(
            acct,
            xyz,
            date.fromisoformat("2022-05-28"),
            Decimal("234.56"),
        ),
        SetPrice(xyz, date.fromisoformat("2022-05-25"), Decimal("34.56")),
        SetBalance(
            acct, Asset.cash(), date.fromisoformat("2022-05-19"), Decimal("123.45")
        ),
    ]

    report = Report.create(RunReport(date.fromisoformat("2022-05-27")), events)
    result = report.compute()

    assert len(result) == 5

    row1 = result[1]
    assert row1[0] == acct
    assert row1[1] == xyz
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
    assert row2[1] == Asset.cash()
    assert row2[2] == Decimal("123.45")
    assert row2[3] == Decimal("1.0")
    assert row2[4] == "2022-05-26"

    total = row2[2] * row2[3]
    tolerance = Decimal("0.0001")
    assert row1[5] == 0
    assert row1[9] == 0
    assert row1[12] == total
    assert row1[13] == 0
    assert row1[14] == total

    row3 = result[3]
    assert row3[0] == ""
    assert row3[1] == ""
    assert row3[2] == ""
    assert row3[3] == ""
    assert row3[4] == ""
    assert row3[5] == row1[5]
    assert row3[6] == row1[6]
    assert row3[7] == row1[7]
    assert row3[8] == row1[8]
    assert row3[9] == row1[9]
    assert row3[10] == row1[10]
    assert row3[11] == row1[11]
    assert row3[12] == row1[12]
    assert row3[13] == row1[13]
    assert row3[14] == row1[14]
