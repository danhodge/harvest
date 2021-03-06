from datetime import date
from decimal import Decimal
import pytest
from harvest.events import RunReport, SetAllocation, SetBalance, SetPrice, Allocation
from harvest.report import Report


# https://docs.pytest.org/en/7.1.x/explanation/goodpractices.html#test-discovery
# https://blog.ionelmc.ro/2014/05/25/python-packaging/#the-structure

# Requirements stuff, requirements-test.txt?
# https://towardsdatascience.com/requirements-vs-setuptools-python-ae3ee66e28af
def test_compute_report():
    sym = "XYZ"
    acct = "account1"
    allocation = Allocation(
        stock_large=Decimal("70.5"),
        stock_mid_small=Decimal("6.5"),
        stock_intl=Decimal("0.1"),
        bond_us=Decimal("7.71"),
        bond_intl=Decimal("1.2"),
        cash=Decimal("3.45"),
    )

    events = [
        SetBalance(acct, sym, date.fromisoformat("2022-05-01"), Decimal("567.89")),
        SetAllocation(sym, date.fromisoformat("2022-05-20"), allocation),
        SetBalance(acct, sym, date.fromisoformat("2022-05-20"), Decimal("123.45")),
        SetPrice(sym, date.fromisoformat("2022-05-21"), Decimal("23.45")),
        SetBalance(acct, sym, date.fromisoformat("2022-05-28"), Decimal("234.56")),
        SetPrice(sym, date.fromisoformat("2022-05-25"), Decimal("34.56")),
    ]

    report = Report.create(RunReport(date.fromisoformat("2022-05-27")), events)
    result = report.compute()

    assert len(result) == 4

    row1 = result[1]
    assert row1[0] == acct
    assert row1[1] == sym
    assert row1[2] == Decimal("123.45")
    assert row1[3] == Decimal("34.56")
    assert row1[4] == "2022-05-25"

    total = row1[2] * row1[3]
    tolerance = Decimal("0.0001")
    assert row1[5] == pytest.approx((total * allocation.stock) / 100, rel=tolerance)
    assert row1[9] == pytest.approx((total * allocation.bond) / 100, rel=tolerance)
    assert row1[12] == pytest.approx((total * allocation.cash) / 100, rel=tolerance)
    assert row1[13] == pytest.approx((total * allocation.other) / 100, rel=tolerance)
    assert row1[14] == round(total, 2)

    row2 = result[2]
    assert row2[0] == ""
    assert row2[1] == ""
    assert row2[2] == ""
    assert row2[3] == ""
    assert row2[4] == ""
    assert row2[5] == row1[5]
    assert row2[6] == row1[6]
    assert row2[7] == row1[7]
    assert row2[8] == row1[8]
    assert row2[9] == row1[9]
    assert row2[10] == row1[10]
    assert row2[11] == row1[11]
    assert row2[12] == row1[12]
    assert row2[13] == row1[13]
    assert row2[14] == row1[14]
