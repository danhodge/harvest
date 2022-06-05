from datetime import date
from decimal import Decimal
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
        equities=Decimal("77.1"), bonds=Decimal("8.91"), cash=Decimal("3.45")
    )

    events = [
        SetAllocation(sym, date.fromisoformat("2022-05-20"), allocation),
        SetBalance(acct, sym, date.fromisoformat("2022-05-20"), Decimal("123.45")),
        SetPrice(sym, date.fromisoformat("2022-05-21"), Decimal("23.45")),
        SetBalance(acct, sym, date.fromisoformat("2022-05-28"), Decimal("234.56")),
        SetPrice(sym, date.fromisoformat("2022-05-25"), Decimal("34.56")),
    ]

    report = Report.create(RunReport(date.fromisoformat("2022-05-27")), events)
    result = report.compute()

    assert len(result) == 2

    row1 = result[0]
    assert row1[0] == acct
    assert row1[1] == sym
    assert row1[2] == Decimal("123.45")
    assert row1[3] == Decimal("34.56")
    assert row1[4] == "2022-05-25"

    total = row1[2] * row1[3]
    assert row1[5] == (total * allocation.equities) / 100
    assert row1[6] == (total * allocation.bonds) / 100
    assert row1[7] == (total * allocation.cash) / 100
    assert row1[8] == (total * allocation.other) / 100
    assert row1[9] == total

    row2 = result[1]
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
