import csv
from datetime import date, timedelta
from decimal import Decimal
from typing import Sequence
import time
import requests


def lookup_prices(symbols: Sequence[str], date: date):
    start_time = int(time.mktime((date - timedelta(days=7)).timetuple()))
    end_time = int(time.mktime(date.timetuple()))

    results = {}
    for symbol in symbols:
        url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_time}&period2={end_time}&interval=1d&events=history&includeAdjustedClose=true"
        resp = requests.get(url, headers={"user-agent": "curl/7.79.1"})
        if resp.status_code == 200:
            rows = list(csv.reader(resp.text.splitlines()))
            header = rows[0]
            results[symbol] = list(map(lambda row: transform(header, row), rows[1:]))

    return results


def transform(header, row):
    def transformer(t):
        if t[0] == "Date":
            return t[0], date.fromisoformat(t[1])
        else:
            return t[0], Decimal(t[1])

    return dict(map(transformer, zip(header, row)))
