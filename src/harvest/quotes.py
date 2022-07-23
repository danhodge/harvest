import csv
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Callable, Dict, List, Sequence
import time
import requests


@dataclass(frozen=True)
class Quote:
    date: date
    price: Decimal


def yahoo_finance_fetcher(symbol: str, date: date, lookback_days=7) -> str | None:
    start_time = int(time.mktime((date - timedelta(days=lookback_days)).timetuple()))
    end_time = int(time.mktime(date.timetuple()))
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_time}&period2={end_time}&interval=1d&events=history&includeAdjustedClose=true"

    resp = requests.get(url, headers={"user-agent": "curl/7.79.1"})
    if resp.status_code == 200:
        return resp.text
    else:
        return None


def lookup_prices(symbols: Sequence[str], date: date):
    results = {}
    for symbol in symbols:
        if quote := fetch_quote(symbol=symbol, date=date):
            results[symbol] = quote

    return results


def fetch_quote(
    symbol: str,
    date: date,
    fetcher: Callable[[str, date], str | None] = yahoo_finance_fetcher,
) -> Quote | None:
    result = fetcher(symbol, date)
    quotes = to_quotes(result) if result else []

    for quote in sorted(quotes, key=lambda quote: quote.date, reverse=True):
        if quote.date <= date:
            return quote


def to_quotes(price_data: str) -> List[Quote]:
    rows = list(csv.reader(price_data.splitlines()))
    header = rows[0]
    return map(
        lambda rec: Quote(date=rec["Date"], price=rec["Adj Close"]),
        map(lambda row: transform(header, row), rows[1:]),
    )


def transform(header: Sequence, row: Sequence) -> Dict:
    def transformer(t):
        if t[0] == "Date":
            return t[0], date.fromisoformat(t[1])
        else:
            return t[0], Decimal(t[1])

    return dict(map(transformer, zip(header, row)))
