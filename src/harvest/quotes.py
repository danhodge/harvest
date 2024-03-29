import csv
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Callable, Dict, List, Sequence, Iterator, Iterable, Tuple
import time
import requests
from harvest.events import Asset, AssetType


@dataclass(frozen=True)
class Quote:
    date: date
    price: Decimal


def yahoo_finance_fetcher(
    symbol: str, date: date, lookback_days: int = 7
) -> str | None:
    start_time = int(time.mktime((date - timedelta(days=lookback_days)).timetuple()))
    end_time = int(time.mktime(date.timetuple()))
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_time}&period2={end_time}&interval=1d&events=history&includeAdjustedClose=true"

    resp = requests.get(url, headers={"user-agent": "curl/7.79.1"})
    if resp.status_code == 200:
        return resp.text
    else:
        return None


def lookup_prices(assets: Iterable[Asset], date: date) -> Dict[Asset, Quote]:
    results = {}
    for asset in assets:
        if quote := fetch_quote(asset=asset, date=date):
            results[asset] = quote

    return results


QuoteFetcher = Callable[[str, date], str | None]


def quote_fetchers_by_asset_type() -> Dict[AssetType, QuoteFetcher]:
    return {
        "investment": yahoo_finance_fetcher,
        "cash": lambda _, date: f"Date,Adj Close\n{date},1.0\n",
    }


def fetch_quote(
    asset: Asset,
    date: date,
    fetcher: QuoteFetcher | None = None,
) -> Quote | None:
    fetcher = fetcher or quote_fetchers_by_asset_type()[asset.type]
    result = fetcher(asset.identifier, date)
    quotes = to_quotes(result) if result else []

    for quote in sorted(quotes, key=lambda quote: quote.date, reverse=True):
        if quote.date <= date:
            return quote

    return None


def to_quotes(price_data: str) -> Iterator[Quote]:
    rows = list(csv.reader(price_data.splitlines()))
    header = rows[0]
    return map(
        lambda rec: Quote(date=rec["Date"], price=rec["Adj Close"]),
        map(lambda row: transform(header, row), rows[1:]),
    )


def transform(header: Sequence[str], row: Sequence[str]) -> Dict[str, date | Decimal]:
    def transformer(t: Tuple[str, str]) -> Tuple[str, date | Decimal]:
        if t[0] == "Date":
            return t[0], date.fromisoformat(t[1])
        else:
            return t[0], Decimal(t[1])

    return dict(map(transformer, zip(header, row)))
