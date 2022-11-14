from datetime import date, datetime, timezone
import json
from typing import List, Iterable, Sequence
import logging
from harvest.report import Report
from harvest.events import (
    Asset,
    Event,
    EventEncoder,
    FileWritten,
    RunReport,
    SetAllocation,
    SetBalance,
    SetPrice,
    UnknownEvent,
    parse_event_json,
)
from harvest.quotes import lookup_prices

logger = logging.getLogger(__name__)


def write_event(command: Event, file_name: str) -> None:
    with open(file_name, "a") as file:
        command.__dict__["type"] = type(command).__name__
        file.write(json.dumps(command, cls=EventEncoder))
        file.write("\n")


def read_events(file_name: str) -> List[Event]:
    with open(file_name, "r") as file:
        events = [parse_event_json(line.strip()) for line in file.readlines()]

    logger.debug("Read %i events from file %s", len(events), file_name)
    return events


def handle_event(command: Event, events_file: str | None = None) -> None:
    events_file = events_file or "harvest.jsonl"
    match command:
        case SetBalance(account, asset, date, amt) as sb:
            write_event(sb, file_name=events_file)
        case SetPrice(asset, date, price) as sp:
            write_event(sp, file_name=events_file)
        case SetAllocation() as sa:
            write_event(sa, file_name=events_file)
        case RunReport(date, account) as rr:
            events = read_events(file_name=events_file)
            assets = {e.asset for e in events if isinstance(e, SetBalance)}
            events.extend(generate_set_price_events(assets, date))
            report = Report.create(rr, events)
            handle_event(
                FileWritten(
                    path=report.write_to_file(),
                    incomplete_symbols={
                        asset.identifier for asset in report.incomplete_assets
                    },
                ),
                events_file=events_file,
            )
        case FileWritten(path, incomplete_symbols):
            print(
                "Report written to file: {} (incomplete symbols: {})".format(
                    path, incomplete_symbols
                )
            )
        case UnknownEvent(event):
            print(f"Unknown event: {event}")


def generate_set_price_events(assets: Iterable[Asset], date: date) -> List[SetPrice]:
    events = []
    for asset, quote in lookup_prices(assets, date).items():
        events.append(
            SetPrice(
                asset=asset,
                date=quote.date,
                amount=quote.price,
                created_at=datetime.now(timezone.utc),
            )
        )

    return events
