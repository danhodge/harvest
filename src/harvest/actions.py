import json
from typing import List
from harvest.report import Report
from harvest.events import (
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


def write_event(command: Event, file: str):
    with open(file, "a") as file:
        command.__dict__["type"] = type(command).__name__
        file.write(json.dumps(command, cls=EventEncoder))
        file.write("\n")


def read_events(file: str) -> List[Event]:
    with open(file, "r") as file:
        events = [parse_event_json(line.strip()) for line in file.readlines()]

    return events


def handle_event(command: Event, events_file=None):
    events_file = events_file or "harvest.jsonl"
    match command:
        case SetBalance(account, symbol, date, amt) as sb:
            write_event(sb, file=events_file)
        case SetPrice(symbol, date, price) as sp:
            write_event(sp, file=events_file)
        case SetAllocation() as sa:
            write_event(sa, file=events_file)
        case RunReport(date, account) as rr:
            report = Report.create(rr, read_events(file=events_file))
            handle_event(
                FileWritten(path=report.write_to_file()), events_file=events_file
            )
        case FileWritten(path):
            print("Report written to file: {}".format(path))
        case UnknownEvent(event):
            print(f"Unknown event: {event}")
