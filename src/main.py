import os
import sys
from datetime import date
from decimal import Decimal
import logging
from harvest.events import parse_event
from harvest.actions import handle_event


def main():
    if len(sys.argv) < 3:
        raise RuntimeError("Usage: {} <event> <date> [args...]".format(__file__))

    env = (os.getenv("PYTHON_ENV") or "DEV").lower()
    logging.basicConfig(filename=f"{env}.log", encoding="utf-8", level=logging.DEBUG)

    cmd = sys.argv[1]
    dte = date.fromisoformat(sys.argv[2])

    event = parse_event(cmd, dte, *sys.argv[3:])
    events_file = f"harvest.{env}.jsonl"
    handle_event(event, events_file=f"harvest.{env}.jsonl")


if __name__ == "__main__":
    main()
