import sys
from datetime import date
from decimal import Decimal
from harvest.events import parse_event
from harvest.actions import handle_event


def main():
    if len(sys.argv) < 5:
        raise RuntimeError(
            "Usage: {} <event> <account> <symbol> <date> [args...]".format(__file__)
        )

    cmd = sys.argv[1]
    account = sys.argv[2]
    symbol = sys.argv[3]
    dte = date.fromisoformat(sys.argv[4])

    event = parse_event(cmd, account, symbol, dte, *sys.argv[5:])
    handle_event(event)


if __name__ == "__main__":
    main()
