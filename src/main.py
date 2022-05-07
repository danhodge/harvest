import sys
from datetime import date
from decimal import Decimal
from harvest.commands import parse_command


def main():
    if len(sys.argv) < 5:
        raise RuntimeError(
            "Usage: {} <command> <account> <symbol> <date> [args...]".format(__file__)
        )

    cmd = sys.argv[1]
    account = sys.argv[2]
    symbol = sys.argv[3]
    d = date.fromisoformat(sys.argv[4])

    command = parse_command(cmd, account, symbol, d, *sys.argv[5:])
    if command:
        pass
    else:
        print("Unknown command: {}".format(" ".join(sys.argv[1:])))


if __name__ == "__main__":
    main()
