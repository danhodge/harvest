import json
from harvest.events import Event, EventEncoder, RunReport, SetBalance


def write_event(command: Event):
    with open("harvest.jsonl", "a") as file:
        file.write(json.dumps(command, cls=EventEncoder))
        file.write("\n")


def handle_event(command: Event):
    match command:
        case SetBalance(account, symbol, date, amt) as sb:
            write_event(sb)
        case RunReport(date, account):
            # read events
            # build report
            # write to file
            # publish file written event
            pass
