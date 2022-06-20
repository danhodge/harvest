from datetime import date
from enum import Enum
import inspect
import re
from typing import Dict
from harvest.events import Event, SetBalance, SetPrice, SetAllocation


class State(Enum):
    SET_EVENT = 1
    SET_DATE = 2
    SET_ACCOUNT = 3
    SET_SYMBOL = 4
    SET_AMOUNT = 5


def snake_to_camel(line: str) -> str:
    return "".join(token.title() for token in line.split("_"))


def camel_to_snake(line: str) -> str:
    snake_tokens = []
    for token in re.split("([A-Z])", line):
        if len(token) == 1 and token.upper() == token:
            snake_tokens.append("_")
        snake_tokens.append(token)

    return "".join(snake_tokens).lower()[1:]


def to_prompt(text: str, default=None) -> str:
    default_val = default if default else ""
    return f">  {text} {default_val} = "


def resolve_event(line: str, cur: Event) -> Event:
    if len(line) > 0:
        try:
            return globals()[snake_to_camel("set_{}".format(line))]
        except KeyError:
            return cur
    else:
        return cur


def create_event(event_class, args: Dict) -> Event:
    print("EVENT CLASS = {}".format(event_class))
    params = set(name for name in inspect.signature(event_class).parameters)
    print("PARAMS = {}".format(params))
    filtered = {k: v for (k, v) in args.items() if k in params}
    print("FILTERED = {}".format(filtered))
    return event_class(**filtered)


def main():
    prompt = to_prompt("event")
    cur_event = None
    state = State.SET_EVENT
    kwargs = {"date": str(date.today())}

    while True:
        line = input(prompt)
        if line in ("q", "quit", "exit"):
            break
        elif state == State.SET_EVENT:
            cur_event = resolve_event(line, cur_event)
            if cur_event:
                state = State.SET_DATE
                prompt = to_prompt("date", default=kwargs.get("date"))
        elif state == State.SET_DATE:
            if len(line) > 0:
                kwargs["date"] = line
            state = State.SET_SYMBOL
            prompt = to_prompt("symbol", default=kwargs.get("symbol"))
        elif state == State.SET_SYMBOL:
            if len(line) > 0:
                kwargs["symbol"] = line.upper()
            if kwargs.get("symbol") and cur_event == SetBalance:
                state = State.SET_ACCOUNT
                prompt = to_prompt("account", default=kwargs.get("account"))
            elif kwargs.get("symbol") and cur_event == SetPrice:
                state = State.SET_AMOUNT
                prompt = to_prompt("amount")
        elif state == State.SET_ACCOUNT:
            if len(line) > 0:
                kwargs["account"] = line
            if kwargs.get("account"):
                state = State.SET_AMOUNT
                prompt = to_prompt("amount")
        elif state == State.SET_AMOUNT and len(line) > 0:
            kwargs["amount"] = line
            state = State.SET_EVENT
            prompt = to_prompt(
                "event",
                default="_".join(camel_to_snake(cur_event.__name__).split("_")[1:]),
            )
            evt = create_event(cur_event, kwargs)
            print("CREATED EVENT: {}".format(evt))


if __name__ == "__main__":
    main()
