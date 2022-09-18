from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import inspect
import os
import re
from typing import Dict
from harvest.actions import write_event
from harvest.events import (
    Allocation,
    Asset,
    Event,
    SetBalance,
    SetPrice,
    SetAllocation,
    SetTargetAllocation,
)


class State(Enum):
    SET_EVENT = 1
    SET_DATE = 2
    SET_ACCOUNT = 3
    SET_ASSET = 4
    SET_AMOUNT = 5
    SET_ALLOCATION = 6
    SET_TARGET_ALLOCATION = 7


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
            return None
    else:
        return cur


def create_event(event_class, args: Dict) -> Event:
    params = {name for name in inspect.signature(event_class).parameters}
    args["created_at"] = datetime.now(timezone.utc).isoformat()
    filtered = {k: v for (k, v) in args.items() if k in params}

    return event_class(**filtered)


def main():
    env = (os.getenv("PYTHON_ENV") or "DEV").lower()
    events_file = f"harvest.{env}.jsonl"
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
            if cur_event == SetTargetAllocation:
                state = State.SET_ALLOCATION
                prompt = to_prompt(
                    "allocation(stock - lg, stock - md/sm, stock - intl, bond - us, bond - intl, cash)"
                )
            else:
                state = State.SET_ASSET
                default_asset = kwargs.get("asset")
                prompt = to_prompt(
                    "asset", default=default_asset.identifier if default_asset else None
                )
        elif state == State.SET_ASSET:
            if len(line) > 0:
                id = line.upper()
                if id == "CASH":
                    kwargs["asset"] = Asset.cash()
                else:
                    kwargs["asset"] = Asset.for_symbol(id)
            if kwargs.get("asset") and cur_event == SetBalance:
                state = State.SET_ACCOUNT
                prompt = to_prompt("account", default=kwargs.get("account"))
            elif kwargs.get("asset") and cur_event == SetPrice:
                state = State.SET_AMOUNT
                prompt = to_prompt("amount")
            elif kwargs.get("asset") and cur_event == SetAllocation:
                state = State.SET_ALLOCATION
                prompt = to_prompt(
                    "allocation(stock - lg, stock - md/sm, stock - intl, bond - us, bond - intl, cash)"
                )
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
            write_event(evt, file=events_file)
        elif state == State.SET_ALLOCATION and len(line) > 0:
            amounts = [Decimal(amt) for amt in line.split(" ")]
            if len(amounts) == 6:
                kwargs["allocation"] = Allocation(*amounts)
                evt = create_event(cur_event, kwargs)
                state = State.SET_EVENT
                prompt = to_prompt(
                    "event",
                    default="_".join(camel_to_snake(cur_event.__name__).split("_")[1:]),
                )
                write_event(evt, file=events_file)


if __name__ == "__main__":
    main()
