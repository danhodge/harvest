import json
import sys
from datetime import datetime, timezone


def main(input_path, output_path):
    with open(input_path, "r") as input:
        with open(output_path, "w") as output:
            for line in input.readlines():
                data = json.loads(line.strip())

                if sym := data["symbol"]:
                    del data["symbol"]
                    data["asset"] = {
                        "identifier": sym,
                        "type": "cash"
                        if sym in ("FCASH", "CASH", "FDRXX")
                        else "investment",
                    }
                data["created_at"] = datetime.now(timezone.utc).isoformat()

                output.write(json.dumps(data))
                output.write("\n")


if __name__ == "__main__":
    assert len(sys.argv) == 3

    main(sys.argv[1], sys.argv[2])
