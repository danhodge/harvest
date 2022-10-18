#### Setup

```bash
poetry run python3 src/main.py
```

## install dependencies/new dependencies
```bash
poetry add <package>
poetry install
```

## run application
```bash
poetry run python3 src/main.py run_report 2022-09-10
PYTHON_ENV=<env> poetry run python3 src/cli.py
```

## run test
```bash
poetry run pytest tests/report_test.py
```