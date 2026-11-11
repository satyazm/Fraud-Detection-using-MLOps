# Contributing

Thanks for your interest in improving this project.

## Setup

```bash
python3.11 -m venv .venv && source .venv/bin/activate
make install-dev      # dev deps, editable install, pre-commit hooks
```

PyFlink needs `setuptools<81` and `--no-build-isolation`; the Makefile targets handle that — a plain `pip install -r requirements/dev.txt` will not work. See [Getting started](docs/guides/getting-started.md).

## Before you open a pull request

```bash
make lint        # ruff + black --check
make typecheck   # mypy
make test        # pytest with coverage
```

Tests that need Kafka, Redis, a JDK or the Flink connector JAR skip automatically when those aren't available (`make infra-up && make flink-jar` runs them for real). CI does not provision that infrastructure.

## Conventions

- **One feature implementation.** Feature logic lives only in `src/fraud_detection/features/`; training, the Flink job and the API all call it. Don't re-implement a feature elsewhere.
- **Respect the layering.** Dependencies point inward toward `domain` ([architecture](docs/architecture.md)).
- **Record significant decisions** as a new ADR in `docs/decisions/` (they're immutable — add a new one rather than editing an old one).
- **Don't edit generated reports** (`docs/data_report.md`, `docs/model_report.md`); rerun the command instead.
- Keep commits focused, with a message that says *why*.

## Reporting bugs

Use the bug-report template and include what you ran, what you expected, and the relevant log output.
