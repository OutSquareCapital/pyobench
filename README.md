# pyobench

This repositery contains the benchmarking code used for the [pyochain](https://github.com/tibo/pyobench) project.

## Installation

```shell
uv add git+https://github.com/OutSquareCapital/pyobench.git
```

## Development

```shell
uv sync --dev
```

Before each commit, make sure to run:

```shell
uv run ruff check --fix --unsafe-fixes
uv run ruff format
```
