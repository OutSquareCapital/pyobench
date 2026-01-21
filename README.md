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
uv run pydoclint src/pyobench/
uv run ruff check --fix --unsafe-fixes
uv run ruff format
```

## Usage

At the root of your project, run:

```shell
uv run pyobench --help
```

to get the list of commands.
The tests will be run automatically on all functions who are decorated with `@pyobench.bench`.

### Example

```python
from pyobench import bench

@bench()
def my_function(x: int) -> int:
    return data.map(str).collect()
```

## TODO

1) run across git commits to get historical performance of the decorated bench func (subprocess?). Need to efficiently manage errors if API of the func wasn't compatible across commits

2) evolution graph should have X axis as observation nb, not timestamps

3) heatmap should be replaced by line graph where each line is a category starting from 1, Y axis is relative performance from initial observation, X axis is observation nb
