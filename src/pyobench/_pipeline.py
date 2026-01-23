"""Benchmarks for pyochain developments."""

import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple, Self

import framelib as fl
import polars as pl
import pyochain as pc

from ._registery import REGISTERY, Benchmark, Row, collect_raw_timings


class BenchmarksSchema(fl.Schema):
    """Schema for aggregated benchmark median results."""

    category = fl.String()
    name = fl.String()
    size = fl.UInt32()
    git_hash = fl.String()
    timestamp = fl.Datetime(time_unit="us")
    median = fl.Float64()
    runs = fl.UInt32()


class BenchDb(fl.DataBase):
    """DuckDB database for storing benchmark results."""

    results = fl.Table(BenchmarksSchema)


class Data(fl.Folder):
    """Folder for storing benchmark databases."""

    __source__ = Path.cwd()
    db = BenchDb()


def run_pipeline(path: Path, category: str | None = None) -> pl.DataFrame:
    """Persist aggregated benchmark results to DuckDB."""
    _discover_benchmarks(path)
    return (
        _filter_by_category(REGISTERY, category)
        .ok_or(Exception("No benchmarks registered!"))
        .map(collect_raw_timings)
        .and_then(_try_collect)
        .expect("Failed to run benchmarks -> \n")
    )


def _filter_by_category(
    registry: pc.Dict[str, pc.Vec[Benchmark]], category: str | None
) -> pc.Option[pc.Seq[Benchmark]] | pc.Option[pc.Vec[Benchmark]]:
    """Filter registry by category name (case-insensitive partial match)."""
    match category:
        case None:
            return registry.values().iter().flatten().collect().then_some()
        case cat:
            return registry.get_item(cat)


def _discover_benchmarks(path: Path) -> None:
    return (
        pc.Iter(path.iterdir())
        .filter(lambda p: p.name.lower().startswith("bench"))
        .flat_map(
            lambda item: pc.Iter.once(item) if item.is_file() else item.rglob("*.py")
        )
        .for_each(_import_module)
    )


def _import_module(path: Path) -> None:
    if not path.is_file() or path.suffix != ".py":
        return
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        return
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)


class GitInfos(NamedTuple):
    """Git commit hash and timestamp."""

    hash: str
    timestamp: str

    @classmethod
    def new(cls) -> Self:
        return cls(
            _run_git("git", "rev-parse", "HEAD").expect("failed to get git hash"),
            _run_git("git", "log", "-1", "--format=%at").expect(
                "failed to get timestamp"
            ),
        )

    def to_exprs(self) -> tuple[pl.Expr, pl.Expr]:
        return (
            pl.lit(self.hash).alias("git_hash"),
            pl.from_epoch(pl.lit(self.timestamp)).alias("timestamp"),
        )


def _run_git(*args: str) -> pc.Result[str, Exception]:
    try:
        result = subprocess.run(  # noqa: S603
            [*args],
            capture_output=True,
            text=True,
            check=True,
        )
        return pc.Ok(result.stdout.strip())
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        return pc.Err(e)


def _try_collect(raw_rows: pc.Seq[Row]) -> pc.Result[pl.DataFrame, Exception]:
    try:
        return pc.Ok(_compute_all_stats(raw_rows))
    except pl.exceptions.PolarsError as e:
        return pc.Err(e)


def _compute_all_stats(raw_rows: pc.Seq[Row]) -> pl.DataFrame:
    """Compute median stats from raw timings, returns atomic rows ready for DB."""
    return (
        pl.LazyFrame(
            raw_rows,
            schema=["category", "name", "size", "run_idx", "time"],
            orient="row",
        )
        .group_by("category", "name", "size")
        .agg(pl.col("time").median().alias("median"), pl.len().alias("runs"))
        .with_columns(GitInfos.new().to_exprs())
        .pipe(BenchDb.results.schema.cast)
        .to_native()
        .collect()
    )
