"""Benchmarks for pyochain developments."""

import importlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import framelib as fl
import polars as pl
import pyochain as pc

from ._registery import REGISTERY, Row, collect_raw_timings


class BenchmarksSchema(fl.Schema):
    """Schema for aggregated benchmark median results."""

    id = fl.String(primary_key=True)
    category = fl.String()
    name = fl.String()
    size = fl.UInt32()
    git_hash = fl.String()
    median = fl.Float64()
    runs = fl.UInt32()


class BenchDb(fl.DataBase):
    """DuckDB database for storing benchmark results."""

    results = fl.Table(BenchmarksSchema)


class Data(fl.Folder):
    """Folder for storing benchmark databases."""

    db = BenchDb()


def run_pipeline() -> pl.DataFrame:
    """Persist aggregated benchmark results to DuckDB."""
    _discover_benchmarks()
    return (
        REGISTERY.ok_or("No benchmarks registered!")
        .map(collect_raw_timings)
        .map(_compute_all_stats)
        .and_then(_try_collect)
        .unwrap()
    )


def _discover_benchmarks() -> None:
    return (
        pc.Iter(Path.cwd().iterdir())
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


def _try_collect(lf: pl.LazyFrame) -> pc.Result[pl.DataFrame, str]:
    """Try to collect a LazyFrame, with error handling."""
    try:
        return pc.Ok(lf.collect())
    except (
        pl.exceptions.ColumnNotFoundError,
        pl.exceptions.InvalidOperationError,
    ) as e:
        return pc.Err(f"{e}")


def _get_git_hash() -> pc.Result[str, Exception]:
    """Get current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
        )
        return pc.Ok(result.stdout.strip())
    except Exception as e:  # noqa: BLE001
        return pc.Err(e)


def _compute_all_stats(raw_rows: pc.Seq[Row]) -> pl.LazyFrame:
    """Compute median stats from raw timings, returns atomic rows ready for DB."""
    return (
        pl.LazyFrame(
            raw_rows,
            schema=["category", "name", "size", "run_idx", "time"],
            orient="row",
        )
        .group_by("category", "name", "size")
        .agg(
            pl.col("time").median().alias("median"),
            pl.len().alias("runs"),
        )
        .with_columns(
            _get_git_hash()
            .map(pl.lit)
            .expect("Failed to get git hash")
            .alias("git_hash"),
        )
        .with_columns(
            pl.concat_str(
                ["category", "name", "size", pl.col("git_hash").str.slice(0, 3)],
                separator="-",
            ).alias("id")
        )
        .pipe(BenchDb.results.schema.cast)
        .to_native()
    )
