"""Visualization functions for benchmark results using Plotly Express."""

from typing import Annotated

import narwhals as nw
import plotly.express as px
import polars as pl
import typer
from plotly import graph_objects as go

from ._pipeline import Data

app = typer.Typer(help="Benchmarks for pyochain developments.")

SizeFilter = Annotated[
    int | None, typer.Option("--size", "-s", help="Filter by specific input size")
]


@app.command("relative")
@Data.db
def plot_relative(
    categories: Annotated[
        list[str] | None, typer.Option("--category", "-c", help="Filter by categories")
    ] = None,
    size: SizeFilter = None,
) -> None:
    """Plot category performance evolution relative to first observation."""
    return (
        Data.db.results.scan()
        .pipe(
            lambda lf: lf.filter(nw.col("category").is_in(categories))
            if categories
            else lf
        )
        .pipe(lambda lf: lf.filter(nw.col("size") == size) if size else lf)
        .select("category", "git_hash", "timestamp", "median")
        .with_columns(nw.col("timestamp").pipe(_observation_nb))
        .to_native()
        .pl(lazy=True)
        .group_by("category", "observation")
        .agg(
            pl.col("median").median().alias("median"),
            pl.col("git_hash").first(),
            pl.col("timestamp").first(),
        )
        .with_columns(
            pl.col("median")
            .sort_by("observation")
            .first()
            .over("category")
            .alias("baseline")
        )
        .with_columns(pl.col("median").truediv(pl.col("baseline")).alias("relative"))
        .sort("observation")
        .collect()
        .pipe(_line_rel)
        .show()
    )


def _line_rel(df: pl.DataFrame) -> go.Figure:
    return px.line(
        df,
        x="observation",
        y="relative",
        color="category",
        title="Relative Performance by Category",
        labels={
            "observation": "Observation #",
            "relative": "Relative to first observation",
            "category": "Category",
        },
        hover_data=["category", "git_hash", "timestamp", "median", "baseline"],
        markers=True,
        template="plotly_dark",
    )


@app.command("absolute")
@Data.db
def plot_absolute(
    category: Annotated[
        str, typer.Option("--category", "-c", help="Category to visualize")
    ],
    size: SizeFilter = None,
) -> None:
    """Plot performance evolution by commit for a specific category. Must specify a category."""
    return (
        Data.db.results.scan()
        .filter(nw.col("category") == category)
        .pipe(
            lambda lf: lf.filter(nw.col("size") == size)
            if size
            else lf.with_columns(
                nw.col("median").median().over("name", "timestamp").alias("median")
            )
        )
        .select("name", "git_hash", nw.col("timestamp").pipe(_observation_nb), "median")
        .to_native()
        .pl()
        .sort("observation")
        .pipe(_line_abs, category)
        .show()
    )


def _line_abs(df: pl.DataFrame, category: str) -> go.Figure:
    return px.line(
        df,
        x="observation",
        y="median",
        color="name",
        title=f"Performance Evolution - Category: {category}",
        labels={
            "observation": "Observation #",
            "median": "Median Time (seconds)",
            "name": "Test Name",
        },
        hover_data=["git_hash", "observation"],
        markers=True,
        template="plotly_dark",
    )


def _observation_nb(expr: nw.Expr) -> nw.Expr:
    return expr.rank(method="dense").cast(nw.Int64).alias("observation")
