"""Visualization functions for benchmark results using Plotly Express."""

from typing import Annotated

import narwhals as nw
import plotly.express as px
import polars as pl
import typer
from plotly import graph_objects as go

from ._pipeline import Data

app = typer.Typer(help="Benchmarks for pyochain developments.")

CatFilter = Annotated[
    list[str] | None, typer.Option("--category", "-c", help="Filter by categories")
]


@app.command("relative")
@Data.db
def plot_relative(categories: CatFilter = None) -> None:
    """Plot category performance evolution relative to first observation."""
    return (
        Data.db.results.scan()
        .pipe(
            lambda lf: lf.filter(nw.col("category").is_in(categories))
            if categories
            else lf
        )
        .select(
            "name",
            "category",
            "git_hash",
            "timestamp",
            "median",
            "size",
            nw.col("timestamp")
            .rank(method="dense")
            .cast(nw.Int64)
            .alias("observation"),
            nw.concat_str(["category", "name", "size"], separator="_").alias(
                "category_name_size"
            ),
        )
        .to_native()
        .pl(lazy=True)
        .with_columns(pl.col("median").pipe(_rel_time))
        .with_columns(  # TODO: move those columns as new values for relative groups. maybe lit for each agg + 2 unpivots?
            pl.col("relative")
            .median()
            .over("observation", "size", "name", "category")
            .alias("median_over_size"),
            pl.col("relative")
            .median()
            .over("observation", "name", "category")
            .alias("median_over_name"),
            pl.col("relative")
            .median()
            .over("observation", "category")
            .alias("median_over_category"),
        )
        .sort("observation")
        .collect()
        .pipe(_line_rel)
        .show()
    )


def _rel_time(expr: pl.Expr) -> pl.Expr:
    return expr.truediv(
        expr.sort_by("observation").first().over("category_name_size")
    ).alias("relative")


def _line_rel(df: pl.DataFrame) -> go.Figure:
    return px.line(
        df,
        x="observation",
        y="relative",
        color="category_name_size",
        title="Relative Performance by Category and Size",
        labels={
            "observation": "Observation #",
            "relative": "Relative to first observation",
            "category": "Category",
        },
        hover_data=["category", "git_hash", "timestamp", "median"],
        markers=True,
        template="plotly_dark",
    )
