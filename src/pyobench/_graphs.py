"""Visualization functions for benchmark results using Plotly Express."""

from typing import Annotated

import narwhals as nw
import plotly.express as px
import typer

from ._pipeline import Data

app = typer.Typer(help="Benchmarks for pyochain developments.")


@app.command("heatmap")
@Data.db
def plot_heatmap_by_commit(
    categories: Annotated[
        list[str] | None, typer.Option("--category", "-c", help="Filter by categories")
    ] = None,
    size: Annotated[
        int | None, typer.Option("--size", "-s", help="Filter by specific input size")
    ] = None,
) -> None:
    """Create heatmap of performance across benchmarks and git commits."""
    return (
        Data.db.results.scan()
        .pipe(
            lambda lf: lf.filter(nw.col("category").is_in(categories))
            if categories
            else lf
        )
        .pipe(lambda lf: lf.filter(nw.col("size") == size) if size else lf)
        .select("category", "name", "git_hash", "timestamp", "median")
        .with_columns(
            nw.concat_str([nw.col("category"), nw.col("name")], separator=" - ").alias(
                "benchmark"
            ),
            nw.col("git_hash").str.slice(0, 7).alias("commit_short"),
            nw.col("timestamp").cast(nw.Date()).alias("commit_date"),
        )
        .sort("timestamp")
        .to_native()
        .pl()
        .pipe(
            lambda df: px.density_heatmap(
                df,
                x="commit_date",
                y="benchmark",
                z="median",
                title="Performance Heatmap by Commit",
                labels={
                    "commit_date": "Commit Date",
                    "benchmark": "Benchmark",
                    "median": "Median Time (seconds)",
                },
                hover_data=["git_hash", "timestamp"],
                template="plotly_dark",
            )
        )
    ).show()


@app.command("evolution")
@Data.db
def plot_performance_evolution(
    category: Annotated[
        str, typer.Option("--category", "-c", help="Category to visualize")
    ],
    size: Annotated[
        int | None, typer.Option("--size", "-s", help="Filter by specific input size")
    ] = None,
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
        .select("name", "git_hash", "timestamp", "median")
        .with_columns(
            nw.col("git_hash").str.slice(0, 7).alias("commit_short"),
            nw.col("timestamp").cast(nw.Date()).alias("commit_date"),
        )
        .to_native()
        .pl()
        .sort("timestamp")
        .pipe(
            lambda df: px.line(
                df,
                x="commit_date",
                y="median",
                color="name",
                title=f"Performance Evolution - Category: {category}",
                labels={
                    "commit_date": "Commit Date",
                    "median": "Median Time (seconds)",
                    "name": "Test Name",
                },
                hover_data=["git_hash", "timestamp"],
                markers=True,
                template="plotly_dark",
            )
        )
    ).show()
