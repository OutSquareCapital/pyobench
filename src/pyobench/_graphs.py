"""Visualization functions for benchmark results using Plotly Express."""

from typing import Annotated

import narwhals as nw
import plotly.express as px
import typer

from ._pipeline import Data

app = typer.Typer(help="Benchmarks for pyochain developments.")


@app.command("by-size")
@Data.db
def plot_median_by_size(
    categories: Annotated[
        list[str] | None, typer.Option("--category", "-c", help="Filter by categories")
    ] = None,
    benchmarks: Annotated[
        list[str] | None, typer.Option("--benchmark", "-b", help="Filter by benchmarks")
    ] = None,
) -> None:
    """Plot median execution time by input size for benchmarks."""
    return (
        Data.db.results.scan()
        .pipe(
            lambda lf: lf.filter(nw.col("category").is_in(categories))
            if categories
            else lf
        )
        .pipe(
            lambda lf: lf.filter(nw.col("name").is_in(benchmarks)) if benchmarks else lf
        )
        .select("category", "name", "size", "median")
        .with_columns(
            benchmark=nw.concat_str(
                [nw.col("category"), nw.col("name")], separator=" - "
            )
        )
        .sort("size")
        .to_native()
        .pl()
        .pipe(
            lambda df: px.line(
                df,
                x="size",
                y="median",
                color="benchmark",
                log_x=True,
                log_y=True,
                markers=True,
                title="Benchmark Performance by Input Size",
                labels={
                    "size": "Input Size",
                    "median": "Median Time (seconds)",
                    "benchmark": "Benchmark",
                },
                template="plotly_dark",
            )
        )
    ).show()


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
        .select("category", "name", "git_hash", "median", "timestamp")
        .with_columns(
            nw.concat_str([nw.col("category"), nw.col("name")], separator=" - ").alias(
                "benchmark"
            ),
            nw.col("git_hash").str.slice(0, 7).alias("commit_short"),
        )
        .sort("timestamp")
        .to_native()
        .pl()
        .pipe(
            lambda df: px.density_heatmap(
                df,
                x="commit_short",
                y="benchmark",
                z="median",
                title="Performance Heatmap by Commit",
                labels={
                    "commit_short": "Git Commit",
                    "benchmark": "Benchmark",
                    "median": "Median Time (seconds)",
                },
                hover_data=["git_hash"],
                template="plotly_dark",
            )
        )
    ).show()
