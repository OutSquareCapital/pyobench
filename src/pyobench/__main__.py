"""Entry point for benchmarks CLI."""

from pathlib import Path
from typing import Annotated

import typer

from . import _graphs
from ._pipeline import Data, run_pipeline
from ._registery import CONSOLE

app = typer.Typer(help="Benchmarks for pyochain developments.")
app.add_typer(_graphs.app, name="viz", help="Visualization commands")

BenchPath = Annotated[
    Path, typer.Argument(help="Path to directory containing benchmark files.")
]


@app.command()
@Data.db
def show() -> None:
    """Show all benchmark results from the database."""
    return Data.db.results.scan().pipe(print)


@app.command()
@Data.db
def setup(
    *,
    overwrite: Annotated[
        bool, typer.Option("--overwrite", help="Overwrite existing benchmark database.")
    ] = False,
) -> None:
    """Setup the benchmark database."""
    Data.source().mkdir(exist_ok=True)
    if overwrite:
        Data.db.results.create_or_replace()
    else:
        Data.db.results.create()
    CONSOLE.print("OK: benchmark database setup complete", style="bold green")


CategoryOpt = Annotated[
    str | None,
    typer.Option(
        "--category", "-c", help="Filter benchmarks by category (partial match)."
    ),
]


@app.command()
@Data.db
def run(
    path: BenchPath,
    category: CategoryOpt = None,
    *,
    debug: Annotated[
        bool, typer.Option("--dry", help="Don't persist results to database.")
    ] = False,
) -> None:
    """Run benchmarks and persist results to database."""
    CONSOLE.print("Running benchmarks...", style="bold blue")
    match debug:
        case True:
            CONSOLE.print("OK: debug mode (results not persisted)", style="bold yellow")
            return run_pipeline(path, category).pipe(print)
        case False:
            run_pipeline(path, category).pipe(Data.db.results.insert_into)

            CONSOLE.print()
            return CONSOLE.print(
                "OK: results persisted to database", style="bold green"
            )


if __name__ == "__main__":
    app()
