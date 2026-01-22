"""Run benchmarks across git commits.

Strategy (ASV-like):
1. Benchmarks stay FIXED (current HEAD version)
2. For each commit: install the project from that commit's worktree
3. Run benchmarks in a SUBPROCESS (to reset the global REGISTERY)
"""

import subprocess
from pathlib import Path

import pyochain as pc

from ._pipeline import Data
from ._registery import CONSOLE


def run_history(
    commits: list[str], bench_path: Path, category: str | None = None
) -> None:
    """Run benchmarks for each commit, store in temp partitions, then ingest into DB."""
    repo = Path.cwd().resolve()

    def _resolve(ref: str) -> str:
        return (
            _git(repo, "rev-parse", ref)
            .inspect_err(
                lambda e: CONSOLE.print(f"✗ Can't resolve {ref}: {e}", style="bold red")
            )
            .unwrap_or(ref)
        )

    pc.Iter(commits).map(_resolve).for_each(
        lambda c: _run_commit(repo, c, bench_path, category)
    )
    CONSOLE.print("\n▶ Ingesting all partitions into DB...", style="bold blue")
    _save_results()
    CONSOLE.print("OK: all results ingested", style="bold green")


def _save_results() -> None:
    (
        Data.temp.scan()
        .pipe(Data.db.results.schema.cast)
        .to_native()
        .pipe(Data.db.results.insert_or_replace)
    )


def _run_commit(
    repo: Path, commit: str, bench_path: Path, category: str | None = None
) -> pc.Result[None, Exception]:
    """Run benchmarks for a single commit.

    1. Create a git worktree for the commit
    2. uv sync in worktree (builds with whatever build system that commit uses)
    3. Run benchmarks in worktree's env but with CURRENT bench files (ASV-like)
    4. Cleanup worktree
    """
    CONSOLE.print(f"\n▶ {commit[:8]}", style="bold blue")
    wt = Data.source().joinpath("worktrees", f"wt_{commit[:8]}")

    result = (
        _git(repo, "worktree", "add", "--detach", wt.as_posix(), commit)
        .and_then(lambda _: _sync_worktree(wt))
        .and_then(
            lambda _: _run_bench_subprocess(wt, repo.joinpath(bench_path), category)
        )
        .inspect(lambda _: CONSOLE.print(f"OK: {commit[:8]}", style="bold green"))
        .inspect_err(lambda e: CONSOLE.print(f"✗ {commit[:8]}: {e}", style="bold red"))
    )
    _git(repo, "worktree", "remove", "--force", wt.as_posix())
    return result


def _sync_worktree(wt: Path) -> pc.Result[None, Exception]:
    """Sync the worktree environment (builds project with its own build system)."""
    try:
        subprocess.run(
            ["uv", "sync"],  # noqa: S607
            check=True,
            cwd=wt,
        )
        return pc.Ok(None)
    except subprocess.CalledProcessError as e:
        CONSOLE.print(f"uv sync failed:\n{e.stderr}", style="bold red")
        return pc.Err(e)
    except FileNotFoundError as e:
        return pc.Err(e)


def _run_bench_subprocess(
    wt: Path, bench_path: Path, category: str | None = None
) -> pc.Result[None, Exception]:
    """Run benchmarks in the worktree's environment but with CURRENT bench files."""
    try:
        subprocess.run(  # noqa: S603
            [  # noqa: S607
                "uv",
                "run",
                "python",
                "-c",
                _BENCH_SCRIPT,
                bench_path.as_posix(),
                Data.temp.source.as_posix(),
                category or "",
            ],
            check=True,
            cwd=wt,
        )
        return pc.Ok(None)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        return pc.Err(e)


_BENCH_SCRIPT = """
import sys
from pathlib import Path
from pyobench._pipeline import run_pipeline, Data

bench_path = Path(sys.argv[1])
temp_path = Path(sys.argv[2])
category = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else None
Data.temp.source = temp_path  # ensure same temp location
run_pipeline(bench_path, category).pipe(Data.temp.write)
"""


def _git(repo: Path, *args: str) -> pc.Result[str, Exception]:
    try:
        result = subprocess.run(  # noqa: S603
            ["git", "-C", repo.as_posix(), *args],  # noqa: S607
            check=True,
            capture_output=True,
            text=True,
        )
        return pc.Ok(result.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        return pc.Err(e)
