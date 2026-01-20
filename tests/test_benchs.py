"""Tests for the benchmark registry."""

import pyochain as pc

from pyobench import REGISTERY, bench


def test_decorator() -> None:
    """Check that the benchmark decorator registers benchmarks."""

    class _Foo:  # pyright: ignore[reportUnusedClass]
        @bench()
        @staticmethod
        def test_func(data: pc.Seq[int]) -> object:
            return data.iter().map(lambda x: x * x).collect()

    assert REGISTERY.length() == 1
