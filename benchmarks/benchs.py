"""Tests for the benchmark registry."""

import pyochain as pc

from pyobench import bench


class _Foo:  # pyright: ignore[reportUnusedClass]
    """Benchmark class with sample operations."""

    @bench()
    @staticmethod
    def test_func(data: pc.Seq[int]) -> object:
        """Benchmark: square each element in a sequence."""
        return data.iter().map(lambda x: x * x).collect()

    @bench()
    @staticmethod
    def test_func_two(data: pc.Seq[int]) -> int:
        """Benchmark: sum all elements in a sequence."""
        return data.iter().sum()

    @bench()
    @staticmethod
    def test_func_three(data: pc.Seq[int]) -> object:
        """Benchmark: find the maximum element in a sequence."""
        return data.iter().max()
