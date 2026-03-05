"""Testes para utilitarios de performance."""

import pytest

from dbt_parser.utils.performance import PerformanceTracker, batch_process


class TestPerformanceTracker:
    def test_track_timing(self) -> None:
        tracker = PerformanceTracker()
        with tracker.track("test_op", item_count=10):
            total = sum(range(1000))
        timings = tracker.get_timings()
        assert len(timings) == 1
        assert timings[0].operation == "test_op"
        assert timings[0].duration_ms > 0

    def test_get_total_time(self) -> None:
        tracker = PerformanceTracker()
        with tracker.track("op1"):
            pass
        with tracker.track("op2"):
            pass
        assert tracker.get_total_time() >= 0

    def test_get_summary(self) -> None:
        tracker = PerformanceTracker()
        with tracker.track("op1"):
            pass
        summary = tracker.get_summary()
        assert summary["total_operations"] == 1

    def test_reset(self) -> None:
        tracker = PerformanceTracker()
        with tracker.track("op"):
            pass
        tracker.reset()
        assert len(tracker.get_timings()) == 0

    def test_empty_summary(self) -> None:
        tracker = PerformanceTracker()
        summary = tracker.get_summary()
        assert summary["total_operations"] == 0


class TestBatchProcess:
    def test_batch_process(self) -> None:
        items = list(range(10))
        results = batch_process(items, batch_size=3, processor=lambda x: x * 2)
        assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    def test_batch_empty(self) -> None:
        results = batch_process([], batch_size=5, processor=lambda x: x)
        assert results == []
