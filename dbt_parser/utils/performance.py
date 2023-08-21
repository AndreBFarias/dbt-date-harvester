"""Utilitarios de performance para parsing de projetos grandes."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Generator

logger = logging.getLogger(__name__)

@dataclass
class TimingResult:
    """Resultado de medicao de tempo."""

    operation: str
    duration_ms: float
    items_processed: int = 0
    items_per_second: float = 0.0

class PerformanceTracker:
    """Rastreia performance de operacoes de parsing."""

    def __init__(self) -> None:
        self._timings: list[TimingResult] = []

    @contextmanager
    def track(self, operation: str, item_count: int = 0) -> Generator[None, None, None]:
        """Context manager para medir tempo de execucao."""
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            rate = (item_count / (elapsed / 1000)) if elapsed > 0 and item_count > 0 else 0

            result = TimingResult(
                operation=operation,
                duration_ms=round(elapsed, 2),
                items_processed=item_count,
                items_per_second=round(rate, 2),
            )
            self._timings.append(result)
            logger.info(
                "Performance [%s]: %.2fms (%d items, %.2f/s)",
                operation,
                elapsed,
                item_count,
                rate,
            )

    def get_timings(self) -> list[TimingResult]:
        """Retorna todas as medicoes."""
        return self._timings.copy()

    def get_total_time(self) -> float:
        """Retorna tempo total em ms."""
        return sum(t.duration_ms for t in self._timings)

    def get_summary(self) -> dict[str, Any]:
        """Retorna resumo de performance."""
        if not self._timings:
            return {"total_operations": 0, "total_time_ms": 0}

        return {
            "total_operations": len(self._timings),
            "total_time_ms": round(self.get_total_time(), 2),
            "slowest_operation": max(self._timings, key=lambda t: t.duration_ms).operation,
            "fastest_operation": min(self._timings, key=lambda t: t.duration_ms).operation,
            "operations": [
                {
                    "name": t.operation,
                    "duration_ms": t.duration_ms,
                    "items": t.items_processed,
                }
                for t in self._timings
            ],
        }

    def reset(self) -> None:
        """Limpa todas as medicoes."""
        self._timings.clear()

def batch_process(items: list, batch_size: int, processor: Callable) -> list[Any]:
    """Processa itens em lotes para melhor performance."""
    results: list[Any] = []
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        batch_results = [processor(item) for item in batch]
        results.extend(batch_results)
    return results

