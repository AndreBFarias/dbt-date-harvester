"""Sistema de cache para resultados de parsing."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

@dataclass
class CacheEntry:
    """Entrada de cache."""

    key: str
    value: Any
    created_at: float
    ttl: float
    file_hash: str | None = None

    @property
    def is_expired(self) -> bool:
        if self.ttl <= 0:
            return False
        return (time.time() - self.created_at) > self.ttl

class ResultCache:
    """Cache em memoria para resultados de parsing."""

    def __init__(self, default_ttl: float = 300.0) -> None:
        self.default_ttl = default_ttl
        self._cache: dict[str, CacheEntry] = {}
        self._hits: int = 0
        self._misses: int = 0

    def get(self, key: str) -> Any | None:
        """Recupera valor do cache."""
        entry = self._cache.get(key)
        if entry is None:
            self._misses += 1
            return None
        if entry.is_expired:
            del self._cache[key]
            self._misses += 1
            return None
        self._hits += 1
        return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        file_hash: str | None = None,
    ) -> None:
        """Armazena valor no cache."""
        self._cache[key] = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            ttl=ttl if ttl is not None else self.default_ttl,
            file_hash=file_hash,
        )

    def invalidate(self, key: str) -> bool:
        """Invalida entrada especifica do cache."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def invalidate_by_file(self, filepath: Path) -> int:
        """Invalida entradas de cache associadas a um arquivo modificado."""
        current_hash = self._compute_file_hash(filepath)
        invalidated = 0

        keys_to_remove = []
        for key, entry in self._cache.items():
            if entry.file_hash and entry.file_hash != current_hash:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._cache[key]
            invalidated += 1

        return invalidated

    def clear(self) -> None:
        """Limpa todo o cache."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: float | None = None,
    ) -> Any:
        """Retorna do cache ou computa e armazena."""
        cached = self.get(key)
        if cached is not None:
            return cached
        value = compute_fn()
        self.set(key, value, ttl)
        return value

    def get_stats(self) -> dict[str, Any]:
        """Retorna estatisticas do cache."""
        total = self._hits + self._misses
        return {
            "entries": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": (self._hits / total * 100) if total > 0 else 0,
        }

    @staticmethod
    def _compute_file_hash(filepath: Path) -> str | None:
        """Computa hash de um arquivo."""
        if not filepath.exists():
            return None
        content = filepath.read_bytes()
        return hashlib.md5(content).hexdigest()

