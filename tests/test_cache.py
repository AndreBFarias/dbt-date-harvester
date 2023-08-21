"""Testes para cache de resultados."""

import time
import pytest
from pathlib import Path

from dbt_parser.utils.cache import ResultCache, CacheEntry


class TestResultCache:
    def test_set_and_get(self) -> None:
        cache = ResultCache()
        cache.set("key1", {"data": "value"})
        result = cache.get("key1")
        assert result == {"data": "value"}

    def test_get_miss(self) -> None:
        cache = ResultCache()
        result = cache.get("nonexistent")
        assert result is None

    def test_invalidate(self) -> None:
        cache = ResultCache()
        cache.set("key1", "value")
        assert cache.invalidate("key1")
        assert cache.get("key1") is None

    def test_clear(self) -> None:
        cache = ResultCache()
        cache.set("key1", "v1")
        cache.set("key2", "v2")
        cache.clear()
        assert cache.get("key1") is None
        assert cache.get("key2") is None

    def test_ttl_expiry(self) -> None:
        cache = ResultCache(default_ttl=0.1)
        cache.set("key1", "value")
        time.sleep(0.15)
        assert cache.get("key1") is None

    def test_get_or_compute(self) -> None:
        cache = ResultCache()
        result = cache.get_or_compute("key1", lambda: 42)
        assert result == 42
        result2 = cache.get_or_compute("key1", lambda: 99)
        assert result2 == 42

    def test_stats(self) -> None:
        cache = ResultCache()
        cache.set("key1", "value")
        cache.get("key1")
        cache.get("key1")
        cache.get("missing")
        stats = cache.get_stats()
        assert stats["hits"] == 2
        assert stats["misses"] == 1

    def test_cache_entry_expired(self) -> None:
        entry = CacheEntry(key="test", value="val", created_at=0, ttl=1)
        assert entry.is_expired

    def test_cache_entry_not_expired(self) -> None:
        entry = CacheEntry(key="test", value="val", created_at=time.time(), ttl=3600)
        assert not entry.is_expired

    def test_infinite_ttl(self) -> None:
        entry = CacheEntry(key="test", value="val", created_at=0, ttl=0)
        assert not entry.is_expired
