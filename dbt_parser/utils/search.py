"""Sistema de busca fuzzy para modelos e artefatos dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

@dataclass
class SearchResult:
    """Resultado de uma busca."""

    name: str
    score: float
    node_type: str
    match_type: str  # "exact", "prefix", "contains", "fuzzy"

class FuzzySearch:
    """Busca fuzzy em nomes de modelos e artefatos dbt."""

    def __init__(self, graph: GraphResolver) -> None:
        self.graph = graph
        self._index: dict[str, dict[str, Any]] = {}
        self._build_index()

    def _build_index(self) -> None:
        """Constroi indice de busca."""
        for name in self.graph.graph.nodes():
            node_data = dict(self.graph.graph.nodes[name])
            self._index[name.lower()] = {
                "original_name": name,
                "node_type": node_data.get("node_type", "model"),
                "tags": node_data.get("tags", []),
            }

    def search(self, query: str, limit: int = 10) -> list[SearchResult]:
        """Busca modelos por nome com matching fuzzy."""
        query_lower = query.lower()
        results: list[SearchResult] = []

        for key, data in self._index.items():
            if key == query_lower:
                results.append(
                    SearchResult(
                        name=data["original_name"],
                        score=1.0,
                        node_type=data["node_type"],
                        match_type="exact",
                    )
                )
            elif key.startswith(query_lower):
                results.append(
                    SearchResult(
                        name=data["original_name"],
                        score=0.8,
                        node_type=data["node_type"],
                        match_type="prefix",
                    )
                )
            elif query_lower in key:
                results.append(
                    SearchResult(
                        name=data["original_name"],
                        score=0.6,
                        node_type=data["node_type"],
                        match_type="contains",
                    )
                )
            else:
                distance = self._levenshtein_distance(query_lower, key)
                max_len = max(len(query_lower), len(key))
                similarity = 1.0 - (distance / max_len) if max_len > 0 else 0
                if similarity > 0.4:
                    results.append(
                        SearchResult(
                            name=data["original_name"],
                            score=similarity * 0.5,
                            node_type=data["node_type"],
                            match_type="fuzzy",
                        )
                    )

        results.sort(key=lambda r: r.score, reverse=True)
        return results[:limit]

    def search_by_tag(self, tag: str) -> list[str]:
        """Busca modelos por tag."""
        results: list[str] = []
        for key, data in self._index.items():
            if tag in data.get("tags", []):
                results.append(data["original_name"])
        return sorted(results)

    @staticmethod
    def _levenshtein_distance(s1: str, s2: str) -> int:
        """Calcula distancia de Levenshtein entre duas strings."""
        if len(s1) < len(s2):
            return FuzzySearch._levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def rebuild_index(self) -> None:
        """Reconstroi indice de busca."""
        self._index.clear()
        self._build_index()

