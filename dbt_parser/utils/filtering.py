"""Sistema de filtragem avancada para modelos dbt."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable

from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

@dataclass
class FilterCriteria:
    """Criterios de filtragem para modelos."""

    node_types: set[str | None] = None
    tags: set[str | None] = None
    name_pattern: str | None = None
    paths: list[str | None] = None
    materializations: set[str | None] = None
    has_tests: bool | None = None
    min_dependencies: int | None = None
    max_dependencies: int | None = None
    exclude_names: set[str] = field(default_factory=set)

class ModelFilter:
    """Filtra modelos baseado em criterios diversos."""

    def __init__(self, graph: GraphResolver) -> None:
        self.graph = graph

    def apply_filter(self, criteria: FilterCriteria) -> set[str]:
        """Aplica filtro e retorna nomes de modelos que atendem aos criterios."""
        result = set(self.graph.graph.nodes())

        if criteria.node_types:
            result = {
                n for n in result
                if self.graph.graph.nodes[n].get("node_type") in criteria.node_types
            }

        if criteria.tags:
            result = {
                n for n in result
                if set(self.graph.graph.nodes[n].get("tags", [])) & criteria.tags
            }

        if criteria.name_pattern:
            pattern = re.compile(criteria.name_pattern)
            result = {n for n in result if pattern.search(n)}

        if criteria.paths:
            result = {
                n for n in result
                if any(
                    p in (self.graph.graph.nodes[n].get("filepath") or "")
                    for p in criteria.paths
                )
            }

        if criteria.materializations:
            result = {
                n for n in result
                if self.graph.graph.nodes[n].get("config", {}).get("materialized")
                in criteria.materializations
            }

        if criteria.min_dependencies is not None:
            result = {
                n for n in result
                if len(self.graph.get_dependencies(n)) >= criteria.min_dependencies
            }

        if criteria.max_dependencies is not None:
            result = {
                n for n in result
                if len(self.graph.get_dependencies(n)) <= criteria.max_dependencies
            }

        if criteria.exclude_names:
            result -= criteria.exclude_names

        return result

    def filter_by_selector(self, selector: str) -> set[str]:
        """Filtra usando sintaxe de seletor dbt-like.

        Exemplos:
        - "stg_*" -> modelos que comecam com stg_
        - "+model_name" -> modelo e suas dependencias upstream
        - "model_name+" -> modelo e seus dependentes downstream
        - "+model_name+" -> upstream e downstream
        - "tag:staging" -> modelos com tag staging
        - "source:raw" -> sources do schema raw
        """
        selector = selector.strip()

        if selector.startswith("tag:"):
            tag = selector[4:]
            return self.apply_filter(FilterCriteria(tags={tag}))

        if selector.startswith("source:"):
            source = selector[7:]
            return self.apply_filter(
                FilterCriteria(node_types={"source"}, name_pattern=f"^{source}\\.")
            )

        upstream = selector.startswith("+")
        downstream = selector.endswith("+")
        name = selector.strip("+")

        result = set()
        if name in self.graph.graph:
            result.add(name)
        else:
            pattern = name.replace("*", ".*")
            matches = self.apply_filter(FilterCriteria(name_pattern=f"^{pattern}$"))
            result.update(matches)

        expanded = set(result)
        for node in result:
            if upstream:
                expanded.update(self.graph.get_all_upstream(node))
            if downstream:
                expanded.update(self.graph.get_all_downstream(node))

        return expanded

    def filter_chain(self, criteria_list: list[FilterCriteria]) -> set[str]:
        """Aplica multiplos filtros em cadeia (intersecao)."""
        if not criteria_list:
            return set(self.graph.graph.nodes())

        result = self.apply_filter(criteria_list[0])
        for criteria in criteria_list[1:]:
            result &= self.apply_filter(criteria)
        return result

