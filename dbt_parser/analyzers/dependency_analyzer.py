"""Analisador de dependencias para projetos dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver
from dbt_parser.parsers.sql_parser import SqlParser

logger = logging.getLogger(__name__)

@dataclass
class DependencyReport:
    """Relatorio de dependencias de um modelo."""

    model_name: str
    direct_dependencies: list[str] = field(default_factory=list)
    transitive_dependencies: list[str] = field(default_factory=list)
    direct_dependents: list[str] = field(default_factory=list)
    transitive_dependents: list[str] = field(default_factory=list)
    depth: int = 0
    is_root: bool = False
    is_leaf: bool = False

class DependencyAnalyzer:
    """Analisa dependencias entre modelos dbt."""

    def __init__(
        self, graph_resolver: GraphResolver, sql_parser: SqlParser
    ) -> None:
        self.graph = graph_resolver
        self.sql_parser = sql_parser

    def build_dependency_graph(self) -> None:
        """Constroi grafo de dependencias a partir dos modelos SQL parseados."""
        from dbt_parser.analyzers.graph_resolver import NodeInfo

        all_refs = self.sql_parser.get_all_refs()
        all_sources = self.sql_parser.get_all_sources()

        for model_name in self.sql_parser._parsed_models:
            model = self.sql_parser.get_model(model_name)
            node = NodeInfo(
                name=model_name,
                node_type="model",
                filepath=str(model.filepath) if model else None,
                config=model.config if model else {},
            )
            self.graph.add_node(node)

        for model_name, refs in all_refs.items():
            for ref in refs:
                if ref not in self.graph._nodes:
                    self.graph.add_node(NodeInfo(name=ref, node_type="model"))
                self.graph.add_edge(model_name, ref)

        for model_name, sources in all_sources.items():
            for source_name, table_name in sources:
                source_key = f"{source_name}.{table_name}"
                if source_key not in self.graph._nodes:
                    self.graph.add_node(
                        NodeInfo(name=source_key, node_type="source")
                    )
                self.graph.add_edge(model_name, source_key)

        logger.info(
            "Grafo construido: %d nos, %d arestas",
            self.graph.node_count(),
            self.graph.edge_count(),
        )

    def analyze_model(self, model_name: str) -> DependencyReport:
        """Analisa dependencias de um modelo especifico."""
        direct_deps = sorted(self.graph.get_dependencies(model_name))
        transitive_deps = sorted(self.graph.get_all_upstream(model_name))
        direct_dependents = sorted(self.graph.get_dependents(model_name))
        transitive_dependents = sorted(self.graph.get_all_downstream(model_name))

        return DependencyReport(
            model_name=model_name,
            direct_dependencies=direct_deps,
            transitive_dependencies=transitive_deps,
            direct_dependents=direct_dependents,
            transitive_dependents=transitive_dependents,
            depth=len(transitive_deps),
            is_root=model_name in self.graph.get_root_nodes(),
            is_leaf=model_name in self.graph.get_leaf_nodes(),
        )

    def analyze_all(self) -> list[DependencyReport]:
        """Analisa dependencias de todos os modelos."""
        reports: list[DependencyReport] = []
        for node_name in self.graph.graph.nodes():
            reports.append(self.analyze_model(node_name))
        return reports

    def find_circular_dependencies(self) -> list[list[str]]:
        """Detecta dependencias circulares."""
        return self.graph.detect_cycles()

    def get_execution_order(self) -> list[str]:
        """Retorna ordem de execucao respeitando dependencias."""
        return self.graph.topological_sort()

    def get_most_depended_on(self, top_n: int = 10) -> list[tuple[str, int]]:
        """Retorna modelos com mais dependentes."""
        counts: list[tuple[str, int]] = []
        for node in self.graph.graph.nodes():
            dependents = len(self.graph.get_dependents(node))
            counts.append((node, dependents))
        return sorted(counts, key=lambda x: x[1], reverse=True)[:top_n]

    def get_most_dependencies(self, top_n: int = 10) -> list[tuple[str, int]]:
        """Retorna modelos com mais dependencias."""
        counts: list[tuple[str, int]] = []
        for node in self.graph.graph.nodes():
            deps = len(self.graph.get_dependencies(node))
            counts.append((node, deps))
        return sorted(counts, key=lambda x: x[1], reverse=True)[:top_n]

    def get_isolated_models(self) -> set[str]:
        """Retorna modelos sem dependencias e sem dependentes."""
        isolated = set()
        for node in self.graph.graph.nodes():
            if (
                self.graph.graph.in_degree(node) == 0
                and self.graph.graph.out_degree(node) == 0
            ):
                isolated.add(node)
        return isolated

