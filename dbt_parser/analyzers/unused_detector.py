"""Detector de modelos nao utilizados em projetos dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

@dataclass
class UnusedReport:
    """Relatorio de modelos nao utilizados."""

    unused_models: list[str] = field(default_factory=list)
    orphan_sources: list[str] = field(default_factory=list)
    dead_end_models: list[str] = field(default_factory=list)
    unused_macros: list[str] = field(default_factory=list)
    total_unused: int = 0

class UnusedDetector:
    """Detecta modelos, sources e macros nao utilizados."""

    def __init__(self, graph: GraphResolver) -> None:
        self.graph = graph

    def detect_unused_models(self, exposure_models: set[str | None] = None) -> list[str]:
        """Detecta modelos sem nenhum dependente (nao expostos)."""
        leaf_nodes = self.graph.get_leaf_nodes()
        exposures = exposure_models or set()

        unused: list[str] = []
        for node in leaf_nodes:
            node_data = dict(self.graph.graph.nodes.get(node, {}))
            if node_data.get("node_type") == "model" and node not in exposures:
                unused.append(node)

        if unused:
            logger.warning("Modelos nao utilizados detectados: %d", len(unused))

        return sorted(unused)

    def detect_orphan_sources(self) -> list[str]:
        """Detecta sources declarados mas nao referenciados por nenhum modelo."""
        orphans: list[str] = []
        for node in self.graph.graph.nodes():
            node_data = dict(self.graph.graph.nodes.get(node, {}))
            if node_data.get("node_type") == "source":
                dependents = self.graph.get_dependents(node)
                if not dependents:
                    orphans.append(node)

        if orphans:
            logger.warning("Sources orfaos detectados: %d", len(orphans))

        return sorted(orphans)

    def detect_dead_end_models(self) -> list[str]:
        """Detecta modelos que nao sao referenciados e nao sao folhas finais."""
        dead_ends: list[str] = []
        for node in self.graph.graph.nodes():
            node_data = dict(self.graph.graph.nodes.get(node, {}))
            if node_data.get("node_type") == "model":
                dependents = self.graph.get_dependents(node)
                dependencies = self.graph.get_dependencies(node)
                if not dependents and dependencies:
                    dead_ends.append(node)
        return sorted(dead_ends)

    def detect_isolated_nodes(self) -> list[str]:
        """Detecta nos completamente isolados (sem dependencias nem dependentes)."""
        isolated: list[str] = []
        for node in self.graph.graph.nodes():
            if (
                self.graph.graph.in_degree(node) == 0
                and self.graph.graph.out_degree(node) == 0
            ):
                isolated.append(node)
        return sorted(isolated)

    def generate_report(
        self,
        exposure_models: set[str | None] = None,
        known_macros: set[str | None] = None,
        used_macros: set[str | None] = None,
    ) -> UnusedReport:
        """Gera relatorio completo de artefatos nao utilizados."""
        unused_models = self.detect_unused_models(exposure_models)
        orphan_sources = self.detect_orphan_sources()
        dead_ends = self.detect_dead_end_models()

        unused_macro_list: list[str] = []
        if known_macros and used_macros:
            unused_macro_list = sorted(known_macros - used_macros)

        total = len(unused_models) + len(orphan_sources) + len(dead_ends) + len(unused_macro_list)

        report = UnusedReport(
            unused_models=unused_models,
            orphan_sources=orphan_sources,
            dead_end_models=dead_ends,
            unused_macros=unused_macro_list,
            total_unused=total,
        )

        logger.info("Relatorio de nao utilizados: %d itens", total)
        return report

