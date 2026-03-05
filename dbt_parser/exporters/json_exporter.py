"""Exportador de resultados para formato JSON."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver
from dbt_parser.analyzers.dependency_analyzer import DependencyReport

logger = logging.getLogger(__name__)

class DataclassEncoder(json.JSONEncoder):
    """Encoder JSON para dataclasses e tipos especiais."""

    def default(self, obj: Any) -> Any:
        if is_dataclass(obj) and not isinstance(obj, type):
            return asdict(obj)
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, set):
            return sorted(list(obj))
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class JsonExporter:
    """Exporta dados de analise dbt para JSON."""

    def __init__(self, indent: int = 2) -> None:
        self.indent = indent

    def export_graph(self, graph: GraphResolver) -> dict[str, Any]:
        """Exporta grafo de dependencias para estrutura JSON."""
        nodes: list[dict[str, Any]] = []
        for name in graph.graph.nodes():
            node_data = dict(graph.graph.nodes[name])
            node_data["name"] = name
            nodes.append(node_data)

        edges: list[dict[str, str]] = []
        for from_node, to_node in graph.graph.edges():
            edges.append({"from": from_node, "to": to_node})

        return {
            "metadata": {
                "node_count": graph.node_count(),
                "edge_count": graph.edge_count(),
                "exported_at": datetime.now().isoformat(),
            },
            "nodes": nodes,
            "edges": edges,
            "root_nodes": sorted(graph.get_root_nodes()),
            "leaf_nodes": sorted(graph.get_leaf_nodes()),
        }

    def export_dependency_reports(
        self, reports: list[DependencyReport]
    ) -> list[dict[str, Any]]:
        """Exporta relatorios de dependencia para JSON."""
        return [asdict(r) for r in reports]

    def export_to_file(self, data: Any, filepath: Path) -> None:
        """Exporta dados para arquivo JSON."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=self.indent, cls=DataclassEncoder, ensure_ascii=False)
        logger.info("Exportado para: %s", filepath)

    def export_to_string(self, data: Any) -> str:
        """Exporta dados para string JSON."""
        return json.dumps(data, indent=self.indent, cls=DataclassEncoder, ensure_ascii=False)

    def export_project_summary(
        self,
        graph: GraphResolver,
        reports: list[DependencyReport],
        validation_summary: dict[str, int | None] = None,
    ) -> dict[str, Any]:
        """Exporta resumo completo do projeto."""
        summary: dict[str, Any] = {
            "graph": self.export_graph(graph),
            "dependency_reports": self.export_dependency_reports(reports),
        }
        if validation_summary:
            summary["validation"] = validation_summary
        return summary

