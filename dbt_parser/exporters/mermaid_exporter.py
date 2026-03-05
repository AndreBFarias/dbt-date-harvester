"""Exportador de grafos para formato Mermaid."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

MERMAID_SHAPES = {
    "model": ("[", "]"),
    "source": ("[(", ")]"),
    "seed": ("{{", "}}"),
    "test": ("{", "}"),
}

class MermaidExporter:
    """Exporta grafos de dependencia para formato Mermaid."""

    def __init__(self, graph: GraphResolver) -> None:
        self.graph = graph

    def to_mermaid(
        self,
        direction: str = "LR",
        title: str | None = None,
        highlight_nodes: set[str | None] = None,
    ) -> str:
        """Gera diagrama Mermaid do grafo."""
        lines: list[str] = []

        if title:
            lines.append(f"---")
            lines.append(f"title: {title}")
            lines.append(f"---")

        lines.append(f"graph {direction}")

        for name in sorted(self.graph.graph.nodes()):
            node_data = dict(self.graph.graph.nodes[name])
            node_type = node_data.get("node_type", "model")
            left, right = MERMAID_SHAPES.get(node_type, ("[", "]"))
            safe_name = self._safe_id(name)
            lines.append(f"    {safe_name}{left}{name}{right}")

        lines.append("")

        for from_node, to_node in sorted(self.graph.graph.edges()):
            safe_from = self._safe_id(from_node)
            safe_to = self._safe_id(to_node)
            lines.append(f"    {safe_from} --> {safe_to}")

        if highlight_nodes:
            lines.append("")
            for node in sorted(highlight_nodes):
                safe = self._safe_id(node)
                lines.append(f"    style {safe} fill:#ff6b6b,stroke:#333,stroke-width:3px")

        return "\n".join(lines)

    def export_to_file(self, filepath: Path, **kwargs: Any) -> None:
        """Exporta diagrama Mermaid para arquivo."""
        content = self.to_mermaid(**kwargs)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info("Mermaid exportado: %s", filepath)

    def to_mermaid_with_subgraphs(self) -> str:
        """Gera Mermaid com subgrafos por tipo."""
        lines: list[str] = ["graph LR"]

        groups: dict[str, list[str]] = {}
        for name in self.graph.graph.nodes():
            node_data = dict(self.graph.graph.nodes[name])
            node_type = node_data.get("node_type", "model")
            if node_type not in groups:
                groups[node_type] = []
            groups[node_type].append(name)

        for group_name, nodes in sorted(groups.items()):
            lines.append(f"    subgraph {group_name}")
            for name in sorted(nodes):
                safe = self._safe_id(name)
                left, right = MERMAID_SHAPES.get(group_name, ("[", "]"))
                lines.append(f"        {safe}{left}{name}{right}")
            lines.append("    end")

        lines.append("")

        for from_node, to_node in sorted(self.graph.graph.edges()):
            safe_from = self._safe_id(from_node)
            safe_to = self._safe_id(to_node)
            lines.append(f"    {safe_from} --> {safe_to}")

        return "\n".join(lines)

    def to_mermaid_lineage(self, model_name: str) -> str:
        """Gera diagrama Mermaid focado na linhagem de um modelo."""
        upstream = self.graph.get_all_upstream(model_name)
        downstream = self.graph.get_all_downstream(model_name)
        relevant = upstream | downstream | {model_name}

        sub = self.graph.get_subgraph(relevant)
        sub_exporter = MermaidExporter(sub)
        return sub_exporter.to_mermaid(highlight_nodes={model_name})

    @staticmethod
    def _safe_id(name: str) -> str:
        """Converte nome para ID seguro para Mermaid."""
        return name.replace(".", "_").replace("-", "_")

