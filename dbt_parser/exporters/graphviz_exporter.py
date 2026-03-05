"""Exportador de grafos para formato GraphViz DOT."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

NODE_STYLES = {
    "model": {
        "shape": "box",
        "style": "filled",
        "fillcolor": "#4A90D9",
        "fontcolor": "white",
    },
    "source": {
        "shape": "cylinder",
        "style": "filled",
        "fillcolor": "#7B68EE",
        "fontcolor": "white",
    },
    "seed": {
        "shape": "folder",
        "style": "filled",
        "fillcolor": "#90EE90",
        "fontcolor": "black",
    },
    "test": {
        "shape": "diamond",
        "style": "filled",
        "fillcolor": "#FFD700",
        "fontcolor": "black",
    },
}

class GraphvizExporter:
    """Exporta grafos de dependencia para formato GraphViz DOT."""

    def __init__(self, graph: GraphResolver) -> None:
        self.graph = graph

    def to_dot(
        self,
        title: str = "dbt Dependency Graph",
        highlight_nodes: set[str | None] = None,
        rankdir: str = "LR",
    ) -> str:
        """Gera representacao DOT do grafo."""
        lines: list[str] = []
        lines.append(f'digraph "{title}" {{')
        lines.append(f"  rankdir={rankdir};")
        lines.append('  node [fontname="Helvetica" fontsize=10];')
        lines.append('  edge [color="#666666"];')
        lines.append("")

        for name in sorted(self.graph.graph.nodes()):
            node_data = dict(self.graph.graph.nodes[name])
            node_type = node_data.get("node_type", "model")
            style = NODE_STYLES.get(node_type, NODE_STYLES["model"])

            attrs = dict(style)
            if highlight_nodes and name in highlight_nodes:
                attrs["penwidth"] = "3"
                attrs["color"] = "red"

            label = name.replace(".", "\\n")
            attrs["label"] = label

            attr_str = " ".join(f'{k}="{v}"' for k, v in attrs.items())
            safe_name = name.replace(".", "_")
            lines.append(f'  "{safe_name}" [{attr_str}];')

        lines.append("")

        for from_node, to_node in sorted(self.graph.graph.edges()):
            safe_from = from_node.replace(".", "_")
            safe_to = to_node.replace(".", "_")
            lines.append(f'  "{safe_from}" -> "{safe_to}";')

        lines.append("}")
        return "\n".join(lines)

    def export_to_file(self, filepath: Path, **kwargs: Any) -> None:
        """Exporta grafo para arquivo DOT."""
        dot_content = self.to_dot(**kwargs)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(dot_content)
        logger.info("GraphViz exportado: %s", filepath)

    def to_dot_subgraph(
        self, nodes: set[str], title: str = "Subgraph"
    ) -> str:
        """Gera DOT para um subgrafo."""
        sub = self.graph.get_subgraph(nodes)
        sub_exporter = GraphvizExporter(sub)
        return sub_exporter.to_dot(title=title)

    def to_dot_with_layers(self) -> str:
        """Gera DOT agrupando por tipo de no (source, staging, marts)."""
        lines: list[str] = []
        lines.append('digraph "dbt Layered Graph" {')
        lines.append("  rankdir=LR;")
        lines.append('  node [fontname="Helvetica" fontsize=10];')
        lines.append("")

        groups: dict[str, list[str]] = {}
        for name in self.graph.graph.nodes():
            node_data = dict(self.graph.graph.nodes[name])
            node_type = node_data.get("node_type", "model")
            if node_type not in groups:
                groups[node_type] = []
            groups[node_type].append(name)

        for idx, (group_name, nodes) in enumerate(sorted(groups.items())):
            lines.append(f"  subgraph cluster_{idx} {{")
            lines.append(f'    label="{group_name}";')
            lines.append('    style="dashed";')
            for name in sorted(nodes):
                safe_name = name.replace(".", "_")
                style = NODE_STYLES.get(group_name, NODE_STYLES["model"])
                attr_str = " ".join(f'{k}="{v}"' for k, v in style.items())
                lines.append(f'    "{safe_name}" [{attr_str} label="{name}"];')
            lines.append("  }")
            lines.append("")

        for from_node, to_node in sorted(self.graph.graph.edges()):
            safe_from = from_node.replace(".", "_")
            safe_to = to_node.replace(".", "_")
            lines.append(f'  "{safe_from}" -> "{safe_to}";')

        lines.append("}")
        return "\n".join(lines)

