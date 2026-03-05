"""Testes para exporters."""

import json
import pytest
from pathlib import Path

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.exporters.json_exporter import JsonExporter
from dbt_parser.exporters.graphviz_exporter import GraphvizExporter
from dbt_parser.exporters.mermaid_exporter import MermaidExporter


@pytest.fixture
def graph() -> GraphResolver:
    g = GraphResolver()
    g.add_node(NodeInfo(name="raw.events", node_type="source"))
    g.add_node(NodeInfo(name="stg_events", node_type="model"))
    g.add_node(NodeInfo(name="fct_events", node_type="model"))
    g.add_edge("stg_events", "raw.events")
    g.add_edge("fct_events", "stg_events")
    return g


class TestJsonExporter:
    def test_export_graph(self, graph: GraphResolver) -> None:
        exporter = JsonExporter()
        result = exporter.export_graph(graph)
        assert len(result["nodes"]) == 3
        assert len(result["edges"]) == 2

    def test_export_to_string(self, graph: GraphResolver) -> None:
        exporter = JsonExporter()
        data = exporter.export_graph(graph)
        json_str = exporter.export_to_string(data)
        parsed = json.loads(json_str)
        assert "nodes" in parsed

    def test_export_to_file(self, graph: GraphResolver, tmp_path: Path) -> None:
        exporter = JsonExporter()
        data = exporter.export_graph(graph)
        filepath = tmp_path / "output.json"
        exporter.export_to_file(data, filepath)
        assert filepath.exists()
        content = json.loads(filepath.read_text())
        assert len(content["nodes"]) == 3


class TestGraphvizExporter:
    def test_to_dot(self, graph: GraphResolver) -> None:
        exporter = GraphvizExporter(graph)
        dot = exporter.to_dot()
        assert "digraph" in dot
        assert "stg_events" in dot

    def test_dot_with_highlight(self, graph: GraphResolver) -> None:
        exporter = GraphvizExporter(graph)
        dot = exporter.to_dot(highlight_nodes={"stg_events"})
        assert "red" in dot

    def test_dot_with_layers(self, graph: GraphResolver) -> None:
        exporter = GraphvizExporter(graph)
        dot = exporter.to_dot_with_layers()
        assert "subgraph" in dot

    def test_export_to_file(self, graph: GraphResolver, tmp_path: Path) -> None:
        exporter = GraphvizExporter(graph)
        filepath = tmp_path / "graph.dot"
        exporter.export_to_file(filepath)
        assert filepath.exists()


class TestMermaidExporter:
    def test_to_mermaid(self, graph: GraphResolver) -> None:
        exporter = MermaidExporter(graph)
        mermaid = exporter.to_mermaid()
        assert "graph LR" in mermaid

    def test_mermaid_with_title(self, graph: GraphResolver) -> None:
        exporter = MermaidExporter(graph)
        mermaid = exporter.to_mermaid(title="Test Graph")
        assert "Test Graph" in mermaid

    def test_mermaid_with_subgraphs(self, graph: GraphResolver) -> None:
        exporter = MermaidExporter(graph)
        mermaid = exporter.to_mermaid_with_subgraphs()
        assert "subgraph" in mermaid

    def test_mermaid_lineage(self, graph: GraphResolver) -> None:
        exporter = MermaidExporter(graph)
        mermaid = exporter.to_mermaid_lineage("stg_events")
        assert "stg_events" in mermaid

    def test_export_to_file(self, graph: GraphResolver, tmp_path: Path) -> None:
        exporter = MermaidExporter(graph)
        filepath = tmp_path / "graph.mmd"
        exporter.export_to_file(filepath)
        assert filepath.exists()
