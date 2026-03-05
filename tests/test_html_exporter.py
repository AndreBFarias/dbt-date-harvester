"""Testes para o modulo HtmlExporter."""

import pytest

try:
    import networkx as nx

    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False

pytestmark = pytest.mark.skipif(not HAS_NETWORKX, reason="networkx nao disponivel")


@pytest.fixture
def sample_graph():
    """Cria grafo de teste com nos e arestas."""
    from dbt_parser.analyzers.graph_resolver import NodeInfo

    G = nx.DiGraph()
    nodes_data = [
        NodeInfo(
            name="stg_users",
            node_type="model",
            filepath="models/staging/stg_users.sql",
            config={},
            tags=["staging"],
        ),
        NodeInfo(
            name="stg_orders",
            node_type="model",
            filepath="models/staging/stg_orders.sql",
            config={},
            tags=["staging"],
        ),
        NodeInfo(
            name="fct_orders",
            node_type="model",
            filepath="models/marts/fct_orders.sql",
            config={},
            tags=["marts"],
        ),
        NodeInfo(
            name="raw_users",
            node_type="source",
            filepath="",
            config={},
            tags=[],
        ),
    ]
    for node in nodes_data:
        G.add_node(node.name, info=node)
    G.add_edge("raw_users", "stg_users")
    G.add_edge("stg_users", "fct_orders")
    G.add_edge("stg_orders", "fct_orders")
    return G


class TestHtmlExporter:
    def test_to_html_returns_string(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        result = exporter.to_html()
        assert isinstance(result, str)
        assert "<!DOCTYPE html>" in result

    def test_to_html_contains_nodes(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        result = exporter.to_html()
        assert "stg_users" in result
        assert "fct_orders" in result

    def test_to_html_contains_edges(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        result = exporter.to_html()
        assert "raw_users" in result

    def test_to_html_custom_title(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        result = exporter.to_html(title="Meu Grafo dbt")
        assert "Meu Grafo dbt" in result

    def test_export_to_file(self, sample_graph, tmp_path):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        output = tmp_path / "graph.html"
        result = exporter.export_to_file(output)
        assert result.exists()
        content = result.read_text()
        assert "<!DOCTYPE html>" in content

    def test_extract_nodes_count(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        nodes = exporter._extract_nodes()
        assert len(nodes) == 4

    def test_extract_edges_count(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        edges = exporter._extract_edges()
        assert len(edges) == 3

    def test_calculate_stats(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        nodes = exporter._extract_nodes()
        edges = exporter._extract_edges()
        stats = exporter._calculate_stats(nodes, edges)
        assert stats["total_nodes"] == 4
        assert stats["total_edges"] == 3
        assert stats["model"] == 3
        assert stats["source"] == 1

    def test_html_escapes_title(self, sample_graph):
        from dbt_parser.exporters.html_exporter import HtmlExporter

        exporter = HtmlExporter(sample_graph)
        result = exporter.to_html(title='<script>alert("xss")</script>')
        assert "<script>alert" not in result
        assert "&lt;script&gt;" in result


# "Nao e porque as coisas sao dificeis que nao ousamos; e porque nao ousamos que sao dificeis." -- Seneca
