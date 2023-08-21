"""Testes para o graph resolver."""

import pytest

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo


@pytest.fixture
def graph() -> GraphResolver:
    """Cria grafo de teste."""
    g = GraphResolver()
    g.add_node(NodeInfo(name="raw.events", node_type="source"))
    g.add_node(NodeInfo(name="raw.dates", node_type="source"))
    g.add_node(NodeInfo(name="stg_events", node_type="model"))
    g.add_node(NodeInfo(name="stg_dates", node_type="model"))
    g.add_node(NodeInfo(name="fct_event_dates", node_type="model"))

    g.add_edge("stg_events", "raw.events")
    g.add_edge("stg_dates", "raw.dates")
    g.add_edge("fct_event_dates", "stg_events")
    g.add_edge("fct_event_dates", "stg_dates")
    return g


class TestGraphResolver:
    """Testes para GraphResolver."""

    def test_node_count(self, graph: GraphResolver) -> None:
        assert graph.node_count() == 5

    def test_edge_count(self, graph: GraphResolver) -> None:
        assert graph.edge_count() == 4

    def test_get_dependencies(self, graph: GraphResolver) -> None:
        deps = graph.get_dependencies("fct_event_dates")
        assert deps == {"stg_events", "stg_dates"}

    def test_get_dependents(self, graph: GraphResolver) -> None:
        dependents = graph.get_dependents("stg_events")
        assert dependents == {"fct_event_dates"}

    def test_get_all_upstream(self, graph: GraphResolver) -> None:
        upstream = graph.get_all_upstream("fct_event_dates")
        assert upstream == {"stg_events", "stg_dates", "raw.events", "raw.dates"}

    def test_get_all_downstream(self, graph: GraphResolver) -> None:
        downstream = graph.get_all_downstream("raw.events")
        assert downstream == {"stg_events", "fct_event_dates"}

    def test_topological_sort(self, graph: GraphResolver) -> None:
        order = graph.topological_sort()
        assert len(order) == 5
        assert order.index("raw.events") < order.index("stg_events")
        assert order.index("stg_events") < order.index("fct_event_dates")

    def test_detect_cycles_no_cycles(self, graph: GraphResolver) -> None:
        cycles = graph.detect_cycles()
        assert cycles == []

    def test_detect_cycles_with_cycle(self) -> None:
        g = GraphResolver()
        g.add_node(NodeInfo(name="a", node_type="model"))
        g.add_node(NodeInfo(name="b", node_type="model"))
        g.add_edge("a", "b")
        g.add_edge("b", "a")
        cycles = g.detect_cycles()
        assert len(cycles) > 0

    def test_get_root_nodes(self, graph: GraphResolver) -> None:
        roots = graph.get_root_nodes()
        assert roots == {"raw.events", "raw.dates"}

    def test_get_leaf_nodes(self, graph: GraphResolver) -> None:
        leaves = graph.get_leaf_nodes()
        assert leaves == {"fct_event_dates"}

    def test_get_node_info(self, graph: GraphResolver) -> None:
        info = graph.get_node_info("stg_events")
        assert info is not None
        assert info.node_type == "model"

    def test_get_node_info_not_found(self, graph: GraphResolver) -> None:
        info = graph.get_node_info("nonexistent")
        assert info is None

    def test_get_subgraph(self, graph: GraphResolver) -> None:
        sub = graph.get_subgraph({"stg_events", "raw.events"})
        assert sub.node_count() == 2

    def test_empty_graph(self) -> None:
        g = GraphResolver()
        assert g.node_count() == 0
        assert g.edge_count() == 0
        assert g.get_root_nodes() == set()

    def test_dependencies_nonexistent(self, graph: GraphResolver) -> None:
        deps = graph.get_dependencies("nonexistent")
        assert deps == set()

    def test_upstream_nonexistent(self, graph: GraphResolver) -> None:
        upstream = graph.get_all_upstream("nonexistent")
        assert upstream == set()
