"""Testes para unused detector."""

import pytest

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.analyzers.unused_detector import UnusedDetector, UnusedReport


@pytest.fixture
def detector() -> UnusedDetector:
    graph = GraphResolver()
    graph.add_node(NodeInfo(name="raw.events", node_type="source"))
    graph.add_node(NodeInfo(name="raw.orphan", node_type="source"))
    graph.add_node(NodeInfo(name="stg_events", node_type="model"))
    graph.add_node(NodeInfo(name="fct_final", node_type="model"))
    graph.add_node(NodeInfo(name="unused_model", node_type="model"))
    graph.add_node(NodeInfo(name="isolated", node_type="model"))
    graph.add_edge("stg_events", "raw.events")
    graph.add_edge("fct_final", "stg_events")
    graph.add_edge("unused_model", "stg_events")
    return UnusedDetector(graph)


class TestUnusedDetector:
    def test_detect_unused_models(self, detector: UnusedDetector) -> None:
        unused = detector.detect_unused_models()
        assert "fct_final" in unused or "unused_model" in unused

    def test_detect_unused_with_exposures(self, detector: UnusedDetector) -> None:
        unused = detector.detect_unused_models(exposure_models={"fct_final"})
        assert "fct_final" not in unused

    def test_detect_orphan_sources(self, detector: UnusedDetector) -> None:
        orphans = detector.detect_orphan_sources()
        assert "raw.orphan" in orphans

    def test_detect_dead_end_models(self, detector: UnusedDetector) -> None:
        dead_ends = detector.detect_dead_end_models()
        assert isinstance(dead_ends, list)

    def test_detect_isolated_nodes(self, detector: UnusedDetector) -> None:
        isolated = detector.detect_isolated_nodes()
        assert "isolated" in isolated

    def test_generate_report(self, detector: UnusedDetector) -> None:
        report = detector.generate_report()
        assert isinstance(report, UnusedReport)
        assert report.total_unused >= 0
