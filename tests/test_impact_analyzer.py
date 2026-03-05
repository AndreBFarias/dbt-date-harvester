"""Testes para impact analyzer."""

import pytest

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.analyzers.impact_analyzer import ImpactAnalyzer, ImpactReport


@pytest.fixture
def impact() -> ImpactAnalyzer:
    graph = GraphResolver()
    graph.add_node(NodeInfo(name="raw.events", node_type="source"))
    graph.add_node(NodeInfo(name="stg_events", node_type="model"))
    graph.add_node(NodeInfo(name="stg_dates", node_type="model"))
    graph.add_node(NodeInfo(name="fct_event_dates", node_type="model"))
    graph.add_node(NodeInfo(name="dim_events", node_type="model"))
    graph.add_edge("stg_events", "raw.events")
    graph.add_edge("fct_event_dates", "stg_events")
    graph.add_edge("fct_event_dates", "stg_dates")
    graph.add_edge("dim_events", "stg_events")
    return ImpactAnalyzer(graph)


class TestImpactAnalyzer:
    def test_analyze_impact(self, impact: ImpactAnalyzer) -> None:
        report = impact.analyze_impact("stg_events")
        assert "fct_event_dates" in report.directly_affected
        assert "dim_events" in report.directly_affected

    def test_impact_of_source(self, impact: ImpactAnalyzer) -> None:
        report = impact.analyze_impact("raw.events")
        assert report.total_affected > 0

    def test_risk_level_low(self, impact: ImpactAnalyzer) -> None:
        report = impact.analyze_impact("fct_event_dates")
        assert report.risk_level == "low"

    def test_combined_impact(self, impact: ImpactAnalyzer) -> None:
        affected = impact.get_combined_impact(["stg_events", "stg_dates"])
        assert isinstance(affected, set)

    def test_find_high_impact(self, impact: ImpactAnalyzer) -> None:
        results = impact.find_high_impact_models(threshold=1)
        assert len(results) > 0

    def test_critical_path(self, impact: ImpactAnalyzer) -> None:
        path = impact.get_critical_path()
        assert isinstance(path, list)

    def test_impact_summary(self, impact: ImpactAnalyzer) -> None:
        summary = impact.get_impact_summary()
        assert "total_nodes" in summary
        assert summary["total_nodes"] == 5
