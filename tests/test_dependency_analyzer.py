"""Testes para dependency analyzer."""

import pytest
from pathlib import Path

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer, DependencyReport
from dbt_parser.parsers.sql_parser import SqlParser


@pytest.fixture
def analyzer(tmp_path: Path) -> DependencyAnalyzer:
    models_dir = tmp_path / "models" / "staging"
    models_dir.mkdir(parents=True)
    marts_dir = tmp_path / "models" / "marts"
    marts_dir.mkdir(parents=True)

    (models_dir / "stg_events.sql").write_text(
        "select * from {{ source('raw', 'events') }}"
    )
    (marts_dir / "fct_event_dates.sql").write_text(
        "select * from {{ ref('stg_events') }} join {{ ref('stg_dates') }}"
    )

    sql_parser = SqlParser(tmp_path)
    sql_parser.parse_all()

    graph = GraphResolver()
    dep = DependencyAnalyzer(graph, sql_parser)
    dep.build_dependency_graph()
    return dep


class TestDependencyAnalyzer:
    def test_build_graph(self, analyzer: DependencyAnalyzer) -> None:
        assert analyzer.graph.node_count() > 0

    def test_analyze_model(self, analyzer: DependencyAnalyzer) -> None:
        report = analyzer.analyze_model("fct_event_dates")
        assert report.model_name == "fct_event_dates"
        assert "stg_events" in report.direct_dependencies

    def test_analyze_all(self, analyzer: DependencyAnalyzer) -> None:
        reports = analyzer.analyze_all()
        assert len(reports) > 0

    def test_find_circular_none(self, analyzer: DependencyAnalyzer) -> None:
        cycles = analyzer.find_circular_dependencies()
        assert cycles == []

    def test_execution_order(self, analyzer: DependencyAnalyzer) -> None:
        order = analyzer.get_execution_order()
        assert len(order) > 0

    def test_most_depended_on(self, analyzer: DependencyAnalyzer) -> None:
        result = analyzer.get_most_depended_on(5)
        assert isinstance(result, list)

    def test_isolated_models(self, analyzer: DependencyAnalyzer) -> None:
        isolated = analyzer.get_isolated_models()
        assert isinstance(isolated, set)
