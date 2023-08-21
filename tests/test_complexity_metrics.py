"""Testes para complexity metrics."""

import pytest
from pathlib import Path

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.parsers.sql_parser import SqlParser
from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.analyzers.complexity_metrics import ComplexityMetrics, ModelComplexity


@pytest.fixture
def metrics(tmp_path: Path) -> ComplexityMetrics:
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    simple_sql = """select event_id, event_name from {{ source('raw', 'events') }}"""
    (models_dir / "simple_model.sql").write_text(simple_sql)

    complex_sql = """{{
  config(materialized='table')
}}
with source as (
    select * from {{ source('raw', 'events') }}
),
dates as (
    select * from {{ ref('stg_dates') }}
),
joined as (
    select s.*, d.year, d.month
    from source s
    left join dates d on s.event_date = d.date_value
    inner join {{ ref('stg_types') }} t on s.event_type = t.type_id
)
select * from joined"""
    (models_dir / "complex_model.sql").write_text(complex_sql)

    sql_parser = SqlParser(tmp_path)
    sql_parser.parse_all()

    graph = GraphResolver()
    graph.add_node(NodeInfo(name="simple_model", node_type="model"))
    graph.add_node(NodeInfo(name="complex_model", node_type="model"))

    return ComplexityMetrics(sql_parser, graph)


class TestComplexityMetrics:
    def test_simple_model_complexity(self, metrics: ComplexityMetrics) -> None:
        result = metrics.calculate_model_complexity("simple_model")
        assert result.model_name == "simple_model"
        assert result.complexity_score >= 0

    def test_complex_model_higher_score(self, metrics: ComplexityMetrics) -> None:
        simple = metrics.calculate_model_complexity("simple_model")
        complex_ = metrics.calculate_model_complexity("complex_model")
        assert complex_.complexity_score > simple.complexity_score

    def test_calculate_all(self, metrics: ComplexityMetrics) -> None:
        results = metrics.calculate_all()
        assert len(results) == 2
        assert results[0].complexity_score >= results[1].complexity_score

    def test_most_complex(self, metrics: ComplexityMetrics) -> None:
        top = metrics.get_most_complex(1)
        assert len(top) == 1

    def test_complexity_summary(self, metrics: ComplexityMetrics) -> None:
        summary = metrics.get_complexity_summary()
        assert "total_models" in summary
        assert summary["total_models"] == 2

    def test_nonexistent_model(self, metrics: ComplexityMetrics) -> None:
        result = metrics.calculate_model_complexity("nonexistent")
        assert result.complexity_score == 0.0
