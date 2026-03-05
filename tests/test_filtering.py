"""Testes para filtering avancado."""

import pytest

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.utils.filtering import ModelFilter, FilterCriteria


@pytest.fixture
def model_filter() -> ModelFilter:
    graph = GraphResolver()
    graph.add_node(NodeInfo(name="raw.events", node_type="source"))
    graph.add_node(NodeInfo(name="stg_events", node_type="model", tags=["staging"]))
    graph.add_node(NodeInfo(name="stg_dates", node_type="model", tags=["staging"]))
    graph.add_node(NodeInfo(name="fct_final", node_type="model", tags=["marts"]))
    graph.add_edge("stg_events", "raw.events")
    graph.add_edge("fct_final", "stg_events")
    graph.add_edge("fct_final", "stg_dates")
    return ModelFilter(graph)


class TestModelFilter:
    def test_filter_by_type(self, model_filter: ModelFilter) -> None:
        result = model_filter.apply_filter(FilterCriteria(node_types={"model"}))
        assert "raw.events" not in result

    def test_filter_by_tag(self, model_filter: ModelFilter) -> None:
        result = model_filter.apply_filter(FilterCriteria(tags={"staging"}))
        assert "stg_events" in result
        assert "fct_final" not in result

    def test_filter_by_pattern(self, model_filter: ModelFilter) -> None:
        result = model_filter.apply_filter(FilterCriteria(name_pattern=r"^stg_"))
        assert len(result) == 2

    def test_filter_exclude(self, model_filter: ModelFilter) -> None:
        result = model_filter.apply_filter(FilterCriteria(exclude_names={"stg_events"}))
        assert "stg_events" not in result

    def test_selector_upstream(self, model_filter: ModelFilter) -> None:
        result = model_filter.filter_by_selector("+fct_final")
        assert "stg_events" in result

    def test_selector_downstream(self, model_filter: ModelFilter) -> None:
        result = model_filter.filter_by_selector("raw.events+")
        assert "stg_events" in result

    def test_selector_tag(self, model_filter: ModelFilter) -> None:
        result = model_filter.filter_by_selector("tag:staging")
        assert len(result) == 2

    def test_filter_chain(self, model_filter: ModelFilter) -> None:
        criteria = [
            FilterCriteria(node_types={"model"}),
            FilterCriteria(name_pattern=r"^stg_"),
        ]
        result = model_filter.filter_chain(criteria)
        assert len(result) == 2
