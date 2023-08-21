"""Testes para busca fuzzy."""

import pytest

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.utils.search import FuzzySearch, SearchResult


@pytest.fixture
def search() -> FuzzySearch:
    graph = GraphResolver()
    graph.add_node(NodeInfo(name="stg_events", node_type="model", tags=["staging"]))
    graph.add_node(NodeInfo(name="stg_dates", node_type="model", tags=["staging"]))
    graph.add_node(NodeInfo(name="fct_event_dates", node_type="model"))
    graph.add_node(NodeInfo(name="dim_date", node_type="model"))
    graph.add_node(NodeInfo(name="raw.events", node_type="source"))
    return FuzzySearch(graph)


class TestFuzzySearch:
    def test_exact_match(self, search: FuzzySearch) -> None:
        results = search.search("stg_events")
        assert results[0].match_type == "exact"
        assert results[0].score == 1.0

    def test_prefix_match(self, search: FuzzySearch) -> None:
        results = search.search("stg_")
        assert any(r.match_type == "prefix" for r in results)

    def test_contains_match(self, search: FuzzySearch) -> None:
        results = search.search("event")
        assert len(results) > 0

    def test_fuzzy_match(self, search: FuzzySearch) -> None:
        results = search.search("stg_evnts")
        assert len(results) > 0

    def test_no_match(self, search: FuzzySearch) -> None:
        results = search.search("zzzzzzzzzzz")
        assert len(results) == 0 or results[0].score < 0.4

    def test_search_by_tag(self, search: FuzzySearch) -> None:
        results = search.search_by_tag("staging")
        assert "stg_events" in results
        assert "stg_dates" in results

    def test_limit(self, search: FuzzySearch) -> None:
        results = search.search("", limit=2)
        assert len(results) <= 2

    def test_levenshtein(self) -> None:
        assert FuzzySearch._levenshtein_distance("kitten", "sitting") == 3
        assert FuzzySearch._levenshtein_distance("", "abc") == 3
        assert FuzzySearch._levenshtein_distance("abc", "abc") == 0
