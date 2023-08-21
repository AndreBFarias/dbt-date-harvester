"""Testes para lineage tracker."""

import pytest

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.analyzers.lineage_tracker import LineageTracker, LineageEntry


@pytest.fixture
def tracker() -> LineageTracker:
    graph = GraphResolver()
    graph.add_node(NodeInfo(name="raw.events", node_type="source"))
    graph.add_node(NodeInfo(name="raw.dates", node_type="source"))
    graph.add_node(NodeInfo(name="stg_events", node_type="model"))
    graph.add_node(NodeInfo(name="stg_dates", node_type="model"))
    graph.add_node(NodeInfo(name="fct_event_dates", node_type="model"))
    graph.add_edge("stg_events", "raw.events")
    graph.add_edge("stg_dates", "raw.dates")
    graph.add_edge("fct_event_dates", "stg_events")
    graph.add_edge("fct_event_dates", "stg_dates")
    return LineageTracker(graph)


class TestLineageTracker:
    def test_track_ref_lineage(self, tracker: LineageTracker) -> None:
        entry = tracker.track_ref_lineage("fct_event_dates", "stg_events")
        assert entry.source_node == "stg_events"
        assert entry.target_node == "fct_event_dates"
        assert entry.lineage_type == "ref"

    def test_track_source_lineage(self, tracker: LineageTracker) -> None:
        entry = tracker.track_source_lineage("stg_events", "raw", "events")
        assert entry.source_node == "raw.events"
        assert entry.lineage_type == "source"

    def test_get_full_lineage(self, tracker: LineageTracker) -> None:
        lineage = tracker.get_full_lineage("fct_event_dates")
        assert "stg_events" in lineage["upstream"]
        assert "raw.events" in lineage["upstream"]

    def test_get_full_lineage_root(self, tracker: LineageTracker) -> None:
        lineage = tracker.get_full_lineage("raw.events")
        assert lineage["upstream"] == []
        assert "stg_events" in lineage["downstream"]

    def test_track_column_lineage(self, tracker: LineageTracker) -> None:
        tracker.track_column_lineage("fct_event_dates", "event_id", ["stg_events.event_id"])
        result = tracker.get_column_lineage("fct_event_dates")
        assert "event_id" in result

    def test_get_lineage_entries(self, tracker: LineageTracker) -> None:
        tracker.track_ref_lineage("fct_event_dates", "stg_events")
        entries = tracker.get_lineage_entries()
        assert len(entries) == 1

    def test_get_lineage_entries_filtered(self, tracker: LineageTracker) -> None:
        tracker.track_ref_lineage("fct_event_dates", "stg_events")
        tracker.track_ref_lineage("fct_event_dates", "stg_dates")
        entries = tracker.get_lineage_entries("fct_event_dates")
        assert len(entries) == 2

    def test_get_lineage_summary(self, tracker: LineageTracker) -> None:
        tracker.track_ref_lineage("fct_event_dates", "stg_events")
        tracker.track_source_lineage("stg_events", "raw", "events")
        summary = tracker.get_lineage_summary()
        assert summary["total_entries"] == 2
        assert summary["ref_entries"] == 1
        assert summary["source_entries"] == 1
