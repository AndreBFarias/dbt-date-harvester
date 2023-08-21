"""Testes para naming validator."""

import pytest

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.analyzers.graph_resolver import GraphResolver, NodeInfo
from dbt_parser.validators.naming_validator import NamingValidator, NamingConvention
from dbt_parser.validators.model_validator import Severity


@pytest.fixture
def validator() -> NamingValidator:
    graph = GraphResolver()
    graph.add_node(NodeInfo(name="stg_events", node_type="model", filepath="models/staging/stg_events.sql"))
    graph.add_node(NodeInfo(name="bad_staging", node_type="model", filepath="models/staging/bad_staging.sql"))
    graph.add_node(NodeInfo(name="fct_event_dates", node_type="model", filepath="models/marts/fct_event_dates.sql"))
    graph.add_node(NodeInfo(name="raw.events", node_type="source"))
    return NamingValidator(graph)


class TestNamingValidator:
    def test_valid_staging_names(self, validator: NamingValidator) -> None:
        results = validator.validate_model_names()
        bad_results = [r for r in results if r.model_name == "bad_staging"]
        assert len(bad_results) > 0

    def test_valid_marts_names(self, validator: NamingValidator) -> None:
        results = validator.validate_model_names()
        fct_results = [r for r in results if r.model_name == "fct_event_dates"]
        assert len(fct_results) == 0

    def test_validate_column_names(self, validator: NamingValidator) -> None:
        columns = {"stg_events": ["event_id", "EventName", "event_date"]}
        results = validator.validate_column_names(columns)
        bad = [r for r in results if "EventName" in r.message]
        assert len(bad) > 0

    def test_validate_all(self, validator: NamingValidator) -> None:
        results = validator.validate_all()
        assert isinstance(results, list)

    def test_add_convention(self, validator: NamingValidator) -> None:
        initial = len(validator.conventions)
        validator.add_convention(
            NamingConvention(
                name="custom",
                pattern=r"^custom_\w+$",
                description="Custom convention",
                applies_to="model",
            )
        )
        assert len(validator.conventions) == initial + 1

    def test_get_summary(self, validator: NamingValidator) -> None:
        validator.validate_all()
        summary = validator.get_summary()
        assert "total" in summary
