"""Testes para ref resolver."""

import pytest
import yaml
from pathlib import Path

from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.sql_parser import SqlParser
from dbt_parser.parsers.schema_extractor import SchemaExtractor
from dbt_parser.analyzers.ref_resolver import RefResolver


@pytest.fixture
def resolver(tmp_path: Path) -> RefResolver:
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    schema = {
        "version": 2,
        "models": [
            {"name": "stg_events"},
            {"name": "stg_dates"},
        ],
        "sources": [
            {
                "name": "raw",
                "tables": [
                    {"name": "events"},
                    {"name": "dates"},
                ],
            }
        ],
    }
    with open(models_dir / "schema.yml", "w") as f:
        yaml.dump(schema, f)

    (models_dir / "stg_events.sql").write_text(
        "select * from {{ source('raw', 'events') }}"
    )
    (models_dir / "stg_dates.sql").write_text(
        "select * from {{ source('raw', 'dates') }}"
    )
    (models_dir / "fct_final.sql").write_text(
        "select * from {{ ref('stg_events') }} join {{ ref('stg_dates') }} join {{ ref('missing_model') }}"
    )

    yaml_parser = YamlParser(tmp_path)
    sql_parser = SqlParser(tmp_path)
    extractor = SchemaExtractor(yaml_parser)

    content = yaml_parser.parse_file(models_dir / "schema.yml")
    extractor.extract_models(content)
    extractor.extract_sources(content)
    sql_parser.parse_all()

    return RefResolver(sql_parser, extractor)


class TestRefResolver:
    def test_resolve_all(self, resolver: RefResolver) -> None:
        resolved, unresolved = resolver.resolve_all()
        assert len(resolved) > 0

    def test_unresolved_refs(self, resolver: RefResolver) -> None:
        resolved, unresolved = resolver.resolve_all()
        unresolved_names = [r.target_model for r in unresolved]
        assert "missing_model" in unresolved_names

    def test_resolution_summary(self, resolver: RefResolver) -> None:
        resolver.resolve_all()
        summary = resolver.get_resolution_summary()
        assert summary["total_refs"] > 0
        assert summary["unresolved"] > 0

    def test_models_with_broken_refs(self, resolver: RefResolver) -> None:
        resolver.resolve_all()
        broken = resolver.get_models_with_broken_refs()
        assert "fct_final" in broken

    def test_get_resolved(self, resolver: RefResolver) -> None:
        resolver.resolve_all()
        resolved = resolver.get_resolved()
        assert len(resolved) > 0
