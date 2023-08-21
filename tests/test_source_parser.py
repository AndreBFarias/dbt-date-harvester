"""Testes para source parser."""

import pytest
import yaml
from pathlib import Path

from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.source_parser import SourceParser, Source, SourceTable


@pytest.fixture
def source_project(tmp_path: Path) -> Path:
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    sources = {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "schema": "raw_data",
                "description": "Dados brutos",
                "tables": [
                    {
                        "name": "events",
                        "description": "Tabela de eventos",
                        "columns": [{"name": "event_id"}],
                    },
                    {
                        "name": "dates",
                        "description": "Tabela de datas",
                    },
                ],
            },
            {
                "name": "external",
                "schema": "ext",
                "tables": [
                    {"name": "api_data"},
                ],
            },
        ],
    }
    with open(models_dir / "sources.yml", "w") as f:
        yaml.dump(sources, f)

    return tmp_path


class TestSourceParser:
    def test_parse_sources(self, source_project: Path) -> None:
        yaml_parser = YamlParser(source_project)
        parser = SourceParser(yaml_parser)
        sources = parser.parse_all(source_project)
        assert len(sources) == 2

    def test_get_source(self, source_project: Path) -> None:
        yaml_parser = YamlParser(source_project)
        parser = SourceParser(yaml_parser)
        parser.parse_all(source_project)
        source = parser.get_source("raw")
        assert source is not None
        assert source.schema == "raw_data"

    def test_get_all_table_names(self, source_project: Path) -> None:
        yaml_parser = YamlParser(source_project)
        parser = SourceParser(yaml_parser)
        parser.parse_all(source_project)
        tables = parser.get_all_table_names()
        assert "raw.events" in tables
        assert "raw.dates" in tables
        assert "external.api_data" in tables

    def test_get_source_table(self, source_project: Path) -> None:
        yaml_parser = YamlParser(source_project)
        parser = SourceParser(yaml_parser)
        parser.parse_all(source_project)
        table = parser.get_source_table("raw", "events")
        assert table is not None
        assert table.name == "events"

    def test_source_not_found(self, source_project: Path) -> None:
        yaml_parser = YamlParser(source_project)
        parser = SourceParser(yaml_parser)
        source = parser.get_source("nonexistent")
        assert source is None
