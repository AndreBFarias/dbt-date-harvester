"""Testes para o parser YAML."""

import tempfile
from pathlib import Path

import pytest
import yaml

from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.schema_extractor import SchemaExtractor, ColumnInfo, ModelInfo


@pytest.fixture
def tmp_project(tmp_path: Path) -> Path:
    """Cria projeto dbt temporario."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()

    schema = {
        "version": 2,
        "models": [
            {
                "name": "stg_events",
                "description": "Staging model para eventos",
                "columns": [
                    {
                        "name": "event_id",
                        "description": "Identificador unico do evento",
                        "tests": ["unique", "not_null"],
                    },
                    {
                        "name": "event_name",
                        "description": "Nome do evento",
                    },
                    {
                        "name": "event_type",
                        "description": "Tipo do evento",
                        "tests": [
                            {
                                "accepted_values": {
                                    "values": ["conference", "workshop", "webinar"]
                                }
                            }
                        ],
                    },
                ],
            },
            {
                "name": "stg_dates",
                "description": "Staging model para datas",
                "columns": [
                    {
                        "name": "date_key",
                        "description": "Chave da data",
                        "tests": ["unique", "not_null"],
                    },
                ],
            },
        ],
    }

    with open(models_dir / "schema.yml", "w") as f:
        yaml.dump(schema, f)

    sources = {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "schema": "raw",
                "description": "Dados brutos",
                "tables": [
                    {"name": "events", "description": "Eventos brutos"},
                    {"name": "dates", "description": "Datas brutas"},
                ],
            }
        ],
    }

    with open(models_dir / "sources.yml", "w") as f:
        yaml.dump(sources, f)

    return tmp_path


class TestYamlParser:
    """Testes para YamlParser."""

    def test_parse_file_success(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        result = parser.parse_file(tmp_project / "models" / "schema.yml")
        assert result is not None
        assert "version" in result
        assert result["version"] == 2

    def test_parse_file_not_found(self, tmp_path: Path) -> None:
        parser = YamlParser(tmp_path)
        result = parser.parse_file(tmp_path / "nonexistent.yml")
        assert result == {}

    def test_find_yaml_files(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        files = parser.find_yaml_files()
        assert len(files) == 2
        names = {f.name for f in files}
        assert "schema.yml" in names
        assert "sources.yml" in names

    def test_parse_all(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        results = parser.parse_all()
        assert len(results) == 2

    def test_extract_version(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        content = parser.parse_file(tmp_project / "models" / "schema.yml")
        version = parser.extract_version(content)
        assert version == 2

    def test_extract_models_section(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        content = parser.parse_file(tmp_project / "models" / "schema.yml")
        models = parser.extract_models_section(content)
        assert len(models) == 2
        assert models[0]["name"] == "stg_events"

    def test_extract_sources_section(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        content = parser.parse_file(tmp_project / "models" / "sources.yml")
        sources = parser.extract_sources_section(content)
        assert len(sources) == 1
        assert sources[0]["name"] == "raw"

    def test_get_parsed_files(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        parser.parse_all()
        parsed = parser.get_parsed_files()
        assert len(parsed) == 2

    def test_empty_yaml(self, tmp_path: Path) -> None:
        empty_file = tmp_path / "empty.yml"
        empty_file.write_text("")
        parser = YamlParser(tmp_path)
        result = parser.parse_file(empty_file)
        assert result == {}


class TestSchemaExtractor:
    """Testes para SchemaExtractor."""

    def test_extract_models(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        content = parser.parse_file(tmp_project / "models" / "schema.yml")
        extractor = SchemaExtractor(parser)
        models = extractor.extract_models(content)
        assert len(models) == 2
        assert models[0].name == "stg_events"
        assert len(models[0].columns) == 3

    def test_extract_sources(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        content = parser.parse_file(tmp_project / "models" / "sources.yml")
        extractor = SchemaExtractor(parser)
        sources = extractor.extract_sources(content)
        assert len(sources) == 1
        assert sources[0].name == "raw"
        assert len(sources[0].tables) == 2

    def test_get_model_by_name(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        content = parser.parse_file(tmp_project / "models" / "schema.yml")
        extractor = SchemaExtractor(parser)
        extractor.extract_models(content)
        model = extractor.get_model_by_name("stg_events")
        assert model is not None
        assert model.name == "stg_events"

    def test_get_model_by_name_not_found(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        extractor = SchemaExtractor(parser)
        model = extractor.get_model_by_name("nonexistent")
        assert model is None

    def test_get_column_tests(self, tmp_project: Path) -> None:
        parser = YamlParser(tmp_project)
        content = parser.parse_file(tmp_project / "models" / "schema.yml")
        extractor = SchemaExtractor(parser)
        extractor.extract_models(content)
        tests = extractor.get_column_tests("stg_events")
        assert "event_id" in tests
        assert len(tests["event_id"]) == 2

    def test_column_info_dataclass(self) -> None:
        col = ColumnInfo(name="test_col", description="descricao teste")
        assert col.name == "test_col"
        assert col.tests == []
        assert col.meta == {}

    def test_model_info_dataclass(self) -> None:
        model = ModelInfo(name="test_model")
        assert model.name == "test_model"
        assert model.columns == []
        assert model.tags == []
