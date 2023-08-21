"""Testes para test extractor."""

import pytest
import yaml
from pathlib import Path

from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.schema_extractor import SchemaExtractor
from dbt_parser.parsers.test_extractor import TestExtractor, DbtTest


@pytest.fixture
def test_project(tmp_path: Path) -> Path:
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()

    schema = {
        "version": 2,
        "models": [
            {
                "name": "stg_events",
                "columns": [
                    {"name": "event_id", "tests": ["unique", "not_null"]},
                    {
                        "name": "event_type",
                        "tests": [
                            {"accepted_values": {"values": ["a", "b", "c"]}}
                        ],
                    },
                ],
            }
        ],
    }
    with open(models_dir / "schema.yml", "w") as f:
        yaml.dump(schema, f)

    (tests_dir / "test_no_orphans.sql").write_text(
        "select * from {{ ref('stg_events') }} where event_id is null"
    )

    return tmp_path


class TestTestExtractor:
    def test_extract_schema_tests(self, test_project: Path) -> None:
        yaml_parser = YamlParser(test_project)
        content = yaml_parser.parse_file(test_project / "models" / "schema.yml")
        extractor = SchemaExtractor(yaml_parser)
        extractor.extract_models(content)
        test_ext = TestExtractor(extractor, test_project)
        tests = test_ext.extract_schema_tests()
        assert len(tests) == 3

    def test_extract_data_tests(self, test_project: Path) -> None:
        yaml_parser = YamlParser(test_project)
        extractor = SchemaExtractor(yaml_parser)
        test_ext = TestExtractor(extractor, test_project)
        tests = test_ext.extract_data_tests()
        assert len(tests) == 1
        assert tests[0].test_type == "data"

    def test_extract_all(self, test_project: Path) -> None:
        yaml_parser = YamlParser(test_project)
        content = yaml_parser.parse_file(test_project / "models" / "schema.yml")
        extractor = SchemaExtractor(yaml_parser)
        extractor.extract_models(content)
        test_ext = TestExtractor(extractor, test_project)
        all_tests = test_ext.extract_all()
        assert len(all_tests) == 4

    def test_tests_by_model(self, test_project: Path) -> None:
        yaml_parser = YamlParser(test_project)
        content = yaml_parser.parse_file(test_project / "models" / "schema.yml")
        extractor = SchemaExtractor(yaml_parser)
        extractor.extract_models(content)
        test_ext = TestExtractor(extractor, test_project)
        test_ext.extract_all()
        model_tests = test_ext.get_tests_by_model("stg_events")
        assert len(model_tests) >= 3

    def test_test_coverage(self, test_project: Path) -> None:
        yaml_parser = YamlParser(test_project)
        content = yaml_parser.parse_file(test_project / "models" / "schema.yml")
        extractor = SchemaExtractor(yaml_parser)
        extractor.extract_models(content)
        test_ext = TestExtractor(extractor, test_project)
        test_ext.extract_all()
        coverage = test_ext.get_test_coverage({"stg_events", "stg_dates"})
        assert coverage["total_models"] == 2
        assert coverage["tested_models"] == 1
