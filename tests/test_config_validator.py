"""Testes para config validator."""

import pytest
import yaml
from pathlib import Path

from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.sql_parser import SqlParser
from dbt_parser.validators.config_validator import ConfigValidator
from dbt_parser.validators.model_validator import Severity


@pytest.fixture
def valid_project(tmp_path: Path) -> Path:
    models_dir = tmp_path / "models"
    seeds_dir = tmp_path / "seeds"
    tests_dir = tmp_path / "tests"
    macros_dir = tmp_path / "macros"
    for d in [models_dir, seeds_dir, tests_dir, macros_dir]:
        d.mkdir()

    project = {
        "name": "test_project",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "test",
        "model-paths": ["models"],
        "seed-paths": ["seeds"],
        "test-paths": ["tests"],
        "macro-paths": ["macros"],
    }
    with open(tmp_path / "dbt_project.yml", "w") as f:
        yaml.dump(project, f)

    (models_dir / "stg_test.sql").write_text(
        "{{ config(materialized='view') }}\nselect 1"
    )
    return tmp_path


@pytest.fixture
def invalid_project(tmp_path: Path) -> Path:
    project = {"name": "bad_project", "version": "0.1.0"}
    with open(tmp_path / "dbt_project.yml", "w") as f:
        yaml.dump(project, f)

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "bad_model.sql").write_text(
        "{{ config(materialized='nonexistent', custom_key='val') }}\nselect 1"
    )
    return tmp_path


class TestConfigValidator:
    def test_valid_project(self, valid_project: Path) -> None:
        yaml_p = YamlParser(valid_project)
        sql_p = SqlParser(valid_project)
        sql_p.parse_all()
        validator = ConfigValidator(yaml_p, sql_p, valid_project)
        results = validator.validate_all()
        errors = [r for r in results if r.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_missing_keys(self, invalid_project: Path) -> None:
        yaml_p = YamlParser(invalid_project)
        sql_p = SqlParser(invalid_project)
        sql_p.parse_all()
        validator = ConfigValidator(yaml_p, sql_p, invalid_project)
        results = validator.validate_project_yml()
        assert len(results) > 0

    def test_invalid_materialization(self, invalid_project: Path) -> None:
        yaml_p = YamlParser(invalid_project)
        sql_p = SqlParser(invalid_project)
        sql_p.parse_all()
        validator = ConfigValidator(yaml_p, sql_p, invalid_project)
        results = validator.validate_model_configs()
        errors = [r for r in results if r.severity == Severity.ERROR]
        assert len(errors) > 0

    def test_missing_project_yml(self, tmp_path: Path) -> None:
        yaml_p = YamlParser(tmp_path)
        sql_p = SqlParser(tmp_path)
        validator = ConfigValidator(yaml_p, sql_p, tmp_path)
        results = validator.validate_project_yml()
        assert any(r.severity == Severity.ERROR for r in results)

    def test_directory_structure(self, valid_project: Path) -> None:
        yaml_p = YamlParser(valid_project)
        sql_p = SqlParser(valid_project)
        validator = ConfigValidator(yaml_p, sql_p, valid_project)
        results = validator.validate_directory_structure()
        assert len(results) == 0

    def test_missing_dirs(self, tmp_path: Path) -> None:
        yaml_p = YamlParser(tmp_path)
        sql_p = SqlParser(tmp_path)
        validator = ConfigValidator(yaml_p, sql_p, tmp_path)
        results = validator.validate_directory_structure()
        assert len(results) == 4

    def test_get_summary(self, invalid_project: Path) -> None:
        yaml_p = YamlParser(invalid_project)
        sql_p = SqlParser(invalid_project)
        sql_p.parse_all()
        validator = ConfigValidator(yaml_p, sql_p, invalid_project)
        validator.validate_all()
        summary = validator.get_summary()
        assert "total" in summary
        assert summary["total"] > 0
