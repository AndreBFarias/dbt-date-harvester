"""Validador de configuracoes de projetos dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.sql_parser import SqlParser
from dbt_parser.validators.model_validator import Severity, ValidationResult

logger = logging.getLogger(__name__)

REQUIRED_PROJECT_KEYS = {
    "name", "version", "config-version", "profile",
    "model-paths", "seed-paths", "test-paths", "macro-paths",
}

VALID_MATERIALIZATIONS = {"view", "table", "incremental", "ephemeral"}

VALID_CONFIG_KEYS = {
    "materialized", "schema", "tags", "pre_hook", "post_hook",
    "enabled", "persist_docs", "full_refresh", "unique_key",
    "strategy", "updated_at", "check_cols",
}

class ConfigValidator:
    """Valida configuracoes de projeto e modelos dbt."""

    def __init__(
        self, yaml_parser: YamlParser, sql_parser: SqlParser, project_dir: Path
    ) -> None:
        self.yaml_parser = yaml_parser
        self.sql_parser = sql_parser
        self.project_dir = project_dir
        self._results: list[ValidationResult] = []

    def validate_project_yml(self) -> list[ValidationResult]:
        """Valida dbt_project.yml."""
        results: list[ValidationResult] = []
        project_file = self.project_dir / "dbt_project.yml"

        if not project_file.exists():
            results.append(
                ValidationResult(
                    rule="project-yml-exists",
                    model_name="dbt_project.yml",
                    message="Arquivo dbt_project.yml nao encontrado",
                    severity=Severity.ERROR,
                )
            )
            self._results.extend(results)
            return results

        content = self.yaml_parser.parse_file(project_file)
        if not content:
            results.append(
                ValidationResult(
                    rule="project-yml-valid",
                    model_name="dbt_project.yml",
                    message="dbt_project.yml esta vazio ou invalido",
                    severity=Severity.ERROR,
                )
            )
            self._results.extend(results)
            return results

        for key in REQUIRED_PROJECT_KEYS:
            if key not in content:
                results.append(
                    ValidationResult(
                        rule="project-yml-required-key",
                        model_name="dbt_project.yml",
                        message=f"Chave obrigatoria ausente: '{key}'",
                        severity=Severity.WARNING,
                    )
                )

        config_version = content.get("config-version")
        if config_version and config_version != 2:
            results.append(
                ValidationResult(
                    rule="project-yml-config-version",
                    model_name="dbt_project.yml",
                    message=f"config-version deveria ser 2, encontrado: {config_version}",
                    severity=Severity.WARNING,
                )
            )

        self._results.extend(results)
        return results

    def validate_model_configs(self) -> list[ValidationResult]:
        """Valida configuracoes dos modelos SQL."""
        results: list[ValidationResult] = []

        for name, model in self.sql_parser._parsed_models.items():
            materialized = model.config.get("materialized")
            if materialized and materialized not in VALID_MATERIALIZATIONS:
                results.append(
                    ValidationResult(
                        rule="valid-materialization",
                        model_name=name,
                        message=f"Materializacao invalida: '{materialized}'",
                        severity=Severity.ERROR,
                    )
                )

            for key in model.config:
                if key not in VALID_CONFIG_KEYS:
                    results.append(
                        ValidationResult(
                            rule="known-config-key",
                            model_name=name,
                            message=f"Chave de config desconhecida: '{key}'",
                            severity=Severity.INFO,
                        )
                    )

        self._results.extend(results)
        return results

    def validate_directory_structure(self) -> list[ValidationResult]:
        """Valida estrutura de diretorios do projeto dbt."""
        results: list[ValidationResult] = []
        expected_dirs = ["models", "seeds", "tests", "macros"]

        for dir_name in expected_dirs:
            dir_path = self.project_dir / dir_name
            if not dir_path.exists():
                results.append(
                    ValidationResult(
                        rule="directory-exists",
                        model_name=dir_name,
                        message=f"Diretorio esperado nao encontrado: '{dir_name}/'",
                        severity=Severity.INFO,
                    )
                )

        self._results.extend(results)
        return results

    def validate_all(self) -> list[ValidationResult]:
        """Executa todas as validacoes de configuracao."""
        self._results.clear()
        self.validate_project_yml()
        self.validate_model_configs()
        self.validate_directory_structure()
        return self._results.copy()

    def get_results(self) -> list[ValidationResult]:
        """Retorna resultados de validacao."""
        return self._results.copy()

    def get_summary(self) -> dict[str, int]:
        """Retorna resumo."""
        return {
            "total": len(self._results),
            "errors": sum(1 for r in self._results if r.severity == Severity.ERROR),
            "warnings": sum(1 for r in self._results if r.severity == Severity.WARNING),
            "info": sum(1 for r in self._results if r.severity == Severity.INFO),
        }
