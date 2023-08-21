"""Validador de modelos dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from dbt_parser.parsers.schema_extractor import ModelInfo, SchemaExtractor
from dbt_parser.parsers.sql_parser import SqlParser

logger = logging.getLogger(__name__)

class Severity(Enum):
    """Severidade de uma violacao."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

@dataclass
class ValidationResult:
    """Resultado de uma validacao."""

    rule: str
    model_name: str
    message: str
    severity: Severity = Severity.WARNING
    filepath: str | None = None

class ModelValidator:
    """Valida modelos dbt contra regras de boas praticas."""

    def __init__(
        self, schema_extractor: SchemaExtractor, sql_parser: SqlParser
    ) -> None:
        self.schema_extractor = schema_extractor
        self.sql_parser = sql_parser
        self._results: list[ValidationResult] = []

    def validate_all(self) -> list[ValidationResult]:
        """Executa todas as validacoes."""
        self._results.clear()
        self._validate_descriptions()
        self._validate_tests()
        self._validate_materializations()
        self._validate_primary_keys()
        return self._results.copy()

    def _validate_descriptions(self) -> None:
        """Valida que todos os modelos tem descricao."""
        for model in self.schema_extractor.get_all_models():
            if not model.description:
                self._results.append(
                    ValidationResult(
                        rule="model-description",
                        model_name=model.name,
                        message=f"Modelo '{model.name}' sem descricao no schema.yml",
                        severity=Severity.WARNING,
                    )
                )
            for col in model.columns:
                if not col.description:
                    self._results.append(
                        ValidationResult(
                            rule="column-description",
                            model_name=model.name,
                            message=f"Coluna '{col.name}' do modelo '{model.name}' sem descricao",
                            severity=Severity.INFO,
                        )
                    )

    def _validate_tests(self) -> None:
        """Valida que colunas criticas tem testes."""
        for model in self.schema_extractor.get_all_models():
            has_pk_test = False
            for col in model.columns:
                test_names = []
                for test in col.tests:
                    if isinstance(test, str):
                        test_names.append(test)
                    elif isinstance(test, dict):
                        test_names.extend(test.keys())

                if "unique" in test_names and "not_null" in test_names:
                    has_pk_test = True

            if not has_pk_test:
                self._results.append(
                    ValidationResult(
                        rule="primary-key-test",
                        model_name=model.name,
                        message=f"Modelo '{model.name}' sem teste de chave primaria (unique + not_null)",
                        severity=Severity.WARNING,
                    )
                )

    def _validate_materializations(self) -> None:
        """Valida configuracoes de materializacao."""
        valid_materializations = {"view", "table", "incremental", "ephemeral"}
        for name, model in self.sql_parser._parsed_models.items():
            materialization = model.config.get("materialized")
            if materialization and materialization not in valid_materializations:
                self._results.append(
                    ValidationResult(
                        rule="valid-materialization",
                        model_name=name,
                        message=f"Materializacao invalida '{materialization}' no modelo '{name}'",
                        severity=Severity.ERROR,
                    )
                )

    def _validate_primary_keys(self) -> None:
        """Valida que modelos tem coluna que parece ser PK."""
        for model in self.schema_extractor.get_all_models():
            has_id_column = any(
                col.name.endswith("_id") or col.name == "id" for col in model.columns
            )
            if not has_id_column and model.columns:
                self._results.append(
                    ValidationResult(
                        rule="primary-key-column",
                        model_name=model.name,
                        message=f"Modelo '{model.name}' parece nao ter coluna de chave primaria",
                        severity=Severity.INFO,
                    )
                )

    def get_results_by_severity(self, severity: Severity) -> list[ValidationResult]:
        """Retorna resultados filtrados por severidade."""
        return [r for r in self._results if r.severity == severity]

    def get_results_by_model(self, model_name: str) -> list[ValidationResult]:
        """Retorna resultados de um modelo especifico."""
        return [r for r in self._results if r.model_name == model_name]

    def get_summary(self) -> dict[str, int]:
        """Retorna resumo de validacoes."""
        return {
            "total": len(self._results),
            "errors": len(self.get_results_by_severity(Severity.ERROR)),
            "warnings": len(self.get_results_by_severity(Severity.WARNING)),
            "info": len(self.get_results_by_severity(Severity.INFO)),
        }
