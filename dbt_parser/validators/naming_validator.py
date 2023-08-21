"""Validador de convencoes de nomenclatura para projetos dbt."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver
from dbt_parser.validators.model_validator import Severity, ValidationResult

logger = logging.getLogger(__name__)

@dataclass
class NamingConvention:
    """Convencao de nomenclatura."""

    name: str
    pattern: str
    description: str
    applies_to: str  # "model", "source", "column", "test"
    severity: Severity = Severity.WARNING

DEFAULT_CONVENTIONS = [
    NamingConvention(
        name="staging-prefix",
        pattern=r"^stg_\w+$",
        description="Modelos staging devem ter prefixo 'stg_'",
        applies_to="model",
    ),
    NamingConvention(
        name="marts-prefix",
        pattern=r"^(fct|dim)_\w+$",
        description="Modelos marts devem ter prefixo 'fct_' ou 'dim_'",
        applies_to="model",
    ),
    NamingConvention(
        name="snake-case",
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Nomes devem usar snake_case",
        applies_to="column",
    ),
    NamingConvention(
        name="no-abbreviations",
        pattern=r"^(?!.*\b(tmp|tbl|col|dt|qty|amt|nr|cd)\b).*$",
        description="Evitar abreviacoes comuns",
        applies_to="column",
        severity=Severity.INFO,
    ),
]

class NamingValidator:
    """Valida convencoes de nomenclatura em projetos dbt."""

    def __init__(
        self,
        graph: GraphResolver,
        conventions: list[NamingConvention | None] = None,
    ) -> None:
        self.graph = graph
        self.conventions = conventions or DEFAULT_CONVENTIONS
        self._results: list[ValidationResult] = []

    def validate_model_names(self) -> list[ValidationResult]:
        """Valida nomes de modelos contra convencoes."""
        results: list[ValidationResult] = []

        model_conventions = [c for c in self.conventions if c.applies_to == "model"]

        for name in self.graph.graph.nodes():
            node_data = dict(self.graph.graph.nodes[name])
            if node_data.get("node_type") != "model":
                continue

            filepath = node_data.get("filepath", "")
            is_staging = "staging" in str(filepath) or name.startswith("stg_")
            is_marts = "marts" in str(filepath) or name.startswith(("fct_", "dim_"))

            for convention in model_conventions:
                if convention.name == "staging-prefix" and is_staging:
                    if not re.match(convention.pattern, name):
                        results.append(
                            ValidationResult(
                                rule=convention.name,
                                model_name=name,
                                message=convention.description,
                                severity=convention.severity,
                            )
                        )
                elif convention.name == "marts-prefix" and is_marts:
                    if not re.match(convention.pattern, name):
                        results.append(
                            ValidationResult(
                                rule=convention.name,
                                model_name=name,
                                message=convention.description,
                                severity=convention.severity,
                            )
                        )

        self._results.extend(results)
        return results

    def validate_column_names(
        self, model_columns: dict[str, list[str]]
    ) -> list[ValidationResult]:
        """Valida nomes de colunas contra convencoes."""
        results: list[ValidationResult] = []
        col_conventions = [c for c in self.conventions if c.applies_to == "column"]

        for model_name, columns in model_columns.items():
            for col_name in columns:
                for convention in col_conventions:
                    if not re.match(convention.pattern, col_name):
                        results.append(
                            ValidationResult(
                                rule=convention.name,
                                model_name=model_name,
                                message=f"Coluna '{col_name}': {convention.description}",
                                severity=convention.severity,
                            )
                        )

        self._results.extend(results)
        return results

    def validate_all(
        self, model_columns: dict[str, list[str | None]] = None
    ) -> list[ValidationResult]:
        """Executa todas as validacoes de nomenclatura."""
        self._results.clear()
        self.validate_model_names()
        if model_columns:
            self.validate_column_names(model_columns)
        return self._results.copy()

    def add_convention(self, convention: NamingConvention) -> None:
        """Adiciona convencao customizada."""
        self.conventions.append(convention)

    def get_results(self) -> list[ValidationResult]:
        """Retorna todos os resultados de validacao."""
        return self._results.copy()

    def get_summary(self) -> dict[str, int]:
        """Retorna resumo de validacoes."""
        return {
            "total": len(self._results),
            "errors": sum(1 for r in self._results if r.severity == Severity.ERROR),
            "warnings": sum(1 for r in self._results if r.severity == Severity.WARNING),
            "info": sum(1 for r in self._results if r.severity == Severity.INFO),
        }
