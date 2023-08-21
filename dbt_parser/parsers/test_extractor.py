"""Extrator de testes dbt (schema tests e data tests)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_parser.parsers.schema_extractor import SchemaExtractor

logger = logging.getLogger(__name__)

@dataclass
class DbtTest:
    """Representacao de um teste dbt."""

    name: str
    test_type: str  # "schema", "data", "custom"
    model_name: str | None = None
    column_name: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    filepath: str | None = None
    severity: str = "error"

class TestExtractor:
    """Extrai e cataloga testes dbt do projeto."""

    BUILTIN_TESTS = {"unique", "not_null", "accepted_values", "relationships"}

    def __init__(self, schema_extractor: SchemaExtractor, project_dir: Path) -> None:
        self.schema_extractor = schema_extractor
        self.project_dir = project_dir
        self._tests: list[DbtTest] = []

    def extract_schema_tests(self) -> list[DbtTest]:
        """Extrai testes definidos no schema.yml."""
        tests: list[DbtTest] = []
        for model in self.schema_extractor.get_all_models():
            for col in model.columns:
                for test in col.tests:
                    if isinstance(test, str):
                        dbt_test = DbtTest(
                            name=test,
                            test_type="schema" if test in self.BUILTIN_TESTS else "custom",
                            model_name=model.name,
                            column_name=col.name,
                        )
                        tests.append(dbt_test)
                    elif isinstance(test, dict):
                        for test_name, test_config in test.items():
                            config = test_config if isinstance(test_config, dict) else {"values": test_config}
                            dbt_test = DbtTest(
                                name=test_name,
                                test_type="schema" if test_name in self.BUILTIN_TESTS else "custom",
                                model_name=model.name,
                                column_name=col.name,
                                config=config,
                            )
                            tests.append(dbt_test)

        self._tests.extend(tests)
        logger.info("Extraidos %d testes de schema", len(tests))
        return tests

    def extract_data_tests(self) -> list[DbtTest]:
        """Extrai testes de dados (arquivos SQL na pasta tests/)."""
        tests: list[DbtTest] = []
        tests_dir = self.project_dir / "tests"
        if not tests_dir.exists():
            return tests

        for filepath in sorted(tests_dir.glob("**/*.sql")):
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

            refs = re.findall(r"{{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*}}", content)
            model_name = refs[0] if refs else None

            dbt_test = DbtTest(
                name=filepath.stem,
                test_type="data",
                model_name=model_name,
                filepath=str(filepath),
            )
            tests.append(dbt_test)

        self._tests.extend(tests)
        logger.info("Extraidos %d testes de dados", len(tests))
        return tests

    def extract_all(self) -> list[DbtTest]:
        """Extrai todos os testes do projeto."""
        self._tests.clear()
        self.extract_schema_tests()
        self.extract_data_tests()
        return self._tests.copy()

    def get_tests_by_model(self, model_name: str) -> list[DbtTest]:
        """Retorna testes de um modelo."""
        return [t for t in self._tests if t.model_name == model_name]

    def get_models_without_tests(self, all_model_names: set[str]) -> set[str]:
        """Retorna modelos sem nenhum teste."""
        tested_models = {t.model_name for t in self._tests if t.model_name}
        return all_model_names - tested_models

    def get_test_coverage(self, all_model_names: set[str]) -> dict[str, Any]:
        """Calcula cobertura de testes."""
        total = len(all_model_names)
        tested = len({t.model_name for t in self._tests if t.model_name})
        return {
            "total_models": total,
            "tested_models": tested,
            "untested_models": total - tested,
            "coverage_pct": (tested / total * 100) if total > 0 else 0,
            "total_tests": len(self._tests),
            "schema_tests": sum(1 for t in self._tests if t.test_type == "schema"),
            "data_tests": sum(1 for t in self._tests if t.test_type == "data"),
            "custom_tests": sum(1 for t in self._tests if t.test_type == "custom"),
        }

    def get_test_summary(self) -> dict[str, int]:
        """Retorna resumo de testes."""
        return {
            "total": len(self._tests),
            "schema": sum(1 for t in self._tests if t.test_type == "schema"),
            "data": sum(1 for t in self._tests if t.test_type == "data"),
            "custom": sum(1 for t in self._tests if t.test_type == "custom"),
        }

