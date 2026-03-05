"""Extrator de informacoes de arquivos schema.yml dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_parser.parsers.yaml_parser import YamlParser

logger = logging.getLogger(__name__)

@dataclass
class ColumnInfo:
    """Informacoes de uma coluna de modelo dbt."""

    name: str
    description: str = ""
    tests: list[dict[str, Any]] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

@dataclass
class ModelInfo:
    """Informacoes de um modelo dbt extraidas do schema.yml."""

    name: str
    description: str = ""
    columns: list[ColumnInfo] = field(default_factory=list)
    tests: list[dict[str, Any]] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

@dataclass
class SourceInfo:
    """Informacoes de um source dbt."""

    name: str
    schema: str = ""
    description: str = ""
    tables: list[dict[str, Any]] = field(default_factory=list)

class SchemaExtractor:
    """Extrai informacoes estruturadas de schema.yml e sources.yml."""

    def __init__(self, yaml_parser: YamlParser) -> None:
        self.yaml_parser = yaml_parser
        self._models: list[ModelInfo] = []
        self._sources: list[SourceInfo] = []

    def extract_models(self, content: dict[str, Any]) -> list[ModelInfo]:
        """Extrai modelos de um schema.yml parseado."""
        raw_models = self.yaml_parser.extract_models_section(content)
        models: list[ModelInfo] = []

        for raw_model in raw_models:
            columns = []
            for col in raw_model.get("columns", []):
                columns.append(
                    ColumnInfo(
                        name=col.get("name", ""),
                        description=col.get("description", ""),
                        tests=col.get("tests", []),
                        meta=col.get("meta", {}),
                    )
                )

            model = ModelInfo(
                name=raw_model.get("name", ""),
                description=raw_model.get("description", ""),
                columns=columns,
                tests=raw_model.get("tests", []),
                meta=raw_model.get("meta", {}),
                tags=raw_model.get("tags", []),
            )
            models.append(model)

        self._models.extend(models)
        logger.info("Extraidos %d modelos do schema", len(models))
        return models

    def extract_sources(self, content: dict[str, Any]) -> list[SourceInfo]:
        """Extrai sources de um sources.yml parseado."""
        raw_sources = self.yaml_parser.extract_sources_section(content)
        sources: list[SourceInfo] = []

        for raw_source in raw_sources:
            source = SourceInfo(
                name=raw_source.get("name", ""),
                schema=raw_source.get("schema", ""),
                description=raw_source.get("description", ""),
                tables=raw_source.get("tables", []),
            )
            sources.append(source)

        self._sources.extend(sources)
        logger.info("Extraidos %d sources", len(sources))
        return sources

    def get_all_models(self) -> list[ModelInfo]:
        """Retorna todos os modelos extraidos."""
        return self._models.copy()

    def get_all_sources(self) -> list[SourceInfo]:
        """Retorna todos os sources extraidos."""
        return self._sources.copy()

    def get_model_by_name(self, name: str) -> ModelInfo | None:
        """Busca modelo por nome."""
        for model in self._models:
            if model.name == name:
                return model
        return None

    def get_column_tests(self, model_name: str) -> dict[str, list[dict[str, Any]]]:
        """Retorna testes por coluna de um modelo."""
        model = self.get_model_by_name(model_name)
        if not model:
            return {}
        return {col.name: col.tests for col in model.columns if col.tests}

