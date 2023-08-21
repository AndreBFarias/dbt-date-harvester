"""Parser dedicado para sources dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_parser.parsers.yaml_parser import YamlParser

logger = logging.getLogger(__name__)

@dataclass
class SourceTable:
    """Tabela de um source dbt."""

    name: str
    description: str = ""
    columns: list[dict[str, Any]] = field(default_factory=list)
    tests: list[dict[str, Any]] = field(default_factory=list)
    loaded_at_field: str | None = None
    freshness: dict[str, Any | None] = None

@dataclass
class Source:
    """Source dbt completo."""

    name: str
    schema: str = ""
    database: str | None = None
    description: str = ""
    tables: list[SourceTable] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

class SourceParser:
    """Parser especializado para sources dbt."""

    def __init__(self, yaml_parser: YamlParser) -> None:
        self.yaml_parser = yaml_parser
        self._sources: dict[str, Source] = {}

    def parse_sources_file(self, filepath: Path) -> list[Source]:
        """Faz parsing de um arquivo sources.yml."""
        content = self.yaml_parser.parse_file(filepath)
        if not content:
            return []

        raw_sources = content.get("sources", [])
        sources: list[Source] = []

        for raw in raw_sources:
            tables: list[SourceTable] = []
            for raw_table in raw.get("tables", []):
                table = SourceTable(
                    name=raw_table.get("name", ""),
                    description=raw_table.get("description", ""),
                    columns=raw_table.get("columns", []),
                    tests=raw_table.get("tests", []),
                    loaded_at_field=raw_table.get("loaded_at_field"),
                    freshness=raw_table.get("freshness"),
                )
                tables.append(table)

            source = Source(
                name=raw.get("name", ""),
                schema=raw.get("schema", ""),
                database=raw.get("database"),
                description=raw.get("description", ""),
                tables=tables,
                tags=raw.get("tags", []),
                meta=raw.get("meta", {}),
            )
            sources.append(source)
            self._sources[source.name] = source

        logger.info("Sources parsed: %d fontes de %s", len(sources), filepath)
        return sources

    def find_sources_files(self, project_dir: Path) -> list[Path]:
        """Encontra arquivos de sources no projeto."""
        sources_files: list[Path] = []
        for filepath in project_dir.glob("**/sources.yml"):
            sources_files.append(filepath)
        for filepath in project_dir.glob("**/sources.yaml"):
            sources_files.append(filepath)
        return sorted(sources_files)

    def parse_all(self, project_dir: Path) -> dict[str, Source]:
        """Faz parsing de todos os arquivos de sources."""
        for filepath in self.find_sources_files(project_dir):
            self.parse_sources_file(filepath)
        return self._sources.copy()

    def get_source(self, name: str) -> Source | None:
        """Retorna source por nome."""
        return self._sources.get(name)

    def get_all_table_names(self) -> set[str]:
        """Retorna todos os nomes de tabelas (source.table)."""
        tables: set[str] = set()
        for source in self._sources.values():
            for table in source.tables:
                tables.add(f"{source.name}.{table.name}")
        return tables

    def get_source_table(
        self, source_name: str, table_name: str
    ) -> SourceTable | None:
        """Retorna tabela especifica de um source."""
        source = self._sources.get(source_name)
        if not source:
            return None
        for table in source.tables:
            if table.name == table_name:
                return table
        return None

    def get_freshness_config(self) -> dict[str, dict[str, Any]]:
        """Retorna configuracoes de freshness por source.table."""
        freshness: dict[str, dict[str, Any]] = {}
        for source in self._sources.values():
            for table in source.tables:
                if table.freshness:
                    key = f"{source.name}.{table.name}"
                    freshness[key] = table.freshness
        return freshness

