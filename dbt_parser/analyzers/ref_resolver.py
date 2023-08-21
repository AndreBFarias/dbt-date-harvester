"""Resolver de referencias ref() e source() para projetos dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_parser.parsers.sql_parser import SqlParser
from dbt_parser.parsers.schema_extractor import SchemaExtractor

logger = logging.getLogger(__name__)

@dataclass
class ResolvedRef:
    """Referencia resolvida."""

    source_model: str
    target_model: str
    ref_type: str  # "ref", "source"
    resolved: bool = True
    resolution_path: str | None = None

class RefResolver:
    """Resolve referencias entre modelos dbt."""

    def __init__(
        self, sql_parser: SqlParser, schema_extractor: SchemaExtractor
    ) -> None:
        self.sql_parser = sql_parser
        self.schema_extractor = schema_extractor
        self._resolved_refs: list[ResolvedRef] = []
        self._unresolved_refs: list[ResolvedRef] = []

    def resolve_all(self) -> tuple[list[ResolvedRef], list[ResolvedRef]]:
        """Resolve todas as referencias do projeto."""
        self._resolved_refs.clear()
        self._unresolved_refs.clear()

        known_models = set(self.sql_parser._parsed_models.keys())
        schema_models = {m.name for m in self.schema_extractor.get_all_models()}
        all_known = known_models | schema_models

        known_sources: set[str] = set()
        for source in self.schema_extractor.get_all_sources():
            for table in source.tables:
                table_name = table.get("name", "") if isinstance(table, dict) else str(table)
                known_sources.add(f"{source.name}.{table_name}")

        for model_name, refs in self.sql_parser.get_all_refs().items():
            for ref in refs:
                if ref in all_known:
                    self._resolved_refs.append(
                        ResolvedRef(
                            source_model=model_name,
                            target_model=ref,
                            ref_type="ref",
                            resolved=True,
                        )
                    )
                else:
                    self._unresolved_refs.append(
                        ResolvedRef(
                            source_model=model_name,
                            target_model=ref,
                            ref_type="ref",
                            resolved=False,
                        )
                    )
                    logger.warning(
                        "Ref nao resolvida: %s referencia '%s'", model_name, ref
                    )

        for model_name, sources in self.sql_parser.get_all_sources().items():
            for source_name, table_name in sources:
                source_key = f"{source_name}.{table_name}"
                if source_key in known_sources:
                    self._resolved_refs.append(
                        ResolvedRef(
                            source_model=model_name,
                            target_model=source_key,
                            ref_type="source",
                            resolved=True,
                        )
                    )
                else:
                    self._unresolved_refs.append(
                        ResolvedRef(
                            source_model=model_name,
                            target_model=source_key,
                            ref_type="source",
                            resolved=False,
                        )
                    )
                    logger.warning(
                        "Source nao resolvido: %s referencia '%s'",
                        model_name,
                        source_key,
                    )

        logger.info(
            "Resolucao concluida: %d resolvidas, %d nao resolvidas",
            len(self._resolved_refs),
            len(self._unresolved_refs),
        )
        return self._resolved_refs.copy(), self._unresolved_refs.copy()

    def get_resolved(self) -> list[ResolvedRef]:
        """Retorna referencias resolvidas."""
        return self._resolved_refs.copy()

    def get_unresolved(self) -> list[ResolvedRef]:
        """Retorna referencias nao resolvidas."""
        return self._unresolved_refs.copy()

    def get_resolution_summary(self) -> dict[str, Any]:
        """Retorna resumo da resolucao."""
        total = len(self._resolved_refs) + len(self._unresolved_refs)
        return {
            "total_refs": total,
            "resolved": len(self._resolved_refs),
            "unresolved": len(self._unresolved_refs),
            "resolution_rate": (
                len(self._resolved_refs) / total * 100 if total > 0 else 100
            ),
        }

    def get_models_with_broken_refs(self) -> set[str]:
        """Retorna modelos com referencias quebradas."""
        return {r.source_model for r in self._unresolved_refs}

