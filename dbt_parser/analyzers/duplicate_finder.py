"""Detector de padroes duplicados em modelos dbt."""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from dbt_parser.parsers.sql_parser import SqlParser

logger = logging.getLogger(__name__)

@dataclass
class DuplicateGroup:
    """Grupo de modelos com logica similar."""

    pattern_hash: str
    models: list[str] = field(default_factory=list)
    similarity_score: float = 0.0
    pattern_description: str = ""

class DuplicateFinder:
    """Detecta padroes SQL duplicados em modelos dbt."""

    def __init__(self, sql_parser: SqlParser) -> None:
        self.sql_parser = sql_parser

    def find_duplicate_ctes(self) -> dict[str, list[str]]:
        """Encontra CTEs com nomes identicos em modelos diferentes."""
        cte_map: dict[str, list[str]] = {}
        for name, model in self.sql_parser._parsed_models.items():
            for cte in model.ctes:
                if cte not in cte_map:
                    cte_map[cte] = []
                cte_map[cte].append(name)

        return {cte: models for cte, models in cte_map.items() if len(models) > 1}

    def find_similar_models(self, threshold: float = 0.7) -> list[DuplicateGroup]:
        """Encontra modelos com SQL estruturalmente similar."""
        models = list(self.sql_parser._parsed_models.items())
        groups: list[DuplicateGroup] = []
        processed: set[str] = set()

        for i, (name1, model1) in enumerate(models):
            if name1 in processed:
                continue

            similar: list[str] = [name1]
            normalized1 = self._normalize_sql(model1.raw_sql)

            for j in range(i + 1, len(models)):
                name2, model2 = models[j]
                if name2 in processed:
                    continue

                normalized2 = self._normalize_sql(model2.raw_sql)
                similarity = self._calculate_similarity(normalized1, normalized2)

                if similarity >= threshold:
                    similar.append(name2)
                    processed.add(name2)

            if len(similar) > 1:
                hash_val = hashlib.md5(normalized1.encode()).hexdigest()[:8]
                groups.append(
                    DuplicateGroup(
                        pattern_hash=hash_val,
                        models=similar,
                        similarity_score=threshold,
                        pattern_description=f"Modelos com estrutura SQL similar",
                    )
                )
                processed.add(name1)

        logger.info("Grupos de duplicados encontrados: %d", len(groups))
        return groups

    def find_duplicate_configs(self) -> dict[str, list[str]]:
        """Encontra modelos com configuracoes identicas."""
        config_map: dict[str, list[str]] = {}
        for name, model in self.sql_parser._parsed_models.items():
            if model.config:
                config_key = str(sorted(model.config.items()))
                if config_key not in config_map:
                    config_map[config_key] = []
                config_map[config_key].append(name)
        return {k: v for k, v in config_map.items() if len(v) > 1}

    def _normalize_sql(self, sql: str) -> str:
        """Normaliza SQL para comparacao (remove comentarios, whitespace, etc)."""
        result = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        result = re.sub(r"--.*$", "", result, flags=re.MULTILINE)
        result = re.sub(r"{{.*?}}", "JINJA_BLOCK", result, flags=re.DOTALL)
        result = re.sub(r"{%.*?%}", "", result, flags=re.DOTALL)
        result = re.sub(r"'\w+'", "STRING", result)
        result = re.sub(r"\b\d+\b", "NUM", result)
        result = re.sub(r"\s+", " ", result).strip().lower()
        return result

    def _calculate_similarity(self, sql1: str, sql2: str) -> float:
        """Calcula similaridade entre dois SQLs normalizados."""
        if not sql1 or not sql2:
            return 0.0

        tokens1 = set(sql1.split())
        tokens2 = set(sql2.split())

        if not tokens1 and not tokens2:
            return 1.0
        if not tokens1 or not tokens2:
            return 0.0

        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        return len(intersection) / len(union)

    def get_duplicate_summary(self) -> dict[str, Any]:
        """Retorna resumo de duplicados."""
        dup_ctes = self.find_duplicate_ctes()
        similar = self.find_similar_models()
        dup_configs = self.find_duplicate_configs()

        return {
            "duplicate_cte_names": len(dup_ctes),
            "similar_model_groups": len(similar),
            "duplicate_config_groups": len(dup_configs),
        }

