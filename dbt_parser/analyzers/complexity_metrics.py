"""Metricas de complexidade para modelos dbt."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from dbt_parser.parsers.sql_parser import SqlParser, SqlModelInfo
from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

@dataclass
class ModelComplexity:
    """Metricas de complexidade de um modelo."""

    model_name: str
    sql_lines: int = 0
    cte_count: int = 0
    join_count: int = 0
    subquery_count: int = 0
    ref_count: int = 0
    source_count: int = 0
    jinja_block_count: int = 0
    dependency_depth: int = 0
    dependents_count: int = 0
    complexity_score: float = 0.0

class ComplexityMetrics:
    """Calcula metricas de complexidade para modelos dbt."""

    WEIGHTS = {
        "sql_lines": 0.1,
        "cte_count": 1.5,
        "join_count": 2.0,
        "subquery_count": 3.0,
        "ref_count": 1.0,
        "source_count": 0.5,
        "jinja_block_count": 0.8,
        "dependency_depth": 1.2,
        "dependents_count": 0.5,
    }

    def __init__(self, sql_parser: SqlParser, graph: GraphResolver) -> None:
        self.sql_parser = sql_parser
        self.graph = graph

    def calculate_model_complexity(self, model_name: str) -> ModelComplexity:
        """Calcula complexidade de um modelo especifico."""
        model = self.sql_parser.get_model(model_name)
        if not model:
            return ModelComplexity(model_name=model_name)

        sql = model.raw_sql
        sql_lines = len([line for line in sql.split("\n") if line.strip()])
        join_count = len(re.findall(r"\bjoin\b", sql, re.IGNORECASE))
        subquery_count = sql.count("(") - sql.count("{{")
        subquery_count = max(0, subquery_count - len(model.ctes))

        jinja_blocks = len(re.findall(r"{{.*?}}", sql, re.DOTALL))
        jinja_blocks += len(re.findall(r"{%.*?%}", sql, re.DOTALL))

        dep_depth = len(self.graph.get_all_upstream(model_name))
        dependents = len(self.graph.get_all_downstream(model_name))

        complexity = ModelComplexity(
            model_name=model_name,
            sql_lines=sql_lines,
            cte_count=len(model.ctes),
            join_count=join_count,
            subquery_count=subquery_count,
            ref_count=len(model.refs),
            source_count=len(model.sources),
            jinja_block_count=jinja_blocks,
            dependency_depth=dep_depth,
            dependents_count=dependents,
        )

        complexity.complexity_score = self._calculate_score(complexity)
        return complexity

    def calculate_all(self) -> list[ModelComplexity]:
        """Calcula complexidade de todos os modelos."""
        results: list[ModelComplexity] = []
        for name in self.sql_parser._parsed_models:
            results.append(self.calculate_model_complexity(name))
        results.sort(key=lambda m: m.complexity_score, reverse=True)
        return results

    def _calculate_score(self, metrics: ModelComplexity) -> float:
        """Calcula score ponderado de complexidade."""
        score = 0.0
        score += metrics.sql_lines * self.WEIGHTS["sql_lines"]
        score += metrics.cte_count * self.WEIGHTS["cte_count"]
        score += metrics.join_count * self.WEIGHTS["join_count"]
        score += metrics.subquery_count * self.WEIGHTS["subquery_count"]
        score += metrics.ref_count * self.WEIGHTS["ref_count"]
        score += metrics.source_count * self.WEIGHTS["source_count"]
        score += metrics.jinja_block_count * self.WEIGHTS["jinja_block_count"]
        score += metrics.dependency_depth * self.WEIGHTS["dependency_depth"]
        score += metrics.dependents_count * self.WEIGHTS["dependents_count"]
        return round(score, 2)

    def get_most_complex(self, top_n: int = 10) -> list[ModelComplexity]:
        """Retorna modelos mais complexos."""
        return self.calculate_all()[:top_n]

    def get_complexity_summary(self) -> dict[str, Any]:
        """Retorna resumo de complexidade do projeto."""
        all_metrics = self.calculate_all()
        if not all_metrics:
            return {"total_models": 0}

        scores = [m.complexity_score for m in all_metrics]
        return {
            "total_models": len(all_metrics),
            "avg_complexity": round(sum(scores) / len(scores), 2),
            "max_complexity": max(scores),
            "min_complexity": min(scores),
            "most_complex": all_metrics[0].model_name if all_metrics else None,
            "models_above_threshold": sum(1 for s in scores if s > 20),
        }

