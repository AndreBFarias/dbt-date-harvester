"""Analisador de impacto para mudancas em modelos dbt."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from dbt_parser.analyzers.graph_resolver import GraphResolver

logger = logging.getLogger(__name__)

@dataclass
class ImpactReport:
    """Relatorio de impacto de uma mudanca."""

    changed_model: str
    directly_affected: list[str] = field(default_factory=list)
    transitively_affected: list[str] = field(default_factory=list)
    total_affected: int = 0
    affected_by_type: dict[str, int] = field(default_factory=dict)
    risk_level: str = "low"  # "low", "medium", "high", "critical"

class ImpactAnalyzer:
    """Analisa impacto de mudancas em modelos dbt."""

    RISK_THRESHOLDS = {
        "low": 2,
        "medium": 5,
        "high": 10,
    }

    def __init__(self, graph: GraphResolver) -> None:
        self.graph = graph

    def analyze_impact(self, model_name: str) -> ImpactReport:
        """Analisa impacto de uma mudanca em um modelo."""
        directly_affected = sorted(self.graph.get_dependents(model_name))
        transitively_affected = sorted(self.graph.get_all_downstream(model_name))

        affected_by_type: dict[str, int] = {}
        for affected in transitively_affected:
            node_data = dict(self.graph.graph.nodes.get(affected, {}))
            node_type = node_data.get("node_type", "unknown")
            affected_by_type[node_type] = affected_by_type.get(node_type, 0) + 1

        total = len(transitively_affected)
        risk_level = self._calculate_risk(total)

        report = ImpactReport(
            changed_model=model_name,
            directly_affected=directly_affected,
            transitively_affected=transitively_affected,
            total_affected=total,
            affected_by_type=affected_by_type,
            risk_level=risk_level,
        )

        logger.info(
            "Impacto de %s: %d afetados (risco: %s)",
            model_name,
            total,
            risk_level,
        )
        return report

    def analyze_multiple_changes(
        self, model_names: list[str]
    ) -> dict[str, ImpactReport]:
        """Analisa impacto de mudancas em multiplos modelos."""
        reports: dict[str, ImpactReport] = {}
        for name in model_names:
            reports[name] = self.analyze_impact(name)
        return reports

    def get_combined_impact(self, model_names: list[str]) -> set[str]:
        """Retorna conjunto combinado de todos os modelos afetados."""
        affected: set[str] = set()
        for name in model_names:
            affected.update(self.graph.get_all_downstream(name))
        return affected

    def find_high_impact_models(self, threshold: int = 5) -> list[tuple[str, int]]:
        """Encontra modelos com alto impacto (muitos dependentes)."""
        results: list[tuple[str, int]] = []
        for node in self.graph.graph.nodes():
            downstream_count = len(self.graph.get_all_downstream(node))
            if downstream_count >= threshold:
                results.append((node, downstream_count))
        return sorted(results, key=lambda x: x[1], reverse=True)

    def get_critical_path(self) -> list[str]:
        """Identifica caminho critico (caminho mais longo no grafo)."""
        import networkx as nx

        if self.graph.node_count() == 0:
            return []

        try:
            longest = nx.dag_longest_path(self.graph.graph)
            return list(reversed(longest))
        except nx.NetworkXError:
            return []

    def _calculate_risk(self, affected_count: int) -> str:
        """Calcula nivel de risco baseado no numero de modelos afetados."""
        if affected_count >= self.RISK_THRESHOLDS["high"]:
            return "critical"
        if affected_count >= self.RISK_THRESHOLDS["medium"]:
            return "high"
        if affected_count >= self.RISK_THRESHOLDS["low"]:
            return "medium"
        return "low"

    def get_impact_summary(self) -> dict[str, Any]:
        """Retorna resumo de impacto do projeto."""
        impacts: list[tuple[str, int]] = []
        for node in self.graph.graph.nodes():
            count = len(self.graph.get_all_downstream(node))
            impacts.append((node, count))

        impacts.sort(key=lambda x: x[1], reverse=True)

        return {
            "total_nodes": self.graph.node_count(),
            "highest_impact": impacts[0] if impacts else None,
            "average_impact": (
                sum(c for _, c in impacts) / len(impacts) if impacts else 0
            ),
            "critical_nodes": [n for n, c in impacts if c >= self.RISK_THRESHOLDS["high"]],
        }

