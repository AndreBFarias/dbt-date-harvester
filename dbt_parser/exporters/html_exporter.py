"""
HTML Exporter Module
Gera paginas HTML interativas para visualizacao de grafos de dependencia dbt.
Inclui busca, filtros e visualizacao hierarquica.
"""

from __future__ import annotations

import html
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class HtmlExporter:
    """Exportador de grafos para HTML interativo com busca integrada."""

    def __init__(self, graph: Any) -> None:
        self._graph = graph

    def to_html(self, title: str = "dbt Dependency Graph") -> str:
        """Gera HTML interativo completo com busca e filtros."""
        nodes = self._extract_nodes()
        edges = self._extract_edges()
        stats = self._calculate_stats(nodes, edges)

        return self._render_template(title, nodes, edges, stats)

    def _extract_nodes(self) -> list[dict[str, Any]]:
        """Extrai informacoes dos nos do grafo."""
        nodes = []
        for node_name in self._graph.nodes():
            node_data = self._graph.nodes[node_name]
            node_info = node_data.get("info")
            nodes.append({
                "id": node_name,
                "type": getattr(node_info, "node_type", "model") if node_info else "model",
                "filepath": str(getattr(node_info, "filepath", "")) if node_info else "",
                "tags": list(getattr(node_info, "tags", [])) if node_info else [],
            })
        return nodes

    def _extract_edges(self) -> list[dict[str, str]]:
        """Extrai conexoes do grafo."""
        return [
            {"source": str(src), "target": str(tgt)}
            for src, tgt in self._graph.edges()
        ]

    def _calculate_stats(
        self, nodes: list[dict], edges: list[dict]
    ) -> dict[str, int]:
        """Calcula estatisticas do grafo."""
        type_counts: dict[str, int] = {}
        for node in nodes:
            node_type = node["type"]
            type_counts[node_type] = type_counts.get(node_type, 0) + 1
        return {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            **type_counts,
        }

    def _render_template(
        self,
        title: str,
        nodes: list[dict],
        edges: list[dict],
        stats: dict[str, int],
    ) -> str:
        """Renderiza o template HTML com dados embutidos."""
        escaped_title = html.escape(title)
        nodes_json = json.dumps(nodes, ensure_ascii=False)
        edges_json = json.dumps(edges, ensure_ascii=False)
        stats_json = json.dumps(stats, ensure_ascii=False)

        return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escaped_title}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
               background: #1e1e2e; color: #cdd6f4; }}
        .header {{ padding: 1rem 2rem; background: #181825;
                   border-bottom: 1px solid #313244; display: flex;
                   align-items: center; gap: 1rem; }}
        .header h1 {{ font-size: 1.2rem; color: #cba6f7; }}
        .search-box {{ padding: 0.5rem 1rem; background: #313244;
                       border: 1px solid #45475a; border-radius: 6px;
                       color: #cdd6f4; font-size: 0.9rem; width: 300px; }}
        .search-box:focus {{ outline: none; border-color: #cba6f7; }}
        .container {{ display: flex; height: calc(100vh - 60px); }}
        .sidebar {{ width: 300px; background: #181825; overflow-y: auto;
                    border-right: 1px solid #313244; padding: 1rem; }}
        .main {{ flex: 1; padding: 1rem; overflow: auto; }}
        .stats {{ display: grid; grid-template-columns: 1fr 1fr;
                  gap: 0.5rem; margin-bottom: 1rem; }}
        .stat-card {{ background: #313244; padding: 0.75rem;
                      border-radius: 6px; text-align: center; }}
        .stat-card .value {{ font-size: 1.5rem; font-weight: bold;
                             color: #89b4fa; }}
        .stat-card .label {{ font-size: 0.75rem; color: #a6adc8; }}
        .node-list {{ list-style: none; }}
        .node-item {{ padding: 0.5rem; margin: 0.25rem 0;
                      border-radius: 4px; cursor: pointer;
                      border-left: 3px solid transparent; }}
        .node-item:hover {{ background: #313244; }}
        .node-item.model {{ border-left-color: #89b4fa; }}
        .node-item.source {{ border-left-color: #a6e3a1; }}
        .node-item.seed {{ border-left-color: #f9e2af; }}
        .node-item.test {{ border-left-color: #f38ba8; }}
        .node-name {{ font-size: 0.85rem; }}
        .node-type {{ font-size: 0.7rem; color: #a6adc8; }}
        .edge-table {{ width: 100%; border-collapse: collapse; }}
        .edge-table th, .edge-table td {{ padding: 0.5rem;
            text-align: left; border-bottom: 1px solid #313244; }}
        .edge-table th {{ color: #cba6f7; font-weight: 600; }}
        .hidden {{ display: none; }}
        .filter-btn {{ padding: 0.3rem 0.8rem; border: 1px solid #45475a;
                       background: transparent; color: #cdd6f4;
                       border-radius: 4px; cursor: pointer;
                       font-size: 0.8rem; margin: 0.2rem; }}
        .filter-btn.active {{ background: #cba6f7; color: #1e1e2e;
                              border-color: #cba6f7; }}
        .filters {{ margin-bottom: 1rem; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{escaped_title}</h1>
        <input type="text" class="search-box" id="searchInput"
               placeholder="Buscar modelo, source, tag...">
    </div>
    <div class="container">
        <div class="sidebar">
            <div class="stats" id="statsContainer"></div>
            <div class="filters" id="filtersContainer"></div>
            <ul class="node-list" id="nodeList"></ul>
        </div>
        <div class="main">
            <h2 style="margin-bottom: 1rem; color: #cba6f7;">Conexoes</h2>
            <table class="edge-table" id="edgeTable">
                <thead><tr><th>Origem</th><th>Destino</th></tr></thead>
                <tbody id="edgeBody"></tbody>
            </table>
        </div>
    </div>
    <script>
        const nodes = {nodes_json};
        const edges = {edges_json};
        const stats = {stats_json};
        let activeFilters = new Set();

        function renderStats() {{
            const container = document.getElementById("statsContainer");
            container.innerHTML = Object.entries(stats)
                .map(([k, v]) => `<div class="stat-card">
                    <div class="value">${{v}}</div>
                    <div class="label">${{k.replace("_", " ")}}</div>
                </div>`).join("");
        }}

        function renderFilters() {{
            const types = [...new Set(nodes.map(n => n.type))];
            const container = document.getElementById("filtersContainer");
            container.innerHTML = types.map(t =>
                `<button class="filter-btn" onclick="toggleFilter('${{t}}')"
                    id="filter-${{t}}">${{t}}</button>`
            ).join("");
        }}

        function toggleFilter(type) {{
            const btn = document.getElementById("filter-" + type);
            if (activeFilters.has(type)) {{
                activeFilters.delete(type);
                btn.classList.remove("active");
            }} else {{
                activeFilters.add(type);
                btn.classList.add("active");
            }}
            renderNodes();
            renderEdges();
        }}

        function renderNodes() {{
            const query = document.getElementById("searchInput")
                .value.toLowerCase();
            const list = document.getElementById("nodeList");
            const filtered = nodes.filter(n => {{
                if (activeFilters.size > 0 && !activeFilters.has(n.type))
                    return false;
                if (query && !n.id.toLowerCase().includes(query)
                    && !n.tags.some(t => t.toLowerCase().includes(query)))
                    return false;
                return true;
            }});
            list.innerHTML = filtered.map(n =>
                `<li class="node-item ${{n.type}}">
                    <div class="node-name">${{n.id}}</div>
                    <div class="node-type">${{n.type}}${{
                        n.tags.length ? " | " + n.tags.join(", ") : ""
                    }}</div>
                </li>`
            ).join("");
        }}

        function renderEdges() {{
            const query = document.getElementById("searchInput")
                .value.toLowerCase();
            const body = document.getElementById("edgeBody");
            const visibleNodes = new Set(
                nodes.filter(n => activeFilters.size === 0
                    || activeFilters.has(n.type)).map(n => n.id)
            );
            const filtered = edges.filter(e => {{
                if (activeFilters.size > 0
                    && !visibleNodes.has(e.source)
                    && !visibleNodes.has(e.target))
                    return false;
                if (query && !e.source.toLowerCase().includes(query)
                    && !e.target.toLowerCase().includes(query))
                    return false;
                return true;
            }});
            body.innerHTML = filtered.map(e =>
                `<tr><td>${{e.source}}</td><td>${{e.target}}</td></tr>`
            ).join("");
        }}

        document.getElementById("searchInput")
            .addEventListener("input", () => {{ renderNodes(); renderEdges(); }});
        renderStats();
        renderFilters();
        renderNodes();
        renderEdges();
    </script>
</body>
</html>"""

    def export_to_file(
        self, filepath: str | Path, title: str = "dbt Dependency Graph"
    ) -> Path:
        """Exporta HTML para arquivo."""
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        content = self.to_html(title)
        filepath.write_text(content, encoding="utf-8")
        logger.info("HTML exportado para: %s", filepath)
        return filepath

# "A riqueza consiste muito mais no desfrute do que na posse." -- Aristoteles

