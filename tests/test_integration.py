"""Testes de integracao para fluxo completo de parsing e analise."""

import pytest
import yaml
from pathlib import Path

pytest.importorskip("networkx", reason="networkx indisponivel (falta _bz2)")

from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.sql_parser import SqlParser
from dbt_parser.parsers.schema_extractor import SchemaExtractor
from dbt_parser.parsers.source_parser import SourceParser
from dbt_parser.parsers.test_extractor import TestExtractor
from dbt_parser.analyzers.graph_resolver import GraphResolver
from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer
from dbt_parser.analyzers.lineage_tracker import LineageTracker
from dbt_parser.analyzers.impact_analyzer import ImpactAnalyzer
from dbt_parser.validators.model_validator import ModelValidator
from dbt_parser.exporters.json_exporter import JsonExporter
from dbt_parser.exporters.graphviz_exporter import GraphvizExporter
from dbt_parser.exporters.mermaid_exporter import MermaidExporter


@pytest.fixture
def dbt_project(tmp_path: Path) -> Path:
    """Cria projeto dbt completo para testes de integracao."""
    project_dir = tmp_path / "dbt_project"
    models_dir = project_dir / "models"
    staging_dir = models_dir / "staging"
    marts_dir = models_dir / "marts"
    macros_dir = project_dir / "macros"
    seeds_dir = project_dir / "seeds"
    tests_dir = project_dir / "tests"

    for d in [staging_dir, marts_dir, macros_dir, seeds_dir, tests_dir]:
        d.mkdir(parents=True)

    project_yml = {
        "name": "test_project",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "test_profile",
        "model-paths": ["models"],
        "seed-paths": ["seeds"],
        "test-paths": ["tests"],
        "macro-paths": ["macros"],
    }
    with open(project_dir / "dbt_project.yml", "w") as f:
        yaml.dump(project_yml, f)

    schema = {
        "version": 2,
        "models": [
            {
                "name": "stg_events",
                "description": "Staging de eventos",
                "columns": [
                    {"name": "event_id", "tests": ["unique", "not_null"]},
                    {"name": "event_name", "description": "Nome"},
                ],
            },
            {
                "name": "stg_dates",
                "description": "Staging de datas",
                "columns": [
                    {"name": "date_key", "tests": ["unique", "not_null"]},
                ],
            },
            {
                "name": "fct_event_dates",
                "description": "Fato eventos e datas",
                "columns": [
                    {"name": "event_id", "tests": ["not_null"]},
                    {"name": "date_key"},
                ],
            },
        ],
    }
    with open(models_dir / "schema.yml", "w") as f:
        yaml.dump(schema, f)

    sources = {
        "version": 2,
        "sources": [
            {
                "name": "raw",
                "schema": "raw",
                "tables": [
                    {"name": "events"},
                    {"name": "dates"},
                ],
            }
        ],
    }
    with open(models_dir / "sources.yml", "w") as f:
        yaml.dump(sources, f)

    (staging_dir / "stg_events.sql").write_text(
        "{{ config(materialized='view') }}\n"
        "select * from {{ source('raw', 'events') }}"
    )
    (staging_dir / "stg_dates.sql").write_text(
        "{{ config(materialized='view') }}\n"
        "select * from {{ source('raw', 'dates') }}"
    )
    (marts_dir / "fct_event_dates.sql").write_text(
        "{{ config(materialized='table') }}\n"
        "select e.*, d.date_key\n"
        "from {{ ref('stg_events') }} e\n"
        "left join {{ ref('stg_dates') }} d on e.event_date = d.date_value"
    )

    return project_dir


class TestFullIntegrationFlow:
    """Testa fluxo completo de parsing, analise e exportacao."""

    def test_full_pipeline(self, dbt_project: Path) -> None:
        yaml_parser = YamlParser(dbt_project)
        sql_parser = SqlParser(dbt_project)
        extractor = SchemaExtractor(yaml_parser)

        yaml_files = yaml_parser.parse_all()
        sql_models = sql_parser.parse_all()

        assert len(yaml_files) >= 2
        assert len(sql_models) == 3

        for content in yaml_parser.get_parsed_files().values():
            if "models" in content:
                extractor.extract_models(content)

        models = extractor.get_all_models()
        assert len(models) == 3

        graph = GraphResolver()
        dep_analyzer = DependencyAnalyzer(graph, sql_parser)
        dep_analyzer.build_dependency_graph()

        assert graph.node_count() >= 5
        assert graph.edge_count() >= 4

        order = dep_analyzer.get_execution_order()
        assert len(order) >= 5

        report = dep_analyzer.analyze_model("fct_event_dates")
        assert "stg_events" in report.direct_dependencies

        tracker = LineageTracker(graph)
        lineage = tracker.get_full_lineage("fct_event_dates")
        assert len(lineage["upstream"]) >= 2

        validator = ModelValidator(extractor, sql_parser)
        validation_results = validator.validate_all()
        assert isinstance(validation_results, list)

    def test_export_pipeline(self, dbt_project: Path) -> None:
        sql_parser = SqlParser(dbt_project)
        sql_parser.parse_all()

        graph = GraphResolver()
        dep_analyzer = DependencyAnalyzer(graph, sql_parser)
        dep_analyzer.build_dependency_graph()

        json_exporter = JsonExporter()
        graph_json = json_exporter.export_graph(graph)
        assert "nodes" in graph_json
        assert "edges" in graph_json
        assert len(graph_json["nodes"]) >= 5

        gv_exporter = GraphvizExporter(graph)
        dot = gv_exporter.to_dot()
        assert "digraph" in dot

        mermaid_exporter = MermaidExporter(graph)
        mermaid = mermaid_exporter.to_mermaid()
        assert "graph" in mermaid

    def test_impact_analysis_pipeline(self, dbt_project: Path) -> None:
        sql_parser = SqlParser(dbt_project)
        sql_parser.parse_all()

        graph = GraphResolver()
        dep_analyzer = DependencyAnalyzer(graph, sql_parser)
        dep_analyzer.build_dependency_graph()

        impact = ImpactAnalyzer(graph)
        report = impact.analyze_impact("raw.events")
        assert report.total_affected > 0

        high_impact = impact.find_high_impact_models(threshold=1)
        assert len(high_impact) > 0

    def test_json_file_export(self, dbt_project: Path, tmp_path: Path) -> None:
        sql_parser = SqlParser(dbt_project)
        sql_parser.parse_all()

        graph = GraphResolver()
        dep_analyzer = DependencyAnalyzer(graph, sql_parser)
        dep_analyzer.build_dependency_graph()

        exporter = JsonExporter()
        output_file = tmp_path / "output" / "graph.json"
        exporter.export_to_file(exporter.export_graph(graph), output_file)
        assert output_file.exists()

    def test_graphviz_file_export(self, dbt_project: Path, tmp_path: Path) -> None:
        sql_parser = SqlParser(dbt_project)
        sql_parser.parse_all()

        graph = GraphResolver()
        dep_analyzer = DependencyAnalyzer(graph, sql_parser)
        dep_analyzer.build_dependency_graph()

        exporter = GraphvizExporter(graph)
        output_file = tmp_path / "output" / "graph.dot"
        exporter.export_to_file(output_file)
        assert output_file.exists()
