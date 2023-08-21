"""Interface de linha de comando para dbt-date-harvester."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dbt_parser import __version__

logger = logging.getLogger(__name__)

QUICK_START = f"""\
dbt-parser {__version__} - análise estática para projetos dbt

Uso rápido (execute dentro do diretório do seu projeto dbt):

  dbt-parser parse                          Ver modelos e dependências
  dbt-parser graph                          Resumo do grafo
  dbt-parser validate                       Checar boas práticas
  dbt-parser lineage --model <nome>         Linhagem de um modelo
  dbt-parser export --format json           Exportar grafo

Opções úteis:
  --project-dir <caminho>   Apontar para outro projeto (default: dir atual)
  -v / -vv                  Mais detalhes / modo debug
  --help                    Ajuda detalhada

Documentação completa: GUIDE.md
"""


def setup_logging(verbosity: int) -> None:
    """Configura logging baseado no nível de verbosidade."""
    levels = {0: logging.ERROR, 1: logging.INFO, 2: logging.DEBUG}
    level = levels.get(verbosity, logging.DEBUG)

    if verbosity >= 2:
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    elif verbosity >= 1:
        fmt = "[%(levelname)s] %(message)s"
    else:
        fmt = "%(message)s"

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)


def _common_args() -> argparse.ArgumentParser:
    """Retorna parser com argumentos comuns a todos os subcommands."""
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        "--project-dir",
        type=Path,
        default=Path("."),
        help="Diretório raiz do projeto dbt (default: diretório atual)",
    )
    parent.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Aumenta verbosidade (use -vv para debug)",
    )
    return parent


def create_parser() -> argparse.ArgumentParser:
    """Cria parser de argumentos da CLI."""
    parent = _common_args()

    parser = argparse.ArgumentParser(
        prog="dbt-parser",
        description="dbt-date-harvester: análise estática para projetos dbt",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemplos:\n"
            "  dbt-parser parse                          Listar modelos do projeto\n"
            "  dbt-parser graph --model fct_orders        Dependências de um modelo\n"
            "  dbt-parser validate --severity info        Validar com todos os níveis\n"
            "  dbt-parser export --format dot -o grafo.dot  Exportar grafo\n"
        ),
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="comandos",
        description="Use 'dbt-parser <comando> --help' para detalhes",
    )

    subparsers.add_parser(
        "parse",
        parents=[parent],
        help="Listar modelos, refs e sources do projeto",
        description="Faz parsing do projeto dbt e mostra os modelos encontrados com suas dependências.",
    ).add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Formato de saída (default: text)",
    )

    graph_cmd = subparsers.add_parser(
        "graph",
        parents=[parent],
        help="Ver grafo de dependências entre modelos",
        description="Analisa e exibe o grafo de dependências. Sem --model, mostra resumo geral.",
    )
    graph_cmd.add_argument(
        "--model",
        type=str,
        help="Analisar modelo específico (dependências e dependentes)",
    )

    validate_cmd = subparsers.add_parser(
        "validate",
        parents=[parent],
        help="Checar boas práticas e convenções",
        description="Valida nomenclatura, documentação e configuração dos modelos.",
    )
    validate_cmd.add_argument(
        "--severity",
        choices=["error", "warning", "info"],
        default="warning",
        help="Nível mínimo para exibição (default: warning)",
    )

    lineage_cmd = subparsers.add_parser(
        "lineage",
        parents=[parent],
        help="Rastrear linhagem de um modelo (upstream/downstream)",
        description="Mostra toda a cadeia de dependências de um modelo: quem alimenta e quem consome.",
    )
    lineage_cmd.add_argument(
        "--model",
        type=str,
        required=True,
        help="Nome do modelo (obrigatório)",
    )

    export_cmd = subparsers.add_parser(
        "export",
        parents=[parent],
        help="Exportar grafo para JSON, DOT ou Mermaid",
        description="Exporta o grafo de dependências. Sem --output, imprime no terminal.",
    )
    export_cmd.add_argument(
        "--format", "-f",
        choices=["json", "dot", "mermaid"],
        default="json",
        help="Formato (default: json)",
    )
    export_cmd.add_argument(
        "--output", "-o",
        type=Path,
        help="Salvar em arquivo (default: stdout)",
    )
    export_cmd.add_argument(
        "--model",
        type=str,
        help="Exportar apenas linhagem de um modelo",
    )

    return parser


def _resolve_project_dir(args: argparse.Namespace) -> Path:
    """Resolve e valida o diretório do projeto."""
    project_dir = args.project_dir.resolve()
    if not project_dir.exists():
        print(f"Erro: diretório não encontrado: {project_dir}", file=sys.stderr)
        print(f"Dica: use --project-dir para apontar para o diretório do projeto dbt", file=sys.stderr)
        sys.exit(1)
    dbt_project = project_dir / "dbt_project.yml"
    if not dbt_project.exists():
        print(
            f"Aviso: dbt_project.yml não encontrado em {project_dir}",
            file=sys.stderr,
        )
        print(
            f"Dica: execute dentro do diretório do projeto ou use --project-dir",
            file=sys.stderr,
        )
    return project_dir


def run_parse(args: argparse.Namespace) -> int:
    """Executa comando de parsing."""
    from dbt_parser.parsers.yaml_parser import YamlParser
    from dbt_parser.parsers.sql_parser import SqlParser

    project_dir = _resolve_project_dir(args)

    yaml_parser = YamlParser(project_dir)
    sql_parser = SqlParser(project_dir)

    yaml_files = yaml_parser.parse_all()
    sql_models = sql_parser.parse_all()

    print(f"Projeto: {project_dir}")
    print(f"Arquivos YAML: {len(yaml_files)}")
    print(f"Modelos SQL: {len(sql_models)}")

    if not sql_models:
        print("\nNenhum modelo SQL encontrado.")
        print("Dica: verifique se o diretório models/ existe no projeto.")
        return 0

    for name, model in sql_models.items():
        print(f"  - {name}: refs={model.refs}, sources={model.sources}")

    return 0


def run_graph(args: argparse.Namespace) -> int:
    """Executa comando de análise de grafo."""
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.analyzers.graph_resolver import GraphResolver
    from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer

    project_dir = _resolve_project_dir(args)
    sql_parser = SqlParser(project_dir)
    sql_parser.parse_all()

    graph = GraphResolver()
    analyzer = DependencyAnalyzer(graph, sql_parser)
    analyzer.build_dependency_graph()

    if args.model:
        if args.model not in graph.graph:
            print(f"Erro: modelo '{args.model}' não encontrado no grafo.", file=sys.stderr)
            print(f"Dica: use 'dbt-parser parse' para ver os modelos disponíveis.", file=sys.stderr)
            return 1
        report = analyzer.analyze_model(args.model)
        print(f"Modelo: {report.model_name}")
        print(f"  Dependências diretas: {report.direct_dependencies}")
        print(f"  Dependentes diretos: {report.direct_dependents}")
    else:
        print(f"Nós: {graph.node_count()}")
        print(f"Arestas: {graph.edge_count()}")
        print(f"Raízes: {sorted(graph.get_root_nodes())}")
        print(f"Folhas: {sorted(graph.get_leaf_nodes())}")

    return 0


def run_validate(args: argparse.Namespace) -> int:
    """Executa comando de validação."""
    from dbt_parser.parsers.yaml_parser import YamlParser
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.parsers.schema_extractor import SchemaExtractor
    from dbt_parser.validators.model_validator import ModelValidator, Severity

    project_dir = _resolve_project_dir(args)
    yaml_parser = YamlParser(project_dir)
    sql_parser = SqlParser(project_dir)
    extractor = SchemaExtractor(yaml_parser)

    yaml_parser.parse_all()
    sql_parser.parse_all()

    for content in yaml_parser.get_parsed_files().values():
        if "models" in content:
            extractor.extract_models(content)

    validator = ModelValidator(extractor, sql_parser)
    results = validator.validate_all()

    severity_map = {"error": Severity.ERROR, "warning": Severity.WARNING, "info": Severity.INFO}
    min_severity = severity_map[args.severity]

    severity_order = [Severity.ERROR, Severity.WARNING, Severity.INFO]
    max_idx = severity_order.index(min_severity)
    allowed = set(severity_order[: max_idx + 1])

    filtered = [r for r in results if r.severity in allowed]
    for result in filtered:
        print(f"[{result.severity.value.upper()}] {result.model_name}: {result.message}")

    summary = validator.get_summary()
    print(f"\nResumo: {summary['errors']} erros, {summary['warnings']} avisos, {summary['info']} info")

    return 1 if summary["errors"] > 0 else 0


def run_lineage(args: argparse.Namespace) -> int:
    """Executa comando de linhagem."""
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.analyzers.graph_resolver import GraphResolver
    from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer
    from dbt_parser.analyzers.lineage_tracker import LineageTracker

    project_dir = _resolve_project_dir(args)
    sql_parser = SqlParser(project_dir)
    sql_parser.parse_all()

    graph = GraphResolver()
    dep_analyzer = DependencyAnalyzer(graph, sql_parser)
    dep_analyzer.build_dependency_graph()

    if args.model not in graph.graph:
        print(f"Erro: modelo '{args.model}' não encontrado no grafo.", file=sys.stderr)
        print(f"Dica: use 'dbt-parser parse' para ver os modelos disponíveis.", file=sys.stderr)
        return 1

    tracker = LineageTracker(graph)
    lineage = tracker.get_full_lineage(args.model)

    print(f"Linhagem de: {lineage['model']}")
    print(f"  Upstream: {lineage['upstream']}")
    print(f"  Downstream: {lineage['downstream']}")
    print(f"  Profundidade upstream: {lineage['lineage_depth_up']}")
    print(f"  Profundidade downstream: {lineage['lineage_depth_down']}")

    return 0


def run_export(args: argparse.Namespace) -> int:
    """Executa comando de exportação."""
    from dbt_parser.parsers.sql_parser import SqlParser
    from dbt_parser.analyzers.graph_resolver import GraphResolver
    from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer
    from dbt_parser.exporters import JsonExporter, GraphvizExporter, MermaidExporter

    project_dir = _resolve_project_dir(args)

    sql_parser = SqlParser(project_dir)
    sql_parser.parse_all()

    graph = GraphResolver()
    analyzer = DependencyAnalyzer(graph, sql_parser)
    analyzer.build_dependency_graph()

    if args.model:
        if args.model not in graph.graph:
            print(f"Erro: modelo '{args.model}' não encontrado no grafo.", file=sys.stderr)
            print(f"Dica: use 'dbt-parser parse' para ver os modelos disponíveis.", file=sys.stderr)
            return 1
        upstream = graph.get_all_upstream(args.model)
        downstream = graph.get_all_downstream(args.model)
        relevant = upstream | downstream | {args.model}
        graph = graph.get_subgraph(relevant)

    fmt = args.format
    output_path = args.output

    if fmt == "json":
        exporter = JsonExporter()
        data = exporter.export_graph(graph)
        content = exporter.export_to_string(data)
    elif fmt == "dot":
        gv_exporter = GraphvizExporter(graph)
        content = gv_exporter.to_dot()
    elif fmt == "mermaid":
        mm_exporter = MermaidExporter(graph)
        content = mm_exporter.to_mermaid()
    else:
        print(f"Erro: formato desconhecido '{fmt}'", file=sys.stderr)
        return 1

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        print(f"Exportado para: {output_path}")
    else:
        print(content)

    return 0


COMMAND_MAP = {
    "parse": run_parse,
    "graph": run_graph,
    "validate": run_validate,
    "lineage": run_lineage,
    "export": run_export,
}


def main(argv: list[str | None] = None) -> int:
    """Ponto de entrada principal da CLI."""
    parser = create_parser()
    args = parser.parse_args(argv)

    if not args.command:
        print(QUICK_START)
        return 0

    setup_logging(args.verbose)

    command_func = COMMAND_MAP.get(args.command)
    if command_func:
        return command_func(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
