# Exemplos de Uso - Harvestr

## Exemplo 1: Parsing Básico

```python
from pathlib import Path
from dbt_parser.parsers.yaml_parser import YamlParser
from dbt_parser.parsers.sql_parser import SqlParser

project_dir = Path("/caminho/projeto/dbt")

yaml_parser = YamlParser(project_dir)
yaml_files = yaml_parser.parse_all()
print(f"Encontrados {len(yaml_files)} arquivos YAML")

sql_parser = SqlParser(project_dir)
models = sql_parser.parse_all()
print(f"Encontrados {len(models)} modelos SQL")

for name, model in models.items():
    print(f"  {name}:")
    print(f"    refs: {model.refs}")
    print(f"    sources: {model.sources}")
    print(f"    config: {model.config}")
```

## Exemplo 2: Análise de Dependências

```python
from dbt_parser.analyzers.graph_resolver import GraphResolver
from dbt_parser.analyzers.dependency_analyzer import DependencyAnalyzer

graph = GraphResolver()
analyzer = DependencyAnalyzer(graph, sql_parser)
analyzer.build_dependency_graph()

print(f"Total nós: {graph.node_count()}")
print(f"Total arestas: {graph.edge_count()}")
print(f"Ordem execução: {analyzer.get_execution_order()}")
print(f"Modelos isolados: {analyzer.get_isolated_models()}")

```

## Exemplo 3: Exportação

```python
from dbt_parser.exporters.json_exporter import JsonExporter
from dbt_parser.exporters.graphviz_exporter import GraphvizExporter
from dbt_parser.exporters.mermaid_exporter import MermaidExporter

json_exp = JsonExporter()
json_exp.export_to_file(json_exp.export_graph(graph), Path("output/graph.json"))

gv_exp = GraphvizExporter(graph)
gv_exp.export_to_file(Path("output/graph.dot"), title="Meu Projeto dbt")

mermaid_exp = MermaidExporter(graph)
mermaid_exp.export_to_file(Path("output/graph.mmd"))
```

## Exemplo 4: Validação Completa

```python
from dbt_parser.validators.model_validator import ModelValidator
from dbt_parser.validators.naming_validator import NamingValidator
from dbt_parser.validators.config_validator import ConfigValidator

model_val = ModelValidator(schema_extractor, sql_parser)
results = model_val.validate_all()

for r in results:
    print(f"[{r.severity.value}] {r.model_name}: {r.message}")
```

## Exemplo 5: CLI

```bash
# Parsing básico
dbt-parser parse --project-dir /caminho/projeto

# Análise de grafo
dbt-parser graph --project-dir /caminho/projeto

# Análise de modelo específico
dbt-parser graph --project-dir /caminho/projeto --model fct_event_dates

# Validação
dbt-parser validate --project-dir /caminho/projeto --severity warning

# Linhagem
dbt-parser lineage --project-dir /caminho/projeto --model fct_event_dates
```
