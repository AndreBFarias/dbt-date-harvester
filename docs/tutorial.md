# Tutorial - dbt-date-harvester

## Passo 1: Instalação

```bash
git clone <repo-url>
cd dbt-date-harvester
pip install -e .
```

## Passo 2: Primeiro Parsing

```bash
dbt-parser parse --project-dir /seu/projeto/dbt -v
```

Saída esperada:
```
Projeto: /seu/projeto/dbt
Arquivos YAML: 3
Modelos SQL: 8
  - stg_events: refs=[], sources=[('raw', 'events')]
  - stg_dates: refs=[], sources=[('raw', 'dates')]
  - fct_event_dates: refs=['stg_events', 'stg_dates'], sources=[]
```

## Passo 3: Visualizar Grafo de Dependências

```bash
dbt-parser graph --project-dir /seu/projeto/dbt
```

Saída:
```
Nós: 5
Arestas: 4
Raízes: ['raw.dates', 'raw.events']
Folhas: ['fct_event_dates']
```

## Passo 4: Análise de Modelo Específico

```bash
dbt-parser graph --project-dir /seu/projeto/dbt --model fct_event_dates
```

## Passo 5: Validação

```bash
dbt-parser validate --project-dir /seu/projeto/dbt --severity info
```

## Passo 6: Linhagem de Dados

```bash
dbt-parser lineage --project-dir /seu/projeto/dbt --model fct_event_dates
```

## Passo 7: Exportação

### JSON
```bash
dbt-parser export --project-dir /seu/projeto/dbt --format json --output output/graph.json
```

### GraphViz DOT
```bash
dbt-parser export --project-dir /seu/projeto/dbt --format dot --output output/graph.dot
dot -Tpng output/graph.dot -o output/graph.png
```

### Mermaid
```bash
dbt-parser export --project-dir /seu/projeto/dbt --format mermaid --output output/graph.mmd
```

### Linhagem de modelo específico
```bash
dbt-parser export --project-dir /seu/projeto/dbt --format dot --model fct_event_dates --output output/lineage.dot
```

## Passo 8: Uso Programático

```python
from pathlib import Path
from dbt_parser.parsers import YamlParser, SqlParser, SchemaExtractor
from dbt_parser.analyzers import GraphResolver, DependencyAnalyzer, ImpactAnalyzer
from dbt_parser.validators import ModelValidator

project = Path("/seu/projeto/dbt")

# Parsing
yaml_parser = YamlParser(project)
sql_parser = SqlParser(project)
extractor = SchemaExtractor(yaml_parser)

yaml_parser.parse_all()
sql_parser.parse_all()

for content in yaml_parser.get_parsed_files().values():
    if "models" in content:
        extractor.extract_models(content)

# Grafo
graph = GraphResolver()
dep = DependencyAnalyzer(graph, sql_parser)
dep.build_dependency_graph()

# Impacto
impact = ImpactAnalyzer(graph)
report = impact.analyze_impact("stg_events")
print(f"Modelos afetados: {report.total_affected}")
print(f"Risco: {report.risk_level}")

# Validação
validator = ModelValidator(extractor, sql_parser)
results = validator.validate_all()
for r in results:
    print(f"[{r.severity.value}] {r.model_name}: {r.message}")
```

## Passo 9: Plugins

```python
from dbt_parser.plugins import BasePlugin, PluginManager

class MeuPlugin(BasePlugin):
    @property
    def name(self) -> str:
        return "meu-plugin"

    @property
    def version(self) -> str:
        return "0.1.0"

    def on_parse_complete(self, graph):
        print(f"Parsing concluído: {graph.node_count()} nós")

manager = PluginManager()
manager.register(MeuPlugin)
```
