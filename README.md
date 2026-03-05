# dbt-date-harvester

Ferramenta de parsing e análise estática para projetos [dbt](https://www.getdbt.com/) (data build tool).

Extrai metadados de arquivos YAML e SQL, constrói grafos de dependência e oferece validação, linhagem e exportação sem precisar de conexão com banco de dados.

## Requisitos

- Python 3.8+
- pyyaml >= 6.0
- networkx >= 2.8

## Instalação

```bash
pip install -e .
```

Para desenvolvimento:

```bash
pip install -e ".[dev]"
```

## Uso rápido

### parse - Extrair metadados do projeto

```bash
dbt-parser parse --project-dir /caminho/projeto/dbt
```

Saída:
```
Projeto: /caminho/projeto/dbt
Arquivos YAML: 3
Modelos SQL: 8
  - stg_events: refs=[], sources=[('raw', 'events')]
  - fct_event_dates: refs=['stg_events', 'stg_dates'], sources=[]
```

### graph - Analisar grafo de dependências

```bash
# Visao geral do grafo
dbt-parser graph --project-dir /caminho/projeto/dbt

# Analise de modelo especifico
dbt-parser graph --project-dir /caminho/projeto/dbt --model fct_event_dates
```

### validate - Validar boas práticas

```bash
dbt-parser validate --project-dir /caminho/projeto/dbt --severity warning
```

### lineage - Rastrear linhagem de dados

```bash
dbt-parser lineage --project-dir /caminho/projeto/dbt --model fct_event_dates
```

### export - Exportar grafo

```bash
# JSON
dbt-parser export --project-dir /caminho/projeto/dbt --format json --output graph.json

# GraphViz DOT
dbt-parser export --project-dir /caminho/projeto/dbt --format dot --output graph.dot

# Mermaid
dbt-parser export --project-dir /caminho/projeto/dbt --format mermaid --output graph.mmd

# Exportar linhagem de um modelo especifico
dbt-parser export --project-dir /caminho/projeto/dbt --format dot --model fct_event_dates --output lineage.dot
```

## Uso como biblioteca

```python
from pathlib import Path
from dbt_parser.parsers import YamlParser, SqlParser
from dbt_parser.analyzers import GraphResolver, DependencyAnalyzer
from dbt_parser.exporters import JsonExporter

project = Path("/caminho/projeto/dbt")

yaml_parser = YamlParser(project)
sql_parser = SqlParser(project)
yaml_parser.parse_all()
sql_parser.parse_all()

graph = GraphResolver()
analyzer = DependencyAnalyzer(graph, sql_parser)
analyzer.build_dependency_graph()

exporter = JsonExporter()
data = exporter.export_graph(graph)
exporter.export_to_file(data, Path("graph.json"))
```

## Estrutura

```
dbt_parser/
  cli.py            - Interface de linha de comando
  parsers/          - Parsing de YAML, SQL e Jinja
  analyzers/        - Grafos, linhagem, dependencias, impacto
  validators/       - Validacao de modelos, nomes, configs
  exporters/        - Exportacao JSON, GraphViz, Mermaid
  utils/            - Cache, busca fuzzy, performance
  plugins/          - Sistema de plugins extensivel
tests/              - Suite de testes pytest
docs/               - Documentacao detalhada
```

## Documentação

- [Guia de usuário](GUIDE.md) - uso detalhado de todos os comandos e funcionalidades
- [Arquitetura](docs/architecture.md)
- [Features](docs/features.md)
- [Tutorial](docs/tutorial.md)
- [API Reference](docs/api_reference.md)
- [Exemplos](docs/examples.md)

## Testes

```bash
pytest
pytest --cov=dbt_parser
```

## Licença

[GPL-3.0](LICENSE)
