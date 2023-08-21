# Arquitetura - dbt-date-harvester

## Visão Geral

O dbt-date-harvester é uma ferramenta de análise estática para projetos dbt.
Faz parsing de arquivos YAML e SQL, constrói grafos de dependência e fornece
ferramentas de validação e análise.

## Componentes Principais

### Parsers

- **YamlParser**: Parsing de arquivos YAML (schema.yml, sources.yml, dbt_project.yml)
- **SqlParser**: Parsing de arquivos SQL com extração de refs, sources e configs
- **JinjaParser**: Análise de templates Jinja em SQL dbt
- **MacroExpander**: Parsing e catálogo de macros Jinja
- **SourceParser**: Parser especializado para sources dbt
- **TestExtractor**: Extração de testes schema e data tests
- **SchemaExtractor**: Extração estruturada de informações de schema.yml

### Analyzers

- **GraphResolver**: Construção e consulta de grafos de dependência com networkx
- **LineageTracker**: Rastreamento de linhagem de dados
- **DependencyAnalyzer**: Análise avançada de dependências entre modelos
- **RefResolver**: Resolução de referências ref() e source()

### Validators

- **ModelValidator**: Validação de modelos contra boas práticas

## Fluxo de Dados

```
Arquivos dbt -> Parsers -> Estruturas de Dados -> Analyzers -> Resultados
                                               -> Validators -> Relatórios
```

## Dependências Externas

- **pyyaml**: Parsing de YAML
- **networkx**: Grafos de dependência

## Estrutura de Diretórios

```
dbt_parser/
  parsers/       - Módulos de parsing (YAML, SQL, Jinja)
  analyzers/     - Módulos de análise (grafos, linhagem, dependências)
  validators/    - Módulos de validação (modelos, nomes, configs)
  exporters/     - Módulos de exportação (JSON, GraphViz, Mermaid)
  utils/         - Utilitários (cache, busca)
tests/           - Suite de testes com pytest
docs/            - Documentação do projeto
```
