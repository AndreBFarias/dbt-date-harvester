# API Reference - dbt-date-harvester

## Parsers

### YamlParser

```python
class YamlParser(project_dir: Path)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `parse_file(filepath)` | `Dict[str, Any]` | Faz parsing de arquivo YAML |
| `find_yaml_files()` | `List[Path]` | Encontra YAML no projeto |
| `parse_all()` | `Dict[str, Dict]` | Faz parsing de todos os YAML |
| `extract_version(content)` | `Optional[int]` | Extrai versão |
| `extract_models_section(content)` | `List[Dict]` | Extrai seção models |
| `extract_sources_section(content)` | `List[Dict]` | Extrai seção sources |

### SqlParser

```python
class SqlParser(project_dir: Path)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `parse_file(filepath)` | `SqlModelInfo` | Faz parsing de arquivo SQL |
| `find_sql_files()` | `List[Path]` | Encontra SQL no projeto |
| `parse_all()` | `Dict[str, SqlModelInfo]` | Faz parsing de todos os SQL |
| `get_model(name)` | `Optional[SqlModelInfo]` | Retorna modelo por nome |
| `get_all_refs()` | `Dict[str, List[str]]` | Mapa de refs por modelo |
| `get_all_sources()` | `Dict[str, List[Tuple]]` | Mapa de sources por modelo |

### JinjaParser

```python
class JinjaParser()
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `parse_content(content, filepath)` | `JinjaAnalysis` | Analisa Jinja em SQL |
| `strip_jinja(content)` | `str` | Remove blocos Jinja |
| `get_jinja_complexity(content)` | `Dict[str, int]` | Complexidade Jinja |
| `get_all_variables()` | `Set[str]` | Todas as variáveis |

### SchemaExtractor

```python
class SchemaExtractor(yaml_parser: YamlParser)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `extract_models(content)` | `List[ModelInfo]` | Extrai modelos |
| `extract_sources(content)` | `List[SourceInfo]` | Extrai sources |
| `get_model_by_name(name)` | `Optional[ModelInfo]` | Busca modelo |
| `get_column_tests(model_name)` | `Dict[str, List]` | Testes por coluna |

## Analyzers

### GraphResolver

```python
class GraphResolver()
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `add_node(node)` | `None` | Adiciona nó |
| `add_edge(from, to)` | `None` | Adiciona aresta |
| `get_dependencies(name)` | `Set[str]` | Dependências diretas |
| `get_dependents(name)` | `Set[str]` | Dependentes diretos |
| `get_all_upstream(name)` | `Set[str]` | Upstream transitivo |
| `get_all_downstream(name)` | `Set[str]` | Downstream transitivo |
| `topological_sort()` | `List[str]` | Ordem topológica |
| `detect_cycles()` | `List[List[str]]` | Ciclos encontrados |
| `get_root_nodes()` | `Set[str]` | Nós raiz |
| `get_leaf_nodes()` | `Set[str]` | Nós folha |

### DependencyAnalyzer

```python
class DependencyAnalyzer(graph: GraphResolver, sql_parser: SqlParser)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `build_dependency_graph()` | `None` | Constrói grafo |
| `analyze_model(name)` | `DependencyReport` | Analisa modelo |
| `analyze_all()` | `List[DependencyReport]` | Analisa todos |
| `get_execution_order()` | `List[str]` | Ordem execução |
| `get_most_depended_on(n)` | `List[Tuple]` | Mais dependidos |

### ImpactAnalyzer

```python
class ImpactAnalyzer(graph: GraphResolver)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `analyze_impact(model)` | `ImpactReport` | Analisa impacto |
| `find_high_impact_models(n)` | `List[Tuple]` | Alto impacto |
| `get_critical_path()` | `List[str]` | Caminho crítico |

## Validators

### ModelValidator

```python
class ModelValidator(extractor: SchemaExtractor, sql_parser: SqlParser)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `validate_all()` | `List[ValidationResult]` | Todas as validações |
| `get_summary()` | `Dict[str, int]` | Resumo |

### NamingValidator

```python
class NamingValidator(graph: GraphResolver, conventions: Optional[List])
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `validate_model_names()` | `List[ValidationResult]` | Valida nomes |
| `validate_column_names(cols)` | `List[ValidationResult]` | Valida colunas |
| `validate_all(cols)` | `List[ValidationResult]` | Todas validações |

## Exporters

### JsonExporter

```python
class JsonExporter(indent: int = 2)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `export_graph(graph)` | `Dict` | Exporta grafo |
| `export_to_file(data, path)` | `None` | Salva em arquivo |
| `export_to_string(data)` | `str` | Exporta para string |

### GraphvizExporter

```python
class GraphvizExporter(graph: GraphResolver)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `to_dot(**kwargs)` | `str` | Gera DOT |
| `to_dot_with_layers()` | `str` | DOT com camadas |
| `export_to_file(path)` | `None` | Salva em arquivo |

### MermaidExporter

```python
class MermaidExporter(graph: GraphResolver)
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `to_mermaid(**kwargs)` | `str` | Gera Mermaid |
| `to_mermaid_with_subgraphs()` | `str` | Mermaid agrupado |
| `to_mermaid_lineage(model)` | `str` | Mermaid de linhagem |
| `export_to_file(path)` | `None` | Salva em arquivo |

## Plugins

### BasePlugin (ABC)

```python
class BasePlugin(ABC)
```

Propriedades abstratas: `name`, `version`

Hooks disponíveis:
- `on_load()`, `on_unload()`
- `on_parse_start(project_dir)`, `on_parse_complete(graph)`
- `on_validate_start()`, `on_validate_complete(results)`
- `register_validators()`, `register_exporters()`

### PluginManager

```python
class PluginManager()
```

| Método | Retorno | Descrição |
|--------|---------|-----------|
| `register(plugin_class)` | `None` | Registra plugin |
| `unregister(name)` | `bool` | Remove plugin |
| `emit(event, *args)` | `None` | Emite evento |
| `collect_validators()` | `List[Callable]` | Coleta validadores |
| `collect_exporters()` | `Dict[str, Callable]` | Coleta exportadores |
