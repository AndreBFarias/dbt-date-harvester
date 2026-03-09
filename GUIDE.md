# Guia de Usuário - dbt-date-harvester

## Introdução

O dbt-date-harvester é uma ferramenta com duas funcionalidades principais:

1. **Harvester** - Extração automática de datas de referência do BigQuery para o CSV do Painel Estratégico
2. **dbt-parser** - Análise estática de projetos dbt com grafos de dependência, validação e exportação

### Harvester

O harvester conecta ao BigQuery, identifica colunas de data nas tabelas listadas no CSV, extrai a data mais recente e preenche automaticamente as colunas de referência. Suporta:

- Detecção automática de colunas de data com classificação (referência, atualização, dados)
- Nível de confiança na classificação
- Modo rastreio: rastreamento de origens via SQLs do dbt quando a tabela final não tem coluna de data
- Fuzzy match de nomes de tabela (ex: `tabela` -> `painel_tabela`)

### dbt-parser

Parser de análise estática que faz parsing dos arquivos YAML e SQL do projeto dbt, constrói grafos de dependência e oferece recursos de validação, linhagem e exportação sem conexão com banco de dados.

## Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)
- Credencial de serviço GCP (JSON) com acesso ao BigQuery (para o harvester)
- Arquivo CSV/XLSX do Painel Estratégico (para o harvester)
- Um projeto dbt com arquivos SQL e YAML (para o dbt-parser)

## Instalação

### Instalação básica

```bash
git clone <url-do-repositorio>
cd dbt-date-harvester
pip install -e .
```

### Instalação para desenvolvimento

```bash
pip install -e ".[dev]"
```

Isso instala dependências de teste (pytest, pytest-cov), linting (flake8, black, isort, mypy) e pre-commit hooks.

### Verificar instalação

```bash
dbt-parser --version
```

---

## Harvester - TUI interativa

### Iniciar

```bash
python main.py
```

A TUI apresenta um wizard com 4 abas:

### 1. Config

Configuração da conexão com BigQuery:
- **Credencial**: selecione o arquivo JSON na pasta `credentials/`
- **Projeto GCP**: identificador do projeto (ex: `br-mec-segape-dev`)
- **Dataset**: dataset do BigQuery (ex: `projeto_painel_ministro`)
- **Localização**: região do BigQuery (ex: `southamerica-east1`)

Se o `.env` já existe e está válido, esta aba é pulada automaticamente.

Botões:
- **Testar Conexão**: valida as credenciais contra o BigQuery
- **Salvar .env**: persiste a configuração em arquivo
- **Confirmar**: avança para a próxima aba

### 2. Arquivo

Seleção do arquivo de entrada e modo de execução:
- Selecione um CSV ou XLSX na pasta `data_input/`
- Para XLSX, selecione a aba desejada
- O preview mostra as colunas de data com status (Vazio/Preenchido)
- Marque "Modo Rastreio" para rastrear origens via SQLs do dbt (requer diretório dos SQLs)

### 3. Execução

Acompanhe o progresso em tempo real:
- Barra de progresso com contagem de tabelas processadas
- Log detalhado de cada operação
- Timer de duração
- Botão "Cancelar" interrompe a execução
- Ao concluir, botão "Ver Resultados" aparece com notificação

### 4. Resultados

Tabela com todos os resultados:
- **Tabela**: nome curto (sem project.dataset)
- **Tipo**: classificação da coluna de data
- **Coluna**: coluna de origem (prefixo `[R]` para modo rastreio)
- **Antigo**: valor anterior no CSV
- **Novo**: valor extraído do BigQuery
- **Confiança**: nível de confiança da classificação (verde >= 85%, amarelo >= 70%)
- **Status**: Novo (verde), Atualizado (amarelo), Erro (vermelho), Rastreio (ciano), Sem data

Filtros: Todos, Novos, Atualizados, Erros, Rastreio, Sem data

### Modo batch (headless)

```bash
python main.py --headless --input "data_input/arquivo.csv"
python main.py --headless --input "data_input/arquivo.csv" --output "data_output"
```

---

## dbt-parser - Comandos da CLI

Todos os comandos aceitam as seguintes opções globais:

| Opção | Descrição |
|-------|-----------|
| `--project-dir PATH` | Diretório raiz do projeto dbt (default: diretório atual) |
| `-v` | Aumenta verbosidade (INFO) |
| `-vv` | Modo debug |
| `--version` | Exibe versão |
| `--help` | Exibe ajuda |

---

### parse

Faz parsing do projeto dbt, extraindo metadados de arquivos YAML e SQL.

```bash
dbt-parser parse --project-dir /caminho/projeto/dbt
```

Saída esperada:
```
Projeto: /caminho/projeto/dbt
Arquivos YAML: 3
Modelos SQL: 8
  - stg_events: refs=[], sources=[('raw', 'events')]
  - stg_dates: refs=[], sources=[('raw', 'dates')]
  - fct_event_dates: refs=['stg_events', 'stg_dates'], sources=[]
```

Para cada modelo SQL, mostra as referências (`ref()`) e fontes (`source()`) encontradas.

---

### graph

Analisa o grafo de dependências do projeto.

```bash
# Visão geral
dbt-parser graph --project-dir /caminho/projeto/dbt
```

Saída:
```
Nós: 5
Arestas: 4
Raízes: ['raw.dates', 'raw.events']
Folhas: ['fct_event_dates']
```

```bash
# Modelo específico
dbt-parser graph --project-dir /caminho/projeto/dbt --model fct_event_dates
```

Saída:
```
Modelo: fct_event_dates
  Dependências diretas: ['stg_events', 'stg_dates']
  Dependentes diretos: []
```

---

### validate

Valida o projeto contra boas práticas de dbt.

```bash
dbt-parser validate --project-dir /caminho/projeto/dbt --severity info
```

Níveis de severidade: `error`, `warning`, `info`. O filtro `--severity` define o nível mínimo exibido (default: warning).

Saída:
```
[WARNING] raw_events: Modelo sem documentação no schema.yml
[INFO] stg_events: Modelo sem testes definidos
[ERROR] fct_dates: Nome não segue convenção (esperado: fct_ ou dim_)
```

O comando retorna código 1 se houver erros, 0 caso contrário.

---

### lineage

Rastreia a linhagem completa de um modelo (upstream e downstream).

```bash
dbt-parser lineage --project-dir /caminho/projeto/dbt --model fct_event_dates
```

Saída:
```
Linhagem de: fct_event_dates
  Upstream: ['stg_events', 'stg_dates', 'raw.events', 'raw.dates']
  Downstream: []
  Profundidade upstream: 2
  Profundidade downstream: 0
```

---

### export

Exporta o grafo de dependências para diferentes formatos.

#### JSON

```bash
# Para stdout
dbt-parser export --project-dir /caminho/projeto/dbt --format json

# Para arquivo
dbt-parser export --project-dir /caminho/projeto/dbt --format json --output graph.json
```

#### GraphViz DOT

```bash
dbt-parser export --project-dir /caminho/projeto/dbt --format dot --output graph.dot

# Para gerar imagem PNG (requer graphviz instalado)
dot -Tpng graph.dot -o graph.png
```

#### Mermaid

```bash
dbt-parser export --project-dir /caminho/projeto/dbt --format mermaid --output graph.mmd
```

O formato Mermaid pode ser renderizado diretamente em Markdown no GitHub, GitLab e outras plataformas.

#### Filtrar por modelo

Use `--model` para exportar apenas a linhagem de um modelo específico:

```bash
dbt-parser export --project-dir /caminho/projeto/dbt --format dot --model fct_event_dates --output lineage.dot
```

Isso gera um subgrafo contendo o modelo, seus ancestrais (upstream) e seus descendentes (downstream).

---

## Uso como biblioteca Python

O dbt-date-harvester pode ser usado programaticamente:

### Parsing básico

```python
from pathlib import Path
from dbt_parser.parsers import YamlParser, SqlParser, SchemaExtractor

project = Path("/caminho/projeto/dbt")

yaml_parser = YamlParser(project)
sql_parser = SqlParser(project)
extractor = SchemaExtractor(yaml_parser)

yaml_parser.parse_all()
sql_parser.parse_all()

for content in yaml_parser.get_parsed_files().values():
    if "models" in content:
        extractor.extract_models(content)
```

### Grafo de dependências

```python
from dbt_parser.analyzers import GraphResolver, DependencyAnalyzer

graph = GraphResolver()
analyzer = DependencyAnalyzer(graph, sql_parser)
analyzer.build_dependency_graph()

print(f"Nós: {graph.node_count()}")
print(f"Raízes: {graph.get_root_nodes()}")

report = analyzer.analyze_model("fct_event_dates")
print(f"Dependências: {report.direct_dependencies}")
```

### Análise de impacto

```python
from dbt_parser.analyzers import ImpactAnalyzer

impact = ImpactAnalyzer(graph)
report = impact.analyze_impact("stg_events")
print(f"Modelos afetados: {report.total_affected}")
print(f"Risco: {report.risk_level}")
```

### Validação

```python
from dbt_parser.validators import ModelValidator

validator = ModelValidator(extractor, sql_parser)
results = validator.validate_all()
for r in results:
    print(f"[{r.severity.value}] {r.model_name}: {r.message}")
```

### Exportação

```python
from dbt_parser.exporters import JsonExporter, GraphvizExporter, MermaidExporter

# JSON
json_exp = JsonExporter()
data = json_exp.export_graph(graph)
json_exp.export_to_file(data, Path("graph.json"))

# GraphViz
gv_exp = GraphvizExporter(graph)
gv_exp.export_to_file(Path("graph.dot"))

# Mermaid
mm_exp = MermaidExporter(graph)
mm_exp.export_to_file(Path("graph.mmd"))
```

---

## Sistema de plugins

Crie plugins para estender a ferramenta com validadores e exportadores customizados.

### Criar um plugin

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

    def register_validators(self):
        return [minha_funcao_validadora]

    def register_exporters(self):
        return {"csv": minha_funcao_exportadora}
```

### Registrar e usar

```python
manager = PluginManager()
manager.register(MeuPlugin)

# Emitir eventos
manager.emit("on_parse_complete", graph)

# Coletar validadores e exportadores de todos os plugins
validadores = manager.collect_validators()
exportadores = manager.collect_exporters()
```

### Hooks disponíveis

| Hook | Quando é chamado |
|------|-----------------|
| `on_load()` | Plugin registrado |
| `on_unload()` | Plugin removido |
| `on_parse_start(project_dir)` | Antes do parsing |
| `on_parse_complete(graph)` | Após parsing |
| `on_validate_start()` | Antes da validação |
| `on_validate_complete(results)` | Após validação |

### Carregar plugin de módulo externo

```python
manager.load_plugin_from_module("meu_pacote.plugins.custom_plugin")
```

O manager detecta automaticamente subclasses de `BasePlugin` no módulo importado.

---

## Troubleshooting

### Erro "Diretório não encontrado"

Verifique se o `--project-dir` aponta para a raiz do projeto dbt (onde está o `dbt_project.yml`).

### Nenhum modelo encontrado no parsing

- O parser busca arquivos `.sql` nos subdiretórios do projeto.
- Verifique se os modelos estão em `models/` ou nos paths configurados no `dbt_project.yml`.

### Modelo não encontrado no grafo

- Use o nome do modelo sem extensão (ex: `fct_event_dates`, não `fct_event_dates.sql`).
- Use `-v` para ver logs detalhados do parsing.

### Erros de validação inesperados

- Use `--severity info` para ver todos os avisos, incluindo os informativos.
- Convenções de nomenclatura esperadas: `stg_` (staging), `fct_` (fatos), `dim_` (dimensões).
