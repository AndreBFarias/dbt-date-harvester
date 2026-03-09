# dbt-date-harvester

Ferramenta para extração automática de datas de referência do BigQuery e preenchimento do CSV do Painel Estratégico, com interface TUI interativa e modo batch.

Inclui também um parser de análise estática para projetos [dbt](https://www.getdbt.com/), com grafos de dependência, validação e exportação.

## Requisitos

- Python 3.8+
- Credencial de serviço GCP (JSON) com acesso ao BigQuery
- Arquivo CSV/XLSX do Painel Estratégico

## Instalação

### Linux / macOS

```bash
./run.sh
```

O script cria o virtualenv, instala dependências e inicia a TUI automaticamente. Para instalação manual:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

### Windows

```cmd
run.bat
```

O script cria o virtualenv, instala dependências e inicia a TUI automaticamente. Para instalação manual:

```cmd
python -m venv venv
venv\Scripts\activate
pip install -e .
```

### Desenvolvimento

```bash
pip install -e ".[dev]"
```

## Uso rápido

### TUI interativa (modo padrão)

```bash
python main.py
```

A interface guia o usuário em 4 etapas:

1. **Config** - Credencial BigQuery, projeto GCP, dataset (auto-skip se `.env` válido)
2. **Arquivo** - Seleção do CSV/XLSX, preview com colunas de data, modo rastreio opcional
3. **Execução** - Progresso em tempo real com timer e log
4. **Resultados** - Tabela com diff antigo/novo, filtros por status, cores por categoria

### Modo batch (headless)

```bash
python main.py --headless --input "data_input/arquivo.csv" --output "data_output"
```

### CLI do dbt-parser (análise estática)

```bash
python main.py --cli parse --project-dir /caminho/projeto/dbt
python main.py --cli graph --project-dir /caminho/projeto/dbt
python main.py --cli validate --project-dir /caminho/projeto/dbt
python main.py --cli lineage --project-dir /caminho/projeto/dbt --model fct_event_dates
python main.py --cli export --project-dir /caminho/projeto/dbt --format json --output graph.json
```

## Configuração

Crie um arquivo `.env` na raiz (ou use a TUI para gerar):

```env
CREDENTIAL_PATH=credentials/sua-credencial.json
GCP_PROJECT=seu-projeto-gcp
BQ_DATASET=seu_dataset
BQ_LOCATION=southamerica-east1
INPUT_FILE=data_input/sua_planilha.xlsx
OUTPUT_DIR=data_output
SQL_SOURCES_DIR=
CSV_DELIMITER=;
CSV_ENCODING=utf-8-sig
MAX_RETRIES=3
LOG_DIR=data_output
```

Veja `.env.example` para referência.

## Estrutura

```
main.py               - Ponto de entrada (TUI, headless, CLI)
harvester/             - Motor de extração de datas
  config.py            - Settings com .env
  date_harvester.py    - Orquestrador principal
  bigquery_client.py   - Consultas ao BigQuery
  csv_handler.py       - Leitura/escrita CSV/XLSX
  date_detector.py     - Classificação de colunas de data
  sql_date_tracer.py   - Rastreamento modo rastreio via SQL
  models.py            - DateResult, HarvestReport, TableInfo
tui/                   - Interface TUI (Textual)
  app.py               - Wizard com TabbedContent
  screens/             - Panes: config, input, execução, resultados
  styles/              - TCSS
dbt_parser/            - Parser de análise estática dbt
  cli.py               - Interface de linha de comando
  parsers/             - Parsing de YAML, SQL e Jinja
  analyzers/           - Grafos, linhagem, dependências
  validators/          - Validação de modelos e configs
  exporters/           - JSON, GraphViz, Mermaid
scripts/               - Scripts auxiliares
tests/                 - Suite de testes pytest
docs/                  - Documentação detalhada
```

## Documentação

- [Guia de usuário](GUIDE.md)
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
