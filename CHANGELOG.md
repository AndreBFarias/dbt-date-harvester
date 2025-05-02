# Changelog

## [2.1.0] - 2026-03-12

### Adicionado
- Colunas de metadados no CSV de saída para transparência de premissas:
  - **Coluna de Origem**: qual coluna do BigQuery foi usada para extrair a data
  - **Granularidade**: precisão do dado-fonte (Anual, Mensal, Diária)
  - **Data Inferida**: indica se dia/mês foi calculado (Sim/Não) ou veio direto da base
- Campo `tipo_bq` no `DateResult` para rastrear o tipo original da coluna
- Funções utilitárias `granularidade_from_tipo_bq` e `data_inferida_from_tipo_bq`
- Coluna "Granularidade" na TUI com destaque amarelo para datas inferidas

### Alterado
- `ColumnLayout` expandido com índices de metadados (colunas 12-14)
- `_aplicar_resultados` estende automaticamente o header e as linhas do CSV
- Funciona com entrada CSV e XLSX (metadados adicionados na fase de processamento)

## [2.0.1] - 2026-03-11

### Corrigido
- Scripts de inicialização (run.bat/run.sh) falham com espaços no caminho do projeto
- Botão Executar sem feedback visível quando nenhum arquivo selecionado
- Falha silenciosa ao listar arquivos com caracteres especiais no nome (encoding Windows)
- Comparação de path do .env falha com caracteres especiais (fallback por nome)

### Adicionado
- Binding F5 para executar via teclado (alternativa ao click do mouse)
- Auto-seleção de arquivo quando apenas um disponível no diretório
- Testes de compatibilidade Windows (test_input_compat.py)

### Alterado
- Instalação via requirements.txt em vez de editable install nos scripts auxiliares
- README.md atualizado com instruções de instalação consistentes

## [2.0.0] - 2026-03-05

### Adicionado
- Motor de extração de datas do BigQuery (`harvester/`)
- Interface TUI interativa com wizard de 4 etapas (Textual)
- Modo batch headless (`--headless --input <csv>`)
- Detecção automática de colunas de data com classificação e confiança
- Modo rastreio: rastreamento de origens via SQLs do dbt
- Preview do CSV com colunas de data e contadores
- Filtros por status na tela de resultados (Novos, Atualizados, Erros, Rastreio)
- Diff visual antigo/novo nos resultados
- Timer de duração na execução
- Auto-skip da configuração quando `.env` válido
- Persistência do `SQL_SOURCES_DIR` no `.env`
- Scripts auxiliares de investigação e validação cruzada

### Alterado
- README.md reescrito para cobrir harvester + TUI + CLI
- GUIDE.md atualizado com seção do harvester
- TUI refatorada de fluxo multi-tela para wizard com TabbedContent
- Tela de modo eliminada (checkbox integrado na seleção de arquivo)
- Botões de execução separados (Cancelar/Ver Resultados) em vez de mutação de ID
- setup.py com dependências do harvester (google-cloud-bigquery, textual, openpyxl)

## [1.1.0] - 2025-03-05

### Adicionado
- Comando `export` na CLI (formatos: json, dot, mermaid)
- Filtro `--model` no export para linhagem específica
- Arquivo LICENSE (GPL-3.0)
- Guia de usuário completo (GUIDE.md)

### Corrigido
- Compatibilidade com Python 3.8 (type hints com `from __future__ import annotations`)
- Versão consolidada em `dbt_parser/version.py` como fonte única

### Alterado
- README.md reescrito com documentação completa dos 5 comandos
- setup.py com metadados completos e leitura dinâmica de versão
- docs/tutorial.md atualizado com comando export

## [1.0.0] - 2023-01-31

### Adicionado
- Parser YAML para schema.yml, sources.yml e dbt_project.yml
- Parser SQL com extração de refs, sources, configs e CTEs
- Parser Jinja para análise de templates
- Extrator de schema com modelos, colunas e testes
- Parser de sources dedicado
- Extrator de testes (schema tests e data tests)
- Expansor de macros Jinja
- Grafo de dependências com networkx
- Rastreador de linhagem de dados
- Analisador de dependências com ordem topológica
- Resolver de referências ref() e source()
- Analisador de impacto de mudanças
- Detector de modelos não utilizados
- Métricas de complexidade SQL
- Detector de padrões duplicados
- Validador de modelos contra boas práticas
- Validador de convenções de nomenclatura
- Validador de configurações de projeto
- Exportação para JSON
- Exportação para GraphViz DOT
- Exportação para Mermaid
- Sistema de filtragem avançado com seletores
- Busca fuzzy de modelos
- Cache de resultados em memória
- Rastreamento de performance
- Sistema de plugins extensível
- Interface CLI completa (parse, graph, validate, lineage)
- Suite de testes com pytest (cobertura acima de 85%)
- CI/CD com GitHub Actions
- Pre-commit hooks configurados
- Documentação completa (arquitetura, features, tutorial, api reference)
