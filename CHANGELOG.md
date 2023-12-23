# Changelog

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
