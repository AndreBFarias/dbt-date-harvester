# Changelog

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
