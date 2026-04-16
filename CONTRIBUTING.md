# Contribuindo com Harvestr

## Configuração do ambiente

1. Clone o repositório
2. Instale as dependências: `pip install -r requirements.txt`
3. Configure as variáveis de ambiente conforme `.env.example`
4. Execute os testes: `python -m pytest`

## Fluxo de contribuição

1. Abra uma issue descrevendo a mudança proposta
2. Faça fork do repositório
3. Crie um branch: `git checkout -b fix/nome-da-correcao`
4. Implemente as mudanças com testes
5. Abra um Pull Request referenciando a issue

## Padrões de código

- Python 3.10+
- Type hints obrigatórios
- Docstrings em PT-BR
- Logging via `logging` padrão (nunca `print()`)
- Formatação: seguir PEP 8

## Padrões de SQL/dbt

- Nomes de modelos em snake_case
- CTEs comentadas com propósito
- Testes de schema para todas as colunas críticas
- Documentação em `schema.yml`

## Mensagens de commit

Formato: `tipo: descrição imperativa em PT-BR`

Tipos: `feat`, `fix`, `refactor`, `docs`, `test`, `perf`, `chore`

## Licença

Ao contribuir, você concorda que suas contribuições serão licenciadas sob GPLv3.
