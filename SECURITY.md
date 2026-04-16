# Política de Segurança

## Versões Suportadas

| Versão | Suportada |
| ------ | --------- |
| 2.1.x  | sim       |
| < 2.1  | não       |

## Reportando uma Vulnerabilidade

Se você descobrir uma vulnerabilidade de segurança, por favor:

1. **Não** abra uma issue pública
2. Envie um email para andre.dsbf@gmail.com com detalhes da vulnerabilidade
3. Inclua:
   - Descrição da vulnerabilidade
   - Passos para reproduzir
   - Impacto potencial
   - Sugestões de correção (se houver)

## Tempo de Resposta

- Confirmação de recebimento: 48 horas
- Avaliação inicial: 7 dias
- Correção (se confirmada): 30 dias

## Escopo

Esta política cobre:

- Código fonte do projeto
- Dependências diretas
- Scripts de instalação e configuração
- Queries SQL geradas para BigQuery

## Fora do Escopo

- Vulnerabilidades em dependências upstream (reporte diretamente aos mantenedores)
- Google Cloud Platform / BigQuery (reporte ao Google)
- Projetos dbt de terceiros analisados pela ferramenta
- Ataques de engenharia social
- Ataques físicos

## Reconhecimento

Contribuidores que reportarem vulnerabilidades válidas serão reconhecidos no CHANGELOG (se desejarem).
