from __future__ import annotations

import re
from harvester.models import DateColumnInfo, DateColumnClass

PATTERNS_REFERENCIA: list[tuple[str, float]] = [
    (r"^dt_ref$", 0.95),
    (r"^data_referencia$", 0.95),
    (r"^data_ref_painel$", 0.93),
    (r"^dt_referencia$", 0.90),
    (r"^data_ref$", 0.90),
    (r"^ano_referencia$", 0.85),
    (r"^ano_ref$", 0.85),
    (r"^ano_base$", 0.80),
    (r"^ano_censo$", 0.80),
    (r"^mes_referencia$", 0.78),
    (r"^mes_ref$", 0.78),
    (r"^mes_ano$", 0.75),
    (r"^ano_tratado$", 0.75),
    (r"^ano$", 0.70),
    (r"^data_base$", 0.70),
    (r"^dt_base$", 0.70),
    (r"referencia", 0.60),
]

PATTERNS_ATUALIZACAO: list[tuple[str, float]] = [
    (r"^data_atualizacao$", 0.95),
    (r"^dt_atualizacao$", 0.95),
    (r"^data_ultima_atualizacao$", 0.95),
    (r"^data_atualizacao_simec$", 0.90),
    (r"^data_atualizacao_cipi$", 0.90),
    (r"^updated_at$", 0.90),
    (r"^data_hora_captura$", 0.88),
    (r"^data_captura$", 0.85),
    (r"^data_carga$", 0.85),
    (r"^dt_carga$", 0.85),
    (r"^data_processamento$", 0.80),
    (r"^dt_processamento$", 0.80),
    (r"^data_insercao$", 0.75),
    (r"^created_at$", 0.70),
    (r"atualizacao", 0.60),
    (r"captura", 0.55),
    (r"carga", 0.50),
]

PATTERNS_DADOS: list[tuple[str, float]] = [
    (r"^data_dado$", 0.90),
    (r"^data_extracao$", 0.85),
    (r"^dt_extracao$", 0.85),
    (r"^data_coleta$", 0.80),
    (r"^data_repasse$", 0.75),
    (r"^data_do_recurso$", 0.70),
    (r"^data_recurso$", 0.70),
    (r"^inicio$", 0.60),
    (r"^inicio_empreendimento$", 0.60),
    (r"^data_assinatura_contrato$", 0.55),
    (r"^data$", 0.50),
    (r"^dt$", 0.50),
]

PATTERNS_IGNORAR: set[str] = {
    "previsao_conclusao",
    "data_conclusao",
    "data_inauguracao",
    "vigencia_inicial",
    "vigencia_final",
    "inicio_contrato",
    "fim_contrato",
    "data_vistoria",
    "data_supervisao",
    "data_situacao_obra",
}


def classificar_coluna(coluna: DateColumnInfo) -> DateColumnInfo:
    nome = coluna.nome.lower().strip()

    if nome in PATTERNS_IGNORAR:
        coluna.classificacao = DateColumnClass.DESCONHECIDO
        coluna.confianca = 0.0
        return coluna

    melhor_classe = DateColumnClass.DESCONHECIDO
    melhor_confianca = 0.0

    for pattern, confianca in PATTERNS_REFERENCIA:
        if re.search(pattern, nome):
            if confianca > melhor_confianca:
                melhor_classe = DateColumnClass.REFERENCIA
                melhor_confianca = confianca
            break

    for pattern, confianca in PATTERNS_ATUALIZACAO:
        if re.search(pattern, nome):
            if confianca > melhor_confianca:
                melhor_classe = DateColumnClass.ATUALIZACAO
                melhor_confianca = confianca
            break

    for pattern, confianca in PATTERNS_DADOS:
        if re.search(pattern, nome):
            if confianca > melhor_confianca:
                melhor_classe = DateColumnClass.DADOS
                melhor_confianca = confianca
            break

    coluna.classificacao = melhor_classe
    coluna.confianca = melhor_confianca
    return coluna


def classificar_colunas(colunas: list[DateColumnInfo]) -> list[DateColumnInfo]:
    for col in colunas:
        classificar_coluna(col)
    return sorted(colunas, key=lambda c: c.confianca, reverse=True)


def melhor_coluna_por_classe(
    colunas: list[DateColumnInfo],
    classe: DateColumnClass,
) -> DateColumnInfo | None:
    candidatas = [c for c in colunas if c.classificacao == classe]
    if not candidatas:
        return None
    return max(candidatas, key=lambda c: c.confianca)
