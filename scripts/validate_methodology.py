"""Validacao da metodologia: compara escolha de coluna e valor MAX do harvester
com queries manuais independentes no BigQuery.

Gera relatorio em data_output/validation_report.csv e stdout com resumo.
"""
from __future__ import annotations

import csv
import logging
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from harvester.bigquery_client import BigQueryClient, DATE_TYPES, INT_DATE_COLUMNS, MONTH_PERIOD_COLUMNS, YEAR_PERIOD_COLUMNS
from harvester.config import Settings
from harvester.csv_handler import ColumnLayout, ler_csv
from harvester.date_detector import classificar_colunas, melhor_coluna_por_classe
from harvester.models import DateColumnClass, TableInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data_output/validate_methodology.log"),
    ],
)
logger = logging.getLogger(__name__)

REPORT_PATH = Path("data_output/validation_report.csv")
SAMPLE_SIZE = 12


def _extrair_tabelas_com_valor(
    csv_path: Path,
    layout: ColumnLayout,
) -> dict[str, str]:
    """Le o CSV de saida e retorna {endereco_tabela: valor_data_ref_base}."""
    csv_data = ler_csv(csv_path, layout)
    resultado: dict[str, str] = {}
    for row in csv_data.rows:
        if row.skip or not row.endereco_tabela:
            continue
        if row.endereco_tabela in resultado:
            continue
        valor = (
            row.dados[layout.col_data_ref_base].strip()
            if len(row.dados) > layout.col_data_ref_base
            else ""
        )
        resultado[row.endereco_tabela] = valor
    return resultado


def _listar_todas_colunas_data(
    bq: BigQueryClient,
    dataset: str,
    tabela: str,
) -> list[dict[str, str]]:
    """Lista TODAS as colunas de data/int-ano via INFORMATION_SCHEMA."""
    colunas = bq.get_table_columns(dataset, tabela)
    resultado: list[dict[str, str]] = []
    for col in colunas:
        nome = col["column_name"].lower()
        tipo = col["data_type"].upper()
        if tipo in DATE_TYPES:
            resultado.append({"column_name": col["column_name"], "data_type": tipo})
        elif tipo == "INT64" and nome in INT_DATE_COLUMNS:
            resultado.append({"column_name": col["column_name"], "data_type": "INT64_ANO"})
    return resultado


def _tipo_efetivo(nome: str, tipo_bq: str) -> str:
    """Replica a logica de tipo efetivo do BigQueryClient.get_date_columns."""
    nome_lower = nome.lower()
    if tipo_bq == "INT64_ANO":
        return "INT64_ANO"
    if nome_lower in YEAR_PERIOD_COLUMNS:
        return "DATE_ANO_PERIODO"
    if nome_lower in MONTH_PERIOD_COLUMNS:
        return "DATE_MES_PERIODO"
    return tipo_bq


def _selecionar_amostra(
    tabelas_com_valor: dict[str, str],
    bq: BigQueryClient,
    settings: Settings,
    tabelas_bq: set[str],
) -> list[str]:
    """Seleciona amostra diversificada de tabelas para validacao."""
    com_valor = {k: v for k, v in tabelas_com_valor.items() if v.strip()}
    if not com_valor:
        logger.warning("Nenhuma tabela com valor para validar")
        return []

    categorias: dict[str, list[str]] = {
        "int_ano": [],
        "date_timestamp": [],
        "outros": [],
    }

    for endereco, valor in com_valor.items():
        info = TableInfo(nome_completo=endereco)
        if info.dataset != settings.bq_dataset:
            continue
        if info.tabela not in tabelas_bq:
            candidata = f"painel_{info.tabela}"
            if candidata not in tabelas_bq:
                continue

        if valor.startswith("31/12/"):
            categorias["int_ano"].append(endereco)
        else:
            categorias["date_timestamp"].append(endereco)

    for cat, enderecos in categorias.items():
        random.shuffle(enderecos)
        logger.info("Categoria '%s': %d tabelas disponiveis", cat, len(enderecos))

    amostra: list[str] = []
    por_categoria = max(SAMPLE_SIZE // 3, 2)

    for cat in ["int_ano", "date_timestamp", "outros"]:
        amostra.extend(categorias[cat][:por_categoria])

    if len(amostra) < SAMPLE_SIZE:
        restantes = [
            e for cat_list in categorias.values()
            for e in cat_list
            if e not in amostra
        ]
        random.shuffle(restantes)
        amostra.extend(restantes[: SAMPLE_SIZE - len(amostra)])

    return amostra[:SAMPLE_SIZE]


def main() -> None:
    settings = Settings.from_env()
    layout = ColumnLayout(
        col_endereco_tabela=settings.col_endereco_tabela,
        col_data_ref_base=settings.col_data_ref_base,
        col_data_ref_painel=settings.col_data_ref_painel,
        csv_delimiter=settings.csv_delimiter,
        csv_encoding=settings.csv_encoding,
    )

    output_dir = Path(settings.output_dir)
    csvs = sorted(output_dir.glob("Mapa_Projeto_*_*.csv"), key=lambda p: p.stat().st_mtime)
    if not csvs:
        logger.error("Nenhum CSV de saida encontrado em %s", output_dir)
        sys.exit(1)

    csv_saida = csvs[-1]
    logger.info("CSV de saida mais recente: %s", csv_saida)

    tabelas_com_valor = _extrair_tabelas_com_valor(csv_saida, layout)
    logger.info("Tabelas com valor no CSV: %d", sum(1 for v in tabelas_com_valor.values() if v.strip()))

    bq = BigQueryClient(
        credential_path=settings.credential_path,
        project=settings.gcp_project,
        location=settings.bq_location,
        max_retries=settings.max_retries,
    )

    tabelas_bq = set(bq.list_tables(settings.bq_dataset))
    amostra = _selecionar_amostra(tabelas_com_valor, bq, settings, tabelas_bq)
    logger.info("Amostra selecionada: %d tabelas", len(amostra))

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    relatorio_rows: list[dict[str, str]] = []
    ok_count = 0
    divergente_count = 0

    for idx, endereco in enumerate(amostra, 1):
        info = TableInfo(nome_completo=endereco)
        tabela_nome = info.tabela
        dataset_alvo = info.dataset or settings.bq_dataset

        if tabela_nome not in tabelas_bq and dataset_alvo == settings.bq_dataset:
            candidata = f"painel_{tabela_nome}"
            if candidata in tabelas_bq:
                tabela_nome = candidata

        logger.info("[%d/%d] Validando %s.%s", idx, len(amostra), dataset_alvo, tabela_nome)

        todas_colunas = _listar_todas_colunas_data(bq, dataset_alvo, tabela_nome)
        logger.info("  Colunas de data encontradas: %s", [c["column_name"] for c in todas_colunas])

        colunas_bq = bq.get_date_columns(dataset_alvo, tabela_nome)
        colunas_classificadas = classificar_colunas(colunas_bq)
        col_ref = melhor_coluna_por_classe(colunas_classificadas, DateColumnClass.REFERENCIA)
        col_escolhida = col_ref or (colunas_classificadas[0] if colunas_classificadas else None)

        nome_escolhida = col_escolhida.nome if col_escolhida else ""
        conf_escolhida = col_escolhida.confianca if col_escolhida else 0.0
        classe_escolhida = col_escolhida.classificacao.value if col_escolhida else ""
        tipo_escolhido = col_escolhida.tipo_bq if col_escolhida else ""

        valor_csv = tabelas_com_valor.get(endereco, "").strip()

        for col in todas_colunas:
            col_name = col["column_name"]
            tipo_raw = col["data_type"]
            tipo_eff = _tipo_efetivo(col_name, tipo_raw)

            max_manual = bq.get_max_date(dataset_alvo, tabela_nome, col_name, tipo_eff)

            eh_escolhida = col_name.lower() == nome_escolhida.lower()
            status = ""
            if eh_escolhida:
                if max_manual and valor_csv and max_manual.strip() == valor_csv.strip():
                    status = "OK"
                    ok_count += 1
                elif max_manual and valor_csv:
                    status = "DIVERGENTE"
                    divergente_count += 1
                elif not valor_csv:
                    status = "CSV_VAZIO"
                else:
                    status = "MAX_NULL"

            row_data = {
                "tabela": f"{dataset_alvo}.{tabela_nome}",
                "endereco_csv": endereco,
                "coluna": col_name,
                "tipo_bq": tipo_raw,
                "tipo_efetivo": tipo_eff,
                "max_manual": max_manual or "",
                "eh_coluna_escolhida": "SIM" if eh_escolhida else "",
                "classe_escolhida": classe_escolhida if eh_escolhida else "",
                "confianca": f"{conf_escolhida:.2f}" if eh_escolhida else "",
                "valor_csv": valor_csv if eh_escolhida else "",
                "status": status,
            }
            relatorio_rows.append(row_data)

            log_mark = " <<<" if eh_escolhida else ""
            logger.info(
                "  %s (%s) -> MAX = %s%s",
                col_name, tipo_eff, max_manual or "NULL", log_mark,
            )

        if not todas_colunas:
            relatorio_rows.append({
                "tabela": f"{dataset_alvo}.{tabela_nome}",
                "endereco_csv": endereco,
                "coluna": "(nenhuma)",
                "tipo_bq": "",
                "tipo_efetivo": "",
                "max_manual": "",
                "eh_coluna_escolhida": "",
                "classe_escolhida": "",
                "confianca": "",
                "valor_csv": valor_csv,
                "status": "SEM_COLUNAS_DATA",
            })

    fieldnames = [
        "tabela", "endereco_csv", "coluna", "tipo_bq", "tipo_efetivo",
        "max_manual", "eh_coluna_escolhida", "classe_escolhida",
        "confianca", "valor_csv", "status",
    ]
    with open(REPORT_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(relatorio_rows)

    logger.info("Relatorio salvo em %s", REPORT_PATH)
    logger.info("=" * 60)
    logger.info("RESUMO DA VALIDACAO")
    logger.info("=" * 60)
    logger.info("Tabelas validadas: %d", len(amostra))
    logger.info("Colunas analisadas: %d", len(relatorio_rows))
    logger.info("Resultados OK: %d", ok_count)
    logger.info("Divergentes: %d", divergente_count)

    if divergente_count > 0:
        logger.warning("ATENCAO: %d divergencia(s) encontrada(s)!", divergente_count)
        for row in relatorio_rows:
            if row["status"] == "DIVERGENTE":
                logger.warning(
                    "  %s | coluna=%s | csv=%s | manual=%s",
                    row["tabela"], row["coluna"], row["valor_csv"], row["max_manual"],
                )
    else:
        logger.info("Nenhuma divergencia encontrada. Metodologia validada.")


if __name__ == "__main__":
    main()

# "A confianca e conquistada em gotas e perdida em litros." -- Jean-Paul Sartre
