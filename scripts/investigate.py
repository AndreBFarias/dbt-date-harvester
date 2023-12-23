"""Investigacao manual: conectar no BigQuery, listar tabelas, descobrir colunas de data.

Gera relatorio em data_output/investigation_report.csv
"""
from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from harvester.bigquery_client import BigQueryClient
from harvester.config import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data_output/investigate.log"),
    ],
)
logger = logging.getLogger(__name__)

DATE_TYPES = {"DATE", "TIMESTAMP", "DATETIME"}
INT_DATE_NAMES = {"ano", "ano_base", "ano_referencia", "ano_ref", "ano_censo"}

OUTPUT_PATH = Path("data_output/investigation_report.csv")


def main() -> None:
    settings = Settings.from_env()
    erros = settings.validate()
    if erros:
        logger.error("Configuracao invalida: %s", erros)
        sys.exit(1)

    bq = BigQueryClient(
        credential_path=settings.credential_path,
        project=settings.gcp_project,
        location=settings.bq_location,
    )

    logger.info("Testando conexao...")
    if not bq.test_connection():
        logger.error("Falha na conexao com BigQuery")
        sys.exit(1)
    logger.info("Conexao OK")

    logger.info("Listando tabelas do dataset '%s'...", settings.bq_dataset)
    tabelas = bq.list_tables(settings.bq_dataset)
    logger.info("Encontradas %d tabelas", len(tabelas))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "tabela", "coluna", "tipo", "min", "max",
            "count_nulls", "count_total", "classificacao",
        ])

        tabelas_sem_data = []
        for i, tabela in enumerate(tabelas):
            logger.info("[%d/%d] Processando %s...", i + 1, len(tabelas), tabela)

            try:
                colunas = bq.get_table_columns(settings.bq_dataset, tabela)
            except Exception:
                logger.exception("Erro ao buscar colunas de %s", tabela)
                writer.writerow([tabela, "ERRO", "", "", "", "", "", ""])
                continue

            cols_data = []
            for col in colunas:
                nome = col["column_name"]
                tipo = col["data_type"].upper()
                if tipo in DATE_TYPES or (tipo == "INT64" and nome.lower() in INT_DATE_NAMES):
                    tipo_label = "INT64_ANO" if tipo == "INT64" else tipo
                    cols_data.append((nome, tipo_label))

            if not cols_data:
                tabelas_sem_data.append(tabela)
                writer.writerow([tabela, "(nenhuma)", "", "", "", "", "", "sem_data"])
                continue

            for nome_col, tipo_col in cols_data:
                stats = bq.get_column_stats(settings.bq_dataset, tabela, nome_col, tipo_col)
                classificacao = _classificar_nome(nome_col)
                writer.writerow([
                    tabela, nome_col, tipo_col,
                    stats.get("min", ""),
                    stats.get("max", ""),
                    stats.get("count_nulls", 0),
                    stats.get("count_total", 0),
                    classificacao,
                ])

    logger.info("Relatorio salvo em %s", OUTPUT_PATH)
    logger.info("Tabelas sem coluna de data (%d): %s", len(tabelas_sem_data), tabelas_sem_data)


def _classificar_nome(nome: str) -> str:
    nome_lower = nome.lower()
    if any(p in nome_lower for p in ("referencia", "ref", "base", "censo")):
        return "referencia"
    if any(p in nome_lower for p in ("atualizacao", "carga", "updated", "processamento")):
        return "atualizacao"
    if any(p in nome_lower for p in ("extracao", "coleta", "dado")):
        return "dados"
    if nome_lower in ("ano", "data", "dt"):
        return "generico"
    return "desconhecido"


if __name__ == "__main__":
    main()
