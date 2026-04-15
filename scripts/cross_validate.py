"""Validacao cruzada: compara tabelas do CSV com tabelas reais no BigQuery.

Gera relatorio em data_output/cross_validation.csv
"""
from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from harvester.bigquery_client import BigQueryClient
from harvester.config import Settings
from harvester.csv_handler import ler_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data_output/cross_validate.log"),
    ],
)
logger = logging.getLogger(__name__)

CSV_PATH = Path(os.environ.get("INPUT_FILE", "data_input/mapa_projeto.csv"))
OUTPUT_PATH = Path("data_output/cross_validation.csv")


def main() -> None:
    settings = Settings.from_env()

    logger.info("Lendo CSV de entrada...")
    csv_data = ler_csv(CSV_PATH)
    stats = csv_data.stats
    logger.info(
        "CSV: %d linhas, %d vazias, %d skip, %d validas, %d tabelas unicas",
        stats["total"], stats["vazias"], stats["skip"], stats["validas"], stats["tabelas_unicas"],
    )

    tabelas_csv = csv_data.tabelas_unicas
    logger.info("Tabelas no CSV: %d", len(tabelas_csv))

    bq = BigQueryClient(
        credential_path=settings.credential_path,
        project=settings.gcp_project,
        location=settings.bq_location,
    )

    logger.info("Listando tabelas do BigQuery...")
    tabelas_bq = set(bq.list_tables(settings.bq_dataset))
    logger.info("Tabelas no BigQuery (%s): %d", settings.bq_dataset, len(tabelas_bq))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "endereco_csv", "tabela", "dataset", "projeto",
            "existe_no_bq", "no_dataset_principal", "status",
        ])

        encontradas = 0
        nao_encontradas = 0
        fora_dataset = 0

        for endereco in tabelas_csv:
            partes = endereco.split(".")
            if len(partes) == 3:
                projeto, dataset, tabela = partes
            elif len(partes) == 2:
                projeto = ""
                dataset, tabela = partes
            else:
                writer.writerow([endereco, endereco, "", "", "N/A", "N/A", "formato_invalido"])
                continue

            no_principal = dataset == settings.bq_dataset
            existe = tabela in tabelas_bq if no_principal else None

            if no_principal and existe:
                status = "ok"
                encontradas += 1
            elif no_principal and not existe:
                status = "nao_encontrada"
                nao_encontradas += 1
            else:
                status = "dataset_externo"
                fora_dataset += 1

            writer.writerow([
                endereco, tabela, dataset, projeto,
                str(existe) if existe is not None else "N/A",
                str(no_principal),
                status,
            ])

        somente_bq = tabelas_bq - {
            e.split(".")[-1] for e in tabelas_csv
            if e.count(".") >= 1
        }
        for tabela in sorted(somente_bq):
            writer.writerow([
                "", tabela, settings.bq_dataset, settings.gcp_project,
                "True", "True", "somente_no_bq",
            ])

    logger.info("Relatorio salvo em %s", OUTPUT_PATH)
    logger.info(
        "Resumo: %d encontradas, %d nao encontradas, %d fora do dataset, %d somente no BQ",
        encontradas, nao_encontradas, fora_dataset, len(somente_bq),
    )


if __name__ == "__main__":
    main()
