from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values, set_key

logger = logging.getLogger(__name__)

_ENV_PATH = Path(".env")


@dataclass
class Settings:
    credential_path: str = ""
    gcp_project: str = ""
    bq_dataset: str = ""
    bq_location: str = "southamerica-east1"
    input_file: str = ""
    sheet_name: str = ""
    output_dir: str = "data_output"
    col_endereco_tabela: int = 4
    col_data_ref_base: int = 8
    col_data_ref_painel: int = 11
    sql_sources_dir: str = ""
    csv_delimiter: str = ";"
    csv_encoding: str = "utf-8-sig"
    max_retries: int = 3
    log_dir: str = "data_output"
    dbt_project_dir: str = ""

    def validate(self) -> list[str]:
        erros: list[str] = []
        if not self.credential_path:
            erros.append("CREDENTIAL_PATH nao definido")
        else:
            cred = Path(self.credential_path)
            if not cred.exists():
                erros.append(f"Credencial nao encontrada: {cred}")
        if not self.gcp_project:
            erros.append("GCP_PROJECT nao definido")
        if not self.bq_dataset:
            erros.append("BQ_DATASET nao definido")
        if self.sql_sources_dir:
            sql_dir = Path(self.sql_sources_dir)
            if not sql_dir.is_dir():
                erros.append(f"Diretorio SQL nao encontrado: {sql_dir}")
        return erros

    def save(self, env_path: Path | None = None) -> None:
        target = env_path or _ENV_PATH
        if not target.exists():
            target.touch()
        set_key(str(target), "CREDENTIAL_PATH", self.credential_path)
        set_key(str(target), "GCP_PROJECT", self.gcp_project)
        set_key(str(target), "BQ_DATASET", self.bq_dataset)
        set_key(str(target), "BQ_LOCATION", self.bq_location)
        set_key(str(target), "INPUT_FILE", self.input_file)
        set_key(str(target), "SHEET_NAME", self.sheet_name)
        set_key(str(target), "OUTPUT_DIR", self.output_dir)
        set_key(str(target), "COL_ENDERECO_TABELA", str(self.col_endereco_tabela))
        set_key(str(target), "COL_DATA_REF_BASE", str(self.col_data_ref_base))
        set_key(str(target), "COL_DATA_REF_PAINEL", str(self.col_data_ref_painel))
        set_key(str(target), "SQL_SOURCES_DIR", self.sql_sources_dir)
        set_key(str(target), "CSV_DELIMITER", self.csv_delimiter)
        set_key(str(target), "CSV_ENCODING", self.csv_encoding)
        set_key(str(target), "MAX_RETRIES", str(self.max_retries))
        set_key(str(target), "LOG_DIR", self.log_dir)
        set_key(str(target), "DBT_PROJECT_DIR", self.dbt_project_dir)
        logger.info("Configuracoes salvas em %s", target)

    @classmethod
    def from_env(cls, env_path: Path | None = None) -> Settings:
        target = env_path or _ENV_PATH
        if not target.exists():
            logger.warning("Arquivo .env nao encontrado em %s, usando defaults", target)
            return cls()
        values = dotenv_values(target)
        return cls(
            credential_path=values.get("CREDENTIAL_PATH", "") or "",
            gcp_project=values.get("GCP_PROJECT", "") or "",
            bq_dataset=values.get("BQ_DATASET", "") or "",
            bq_location=values.get("BQ_LOCATION", "") or "southamerica-east1",
            input_file=values.get("INPUT_FILE", "") or "",
            sheet_name=values.get("SHEET_NAME", "") or "",
            output_dir=values.get("OUTPUT_DIR", "") or "data_output",
            col_endereco_tabela=int(values.get("COL_ENDERECO_TABELA", "") or 4),
            col_data_ref_base=int(values.get("COL_DATA_REF_BASE", "") or 8),
            col_data_ref_painel=int(values.get("COL_DATA_REF_PAINEL", "") or 11),
            sql_sources_dir=values.get("SQL_SOURCES_DIR", "") or "",
            csv_delimiter=values.get("CSV_DELIMITER", "") or ";",
            csv_encoding=values.get("CSV_ENCODING", "") or "utf-8-sig",
            max_retries=int(values.get("MAX_RETRIES", "") or 3),
            log_dir=values.get("LOG_DIR", "") or "data_output",
            dbt_project_dir=values.get("DBT_PROJECT_DIR", "") or "",
        )
