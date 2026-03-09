from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

from google.cloud import bigquery
from google.oauth2 import service_account

from harvester.models import DateColumnInfo, DateColumnClass

logger = logging.getLogger(__name__)

DATE_TYPES = {"DATE", "TIMESTAMP", "DATETIME"}
INT_DATE_COLUMNS = {"ano", "ano_base", "ano_referencia", "ano_ref", "ano_censo"}
MONTH_PERIOD_COLUMNS = {"mes_ano"}
YEAR_PERIOD_COLUMNS = {"ano_tratado"}

DEFAULT_MAX_RETRIES = 3
BACKOFF_BASE = 2.0

MAX_ANO_FUTURO = date.today().year + 1
MAX_DATA_FUTURA = date.today() + timedelta(days=30)


class BigQueryClient:
    def __init__(
        self,
        credential_path: str,
        project: str,
        location: str = "southamerica-east1",
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        cred_path = Path(credential_path)
        credentials = service_account.Credentials.from_service_account_file(str(cred_path))
        self._client = bigquery.Client(
            project=project,
            credentials=credentials,
            location=location,
        )
        self._project = project
        self._max_retries = max_retries
        logger.info("BigQuery client inicializado: projeto=%s, max_retries=%d", project, max_retries)

    def test_connection(self) -> bool:
        try:
            query = "SELECT 1 AS test"
            result = self._execute_query(query)
            return any(True for _ in result)
        except Exception:
            logger.exception("Falha no teste de conexao")
            return False

    def list_tables(self, dataset: str) -> list[str]:
        dataset_ref = f"{self._project}.{dataset}"
        tables = self._client.list_tables(dataset_ref)
        return [t.table_id for t in tables]

    def get_table_columns(self, dataset: str, table: str) -> list[dict[str, str]]:
        query = (
            f"SELECT column_name, data_type "
            f"FROM `{self._project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = @table_name"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", table),
            ]
        )
        result = self._execute_query(query, job_config)
        return [{"column_name": r.column_name, "data_type": r.data_type} for r in result]

    def get_date_columns(self, dataset: str, table: str) -> list[DateColumnInfo]:
        columns = self.get_table_columns(dataset, table)
        date_cols: list[DateColumnInfo] = []
        for col in columns:
            nome = col["column_name"].lower()
            tipo = col["data_type"].upper()
            if tipo in DATE_TYPES:
                if nome in YEAR_PERIOD_COLUMNS:
                    tipo_efetivo = "DATE_ANO_PERIODO"
                elif nome in MONTH_PERIOD_COLUMNS:
                    tipo_efetivo = "DATE_MES_PERIODO"
                else:
                    tipo_efetivo = tipo
                date_cols.append(DateColumnInfo(
                    nome=col["column_name"],
                    tipo_bq=tipo_efetivo,
                ))
            elif tipo == "INT64" and nome in INT_DATE_COLUMNS:
                date_cols.append(DateColumnInfo(
                    nome=col["column_name"],
                    tipo_bq="INT64_ANO",
                ))
        return date_cols

    def get_max_date(self, dataset: str, table: str, column: str, tipo_bq: str = "DATE") -> Optional[str]:
        fqn = f"`{self._project}.{dataset}.{table}`"
        if tipo_bq == "INT64_ANO":
            expr = f"CAST(MAX(`{column}`) AS STRING)"
            where = f"WHERE `{column}` <= {MAX_ANO_FUTURO}"
        elif tipo_bq == "DATE_ANO_PERIODO":
            limite = MAX_DATA_FUTURA.isoformat()
            expr = f"FORMAT_DATE('%d/%m/%Y', DATE_SUB(DATE_ADD(MAX(`{column}`), INTERVAL 1 YEAR), INTERVAL 1 DAY))"
            where = f"WHERE `{column}` <= DATE('{limite}')"
        elif tipo_bq == "DATE_MES_PERIODO":
            limite = MAX_DATA_FUTURA.isoformat()
            expr = f"FORMAT_DATE('%d/%m/%Y', DATE_SUB(DATE_ADD(MAX(`{column}`), INTERVAL 1 MONTH), INTERVAL 1 DAY))"
            where = f"WHERE `{column}` <= DATE('{limite}')"
        else:
            limite = MAX_DATA_FUTURA.isoformat()
            if tipo_bq == "TIMESTAMP":
                expr = f"FORMAT_DATE('%d/%m/%Y', DATE(MAX(`{column}`)))"
                where = f"WHERE `{column}` <= TIMESTAMP('{limite}')"
            else:
                expr = f"FORMAT_DATE('%d/%m/%Y', MAX(`{column}`))"
                where = f"WHERE `{column}` <= DATE('{limite}')"

        query = f"SELECT {expr} AS max_val FROM {fqn} {where}"
        try:
            result = self._execute_query(query)
            for row in result:
                val = row.max_val
                if val is None:
                    return None
                if tipo_bq == "INT64_ANO":
                    return f"31/12/{val}"
                return str(val)
        except Exception:
            logger.exception("Erro ao buscar MAX(%s) de %s.%s", column, dataset, table)
            return None
        return None

    def get_column_stats(
        self, dataset: str, table: str, column: str, tipo_bq: str = "DATE"
    ) -> dict[str, Any]:
        fqn = f"`{self._project}.{dataset}.{table}`"
        if tipo_bq == "INT64_ANO":
            min_expr = f"CAST(MIN(`{column}`) AS STRING)"
            max_expr = f"CAST(MAX(`{column}`) AS STRING)"
        else:
            min_expr = f"CAST(MIN(`{column}`) AS STRING)"
            max_expr = f"CAST(MAX(`{column}`) AS STRING)"

        query = (
            f"SELECT {min_expr} AS min_val, {max_expr} AS max_val, "
            f"COUNTIF(`{column}` IS NULL) AS count_nulls, "
            f"COUNT(*) AS count_total "
            f"FROM {fqn}"
        )
        try:
            result = self._execute_query(query)
            for row in result:
                return {
                    "min": row.min_val,
                    "max": row.max_val,
                    "count_nulls": row.count_nulls,
                    "count_total": row.count_total,
                }
        except Exception:
            logger.exception("Erro ao buscar stats de %s.%s.%s", dataset, table, column)
        return {"min": None, "max": None, "count_nulls": 0, "count_total": 0}

    def _execute_query(
        self,
        query: str,
        job_config: bigquery.QueryJobConfig | None = None,
    ) -> bigquery.table.RowIterator:
        for attempt in range(self._max_retries):
            try:
                job = self._client.query(query, job_config=job_config)
                return job.result()
            except Exception:
                if attempt == self._max_retries - 1:
                    raise
                wait = BACKOFF_BASE ** attempt
                logger.warning("Tentativa %d falhou, aguardando %.1fs", attempt + 1, wait)
                time.sleep(wait)
        raise RuntimeError("Maximo de tentativas excedido")
