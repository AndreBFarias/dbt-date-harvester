"""Modo rastreio: rastreia colunas de data via SQLs do dbt.

Para tabelas sem coluna de data direta, analisa o SQL que as gera
e busca MAX(ano) ou MAX(dt_ref) nas tabelas-fonte referenciadas.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_ANO_PATTERN = re.compile(
    r"WHERE\s+\w+\s*=\s*\(\s*SELECT\s+MAX\s*\(\s*\w+\s*\)\s+FROM",
    re.IGNORECASE | re.DOTALL,
)

ANO_BETWEEN_PATTERN = re.compile(
    r"WHERE\s+ano\s+BETWEEN\s+\d+\s+AND\s+\(\s*SELECT\s+MAX\s*\(\s*ano\s*\)",
    re.IGNORECASE | re.DOTALL,
)

ANO_FIXO_PATTERN = re.compile(
    r"WHERE\s+ano\s*=\s*(\d{4})\b",
    re.IGNORECASE,
)

DT_REF_PATTERN = re.compile(
    r"CAST\s*\(\s*SUBSTR\s*\(\s*dt_ref\s*,\s*1\s*,\s*4\s*\)\s+AS\s+(?:INT|INT64|int)\s*\)",
    re.IGNORECASE,
)

DT_REF_COLUMN_PATTERN = re.compile(
    r"\bdt_ref\b",
    re.IGNORECASE,
)

REF_PATTERN = re.compile(r"{{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*}}")
SOURCE_PATTERN = re.compile(
    r"{{\s*source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*}}"
)


class TemporalOrigin:
    UNKNOWN = "desconhecido"
    MAX_ANO_FILTER = "max_ano_filter"
    DT_REF_PARSED = "dt_ref_parsed"
    DT_REF_COLUMN = "dt_ref_column"
    ANO_FIXO = "ano_fixo"
    NO_TEMPORAL = "sem_temporal"


@dataclass
class SqlTraceResult:
    tabela: str
    sql_path: str = ""
    refs: list[str] = field(default_factory=list)
    sources: list[tuple[str, str]] = field(default_factory=list)
    temporal_origin: str = TemporalOrigin.UNKNOWN
    source_table_for_date: str = ""
    date_column_in_source: str = ""
    confianca: float = 0.0


class SqlDateTracer:
    def __init__(self, sql_dir: Path) -> None:
        self._sql_dir = sql_dir
        self._sql_cache: dict[str, str] = {}
        self._trace_cache: dict[str, SqlTraceResult] = {}

    def load_all(self) -> int:
        count = 0
        for sql_file in sorted(self._sql_dir.rglob("*.sql")):
            nome = sql_file.stem
            try:
                content = sql_file.read_text(encoding="utf-8")
                self._sql_cache[nome] = content
                count += 1
            except Exception:
                logger.warning("Erro lendo %s", sql_file)
        logger.info("Carregados %d SQLs de %s", count, self._sql_dir)
        return count

    def trace(self, tabela: str) -> SqlTraceResult:
        if tabela in self._trace_cache:
            return self._trace_cache[tabela]

        result = SqlTraceResult(tabela=tabela)
        sql = self._sql_cache.get(tabela)
        if not sql:
            result.temporal_origin = TemporalOrigin.UNKNOWN
            self._trace_cache[tabela] = result
            return result

        result.sql_path = tabela
        result.refs = REF_PATTERN.findall(sql)
        result.sources = SOURCE_PATTERN.findall(sql)

        if MAX_ANO_PATTERN.search(sql) or ANO_BETWEEN_PATTERN.search(sql):
            result.temporal_origin = TemporalOrigin.MAX_ANO_FILTER
            result.date_column_in_source = "ano"
            result.confianca = 0.90
            source_match = self._find_ano_source(sql)
            if source_match:
                result.source_table_for_date = source_match

        elif DT_REF_PATTERN.search(sql):
            result.temporal_origin = TemporalOrigin.DT_REF_PARSED
            result.date_column_in_source = "dt_ref"
            result.confianca = 0.85
            gaia_ref = self._find_gaia_ref(result.refs)
            if gaia_ref:
                result.source_table_for_date = gaia_ref

        elif DT_REF_COLUMN_PATTERN.search(sql):
            result.temporal_origin = TemporalOrigin.DT_REF_COLUMN
            result.date_column_in_source = "dt_ref"
            result.confianca = 0.80
            gaia_ref = self._find_gaia_ref(result.refs)
            if gaia_ref:
                result.source_table_for_date = gaia_ref

        elif ANO_FIXO_PATTERN.search(sql):
            match = ANO_FIXO_PATTERN.search(sql)
            result.temporal_origin = TemporalOrigin.ANO_FIXO
            result.date_column_in_source = f"ano={match.group(1)}"
            result.confianca = 0.95
            result.source_table_for_date = f"fixo:{match.group(1)}"

        elif not result.refs and not result.sources:
            result.temporal_origin = TemporalOrigin.NO_TEMPORAL
            result.confianca = 0.0

        self._trace_cache[tabela] = result
        return result

    def trace_date_for_table(
        self,
        tabela: str,
        bq_client: "BigQueryClient",
        dataset: str,
    ) -> tuple[str | None, str, float]:
        """Rastreia data via SQL e consulta no BQ. Retorna (valor, coluna_origem, confianca)."""
        trace = self.trace(tabela)

        if trace.temporal_origin == TemporalOrigin.MAX_ANO_FILTER:
            source_table = trace.source_table_for_date
            if source_table:
                val = self._query_source_max_ano(bq_client, dataset, source_table, tabela)
                if val:
                    return val, f"ano (via {source_table})", trace.confianca

        elif trace.temporal_origin in (TemporalOrigin.DT_REF_PARSED, TemporalOrigin.DT_REF_COLUMN):
            source_ref = trace.source_table_for_date
            if source_ref:
                val = self._query_ref_max_dt_ref(bq_client, dataset, source_ref)
                if val:
                    return val, f"dt_ref (via {source_ref})", trace.confianca

        elif trace.temporal_origin == TemporalOrigin.ANO_FIXO:
            if trace.source_table_for_date.startswith("fixo:"):
                ano = trace.source_table_for_date.split(":")[1]
                return f"31/12/{ano}", f"ano fixo={ano}", trace.confianca

        if trace.temporal_origin == TemporalOrigin.UNKNOWN and trace.refs:
            for ref in trace.refs:
                if ref.startswith("painel_") or ref.startswith("enec_") or ref.startswith("matricula_"):
                    sub_trace = self.trace(ref)
                    if sub_trace.temporal_origin != TemporalOrigin.UNKNOWN:
                        val, col, conf = self.trace_date_for_table(ref, bq_client, dataset)
                        if val:
                            return val, f"{col} (via ref {ref})", conf * 0.9

        return None, "", 0.0

    def _find_gaia_ref(self, refs: list[str]) -> str:
        for ref in refs:
            if ref.startswith("gaia_"):
                return ref
        skip = {"filtro_territorio", "municipio"}
        for ref in refs:
            if ref not in skip:
                return ref
        return refs[0] if refs else ""

    def _find_ano_source(self, sql: str) -> str:
        match = re.search(
            r"SELECT\s+MAX\s*\(\s*ano\s*\)\s+FROM\s+{{\s*source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*}}",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if match:
            return f"{match.group(1)}.{match.group(2)}"

        match_ref = re.search(
            r"SELECT\s+MAX\s*\(\s*ano\s*\)\s+FROM\s+{{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*}}",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if match_ref:
            return match_ref.group(1)

        sources = SOURCE_PATTERN.findall(sql)
        if sources:
            return f"{sources[0][0]}.{sources[0][1]}"

        refs = REF_PATTERN.findall(sql)
        skip = {"filtro_territorio", "municipio"}
        for ref in refs:
            if ref not in skip:
                return ref
        return ""

    def _resolve_ref_dataset(self, ref_name: str, default_dataset: str) -> str:
        REF_DATASET_MAP: dict[str, str] = {
            "gaia_fies": "projeto_gaia",
            "gaia_pnae": "projeto_gaia",
            "gaia_pnate": "projeto_gaia",
            "gaia_prouni": "projeto_gaia",
            "gaia_pnld": "projeto_gaia",
            "gaia_capes": "projeto_gaia",
            "gaia_pbp_prouni": "projeto_gaia",
            "gaia_pdde_basico": "projeto_gaia",
            "gaia_pdde_ai": "projeto_gaia",
            "gaia_pdde_especial": "projeto_gaia",
            "ibge_estimativa_populacao": "educacao_ibge_dados_abertos",
            "sisu_vaga_ofertada": "educacao_sisu_dados_abertos",
            "cnca_meta": "educacao_politica_cnca",
            "cnca_selo_nacional": "educacao_politica_cnca",
            "enec_conectividade": "educacao_enec_dados_abertos",
            "enec_evolucao_conectividade": "educacao_enec_dados_abertos",
            "matricula_unica_pdm": "educacao_politica_pdm",
            "simec_adesao_pnd_resposta": "educacao_politica_sesu",
        }
        return REF_DATASET_MAP.get(ref_name, default_dataset)

    def _query_source_max_ano(
        self, bq_client: "BigQueryClient", dataset: str, source_ref: str, tabela: str
    ) -> str | None:
        from harvester.bigquery_client import MAX_ANO_FUTURO

        if "." in source_ref:
            src_dataset, src_table = source_ref.split(".", 1)
        else:
            src_dataset = self._resolve_ref_dataset(source_ref, dataset)
            src_table = source_ref

        project = bq_client._project
        fqn = f"`{project}.{src_dataset}.{src_table}`"
        query = f"SELECT CAST(MAX(ano) AS STRING) AS max_val FROM {fqn} WHERE ano <= {MAX_ANO_FUTURO}"

        try:
            result = bq_client._execute_query(query)
            for row in result:
                if row.max_val:
                    return f"31/12/{row.max_val}"
        except Exception:
            logger.warning("Erro consultando source %s.%s para tabela %s", src_dataset, src_table, tabela)
        return None

    def _query_ref_max_dt_ref(
        self, bq_client: "BigQueryClient", dataset: str, ref_name: str
    ) -> str | None:
        from harvester.bigquery_client import MAX_ANO_FUTURO
        import calendar

        src_dataset = self._resolve_ref_dataset(ref_name, dataset)
        project = bq_client._project
        fqn = f"`{project}.{src_dataset}.{ref_name}`"

        query_len = f"SELECT MAX(LENGTH(dt_ref)) AS max_len FROM {fqn} WHERE dt_ref IS NOT NULL LIMIT 1"
        try:
            result = bq_client._execute_query(query_len)
            max_len = 0
            for row in result:
                max_len = row.max_len or 0
        except Exception:
            logger.warning("Erro verificando dt_ref de %s.%s", src_dataset, ref_name)
            return None

        if max_len <= 4:
            query = (
                f"SELECT CAST(MAX(CAST(dt_ref AS INT64)) AS STRING) AS max_ano "
                f"FROM {fqn} "
                f"WHERE CAST(dt_ref AS INT64) <= {MAX_ANO_FUTURO}"
            )
            try:
                result = bq_client._execute_query(query)
                for row in result:
                    if row.max_ano:
                        return f"31/12/{row.max_ano}"
            except Exception:
                logger.warning("Erro consultando dt_ref (YYYY) de %s.%s", src_dataset, ref_name)
        else:
            query = (
                f"SELECT CAST(MAX(CAST(SUBSTR(dt_ref, 1, 4) AS INT64)) AS STRING) AS max_ano, "
                f"CAST(MAX(CAST(SUBSTR(dt_ref, 5, 2) AS INT64)) AS STRING) AS max_mes "
                f"FROM {fqn} "
                f"WHERE CAST(SUBSTR(dt_ref, 1, 4) AS INT64) = "
                f"(SELECT MAX(CAST(SUBSTR(dt_ref, 1, 4) AS INT64)) FROM {fqn} "
                f"WHERE CAST(SUBSTR(dt_ref, 1, 4) AS INT64) <= {MAX_ANO_FUTURO})"
            )
            try:
                result = bq_client._execute_query(query)
                for row in result:
                    if row.max_ano and row.max_mes:
                        ano = int(row.max_ano)
                        mes = int(row.max_mes)
                        ultimo_dia = calendar.monthrange(ano, mes)[1]
                        return f"{ultimo_dia:02d}/{mes:02d}/{ano}"
            except Exception:
                logger.warning("Erro consultando dt_ref (YYYYMM) de %s.%s", src_dataset, ref_name)
        return None
