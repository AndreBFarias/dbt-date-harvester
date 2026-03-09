from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Optional

from harvester.bigquery_client import BigQueryClient
from harvester.config import Settings
from harvester.sql_date_tracer import SqlDateTracer
from harvester.csv_handler import (
    ColumnLayout,
    CsvData,
    CsvRow,
    atualizar_data_linha,
    ler_arquivo,
    salvar_csv,
)
from harvester.date_detector import classificar_colunas, melhor_coluna_por_classe
from harvester.models import (
    DateColumnClass,
    DateResult,
    HarvestReport,
    TableInfo,
)

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, str], None]


class DateHarvester:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._layout = ColumnLayout(
            col_endereco_tabela=settings.col_endereco_tabela,
            col_data_ref_base=settings.col_data_ref_base,
            col_data_ref_painel=settings.col_data_ref_painel,
            csv_delimiter=settings.csv_delimiter,
            csv_encoding=settings.csv_encoding,
        )
        self._bq: Optional[BigQueryClient] = None
        self._tracer: Optional[SqlDateTracer] = None
        self._on_progress: Optional[ProgressCallback] = None
        self._cancelado = False

    def set_progress_callback(self, callback: ProgressCallback) -> None:
        self._on_progress = callback

    def cancelar(self) -> None:
        self._cancelado = True

    def _emit_progress(self, atual: int, total: int, msg: str) -> None:
        if self._on_progress:
            self._on_progress(atual, total, msg)

    def _get_tracer(self) -> Optional[SqlDateTracer]:
        if self._tracer is None and self._settings.sql_sources_dir:
            sql_dir = Path(self._settings.sql_sources_dir)
            if sql_dir.is_dir():
                self._tracer = SqlDateTracer(sql_dir)
                n = self._tracer.load_all()
                logger.info("SQL tracer carregado: %d arquivos", n)
        return self._tracer

    def _get_bq(self) -> BigQueryClient:
        if self._bq is None:
            self._bq = BigQueryClient(
                credential_path=self._settings.credential_path,
                project=self._settings.gcp_project,
                location=self._settings.bq_location,
                max_retries=self._settings.max_retries,
            )
        return self._bq

    def executar(
        self,
        caminho_input: Path,
        caminho_output: Path,
        aba: str | None = None,
    ) -> HarvestReport:
        self._cancelado = False
        report = HarvestReport(arquivo_entrada=str(caminho_input))

        csv_data = ler_arquivo(caminho_input, aba, self._layout)
        tabelas_unicas = csv_data.tabelas_unicas
        report.total_tabelas = len(tabelas_unicas)

        bq = self._get_bq()
        tabelas_existentes = set(bq.list_tables(self._settings.bq_dataset))

        resultados_por_tabela: dict[str, dict[str, str]] = {}

        for i, endereco in enumerate(tabelas_unicas):
            if self._cancelado:
                logger.info("Execucao cancelada pelo usuario")
                break

            info = TableInfo(nome_completo=endereco)
            self._emit_progress(i + 1, len(tabelas_unicas), f"Processando {info.tabela}...")

            if info.dataset != self._settings.bq_dataset:
                logger.warning("Tabela fora do dataset principal: %s", endereco)

            tabela_nome = info.tabela
            if not tabela_nome:
                report.tabelas_inexistentes += 1
                report.resultados.append(DateResult(
                    tabela=endereco,
                    tipo_data=DateColumnClass.DESCONHECIDO,
                    valor_antigo="",
                    valor_novo="",
                    coluna_origem="",
                    sucesso=False,
                    erro="Endereco vazio ou incompleto",
                ))
                continue

            usou_fuzzy = False
            if tabela_nome not in tabelas_existentes and info.dataset == self._settings.bq_dataset:
                candidata = f"painel_{tabela_nome}"
                if candidata in tabelas_existentes:
                    logger.info("Fuzzy match: %s -> %s", tabela_nome, candidata)
                    tabela_nome = candidata
                    info.tabela = candidata
                    usou_fuzzy = True
                else:
                    report.tabelas_inexistentes += 1
                    report.resultados.append(DateResult(
                        tabela=endereco,
                        tipo_data=DateColumnClass.DESCONHECIDO,
                        valor_antigo="",
                        valor_novo="",
                        coluna_origem="",
                        sucesso=False,
                        erro="Tabela nao encontrada no BigQuery",
                    ))
                    continue

            try:
                dataset_alvo = info.dataset or self._settings.bq_dataset
                colunas = bq.get_date_columns(dataset_alvo, info.tabela)
                if not colunas:
                    tracer = self._get_tracer()
                    if tracer:
                        val, col_info, conf = tracer.trace_date_for_table(
                            info.tabela, bq, self._settings.bq_dataset
                        )
                        if val:
                            dados_tabela: dict[str, str] = {"ref_base": val}
                            resultados_por_tabela[endereco] = dados_tabela
                            report.tabelas_atualizadas += 1
                            report.resultados.append(DateResult(
                                tabela=endereco,
                                tipo_data=DateColumnClass.REFERENCIA,
                                valor_antigo="",
                                valor_novo=val,
                                coluna_origem=f"[rastreio] {col_info}",
                                confianca=conf,
                                fuzzy_match=usou_fuzzy,
                            ))
                            continue

                    report.tabelas_sem_data += 1
                    report.resultados.append(DateResult(
                        tabela=endereco,
                        tipo_data=DateColumnClass.DESCONHECIDO,
                        valor_antigo="",
                        valor_novo="",
                        coluna_origem="",
                        sucesso=True,
                        erro="Nenhuma coluna de data encontrada",
                    ))
                    continue

                colunas = classificar_colunas(colunas)
                dados_tabela: dict[str, str] = {}

                col_ref = melhor_coluna_por_classe(colunas, DateColumnClass.REFERENCIA)
                col_escolhida = col_ref or colunas[0] if colunas else None

                if col_escolhida:
                    max_val = bq.get_max_date(dataset_alvo, info.tabela, col_escolhida.nome, col_escolhida.tipo_bq)
                    if max_val:
                        dados_tabela["ref_base"] = max_val
                        report.resultados.append(DateResult(
                            tabela=endereco,
                            tipo_data=col_escolhida.classificacao,
                            valor_antigo="",
                            valor_novo=max_val,
                            coluna_origem=col_escolhida.nome,
                            confianca=col_escolhida.confianca,
                            fuzzy_match=usou_fuzzy,
                        ))

                resultados_por_tabela[endereco] = dados_tabela
                report.tabelas_atualizadas += 1

            except Exception as exc:
                report.tabelas_erro += 1
                report.resultados.append(DateResult(
                    tabela=endereco,
                    tipo_data=DateColumnClass.DESCONHECIDO,
                    valor_antigo="",
                    valor_novo="",
                    coluna_origem="",
                    sucesso=False,
                    erro=str(exc),
                ))
                logger.exception("Erro processando tabela %s", endereco)

        self._propagar_valores_antigos(csv_data, report)
        self._aplicar_resultados(csv_data, resultados_por_tabela)

        arquivo_saida = salvar_csv(csv_data, caminho_output, self._layout)
        report.arquivo_saida = str(arquivo_saida)

        self._emit_progress(len(tabelas_unicas), len(tabelas_unicas), "Concluido")
        logger.info("Harvest concluido: %s", report.resumo)
        return report

    def _propagar_valores_antigos(
        self,
        csv_data: CsvData,
        report: HarvestReport,
    ) -> None:
        col_ref = self._layout.col_data_ref_base
        old_values: dict[str, str] = {}
        for row in csv_data.rows:
            if row.skip or row.endereco_tabela in old_values:
                continue
            old_ref = (
                row.dados[col_ref].strip()
                if len(row.dados) > col_ref
                else ""
            )
            old_values[row.endereco_tabela] = old_ref
        for resultado in report.resultados:
            if resultado.tabela in old_values and not resultado.valor_antigo:
                resultado.valor_antigo = old_values[resultado.tabela]

    def _aplicar_resultados(
        self,
        csv_data: CsvData,
        resultados: dict[str, dict[str, str]],
    ) -> None:
        col_base = self._layout.col_data_ref_base
        col_painel = self._layout.col_data_ref_painel
        for row in csv_data.rows:
            if row.skip:
                continue
            dados = resultados.get(row.endereco_tabela, {})
            if "ref_base" in dados:
                atualizar_data_linha(row, col_base, dados["ref_base"])
                atualizar_data_linha(row, col_painel, dados["ref_base"])
