from __future__ import annotations

import logging
from pathlib import Path

from textual import on
from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.message import Message
from textual.widgets import Button, Checkbox, Input, Label, Select, Static

from harvester.config import Settings
from harvester.csv_handler import (
    ColumnLayout,
    CsvData,
    ler_arquivo,
    listar_abas_xlsx,
)

logger = logging.getLogger(__name__)


def _listar_arquivos(diretorio: Path) -> list[tuple[str, str]]:
    if not diretorio.is_dir():
        return []
    extensoes = {".csv", ".xlsx"}
    resultado: list[tuple[str, str]] = []
    for f in sorted(diretorio.iterdir()):
        if f.suffix.lower() in extensoes:
            resultado.append((f.name, str(f)))
    return resultado


class InputPane(Vertical):

    class Completed(Message):
        def __init__(
            self,
            caminho: str,
            aba: str | None,
            modo: str,
            sql_dir: str,
        ) -> None:
            self.caminho = caminho
            self.aba = aba
            self.modo = modo
            self.sql_dir = sql_dir
            super().__init__()

    def __init__(self) -> None:
        super().__init__()
        self._csv_data: CsvData | None = None
        self._settings = Settings.from_env()
        self._layout = ColumnLayout(
            col_endereco_tabela=self._settings.col_endereco_tabela,
            col_data_ref_base=self._settings.col_data_ref_base,
            col_data_ref_painel=self._settings.col_data_ref_painel,
            csv_delimiter=self._settings.csv_delimiter,
            csv_encoding=self._settings.csv_encoding,
        )
        self._input_dir = (
            Path(self._settings.input_file).parent
            if self._settings.input_file
            else Path("data_input")
        )

    def compose(self) -> ComposeResult:
        arquivos = _listar_arquivos(self._input_dir)
        with Vertical(classes="form-group"):
            yield Label("Arquivo de entrada:", classes="form-label")
            yield Select(
                options=[(label, val) for label, val in arquivos],
                id="sel-arquivo",
                prompt="Selecionar arquivo...",
            )

        with Vertical(classes="form-group", id="grp-aba"):
            yield Label("Aba (XLSX):", classes="form-label")
            yield Select(options=[], id="sel-aba", prompt="Selecionar aba...")

        yield Label("", id="lbl-input-stats", classes="stats-label")
        yield Static("", id="stc-preview", classes="preview-box")

        with Vertical(classes="form-group"):
            yield Checkbox(
                "Modo Rastreio (rastrear origens via SQLs do dbt)", id="chk-rastreio"
            )
            with Vertical(id="grp-sql-dir"):
                yield Label("Diretorio dos SQLs:", classes="form-label")
                yield Input(
                    id="inp-sql-dir",
                    placeholder="caminho/para/pipelines-main/models/",
                )

        with Horizontal(classes="button-bar-inline"):
            yield Button("Executar", id="btn-input-next", variant="success")

    def on_mount(self) -> None:
        self.query_one("#grp-aba").display = False
        self.query_one("#grp-sql-dir").display = False

        if self._settings.sql_sources_dir:
            self.query_one("#inp-sql-dir", Input).value = self._settings.sql_sources_dir
            self.query_one("#chk-rastreio", Checkbox).value = True
            self.query_one("#grp-sql-dir").display = True

        if self._settings.input_file:
            input_path = Path(self._settings.input_file)
            sel = self.query_one("#sel-arquivo", Select)
            for _, val in _listar_arquivos(self._input_dir):
                if Path(val).resolve() == input_path.resolve():
                    sel.value = val
                    break

    @on(Checkbox.Changed, "#chk-rastreio")
    def _deep_mode_changed(self, event: Checkbox.Changed) -> None:
        self.query_one("#grp-sql-dir").display = event.value

    @on(Select.Changed, "#sel-arquivo")
    def _arquivo_selecionado(self, event: Select.Changed) -> None:
        if event.value == Select.BLANK:
            return

        caminho = Path(str(event.value))
        if caminho.suffix.lower() == ".xlsx":
            abas = listar_abas_xlsx(caminho)
            self.query_one("#sel-aba", Select).set_options([(a, a) for a in abas])
            self.query_one("#grp-aba").display = True

            if self._settings.sheet_name and self._settings.sheet_name in abas:
                self.query_one("#sel-aba", Select).value = self._settings.sheet_name
            else:
                self._carregar_preview(caminho, None)
        else:
            self.query_one("#grp-aba").display = False
            self._carregar_preview(caminho, None)

    @on(Select.Changed, "#sel-aba")
    def _aba_selecionada(self, event: Select.Changed) -> None:
        sel_arq = self.query_one("#sel-arquivo", Select)
        if sel_arq.value == Select.BLANK or event.value == Select.BLANK:
            return
        self._carregar_preview(Path(str(sel_arq.value)), str(event.value))

    def _carregar_preview(self, caminho: Path, aba: str | None) -> None:
        col_tab = self._layout.col_endereco_tabela
        col_base = self._layout.col_data_ref_base
        col_painel = self._layout.col_data_ref_painel

        try:
            self._csv_data = ler_arquivo(caminho, aba, self._layout)
            stats = self._csv_data.stats

            vazias_data = 0
            preenchidas = 0
            for row in self._csv_data.rows:
                if row.skip:
                    continue
                ref_base = (
                    row.dados[col_base].strip()
                    if len(row.dados) > col_base
                    else ""
                )
                ref_painel = (
                    row.dados[col_painel].strip()
                    if len(row.dados) > col_painel
                    else ""
                )
                if ref_base or ref_painel:
                    preenchidas += 1
                elif row.endereco_tabela:
                    vazias_data += 1

            self.query_one("#lbl-input-stats", Label).update(
                f"Tabelas: {stats['tabelas_unicas']} | "
                f"Datas preenchidas: {preenchidas} | "
                f"Datas vazias: {vazias_data} | "
                f"Skip: {stats['skip']}"
            )

            cw = [40, 15, 15, 12]
            h = self._csv_data.header
            h_tab = h[col_tab][:cw[0]] if len(h) > col_tab else "Tabela"
            h_ref = h[col_base][:cw[1]] if len(h) > col_base else "Ref Base"
            h_pan = h[col_painel][:cw[2]] if len(h) > col_painel else "Ref Painel"

            lines: list[str] = [
                f"{h_tab:<{cw[0]}} | {h_ref:<{cw[1]}} | {h_pan:<{cw[2]}} | {'Status':<{cw[3]}}",
                "-" * (sum(cw) + 9),
            ]

            shown = 0
            for row in self._csv_data.rows:
                if row.skip or shown >= 8:
                    break
                tab = (row.endereco_tabela or "")[:cw[0]]
                rb = (
                    row.dados[col_base].strip()[:cw[1]]
                    if len(row.dados) > col_base
                    else ""
                )
                rp = (
                    row.dados[col_painel].strip()[:cw[2]]
                    if len(row.dados) > col_painel
                    else ""
                )
                status = "Preenchido" if (rb or rp) else "Vazio"
                lines.append(f"{tab:<{cw[0]}} | {rb:<{cw[1]}} | {rp:<{cw[2]}} | {status:<{cw[3]}}")
                shown += 1

            self.query_one("#stc-preview", Static).update("\n".join(lines))
        except Exception as exc:
            self.query_one("#lbl-input-stats", Label).update(f"[red]Erro: {exc}[/]")

    @on(Button.Pressed, "#btn-input-next")
    def _proximo(self) -> None:
        sel = self.query_one("#sel-arquivo", Select)
        if sel.value == Select.BLANK:
            self.query_one("#lbl-input-stats", Label).update(
                "[red]Selecione um arquivo[/]"
            )
            return

        caminho = str(sel.value)
        sel_aba = self.query_one("#sel-aba", Select)
        aba = str(sel_aba.value) if sel_aba.value != Select.BLANK else None

        chk_rastreio = self.query_one("#chk-rastreio", Checkbox)
        modo = "rastreio" if chk_rastreio.value else "direto"
        sql_dir = (
            self.query_one("#inp-sql-dir", Input).value.strip()
            if chk_rastreio.value
            else ""
        )

        self.post_message(self.Completed(caminho, aba, modo, sql_dir))
