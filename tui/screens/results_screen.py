from __future__ import annotations

from textual import on
from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.message import Message
from textual.widgets import Button, DataTable, Label

from rich.text import Text

from harvester.models import DateResult, HarvestReport


def _categorizar(r: DateResult) -> str:
    if not r.sucesso:
        return "erro"
    if not r.valor_novo:
        return "sem_data"
    if r.coluna_origem.startswith("[rastreio]"):
        return "rastreio"
    if not r.valor_antigo or not r.valor_antigo.strip():
        return "novo"
    if r.valor_novo != r.valor_antigo:
        return "atualizado"
    return "mantido"


def _nome_curto(tabela: str) -> str:
    partes = tabela.split(".")
    return partes[-1] if partes else tabela


def _confianca_styled(valor: float) -> Text:
    texto = f"{valor:.0%}" if valor > 0 else "-"
    if valor >= 0.85:
        return Text(texto, style="bold green")
    if valor >= 0.70:
        return Text(texto, style="yellow")
    if valor > 0:
        return Text(texto, style="red")
    return Text(texto, style="dim")


def _status_styled(r: DateResult, categoria: str) -> Text:
    mapa = {
        "novo": ("Novo", "bold green"),
        "atualizado": ("Atualizado", "bold yellow"),
        "erro": (f"Erro: {r.erro[:30]}", "bold red"),
        "rastreio": ("Rastreio", "bold cyan"),
        "sem_data": ("Sem data", "dim"),
        "mantido": ("Mantido", ""),
    }
    texto, estilo = mapa.get(categoria, ("", ""))
    return Text(texto, style=estilo)


FILTROS = [
    ("Todos", "todos"),
    ("Novos", "novo"),
    ("Atualizados", "atualizado"),
    ("Erros", "erro"),
    ("Rastreio", "rastreio"),
    ("Sem data", "sem_data"),
]


class ResultsPane(Vertical):

    class RestartRequested(Message):
        pass

    def __init__(self) -> None:
        super().__init__()
        self._report: HarvestReport | None = None

    def compose(self) -> ComposeResult:
        yield Label("", id="lbl-results-resumo", classes="stats-label")
        yield Label("", id="lbl-results-arquivo", classes="success-label")

        with Horizontal(classes="filter-bar"):
            for label, value in FILTROS:
                variant = "primary" if value == "todos" else "default"
                yield Button(
                    label,
                    id=f"btn-filtro-{value}",
                    variant=variant,
                    classes="filter-btn",
                )

        yield DataTable(id="dt-results", zebra_stripes=True)

        with Horizontal(classes="button-bar-inline"):
            yield Button("Nova Execucao", id="btn-restart", variant="primary")
            yield Button("Sair", id="btn-exit", variant="default")

    def carregar(self, report: HarvestReport) -> None:
        self._report = report
        self.query_one("#lbl-results-resumo", Label).update(report.resumo)
        if report.arquivo_saida:
            self.query_one("#lbl-results-arquivo", Label).update(
                f"Arquivo: {report.arquivo_saida}"
            )
        self._popular_tabela("todos")

    def _popular_tabela(self, filtro: str) -> None:
        if not self._report:
            return

        table = self.query_one("#dt-results", DataTable)
        table.clear(columns=True)
        table.add_columns(
            "Tabela", "Tipo", "Coluna", "Antigo", "Novo", "Confianca", "Status"
        )

        for r in self._report.resultados:
            categoria = _categorizar(r)
            if filtro != "todos" and categoria != filtro:
                continue

            rastreio_flag = "[R] " if r.coluna_origem.startswith("[rastreio]") else ""
            coluna = r.coluna_origem.replace("[rastreio] ", "") if r.coluna_origem else ""

            antigo_text = Text(r.valor_antigo, style="dim") if r.valor_antigo else Text("-", style="dim")
            novo_text = Text(r.valor_novo, style="green") if r.valor_novo else Text("-", style="dim")

            table.add_row(
                Text(_nome_curto(r.tabela)),
                r.tipo_data.value if r.tipo_data else "",
                f"{rastreio_flag}{coluna}",
                antigo_text,
                novo_text,
                _confianca_styled(r.confianca),
                _status_styled(r, categoria),
            )

        for _, value in FILTROS:
            btn = self.query_one(f"#btn-filtro-{value}", Button)
            btn.variant = "primary" if value == filtro else "default"

    @on(Button.Pressed, ".filter-btn")
    def _filtrar(self, event: Button.Pressed) -> None:
        btn_id = event.button.id or ""
        filtro = btn_id.replace("btn-filtro-", "")
        self._popular_tabela(filtro)

    @on(Button.Pressed, "#btn-restart")
    def _reiniciar(self) -> None:
        self.post_message(self.RestartRequested())

    @on(Button.Pressed, "#btn-exit")
    def _sair(self) -> None:
        self.app.exit()
