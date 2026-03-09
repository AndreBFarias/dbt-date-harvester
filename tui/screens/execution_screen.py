from __future__ import annotations

import logging
import time
from pathlib import Path

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.message import Message
from textual.widgets import Button, Label, ProgressBar, RichLog

from harvester.config import Settings
from harvester.date_harvester import DateHarvester
from harvester.models import HarvestReport

logger = logging.getLogger(__name__)


class ExecutionPane(Vertical):

    class Completed(Message):
        def __init__(self, report: HarvestReport) -> None:
            self.report = report
            super().__init__()

    def __init__(self) -> None:
        super().__init__()
        self._harvester: DateHarvester | None = None
        self._running = False
        self._start_time: float = 0
        self._timer_handle = None

    def compose(self) -> ComposeResult:
        yield Label("Aguardando inicio...", id="lbl-exec-status")
        yield Label("", id="lbl-timer")
        yield ProgressBar(id="pgb-main", total=100)
        yield RichLog(id="rlog-output", highlight=True, markup=True)
        with Horizontal(classes="button-bar-inline"):
            yield Button("Cancelar", id="btn-cancel", variant="error")
            yield Button("Ver Resultados", id="btn-results", variant="success")

    def on_mount(self) -> None:
        self.query_one("#btn-results").display = False

    def iniciar(
        self,
        settings: Settings,
        caminho_input: str,
        aba: str | None,
        modo: str,
    ) -> None:
        if self._running:
            return
        self._running = True

        self.query_one("#btn-cancel").display = True
        self.query_one("#btn-results").display = False
        rlog = self.query_one("#rlog-output", RichLog)
        rlog.clear()
        self.query_one("#pgb-main", ProgressBar).update(total=100, progress=0)
        self.query_one("#lbl-exec-status", Label).update("Iniciando...")

        self._start_time = time.monotonic()
        self._timer_handle = self.set_interval(1.0, self._update_timer)

        self._executar_harvest(settings, caminho_input, aba, modo)

    def _update_timer(self) -> None:
        if not self._running:
            return
        elapsed = time.monotonic() - self._start_time
        mins, secs = divmod(int(elapsed), 60)
        self.query_one("#lbl-timer", Label).update(f"Tempo: {mins:02d}:{secs:02d}")

    @work(thread=True)
    def _executar_harvest(
        self,
        settings: Settings,
        caminho_input: str,
        aba: str | None,
        modo: str,
    ) -> None:
        rlog = self.query_one("#rlog-output", RichLog)
        pgb = self.query_one("#pgb-main", ProgressBar)
        lbl = self.query_one("#lbl-exec-status", Label)

        def on_progress(atual: int, total: int, msg: str) -> None:
            self.call_from_thread(pgb.update, total=total, progress=atual)
            self.call_from_thread(lbl.update, f"[{atual}/{total}] {msg}")
            self.call_from_thread(rlog.write, msg)

        try:
            self._harvester = DateHarvester(settings)
            self._harvester.set_progress_callback(on_progress)
            self.call_from_thread(rlog.write, f"Modo: {modo}")
            self.call_from_thread(rlog.write, "Iniciando harvest...")

            output_dir = Path(settings.output_dir)
            report = self._harvester.executar(
                caminho_input=Path(caminho_input),
                caminho_output=output_dir,
                aba=aba,
            )

            self.call_from_thread(rlog.write, f"\n{report.resumo}")
            self.call_from_thread(rlog.write, f"Arquivo salvo: {report.arquivo_saida}")
            self.call_from_thread(lbl.update, "[green]Concluido[/]")
            self.call_from_thread(self._finalizar, report)
        except Exception as exc:
            self.call_from_thread(rlog.write, f"[red]Erro: {exc}[/]")
            self.call_from_thread(lbl.update, "[red]Falha na execucao[/]")
            logger.exception("Erro na execucao do harvest")
            self._running = False
            if self._timer_handle:
                self._timer_handle.stop()

    def _finalizar(self, report: HarvestReport) -> None:
        self._running = False
        if self._timer_handle:
            self._timer_handle.stop()
        elapsed = time.monotonic() - self._start_time
        mins, secs = divmod(int(elapsed), 60)
        self.query_one("#lbl-timer", Label).update(
            f"Tempo total: {mins:02d}:{secs:02d}"
        )
        self.query_one("#btn-cancel").display = False
        self.query_one("#btn-results").display = True
        self._report = report
        self.app.notify("Harvest concluido", title="dbt-date-harvester")

    @on(Button.Pressed, "#btn-cancel")
    def _cancelar(self) -> None:
        if self._harvester:
            self._harvester.cancelar()
        self._running = False
        if self._timer_handle:
            self._timer_handle.stop()

    @on(Button.Pressed, "#btn-results")
    def _ver_resultados(self) -> None:
        report = getattr(self, "_report", None)
        if report:
            self.post_message(self.Completed(report))
