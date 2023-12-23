from __future__ import annotations

import logging
from pathlib import Path

from textual import on
from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, TabbedContent, TabPane

from harvester.config import Settings
from tui.screens.config_screen import ConfigPane
from tui.screens.execution_screen import ExecutionPane
from tui.screens.input_screen import InputPane
from tui.screens.results_screen import ResultsPane

logger = logging.getLogger(__name__)

CSS_PATH = Path(__file__).parent / "styles" / "app.tcss"


class DateHarvesterApp(App[None]):

    TITLE = "dbt-date-harvester"
    SUB_TITLE = "Extracao automatica de datas do BigQuery"
    CSS_PATH = CSS_PATH

    BINDINGS = [
        ("q", "quit", "Sair"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._settings: Settings | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent(id="wizard"):
            with TabPane("1. Config", id="tab-config"):
                yield ConfigPane()
            with TabPane("2. Arquivo", id="tab-input"):
                yield InputPane()
            with TabPane("3. Execucao", id="tab-exec"):
                yield ExecutionPane()
            with TabPane("4. Resultados", id="tab-results"):
                yield ResultsPane()
        yield Footer()

    def on_mount(self) -> None:
        wizard = self.query_one("#wizard", TabbedContent)
        wizard.disable_tab("tab-input")
        wizard.disable_tab("tab-exec")
        wizard.disable_tab("tab-results")

    @on(ConfigPane.Completed)
    def _config_concluida(self, event: ConfigPane.Completed) -> None:
        self._settings = event.settings
        wizard = self.query_one("#wizard", TabbedContent)
        wizard.enable_tab("tab-input")
        wizard.active = "tab-input"

    @on(InputPane.Completed)
    def _input_concluido(self, event: InputPane.Completed) -> None:
        if not self._settings:
            return

        if event.sql_dir:
            self._settings.sql_sources_dir = event.sql_dir
            self._settings.save()

        wizard = self.query_one("#wizard", TabbedContent)
        wizard.enable_tab("tab-exec")
        wizard.active = "tab-exec"

        exec_pane = self.query_one(ExecutionPane)
        exec_pane.iniciar(
            self._settings, event.caminho, event.aba, event.modo
        )

    @on(ExecutionPane.Completed)
    def _execucao_concluida(self, event: ExecutionPane.Completed) -> None:
        wizard = self.query_one("#wizard", TabbedContent)
        wizard.enable_tab("tab-results")
        wizard.active = "tab-results"

        results_pane = self.query_one(ResultsPane)
        results_pane.carregar(event.report)

    @on(ResultsPane.RestartRequested)
    def _reiniciar(self, event: ResultsPane.RestartRequested) -> None:
        wizard = self.query_one("#wizard", TabbedContent)
        wizard.disable_tab("tab-exec")
        wizard.disable_tab("tab-results")
        wizard.active = "tab-input"


def run_tui() -> None:
    settings = Settings.from_env()
    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.FileHandler(log_dir / "tui.log")],
    )
    app = DateHarvesterApp()
    app.run()
