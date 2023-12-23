from __future__ import annotations

import json
import logging
from pathlib import Path

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.message import Message
from textual.widgets import Button, Input, Label, Select

from harvester.config import Settings

logger = logging.getLogger(__name__)


def _listar_credenciais(diretorio: str = "credentials") -> list[tuple[str, str]]:
    cred_dir = Path(diretorio)
    if not cred_dir.is_dir():
        return []
    resultado: list[tuple[str, str]] = []
    for arquivo in sorted(cred_dir.glob("*.json")):
        try:
            data = json.loads(arquivo.read_text(encoding="utf-8"))
            project_id = data.get("project_id", "desconhecido")
            label = f"{arquivo.name} ({project_id})"
            resultado.append((label, str(arquivo)))
        except Exception:
            resultado.append((arquivo.name, str(arquivo)))
    return resultado


class ConfigPane(Vertical):

    class Completed(Message):
        def __init__(self, settings: Settings) -> None:
            self.settings = settings
            super().__init__()

    def compose(self) -> ComposeResult:
        credenciais = _listar_credenciais()
        with Vertical(classes="form-group"):
            yield Label("Credencial BigQuery:", classes="form-label")
            yield Select(
                options=[(label, val) for label, val in credenciais],
                id="sel-credential",
                prompt="Selecionar credencial...",
            )

        with Vertical(classes="form-group"):
            yield Label("Projeto GCP:", classes="form-label")
            yield Input(id="inp-project", placeholder="seu-projeto-gcp")

        with Vertical(classes="form-group"):
            yield Label("Dataset BigQuery:", classes="form-label")
            yield Input(id="inp-dataset", placeholder="seu_dataset")

        with Vertical(classes="form-group"):
            yield Label("Localizacao:", classes="form-label")
            yield Input(id="inp-location", placeholder="southamerica-east1")

        yield Label("", id="lbl-config-status")

        with Horizontal(classes="button-bar-inline"):
            yield Button("Testar Conexao", id="btn-test", variant="default")
            yield Button("Salvar .env", id="btn-save", variant="primary")
            yield Button("Confirmar", id="btn-config-next", variant="success")

    def on_mount(self) -> None:
        settings = Settings.from_env()
        self.query_one("#inp-project", Input).value = settings.gcp_project
        self.query_one("#inp-dataset", Input).value = settings.bq_dataset
        self.query_one("#inp-location", Input).value = settings.bq_location

        sel = self.query_one("#sel-credential", Select)
        credenciais = _listar_credenciais()
        if settings.credential_path and credenciais:
            cred_resolved = Path(settings.credential_path).resolve()
            for _, val in credenciais:
                if Path(val).resolve() == cred_resolved:
                    sel.value = val
                    break

        erros = settings.validate()
        if not erros and Path(".env").exists():
            self.query_one("#lbl-config-status", Label).update(
                "[green]Configuracao OK (via .env)[/]"
            )
            self.set_timer(0.3, lambda: self.post_message(self.Completed(settings)))

    def _build_settings(self) -> Settings:
        sel = self.query_one("#sel-credential", Select)
        cred_path = str(sel.value) if sel.value != Select.BLANK else ""
        return Settings(
            credential_path=cred_path,
            gcp_project=self.query_one("#inp-project", Input).value.strip(),
            bq_dataset=self.query_one("#inp-dataset", Input).value.strip(),
            bq_location=self.query_one("#inp-location", Input).value.strip(),
        )

    @on(Button.Pressed, "#btn-test")
    @work(thread=True)
    def _testar_conexao(self) -> None:
        lbl = self.query_one("#lbl-config-status", Label)
        lbl.update("Testando conexao...")
        settings = self._build_settings()
        erros = settings.validate()
        if erros:
            lbl.update(f"[red]Erros: {', '.join(erros)}[/]")
            return
        try:
            from harvester.bigquery_client import BigQueryClient

            bq = BigQueryClient(
                credential_path=settings.credential_path,
                project=settings.gcp_project,
                location=settings.bq_location,
            )
            if bq.test_connection():
                lbl.update("[green]Conexao OK[/]")
            else:
                lbl.update("[red]Falha na conexao[/]")
        except Exception as exc:
            lbl.update(f"[red]Erro: {exc}[/]")

    @on(Button.Pressed, "#btn-save")
    def _salvar_env(self) -> None:
        settings = self._build_settings()
        settings.save()
        self.query_one("#lbl-config-status", Label).update("[green].env salvo[/]")

    @on(Button.Pressed, "#btn-config-next")
    def _proximo(self) -> None:
        settings = self._build_settings()
        erros = settings.validate()
        if erros:
            self.query_one("#lbl-config-status", Label).update(
                f"[red]{', '.join(erros)}[/]"
            )
            return
        self.post_message(self.Completed(settings))
