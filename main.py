"""Ponto de entrada do dbt-date-harvester.

Modos de execucao:
    python main.py                                    -> TUI interativa
    python main.py --cli <comando> [opcoes]           -> CLI legada (dbt_parser)
    python main.py --headless [--input <csv>] [--output <dir>] [--sheet <aba>]  -> batch sem TUI
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

MIN_PYTHON = (3, 8)


def _check_python_version() -> None:
    if sys.version_info < MIN_PYTHON:
        sys.stderr.write(
            f"Erro: Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ necessario. "
            f"Versao atual: {sys.version_info.major}.{sys.version_info.minor}\n"
        )
        sys.exit(1)


def _run_headless(args: argparse.Namespace) -> int:
    from harvester.config import Settings

    settings = Settings.from_env()

    caminho_output = Path(args.output) if args.output else Path(settings.output_dir)
    caminho_output.mkdir(parents=True, exist_ok=True)

    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / "headless.log"),
        ],
    )

    erros = settings.validate()
    if erros:
        sys.stderr.write(f"Configuracao invalida: {', '.join(erros)}\n")
        return 1

    input_file = args.input or settings.input_file
    if not input_file:
        sys.stderr.write("Erro: --input ou INPUT_FILE no .env obrigatorio no modo --headless\n")
        return 1

    caminho_input = Path(input_file)
    if not caminho_input.exists():
        sys.stderr.write(f"Erro: arquivo nao encontrado: {caminho_input}\n")
        return 1

    aba = args.sheet or settings.sheet_name or None

    from harvester.date_harvester import DateHarvester

    harvester = DateHarvester(settings)

    def on_progress(atual: int, total: int, msg: str) -> None:
        sys.stdout.write(f"\r[{atual}/{total}] {msg}")
        sys.stdout.flush()

    harvester.set_progress_callback(on_progress)
    report = harvester.executar(caminho_input, caminho_output, aba=aba)
    sys.stdout.write(f"\n{report.resumo}\n")
    if report.arquivo_saida:
        sys.stdout.write(f"Arquivo salvo: {report.arquivo_saida}\n")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="dbt-date-harvester: extracao automatica de datas do BigQuery",
    )
    parser.add_argument("--cli", nargs=argparse.REMAINDER, help="Modo CLI legado (dbt_parser)")
    parser.add_argument("--headless", action="store_true", help="Execucao batch sem TUI")
    parser.add_argument("--input", type=str, help="Arquivo CSV/XLSX de entrada (modo headless)")
    parser.add_argument("--output", type=str, help="Diretorio de saida (modo headless)")
    parser.add_argument("--sheet", type=str, help="Nome da aba XLSX (modo headless)")
    return parser


if __name__ == "__main__":
    _check_python_version()

    parser = _build_parser()
    args = parser.parse_args()

    if args.cli is not None:
        from harvester.config import Settings

        settings = Settings.from_env()
        cli_args = args.cli or []
        has_project_dir = any(a == "--project-dir" for a in cli_args)
        if not has_project_dir and settings.dbt_project_dir:
            cli_args = ["--project-dir", settings.dbt_project_dir] + cli_args

        sys.argv = ["dbt-parser"] + cli_args
        from dbt_parser.cli import main
        sys.exit(main())
    elif args.headless:
        sys.exit(_run_headless(args))
    else:
        from tui.app import run_tui
        run_tui()
