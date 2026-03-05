"""Ponto de entrada direto para o dbt-date-harvester.

Permite execução via:
    python main.py <comando> [opções]

Exemplo:
    python main.py parse --project-dir /caminho/projeto/dbt
    python main.py graph --project-dir /caminho/projeto/dbt --model fct_event_dates
    python main.py export --project-dir /caminho/projeto/dbt --format json
"""

from __future__ import annotations

import sys

MIN_PYTHON = (3, 8)


def _check_python_version() -> None:
    if sys.version_info < MIN_PYTHON:
        sys.stderr.write(
            f"Erro: Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ é necessário. "
            f"Versão atual: {sys.version_info.major}.{sys.version_info.minor}\n"
        )
        sys.exit(1)


def _check_dependencies() -> None:
    missing = []
    try:
        import yaml  # noqa: F401
    except ImportError:
        missing.append("pyyaml")

    if missing:
        sys.stderr.write(
            f"Erro: dependências não encontradas: {', '.join(missing)}\n"
            f"Instale com: pip install {' '.join(missing)}\n"
        )
        sys.exit(1)


def _is_help_or_version() -> bool:
    args = set(sys.argv[1:])
    return bool(args & {"--help", "-h", "--version"}) or not args


if __name__ == "__main__":
    _check_python_version()

    if not _is_help_or_version():
        _check_dependencies()

    from dbt_parser.cli import main

    sys.exit(main())
