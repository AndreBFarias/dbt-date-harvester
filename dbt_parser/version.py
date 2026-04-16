"""Informacoes de versao do dbt-date-harvester."""

from __future__ import annotations

import re

MAJOR = 2
MINOR = 1
PATCH = 1

VERSION = f"{MAJOR}.{MINOR}.{PATCH}"
VERSION_INFO: tuple[int, int, int] = (MAJOR, MINOR, PATCH)

def get_version() -> str:
    """Retorna versao atual."""
    return VERSION

def parse_version(version_str: str) -> tuple[int, int, int]:
    """Faz parsing de string de versao."""
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)$", version_str)
    if not match:
        raise ValueError(f"Formato de versao invalido: {version_str}")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))

def is_compatible(required: str, current: str) -> bool:
    """Verifica compatibilidade de versao (semver major)."""
    req = parse_version(required)
    cur = parse_version(current)
    return req[0] == cur[0] and (cur[1] > req[1] or (cur[1] == req[1] and cur[2] >= req[2]))
